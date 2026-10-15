"""P2-DEX: the DEX feed process (Pavel 2026-10-15: "build the separate DEX feed process ... our goal is for hot-path to be as
clean as possible, and local orderbooks for CEX to be as fresh as possible, DEX speed is lower priority and DEX related
processes ideally should not affect CEX speed and recency").

Everything the `bsc` connector does with the chain runs here, in a child process of HMB, so none of it holds HMB's event loop,
which serves every CEX book and order. Inside HMB the DEX had held that loop 0.5-0.7 ms a second with 3 tokens, 5-6 ms with
20, and 0.42 ms per swap. The child does:
  - the 1 s book poll (bsc_quoter: the call built, BlockPI asked, the answer decoded into books), the wallet's reads riding in
    it, and the books gated by the approvals (a side the wallet can't pay into its executor is not shown);
  - each swap's build (its book's route, its limit and room), signing and bundle send; each approval's;
  - the wallet's nonce, and every transaction it sends followed to its receipt or its last block.
HMB's side (bsc_exchange.BscExchange) keeps the framework's state: the orders, the balances, the wallet's one turn, the
approvals' back-off. They exchange one small message per poll and per transaction (FeedClient <-> Feed): framed pickles on
the child's stdin and stdout.
  - The connector starts the child when it has a DEX pair to quote, and stops it when none is left and nothing is out (idle:
    no process at all).
  - The child's code is the connector's own, captured as HMB imported it (SOURCES) and handed over at the start (BOOT): a pull
    without a restart never runs other code there than in HMB.
  - It is niced (FEED_NICE): under CPU contention HMB's process goes first, as DEX speed is the lower priority.
  - It ends with HMB: when its stdin closes, it ends."""
import asyncio
import json
import os
import pickle
import re
import struct
import sys
import time
import types
from collections import deque
from decimal import ROUND_DOWN, Decimal, localcontext
from typing import Callable, Dict, List, Optional, Tuple

import aiohttp

from hummingbot.connector.exchange.bsc import bsc_constants as CONSTANTS
from hummingbot.connector.exchange.bsc.bsc_p1_quoter import adr, word
from hummingbot.connector.exchange.bsc.bsc_pool_list import PoolList
from hummingbot.connector.exchange.bsc.bsc_quoter import BscQuoter
from hummingbot.connector.exchange.bsc.bsc_swap import build_swap, executor
from hummingbot.connector.exchange.bsc.bsc_tx import allowance_calls, build_tx, missing_approvals, parse_receipt, sign

FEED_NICE = 10            # the child's niceness: under CPU contention HMB's own process goes first
FEED_RESTART_S = 5.0      # a child that ended on its own is started again no sooner than this after it ended
FEED_STOP_S = 2.0         # a stop waits this long for the child to end, then kills it
FEED_HUNG_S = 30.0        # a running child that sent no poll for this long is killed and started again
STDERR_KEPT = 30          # the child's last stderr lines kept, shown when it ends on its own (a traceback fits)
WORKER_MODULES = ("bsc_constants", "bsc_p1_quoter", "bsc_swap", "bsc_pool_list", "bsc_quoter", "bsc_tx", "bsc_feed")
# what HMB keeps of a swap out (its settle, its line): the rest of the swap (its calldata, its executor) stays in the child
SWAP_KEYS = ("side", "ca", "ca_decimals", "token_in", "token_out", "amount_in", "min_out", "pool", "book_block", "book_at",
             "head", "expected_out")


class Refused(Exception):
    """a transaction that did not go out: why, for HMB (no book, no route, no approval, the nonce or the head unreadable)"""


class SendRefused(Refused):
    """every builder refused the bundle: nothing is out, and its nonce is free again"""


def merged(levels: List[list]) -> List[list]:
    """[[price, qty], ...] with equal neighbouring prices summed: the book keeps one entry per price, so a second level at
    the same price (to_levels clamps a rounding wobble to the previous price) would be dropped with its quantity"""
    out = []
    for price, qty in levels:
        if out and out[-1][0] == price:
            out[-1][1] += qty
        else:
            out.append([price, qty])
    return out


# ---------------------------------------------------------------- the frames: a 4-byte length, then a pickle
def pack(message: tuple) -> bytes:
    data = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
    return struct.pack(">I", len(data)) + data


async def read_frame(reader: asyncio.StreamReader) -> Optional[tuple]:
    """the next message, or None when the other side has ended"""
    try:
        head = await reader.readexactly(4)
        return pickle.loads(await reader.readexactly(struct.unpack(">I", head)[0]))
    except asyncio.IncompleteReadError:
        return None


# ---------------------------------------------------------------- the wallet's reads (both sides: `connect bsc` reads in HMB)
def wallet_plan(wallet: str, cas: List[str], pool_list) -> dict:
    """the wallet's reads: its BNB, USDT and the traded CAs, and whether each token it may pay is approved for each router
    its CAs' pools swap through (USDT for a buy, the CA for a sell)"""
    tokens = [CONSTANTS.USDT] + [ca for ca in cas if ca != CONSTANTS.USDT]
    calls = [(CONSTANTS.MULTICALL3, bytes.fromhex(CONSTANTS.SEL_ETH_BALANCE) + adr(wallet))]
    calls += [(token, bytes.fromhex(CONSTANTS.SEL_BALANCE_OF) + adr(wallet)) for token in tokens]
    pairs = {}
    for ca in cas:
        for pool in pool_list.pools(ca):
            ex = executor(pool)
            if ex:
                for token in (CONSTANTS.USDT, ca):
                    pairs[(token, ex["router"])] = ex
    approvals = [(key, ex, allowance_calls(wallet, key[0], ex)) for key, ex in pairs.items()]
    return {"tokens": tokens, "approvals": approvals, "calls": calls + [c for _, _, cs in approvals for c in cs]}


def decode_wallet(plan: dict, res: list, block: Optional[int], wallet: str) -> dict:
    """the plan's answers -> {"block", "bnb": raw or None (its call failed), "balances": {token: raw or None},
    "approvals": {(token, router): approved}, "missing": the approval calls still needed}"""
    def raw(answer):
        ok, ret = answer
        return word(ret, 0) if ok and len(ret) >= 32 else None
    tokens = plan["tokens"]
    out = {"block": block, "bnb": raw(res[0]), "balances": {t: raw(r) for t, r in zip(tokens, res[1:1 + len(tokens)])},
           "approvals": {}, "missing": []}
    i, now = 1 + len(tokens), int(time.time())
    for key, ex, cs in plan["approvals"]:
        answers, i = res[i:i + len(cs)], i + len(cs)
        need = missing_approvals(wallet, key[0], ex, answers, CONSTANTS.APPROVED_AT_LEAST, now)
        out["approvals"][key] = not need
        out["missing"] += [c for c in need if c not in out["missing"]]      # a token -> Permit2 approve serves both routers
    return out


def mask(text, rpc_url: str, key_hex: Optional[str]) -> str:
    """the node URL carries its key, and the wallet key is a key: neither reaches a log or a message"""
    text = str(text)
    if rpc_url:
        text = text.replace(rpc_url, "<rpc>")
        node_key = rpc_url.rstrip("/").rsplit("/", 1)[-1]
        if len(node_key) >= 16:
            text = text.replace(node_key, "<key>")
    if key_hex:
        text = re.sub(key_hex, "<pk>", text, flags=re.IGNORECASE)
    return text


# ---------------------------------------------------------------- the child's side: the chain
class Feed:
    """The feed process's work (one per child; a test runs it in-process over the same frames). It polls on its own clock
    and answers HMB's commands:
      ("cas", [ca]) the CAs to quote · ("caps", {ca: {"buy", "sell"}}) the ladders' ends · ("swap", order id, params) ·
      ("approve", id, call) · ("nonce_lost",) a transaction it didn't send took a nonce · ("stop",).
    It sends: ("poll", books and wallet) or ("poll_failed", why) each poll · ("pools_changed", [ca]) · ("signed", order id,
    swap) the moment a swap may be out · ("swap", order id, "sent", swap) or ("swap", order id, "refused", why, "send" when
    every builder refused it, else "order") · ("ended", order id, receipt
    parsed or None (dropped), where it landed) · ("approval", id, "landed" | "reverted" | "dropped" | "refused", block | why)
    · ("log", level, text)."""

    def __init__(self, init: dict, send: Callable[[tuple], None]):
        self.send = send
        self.rpc_url = init["rpc_url"]
        self.wallet = init["wallet"]
        self.account = None
        if init.get("key"):                           # signing only: a feed with no key (books and balances) runs without it
            from eth_account import Account
            self.account = Account.from_key(init["key"])
        self.read_wallet = init["read_wallet"]        # the wallet rides in each poll (a trading connector with a good key)
        self.gate = init["gate"]                      # a book side shows only if the wallet can pay into its executor
        self.poll_s = init["poll_s"]
        self.builders = tuple(init["builders"])
        self.read_url = init["swap_read_url"]
        self.pool_list = PoolList(init["data_dir"])
        self.quoter = BscQuoter(self._post)
        self.wanted: List[str] = list(init.get("cas") or [])
        self.caps: Dict[str, dict] = dict(init.get("caps") or {})
        self.approvals: Dict[tuple, bool] = {}
        self.nonce: Optional[int] = None              # the wallet's next nonce (read before the first send, then counted)
        self.out: Dict[str, dict] = dict(init.get("follow") or {})   # order id -> a swap out (HMB's, after a restart too)
        self.reported: Dict[str, str] = {}
        self._quoted: Optional[tuple] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._nonce_task: Optional[asyncio.Task] = None
        self._tasks: set = set()
        self._key_hex = self.account.key.hex().removeprefix("0x") if self.account is not None else None

    # ---------------------------------------------------------------- the loop
    async def serve(self, reader: asyncio.StreamReader):
        """until HMB says stop or its end of the pipe closes: the poll loop, the swaps out followed, each command as it comes"""
        self._spawn(self._poll_loop())
        for order_id, record in list(self.out.items()):
            self._spawn(self._follow(order_id, record))
        try:
            while True:
                message = await read_frame(reader)
                if message is None or message[0] == "stop":
                    return
                self._command(message)
        finally:
            for task in list(self._tasks):
                task.cancel()
            if self._session is not None and not self._session.closed:
                await self._session.close()

    def _command(self, message: tuple):
        kind = message[0]
        if kind == "cas":
            self.wanted = list(message[1])
        elif kind == "caps":
            self.caps = dict(message[1])
        elif kind == "swap":
            self._spawn(self._swap(message[1], message[2]))
        elif kind == "approve":
            self._spawn(self._approve(message[1], message[2]))
        elif kind == "nonce_lost":
            self.nonce = None                         # read anew after the next wallet read (_after_wallet_read)

    def _spawn(self, coro):
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task):
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            e = task.exception()
            self._log("error", self._mask(f"bsc: the feed's task failed: {type(e).__name__}: {e}"))

    def _log(self, level: str, text: str):
        self.send(("log", level, text))

    def _report(self, key: str, text: str, level: str = "info"):
        if self.reported.get(key) != text:
            self.reported[key] = text
            self._log(level, f"bsc: {key}: {text}")

    def _mask(self, text) -> str:
        return mask(text, self.rpc_url, self._key_hex)

    # ---------------------------------------------------------------- the node
    async def _post(self, body: bytes) -> bytes:
        """one JSON-RPC POST to the node (BlockPI): the quoter's transport"""
        if not self.rpc_url:
            raise RuntimeError("no BSC RPC URL: connect bsc")
        return await self._post_to(self.rpc_url, body)

    async def _post_to(self, url: str, body: bytes, timeout: Optional[float] = None) -> bytes:
        """one JSON-RPC POST on the feed's kept-alive session: the node, the swap reads, the builders. Every request has a
        timeout, RPC_TIMEOUT_S unless the caller gives its own (aiohttp takes timeout=None as no timeout at all)"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=CONSTANTS.RPC_TIMEOUT_S))
        try:
            async with self._session.post(url, data=body, headers={"Content-Type": "application/json"},
                                          timeout=aiohttp.ClientTimeout(total=timeout or CONSTANTS.RPC_TIMEOUT_S)) as r:
                return await r.read()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise RuntimeError(self._mask(f"{type(e).__name__}: {e}")) from None

    async def _read(self, method: str, params: list):
        """one read on the swap lifecycle's endpoint (receipts, the head, the nonce): SWAP_READ_URL"""
        ans = json.loads(await self._post_to(self.read_url, json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), timeout=CONSTANTS.READ_TIMEOUT_S))
        if "error" in ans:
            raise RuntimeError(self._mask(f"{method}: {ans['error']}"))
        return ans.get("result")

    # ---------------------------------------------------------------- the books
    async def _poll_loop(self):
        """one poll every poll_s while there is a CA to quote: the books and the wallet in one eth_call (Pavel 2026-10-15: the
        wallet read rides along, no request of its own; BlockPI bills an eth_call 20 RU whatever it carries, up to a 1 MB
        answer). Nothing is polled without a CA (HMB then stops this process, unless a transaction is still out)"""
        while True:
            t0 = time.time()
            try:
                self._load_pools()
                if self.quoter.cas:
                    await self._poll()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.send(("poll_failed", self._mask(f"{type(e).__name__}: {e}")[:300]))
            await asyncio.sleep(max(0.0, self.poll_s - (time.time() - t0)))

    def _load_pools(self):
        """the payloads of the CAs HMB's pairs are bound to: a file that changed is read again (HMB is told, for its trading
        rules), and the quoter is set up again when they or the CAs change (a changed CA is elected again)"""
        try:
            changed = self.pool_list.load()
        except Exception as e:
            self._report("pools", f"the pool payloads could not be read: {e}", "warning")
            return
        if changed:
            self.send(("pools_changed", list(changed)))
        wanted = tuple(ca for ca in self.wanted if ca in self.pool_list.cas)
        if changed or wanted != self._quoted:
            self.quoter.set_pools(types.SimpleNamespace(cas={ca: self.pool_list.cas[ca] for ca in wanted}), changed)
            self._quoted = wanted

    async def _poll(self):
        self.quoter.caps = self.caps
        plan = wallet_plan(self.wallet, list(self.quoter.cas), self.pool_list) if self.read_wallet else None
        self.quoter.extra = plan["calls"] if plan else []
        last = await self.quoter.poll()
        got = last.get("extra") or []
        wallet = None
        if plan and [c for c, _ in got] == plan["calls"]:   # this poll's answers (an older block's poll gives back the last)
            wallet = decode_wallet(plan, [a for _, a in got], last.get("block"), self.wallet)
            self.approvals.update(wallet["approvals"])
        books = {ca: {side: self._side(ca, side, book) for side, book in sides.items()}
                 for ca, sides in (last.get("books") or {}).items()}
        self.send(("poll", {"block": last["block"], "ts": last["ts"], "at": last["at"], "books": books, "wallet": wallet,
                            "stats": dict(self.quoter.stats)}))
        if wallet is not None:
            self._after_wallet_read()

    def _side(self, ca: str, side: str, book: Optional[dict]) -> List[list]:
        """a book side's levels, or none while the wallet can't pay that side's input (USDT to buy, the CA to sell) into its
        route's executor: a side on show is a side that can execute, so a strategy never fires a CEX leg against a DEX leg
        that would be refused. A trading connector's only; the approvals are read with the balances"""
        if not book:
            return []
        if self.gate:
            ex = executor(book.get("record") or {})
            if not ex or not self.approvals.get((CONSTANTS.USDT if side == "buy" else ca, ex["router"])):
                return []
        return merged(book.get("levels", []))

    def _after_wallet_read(self):
        """the nonce, while it is unknown and nothing is out, is read in the background, never inside the poll (the books
        don't wait for it), so the run's first order finds it"""
        if (self.nonce is None and not self.out and self.account is not None and CONSTANTS.TRADING_ENABLED
                and (self._nonce_task is None or self._nonce_task.done())):
            self._nonce_task = asyncio.ensure_future(self._read_nonce())

    # ---------------------------------------------------------------- a swap: one exact-input swap along its book's route
    def _book_route(self, ca: str, side: str, usd: float) -> Tuple[dict, dict, List[dict]]:
        """the current (ca, side) book, its CA pool's record and its hop route at the smallest priced rung of at least `usd`
        (else the largest): the swap runs exactly where its book was priced. A book whose block is STALE_S old is no book,
        the rule that empties the books on show"""
        last = self.quoter.last
        if not last or time.time() - (last.get("ts") or 0) > CONSTANTS.STALE_S:
            raise Refused("no fresh book")
        book = (last.get("books") or {}).get(ca, {}).get(side)
        if not book:
            raise Refused(f"no {side} book")
        rungs = sorted(r for r, hops in (book.get("routes") or {}).items() if hops is not None)
        if not rungs:
            raise Refused("the book has no priced route")
        rung = next((r for r in rungs if r >= usd), rungs[-1])
        return book, book["record"], book["routes"][rung]

    @staticmethod
    def _book_value(levels: List[list], amount: Decimal, price: Decimal) -> Decimal:
        """the USD the book's side gives for `amount` CA: its levels walked best first; any part past its depth at the limit
        `price` (so the depth neither refuses nor over-budgets an order)"""
        left, value = amount, Decimal(0)
        for px, qty in levels:
            take = min(left, Decimal(str(qty)))
            value += take * Decimal(str(px))
            left -= take
            if left <= 0:
                break
        return value + max(left, Decimal(0)) * price

    def _swap_for(self, ca: str, side: str, amount: Decimal, price: Decimal) -> dict:
        """the order as one exact-input swap along its book's route. The limit price, plus the SWAP_SLIPPAGE room, is the worst
        AVERAGE price accepted (Pavel 2026-10-14: output at least x is the protection; 2026-10-15: "build it with 0.3%"):
          - a buy spends the book's own cost of `amount` (not amount x limit, which bought past `amount` from the curve's
            tail) and must receive at least cost / (limit x (1 + room)) CA;
          - a sell spends `amount` CA and must receive at least amount x limit x (1 - room) USDT.
        Without the room, an order inside one level of the book (its limit is that level's price) had a minimum equal to
        the book's own output, so any adverse tick before it landed made it revert (CONSTANTS.SWAP_SLIPPAGE).
        An order the current book already prices past its limit and the room (beyond PRICE_SLACK) is refused: it could only
        revert. Within the slack the minimum is the book's own output: the limit's, uncapped, would ask for more than the
        book gives."""
        if ca not in self.quoter.cas:
            raise Refused(f"{ca} is not quoted")
        dec = int(self.quoter.cas[ca]["decimals"])
        book, record, hops = self._book_route(ca, side, float(amount * price))
        last = self.quoter.last                                          # fresh: _book_route refuses a stale one
        book_block, book_at = last["block"], last["at"]                  # the block that book was priced at; its poll's send
        # the chain's head now: that block plus the blocks since (Pavel 2026-10-15: the send reads nothing). A node behind
        # makes it low by its lag, and the bundle's window (BUNDLE_BLOCKS) then ends that many blocks sooner
        head = book_block + int(max(time.time() - book_at, 0.0) / CONSTANTS.BLOCK_S)
        with localcontext() as ctx:
            ctx.prec = 60
            qty = int((amount * Decimal(10) ** dec).to_integral_value(rounding=ROUND_DOWN))
            at_limit = amount * price
            if qty <= 0 or at_limit <= 0:
                raise Refused(f"{amount} at {price} is nothing to swap")
            value = self._book_value(book["levels"], amount, price)      # the book's cost (buy) / proceeds (sell) of `amount`
            slack = at_limit * CONSTANTS.PRICE_SLACK
            room = f"{CONSTANTS.SWAP_SLIPPAGE * 100:g}%"
            if side == "buy":
                worst_px = price * (1 + CONSTANTS.SWAP_SLIPPAGE)              # the worst average price accepted
                if value > amount * worst_px + slack:
                    raise Refused(f"the book moved past the limit and its {room} room: {amount} costs {value:.8g} > "
                                  f"{amount * worst_px:.8g} USDT")
                amount_in = int((value * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(rounding=ROUND_DOWN))
                min_out = int((min(value / worst_px, amount) * Decimal(10) ** dec).to_integral_value(rounding=ROUND_DOWN))
                expected_out = qty                                            # the book: `value` buys `amount`
                tax = Decimal(str((record.get("tax_pct") or {}).get("buy") or 0)) / 100
                if tax and record.get("type") != "v2":  # the router checks a v3 / v4 / Infinity pool's output before the tax
                    min_out = int((min_out / (1 - tax)).to_integral_value(rounding=ROUND_DOWN)) + 1
            else:
                worst_px = price * (1 - CONSTANTS.SWAP_SLIPPAGE)              # the worst average price accepted
                if value < amount * worst_px - slack:
                    raise Refused(f"the book moved past the limit and its {room} room: {amount} brings {value:.8g} < "
                                  f"{amount * worst_px:.8g} USDT")
                amount_in = qty
                min_out = int((min(amount * worst_px, value) * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(
                    rounding=ROUND_DOWN))
                expected_out = int((value * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(rounding=ROUND_DOWN))
        if amount_in <= 0 or min_out <= 0:
            raise Refused(f"{amount} at {price} is too small to swap protected (its input or minimum output is 0)")
        try:
            swap = build_swap(record, hops, side, amount_in, min_out, self.wallet)
        except ValueError as e:
            raise Refused(str(e)) from None
        return dict(swap, side=side, ca=ca, ca_decimals=dec, amount_in=amount_in, min_out=min_out, pool=record["pool"],
                    book_block=book_block, book_at=book_at, head=head, expected_out=expected_out)

    async def _swap(self, order_id: str, p: dict):
        """HMB's order (its turn already taken there) as one swap: built (_swap_for), signed and sent (_send_tx). HMB is told
        "signed" with its hash the moment it may be out, then "sent" or "refused" (nothing went out, why); a swap out is
        followed to its end here (_follow)"""
        record = {}

        def signed():
            self.out[order_id] = record
            self.send(("signed", order_id, dict(record)))
        try:
            if self.account is None:
                raise Refused("no wallet key: connect bsc with bsc_private_key")
            swap = self._swap_for(p["ca"], p["side"], p["amount"], p["price"])
            if not self.approvals.get((swap["token_in"], swap["executor"]["router"])):
                raise Refused(f"{swap['token_in']} is not approved for {swap['executor']['router']}")
            record.update({k: swap[k] for k in SWAP_KEYS}, trading_pair=p["trading_pair"])
            await self._send_tx(record, swap["to"], swap["data"], swap["value"], CONSTANTS.SWAP_GAS_LIMIT, head=swap["head"],
                                on_signed=signed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.out.pop(order_id, None)
            why = str(e) if isinstance(e, Refused) else f"{type(e).__name__}: {e}"
            self.send(("swap", order_id, "refused", self._mask(why), "send" if isinstance(e, SendRefused) else "order"))
            return
        self.send(("swap", order_id, "sent", dict(record)))
        self._spawn(self._follow(order_id, record))

    async def _send_tx(self, record: dict, to: str, data: bytes, value: int, gas: int, head: Optional[int] = None,
                       on_signed: Optional[Callable[[], None]] = None):
        """sign the wallet's next transaction and send it as a bundle to every builder, valid for BUNDLE_BLOCKS blocks after
        the head; its hash, nonce, last block and send time go into `record` (what the lifecycle follows). A swap gives the
        head (its book's, _swap_for) and finds the nonce known (_read_nonce), so its bundle is its one request; an approval
        has no book and reads the head. The nonce is read here only while it is still unknown.
        Raises Refused when nothing went out: the nonce or the head unreadable, or every builder refused it (SendRefused: the
        nonce is read anew, and `record` gets no hash). Any other error is an unknown outcome: it may be out, so it is
        followed."""
        try:
            if self.nonce is None:
                self.nonce = int(await self._read("eth_getTransactionCount", [self.wallet, "pending"]), 16)
            if head is None:
                head = int(await self._read("eth_blockNumber", []), 16)
            last_block = head + CONSTANTS.BUNDLE_BLOCKS
            nonce = self.nonce
            raw, tx_hash = sign(self.account, build_tx(to, data, value, nonce, gas, CONSTANTS.GAS_PRICE_WEI))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise Refused(f"not sent: {self._mask(e)}") from None
        self.nonce = nonce + 1                        # taken before the send: the transaction may be out even if the call fails
        record.update(tx_hash=tx_hash, nonce=nonce, last_block=last_block, sent=time.time())
        if on_signed is not None:
            on_signed()
        try:
            await self._send_bundle(raw, last_block)
        except SendRefused:
            for k in ("tx_hash", "nonce", "last_block", "sent"):
                record.pop(k, None)                   # nothing to follow
            await self._nonce_lost()                  # nothing went out: its nonce is free again, read anew now
            raise
        except Exception as e:
            self._log("warning", f"bsc: {tx_hash}: no builder took it for sure ({self._mask(e)}); following it on chain")

    async def _read_nonce(self):
        """the wallet's next nonce, read OFF the order path (Pavel 2026-10-15): after a wallet read while it is unknown and
        nothing is out (so the run's first order finds it), and the moment a transaction ends without landing (_nonce_lost).
        Kept only if no send took a nonce meanwhile; a read that fails leaves it unknown, for the next send to read"""
        try:
            nonce = int(await self._read("eth_getTransactionCount", [self.wallet, "pending"]), 16)
        except asyncio.CancelledError:
            raise
        except Exception:
            return
        if self.nonce is None:
            self.nonce = nonce

    async def _nonce_lost(self):
        """a transaction that never went out, or never landed, leaves its nonce free: it is forgotten and read anew at once,
        before HMB hears the transaction ended (so before the wallet's turn is free: no send can race the read)"""
        self.nonce = None
        await self._read_nonce()

    async def _send_bundle(self, raw: str, last_block: int):
        """the transaction as a one-transaction bundle to every builder at once, valid up to `last_block`. Returns as soon as
        one builder takes it. Raises SendRefused when every builder refused it (each answered with an error: nothing is
        out), and RuntimeError when none took it and at least one never answered (unknown: it may be out)"""
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_sendBundle",
                           "params": [{"txs": [raw], "maxBlockNumber": last_block}]}).encode()

        async def ask(url):
            try:
                return url, json.loads(await self._post_to(url, body, timeout=CONSTANTS.SEND_TIMEOUT_S)), None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                return url, None, e
        refused, silent = [], []
        for answer in asyncio.as_completed([asyncio.ensure_future(ask(url)) for url in self.builders]):
            url, ans, err = await answer
            host = url.split("//", 1)[-1]
            if err is not None or not isinstance(ans, dict):
                silent.append(f"{host}: {err or 'not JSON'}")
            elif "error" not in ans:
                return                                # taken: anything but an error (the other's answer finishes on its own)
            else:
                refused.append(f"{host}: {ans['error']}")
        if refused and not silent:
            raise SendRefused("every builder refused it: " + "; ".join(refused))
        raise RuntimeError("no builder took it: " + "; ".join(silent + refused))

    # ---------------------------------------------------------------- a transaction out, followed to its end
    async def _landed(self, record: dict) -> Tuple[bool, Optional[dict]]:
        """one look at a transaction out -> (ended, its receipt): mined (the receipt), or ended without one when the head is
        past its last block (a bundle lands by then or never; +1 for a receipt a node behind). The head is asked only once
        the window can have passed, so a transaction that lands costs one read a look"""
        receipt = await self._read("eth_getTransactionReceipt", [record["tx_hash"]])
        if receipt is not None:
            return True, receipt
        if time.time() - record["sent"] < CONSTANTS.BUNDLE_BLOCKS * CONSTANTS.BLOCK_S:
            return False, None
        return int(await self._read("eth_blockNumber", []), 16) > record["last_block"] + 1, None

    async def _until_ended(self, record: dict, what: str) -> Optional[dict]:
        """its receipt asked for every RECEIPT_POLL_S until it ends -> the receipt, or None (not included by its last block)"""
        while True:
            await asyncio.sleep(CONSTANTS.RECEIPT_POLL_S)
            try:
                ended, receipt = await self._landed(record)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn(record, f"bsc: {what}: its receipt could not be read: {self._mask(e)}")
                continue
            if ended:
                return receipt

    def _warn(self, record: dict, text: str):
        """a transaction's trouble, said at most once per WARN_EVERY_S (it is looked at ten times a second)"""
        now = time.time()
        if now - record.get("warned", 0.0) >= CONSTANTS.WARN_EVERY_S:
            record["warned"] = now
            self._log("warning", text)

    async def _follow(self, order_id: str, record: dict):
        """a swap out, to its end: HMB gets ("ended", its receipt parsed, where it landed), or None when it was not included
        by its last block (its nonce is then read anew first, so HMB frees the turn with the nonce known)"""
        receipt = await self._until_ended(record, f"{order_id} {record['tx_hash']}")
        parsed = info = None
        if receipt is None:
            await self._nonce_lost()
        else:
            parsed = parse_receipt(receipt, self.wallet, record["token_in"], record["token_out"])
            index = receipt.get("transactionIndex")
            info = {"block": int(receipt["blockNumber"], 16), "index": int(index, 16) if index else None,
                    "gas_used": int(receipt["gasUsed"], 16)}
        self.out.pop(order_id, None)
        self.send(("ended", order_id, parsed, info))

    async def _approve(self, call_id: str, call: Tuple[str, bytes]):
        """one one-time approval, sent like a swap (its turn taken in HMB) and followed to its end here"""
        to, data = call
        record = {}
        try:
            if self.account is None:
                raise Refused("no wallet key: connect bsc with bsc_private_key")
            await self._send_tx(record, to, data, 0, CONSTANTS.APPROVE_GAS_LIMIT)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            why = str(e) if isinstance(e, Refused) else f"{type(e).__name__}: {e}"
            self.send(("approval", call_id, "refused", self._mask(why)))
            return
        receipt = await self._until_ended(record, f"approval {record['tx_hash']}")
        if receipt is None:
            await self._nonce_lost()                  # it never landed: its nonce is free, read anew before the turn ends
            self.send(("approval", call_id, "dropped", None))
        else:
            landed = int(receipt.get("status") or "0x0", 16) == 1
            self.send(("approval", call_id, "landed" if landed else "reverted", int(receipt["blockNumber"], 16)))


# ---------------------------------------------------------------- HMB's side: the child, started, stopped, spoken to
class FeedClient:
    """HMB's end of the feed process (BscExchange._feed): it starts and stops the child, sends it commands and hands its
    messages on. This is all HMB's loop spends on the DEX: a pickle per message, and a dict or two."""

    def __init__(self, on_message: Callable[[tuple], None], log: Callable[[str, str], None], mask_text: Callable[[str], str]):
        self.on_message, self.log, self.mask = on_message, log, mask_text
        self.spawn = spawn_process                # (sources, init) -> (reader, writer, handle); a test runs it in-process
        self.handle = None
        self.writer = None
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._stderr: deque = deque(maxlen=STDERR_KEPT)
        self._poll: Optional[tuple] = None
        self._poll_event: Optional[asyncio.Event] = None
        self._replies: Dict[tuple, asyncio.Future] = {}
        self._ended_at = 0.0                      # when a child last ended on its own (FEED_RESTART_S runs from there)
        self._stopping = False

    @property
    def running(self) -> bool:
        return self.handle is not None and self._reader_task is not None and not self._reader_task.done()

    async def start(self, init: dict):
        """the child, with the connector's code (SOURCES) and `init` (the node, the wallet and its key, what to quote)"""
        if self.running:
            return
        wait = self._ended_at + FEED_RESTART_S - time.time()
        if wait > 0:
            raise RuntimeError(f"the feed process ended; it is started again in {wait:.1f} s")
        self._poll, self._stopping = None, False
        self._poll_event = asyncio.Event()
        self._stderr.clear()
        reader, self.writer, self.handle = await self.spawn(SOURCES, init)
        self._reader_task = asyncio.ensure_future(self._read(reader))
        if getattr(self.handle, "stderr", None) is not None:
            self._stderr_task = asyncio.ensure_future(self._read_stderr(self.handle.stderr))
        self.log("info", f"bsc: the feed process started (pid {getattr(self.handle, 'pid', None)})")

    async def stop(self):
        """the child ended: asked to, then killed if it hasn't within FEED_STOP_S"""
        handle = self.handle
        if handle is None:
            return
        self._stopping = True
        try:
            self.writer.write(pack(("stop",)))
        except Exception:
            pass
        try:
            await asyncio.wait_for(handle.wait(), FEED_STOP_S)
        except asyncio.TimeoutError:
            handle.kill()
            await handle.wait()
        for task in (self._reader_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
        self.handle = self.writer = self._reader_task = self._stderr_task = None
        self._closed("the feed process was stopped")
        self.log("info", "bsc: the feed process stopped")

    def send(self, message: tuple):
        if not self.running:
            raise RuntimeError("the feed process is not running")
        self.writer.write(pack(message))

    async def request(self, message: tuple, key: tuple) -> tuple:
        """`message` sent and its reply awaited, the one keyed (kind, id) -> the reply's rest"""
        future = asyncio.get_running_loop().create_future()
        self._replies[key] = future
        try:
            self.send(message)
            return await future
        finally:
            self._replies.pop(key, None)

    async def next_poll(self) -> tuple:
        """the next ("poll", ...) or ("poll_failed", why) from the child: never one already handed out, and the newest when
        several came meanwhile. A running child that sends none for FEED_HUNG_S is killed (and started again by the next poll)"""
        end = time.time() + FEED_HUNG_S
        while self._poll is None:
            if not self.running:
                raise RuntimeError("the feed process is not running")
            self._poll_event.clear()
            try:
                await asyncio.wait_for(self._poll_event.wait(), max(end - time.time(), 0.0))
            except asyncio.TimeoutError:
                self.log("warning", f"bsc: the feed process sent no poll for {FEED_HUNG_S:.0f} s: restarting it")
                await self.stop()
                self._ended_at = time.time()
                raise RuntimeError(f"the feed process sent no poll for {FEED_HUNG_S:.0f} s") from None
        message, self._poll = self._poll, None
        return message

    async def _read(self, reader: asyncio.StreamReader):
        try:
            while True:
                message = await read_frame(reader)
                if message is None:
                    return
                kind = message[0]
                if kind in ("poll", "poll_failed"):
                    self._poll = message
                    self._poll_event.set()
                    continue
                future = self._replies.get((kind, message[1])) if kind in ("swap", "approval") else None
                if future is not None and not future.done():
                    future.set_result(message[2:])
                    continue
                self.on_message(message)
        finally:
            if not self._stopping:
                self._ended_at = time.time()
                handle = self.handle
                code = getattr(handle, "returncode", None)
                tail = " | ".join(self._stderr)
                self.log("warning", self.mask(f"bsc: the feed process ended on its own (exit code {code}); it is started "
                                              f"again in {FEED_RESTART_S:.0f} s{': ' + tail if tail else ''}")[:2000])
                self._closed("the feed process ended")

    async def _read_stderr(self, stream: asyncio.StreamReader):
        """the child's stderr: kept (its last lines go with an unexpected end) and logged at DEBUG, masked"""
        while True:
            line = await stream.readline()
            if not line:
                return
            text = self.mask(line.decode(errors="replace").rstrip())
            self._stderr.append(text)
            self.log("debug", f"bsc feed: {text}")

    def _closed(self, why: str):
        """a stop or an end: a poll awaited wakes (and finds no child), a reply awaited fails"""
        if self._poll_event is not None:
            self._poll_event.set()
        for future in list(self._replies.values()):
            if not future.done():
                future.set_exception(RuntimeError(why))


async def spawn_process(sources: List[Tuple[str, str]], init: dict):
    """the child: this interpreter running BOOT, its code and `init` as the first frame on its stdin (never in its arguments
    or its environment: init carries the wallet key)"""
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", BOOT, stdin=asyncio.subprocess.PIPE,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    proc.stdin.write(pack((sources, init)))
    await proc.stdin.drain()
    return proc.stdout, proc.stdin, proc


# ---------------------------------------------------------------- the child's start
# BOOT nices the child first (its imports are CPU too), reads the first frame from fd 0 unbuffered (a buffered read could
# swallow the next frames), points fd 1 at stderr (a stray print can't corrupt the frames, which go out on a copy of the
# original fd 1), installs the code HMB handed over under its own module names (empty parent packages: nothing of HMB is
# imported; each module's loader gives its source, for a traceback's lines) and runs child_main
BOOT = r"""
import os, pickle, struct, sys, types
try:
    os.nice(%d)
except OSError:
    pass
def _read(n):
    data = b""
    while len(data) < n:
        chunk = os.read(0, n - len(data))
        if not chunk:
            raise SystemExit(0)
        data += chunk
    return data
class _Source:
    def __init__(self, src):
        self.src = src
    def get_source(self, name):
        return self.src
_out = os.dup(1)
os.dup2(2, 1)
_sources, _init = pickle.loads(_read(struct.unpack(">I", _read(4))[0]))
_parents = ("hummingbot", "hummingbot.connector", "hummingbot.connector.exchange", "hummingbot.connector.exchange.bsc")
for _name in _parents:
    _m = types.ModuleType(_name)
    _m.__path__ = []
    sys.modules[_name] = _m
for _name, _src in _sources:
    _m = types.ModuleType(_name)
    _m.__file__ = "bsc-feed/" + _name.rsplit(".", 1)[1] + ".py"
    _m.__package__ = _name.rsplit(".", 1)[0]
    _m.__loader__ = _Source(_src)
    sys.modules[_name] = _m
    setattr(sys.modules[_m.__package__], _name.rsplit(".", 1)[1], _m)
    exec(compile(_src, _m.__file__, "exec"), _m.__dict__)
sys.modules[_parents[-1] + ".bsc_feed"].child_main(_init, _out)
""" % FEED_NICE


def child_main(init: dict, out_fd: int):
    """the feed process (niced by BOOT): serving HMB until it says stop or its stdin closes"""
    asyncio.run(_child(init, out_fd))


async def _child(init: dict, out_fd: int):
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(0, "rb", buffering=0))
    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, os.fdopen(out_fd, "wb", buffering=0))
    writer = asyncio.StreamWriter(transport, protocol, None, loop)
    await Feed(init, lambda message: writer.write(pack(message))).serve(reader)


# ---------------------------------------------------------------- the child's code: the connector's own, as HMB imported it
def _sources() -> List[Tuple[str, str]]:
    """the source of every module the child runs, read now, as HMB imports this package (they are all imported by this
    point, this one included): a pull without a restart never hands the child other code than HMB's"""
    package = __name__.rsplit(".", 1)[0]
    out = []
    for module in WORKER_MODULES:
        name = f"{package}.{module}"
        out.append((name, sys.modules[name].__loader__.get_source(name)))
    return out


SOURCES = _sources()
