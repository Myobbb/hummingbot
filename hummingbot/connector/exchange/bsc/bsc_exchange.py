import asyncio
import json
import re
import time
import types
from decimal import ROUND_DOWN, Decimal, localcontext
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from bidict import bidict
from eth_account import Account
from eth_account.signers.local import LocalAccount

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.bsc import bsc_constants as CONSTANTS
from hummingbot.connector.exchange.bsc.bsc_api_order_book_data_source import BscAPIOrderBookDataSource
from hummingbot.connector.exchange.bsc.bsc_api_user_stream_data_source import BscAPIUserStreamDataSource
from hummingbot.connector.exchange.bsc.bsc_p1_quoter import adr, word
from hummingbot.connector.exchange.bsc.bsc_pool_list import PoolList
from hummingbot.connector.exchange.bsc.bsc_quoter import BscQuoter
from hummingbot.connector.exchange.bsc.bsc_swap import EXECUTORS, build_swap, executor
from hummingbot.connector.exchange.bsc.bsc_tx import (
    allowance_calls,
    build_tx,
    missing_approvals,
    parse_receipt,
    receipt_problem,
    sign,
)
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BscTradingDisabled(Exception):
    """an order on `bsc` before the send method (its own session) exists"""


class BscOrderRefused(Exception):
    """an order the connector can't turn into a swap: no fresh book, no route, no approval, no wallet key, another swap out"""


class BscSendRefused(BscOrderRefused):
    """the send method's answer when the node REFUSED the transaction (nothing was broadcast, its nonce is free again). Any
    other exception from the send is an unknown outcome: the transaction may be out, so it is followed by its hash"""


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


class BscExchange(ExchangePyBase):
    """P2-DEX build step 3 (2026-10-14): the `bsc` connector, a DEX leg arb_l reads like any venue.

    - Books: the order book data source polls one eth_call a second (bsc_quoter.BscQuoter): per traded CA one book per side,
      from the depth-elected pool, with a $100 watch on its other pools, priced in USDT along each pool's path (Q5, Q7).
    - Pools: P1 hands over each candidate's pool records through the page -> watcher path; the watcher writes them to
      ~/p2_dex/<ca>.json (bsc_pool_list.PoolList). A trading pair is P1's ticker + "-USDT", bound to the CA its strategy's
      leg names (`bsc:MUBARAK-USDT:0x5c85…`, bind_ca by the orchestrator); an unbound pair has no book.
    - Balances: the wallet's BNB, USDT and traded tokens (and its approvals), read inside each book poll's one eth_call at
      the books' block, with no request of their own (Pavel 2026-10-15), and each swap's receipt booked on them the moment
      it is read (a strategy sees a fill in the balances when its event fires).
    - Trading rules: from P1's records (the CA's decimals and reference price). No minimum notional: a DEX has none.
    - Orders (step 4): one exact-input swap per order along the route that priced its book (bsc_swap), signed by the wallet
      (bsc_tx); the transaction hash is the exchange order id; the receipt gives the fill (what really moved at the wallet,
      the gas in BNB) or a revert (a failed order). While CONSTANTS.TRADING_ENABLED is False every order is refused and the
      connector never reports ready, so the orchestrator never ticks a strategy with a bsc leg.
    - Sending (the Sending session, Pavel 2026-10-14): every transaction goes as a one-transaction bundle to both builders
      (CONSTANTS.BUILDERS), valid for BUNDLE_BLOCKS blocks: it lands by then or never (nothing paid, its nonce free again), and
      one that would revert is not included. The lifecycle's reads (receipts, the head, the nonce) are on SWAP_READ_URL.
    - Placing a swap is ONE request, its bundle (Pavel 2026-10-15: an arb leg's placement must not wait on anything else):
      its last block comes from its book's block and age (no head read), and the wallet's nonce is known beforehand, read off
      the order path after a wallet read, or the moment a transaction ends without landing, in its turn. The balances it is
      sized by are memory: the receipts' moves booked as each swap ends, and each poll's wallet read for the rest.
    - A swap venue (`is_swap_venue`, 2026-10-14): an order is one immediate swap, never resting, never cancelled; the limit
      price plus the SWAP_SLIPPAGE room is the worst average price accepted (no price grid). ONE swap at a time per
      wallet: an order takes the wallet's turn when it is placed and every available balance reads 0 until it ends, so
      the strategies (arb_l, the balancer), which size by it, place nothing here meanwhile. Each swap out is followed on
      chain (_receipt_loop) and ends about a block after it is mined: FILLED with what it really got, or FAILED (reverted,
      or not included by its last block).
    - Approvals are read with the balances. A book side the wallet can't pay into its executor is not shown, so no
      strategy fires a CEX leg against a DEX leg that would be refused. A missing approval is sent by the connector itself
      (trading on), one at a time in the wallet's turn, to the pinned executors' contracts only (bsc_swap).
    - The wallet, like a CEX key pair: `bsc_wallet_address` names it, `bsc_private_key` (stored encrypted by `connect bsc`)
      must control it. The key lives only inside its signer (eth_account, local signing: no network hop). A key that is
      not valid, or controls another address, fails the balance read, so `connect bsc` reports it and keeps no bad key.
      No key: books and balances only, never ready.
    The node URL carries its key, and the wallet key is a key: neither reaches a log (_mask)."""

    web_utils = None
    is_swap_venue = True                               # the contract: ExchangePyBase.is_swap_venue

    def __init__(self,
                 bsc_rpc_url: str,
                 bsc_wallet_address: str,
                 bsc_private_key: str = "",
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        self._rpc_url = (bsc_rpc_url or "").strip()
        self._wallet = (bsc_wallet_address or "").strip().lower()
        self._account, self._key_problem = self._load_key(bsc_private_key)
        self._nonce: Optional[int] = None              # the wallet's next nonce (read before the first send, then counted)
        self._busy: Optional[str] = None               # the order holding the wallet's one turn (placed, not yet ended)
        self._swaps: Dict[str, dict] = {}              # client order id -> its swap out: tx hash, nonce, tokens, side, deadline
        self._swap_sent = asyncio.Event()              # wakes the receipt loop when a swap goes out
        self._receipt_task: Optional[asyncio.Task] = None
        self._balances_block = 0                       # the block the balances are known at (a read's, or a booked receipt's)
        self._approvals: Dict[tuple, bool] = {}        # (token, router) -> the wallet can pay the token into the router
        self._missing_approvals: List[Tuple[str, bytes]] = []   # the approval calls the last balance read found missing
        self._approved_sent: set = set()               # approval calls included this run (a read behind can't resend them)
        self._approve_retry: Dict[tuple, Tuple[float, float]] = {}   # approval call -> (not before, its last back-off), after a failure
        self._approve_task: Optional[asyncio.Task] = None
        self._domain = domain
        self._trading_pairs = list(trading_pairs or [])
        self._trading_required = trading_required
        self._pool_list = PoolList()
        self._cas: Dict[str, str] = {}                 # trading pair -> the CA its leg names (bind_ca: the orchestrator)
        self._quoter = BscQuoter(self._post)
        self._quoted_pairs: Optional[tuple] = None    # the trading pairs and their CAs the quoter was last set for
        self._reported: Dict[str, str] = {}           # what was last said about the payloads (a change is logged once)
        self._session: Optional[aiohttp.ClientSession] = None
        self._nonce_task: Optional[asyncio.Task] = None   # the nonce read in the background after a wallet read
        self._idle = False                             # the last poll found no DEX pair to quote (and made no call)
        self._balances_read = False
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---------------------------------------------------------------- identity
    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self):
        return None

    @property
    def rate_limits_rules(self):
        return []

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return ""

    @property
    def trading_pairs_request_path(self) -> str:
        return ""

    @property
    def check_network_request_path(self) -> str:
        return ""

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    @property
    def trading_enabled(self) -> bool:
        """False: the connector is never ready, so its strategies are never ticked; the orchestrator's all-connectors-ready
        gate (its one-time init) skips a connector that says so"""
        return CONSTANTS.TRADING_ENABLED

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "symbols_mapping_initialized": self.trading_pair_symbol_map_ready(),
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": not self.is_trading_required or self._balances_read,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self.is_trading_required else True,
            "wallet_key": self._account is not None,          # a key that controls the wallet: no key, no trading
            "trading_enabled": self.trading_enabled,          # step 3: books and balances only (bsc_constants)
        }

    # ---------------------------------------------------------------- the wallet
    def _load_key(self, private_key: str) -> Tuple[Optional[LocalAccount], str]:
        """-> (the wallet's signer, "") or (None, why). The key is never kept as text, printed or put in a message: only
        the signer holds it. No key: (None, "")."""
        private_key = (private_key or "").strip()
        if not private_key:
            return None, ""
        try:
            account = Account.from_key(private_key)
        except Exception:
            return None, "bsc_private_key is not a valid private key"
        if account.address.lower() != self._wallet:
            return None, f"bsc_private_key does not control {self._wallet or 'bsc_wallet_address (empty)'}"
        return account, ""

    # ---------------------------------------------------------------- the node
    def _mask(self, text: Any) -> str:
        text = str(text)
        if self._rpc_url:
            text = text.replace(self._rpc_url, "<rpc>")
            key = self._rpc_url.rstrip("/").rsplit("/", 1)[-1]
            if len(key) >= 16:
                text = text.replace(key, "<key>")
        if self._account is not None:
            text = re.sub(self._account.key.hex().removeprefix("0x"), "<pk>", text, flags=re.IGNORECASE)
        return text

    async def _post(self, body: bytes) -> bytes:
        """one JSON-RPC POST to the node (BlockPI): the quoter's transport, the balances' and check_network's"""
        if not self._rpc_url:
            raise RuntimeError("no BSC RPC URL: connect bsc")
        return await self._post_to(self._rpc_url, body)

    async def _post_to(self, url: str, body: bytes, timeout: Optional[float] = None) -> bytes:
        """one JSON-RPC POST on the connector's kept-alive session: the node, the swap reads, the builders. Every request has
        a timeout, RPC_TIMEOUT_S unless the caller gives its own (aiohttp takes timeout=None as no timeout at all)"""
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
        """one read on the swap lifecycle's endpoint (receipts, the head, the nonce): CONSTANTS.SWAP_READ_URL"""
        ans = json.loads(await self._post_to(CONSTANTS.SWAP_READ_URL, json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), timeout=CONSTANTS.READ_TIMEOUT_S))
        if "error" in ans:
            raise RuntimeError(self._mask(f"{method}: {ans['error']}"))
        return ans.get("result")

    # ---------------------------------------------------------------- network
    async def start_network(self):
        self._throttler.label = self.name
        await self.stop_network()
        self._reload_pools()
        self._orderbook_ds.reset()
        self.order_book_tracker.start()            # its data source runs the poll loop, the wallet read in each poll
        if self.is_trading_required:
            self._receipt_task = safe_ensure_future(self._receipt_loop())   # also picks up swaps still out from before

    async def stop_network(self):
        await super().stop_network()
        for task in (self._receipt_task, self._approve_task, self._nonce_task):
            if task is not None:
                task.cancel()
        self._receipt_task = self._approve_task = self._nonce_task = None
        self._idle = False                             # a restart's first network check reads the node again
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def check_network(self) -> NetworkStatus:
        """a fresh poll answers by itself; so does an idle connector (no DEX pair to quote: nothing is polled, and the first
        poll when a pair comes checks the node itself); otherwise one eth_blockNumber (the poll only runs after
        start_network)"""
        if self._idle or time.time() - self._orderbook_ds.last_ok < CONSTANTS.STALE_S:
            return NetworkStatus.CONNECTED
        try:
            ans = json.loads(await self._post(b'{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'))
            return NetworkStatus.CONNECTED if "result" in ans else NetworkStatus.NOT_CONNECTED
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    # ---------------------------------------------------------------- P1's pools: quoter, symbol map, trading rules
    def bind_ca(self, trading_pair: str, ca: str):
        """the CA a strategy's leg names (`control create … bsc:MUBARAK-USDT:0x5c85…`, kept in test_multi.yml): the orchestrator
        binds each bsc leg before its strategy is built, at start and at create. A pair is P1's ticker + "-USDT"; its CA is the
        token, so two CAs sharing a ticker can't be confused. The orchestrator refuses a second strategy naming another CA
        for a bound pair"""
        self._cas[trading_pair] = ca.lower()           # the next poll sets the quoter up for it (_reload_pools)

    def unbind_ca(self, trading_pair: str):
        """the orchestrator, when no strategy trades the pair any more"""
        self._cas.pop(trading_pair, None)

    def _ca_for_pair(self, trading_pair: str) -> Tuple[Optional[str], str]:
        base, _, quote = trading_pair.partition("-")
        if quote.upper() != CONSTANTS.QUOTE:
            return None, f"quote {quote}: bsc books are priced in {CONSTANTS.QUOTE}"
        ca = self._cas.get(trading_pair)
        if not ca:
            return None, f"no CA bound: a bsc leg is created as bsc:{trading_pair}:0x<CA>"
        if ca not in self._pool_list.cas:
            return None, f"no P1 records for {ca} in {self._pool_list.data_dir} (the watcher writes them from the page)"
        return ca, ""

    def _reload_pools(self):
        """the payloads in the pool directory: the quoter, the symbol map and the trading rules follow them, for the CAs of
        this connector's trading pairs only"""
        try:
            changed = self._pool_list.load()
        except Exception as e:
            self.logger().warning(f"bsc: the pool payloads could not be read: {e}")
            return
        pairs = tuple(sorted((tp, self._cas.get(tp) or "") for tp in self._trading_pairs))
        if not changed and pairs == self._quoted_pairs:
            return
        cas = {}
        for trading_pair, _ in pairs:
            ca, why = self._ca_for_pair(trading_pair)
            self._report(trading_pair, f"CA {ca}, {len(self._pool_list.pools(ca))} tradable pools" if ca else f"no book: {why}")
            if ca:
                cas[ca] = self._pool_list.cas[ca]
        for (pool, ca), why in self._pool_list.left_out.items():
            if ca in cas:
                self._report(f"{ca}/{pool}", f"left out: {why}")
        for path, why in self._pool_list.bad_files.items():
            self._report(path, f"payload not read: {why}")
        self._quoter.set_pools(types.SimpleNamespace(cas=cas), changed)
        self._quoted_pairs = pairs
        self._set_trading_pair_symbol_map(bidict({tp: tp for tp in self._trading_pairs}))
        self._trading_rules = {tp: rule for tp in self._trading_pairs if (rule := self._trading_rule(tp)) is not None}

    def _report(self, key: str, text: str):
        if self._reported.get(key) != text:
            self._reported[key] = text
            self.logger().info(f"bsc: {key}: {text}")

    def _trading_rule(self, trading_pair: str) -> Optional[TradingRule]:
        ca, _ = self._ca_for_pair(trading_pair)
        if not ca:
            return None
        m = self._pool_list.cas[ca]
        step = Decimal(1).scaleb(-min(int(m["decimals"]), 8))
        price_step = Decimal(1).scaleb(Decimal(str(m["reference_usd"])).adjusted() - 6)     # 7 significant digits
        # No minimum notional: a DEX has none (Pavel 2026-10-14). The strategies' own floor (min_order_usd) applies to every
        # order but a sell-to-zero sweep, which therefore sells any residue here down to the step.
        return TradingRule(trading_pair=trading_pair, min_order_size=step, min_price_increment=price_step,
                           min_base_amount_increment=step, min_notional_size=Decimal(0))

    async def _poll_books(self) -> Optional[dict]:
        """one eth_call: the books, and the wallet's balances and approvals at the same block (Pavel 2026-10-15: the wallet
        read rides along, no request of its own; BlockPI bills an eth_call 20 RU whatever it carries, up to a 1 MB answer).
        None, and no call at all, while no traded pair has a CA to quote (Pavel 2026-10-15: "obviously we should stop
        polling when no dex strategy is left"; audit finding 9: the orchestrator evicts a pair, not the connector, so an
        empty poll went out every second, ~52M RU a month). The next poll after a pair is bound quotes it"""
        self._reload_pools()
        if not self._quoter.cas:
            self._idle = True
            self._report("poll", "idle: no DEX pair to quote, nothing is polled")
            return None
        if self._idle:
            self._idle = False
            self._report("poll", f"polling again: {len(self._quoter.cas)} CA(s) to quote")
        self._quoter.caps = self._ladder_caps()
        trading = self.is_trading_required and bool(self._wallet)
        if trading and self._key_problem:
            self._report("wallet", f"not read: {self._key_problem}")
        plan = self._wallet_plan() if trading and not self._key_problem else None
        self._quoter.extra = plan["calls"] if plan else []
        last = await self._quoter.poll()
        got = last.get("extra") or []
        if plan and [c for c, _ in got] == plan["calls"]:   # this poll's answers (an older block's poll gives back the last)
            self._apply_wallet(plan, [a for _, a in got], last.get("block"))
            self._after_wallet_read()
        return last

    def _ladder_caps(self) -> Dict[str, dict]:
        """per traded CA what the wallet can put into one swap, where its book's ladders end (Pavel 2026-10-15: 350 USDT ->
        "100 / 250 / 325 quotes and executables"): {"buy": the USDT less arb_l's ARB_QUOTE_RESERVE (the most arb_l spends),
        "sell": the CA held, raw}. From the TOTAL balances: a swap out reads 0 available, and its book must not collapse
        meanwhile. {} until the first balance read (the quoter then quotes the $100 rung only)"""
        if not self._balances_read:
            return {}
        usdt = float(self._account_balances.get(CONSTANTS.QUOTE, Decimal(0)) - CONSTANTS.ARB_QUOTE_RESERVE)
        caps = {}
        for trading_pair in self._trading_pairs:
            ca, _ = self._ca_for_pair(trading_pair)
            if ca:
                held = self._account_balances.get(trading_pair.split("-")[0].upper(), Decimal(0))
                caps[ca] = {"buy": max(usdt, 0.0),
                            "sell": int(held.scaleb(int(self._pool_list.cas[ca]["decimals"])).to_integral_value(
                                rounding=ROUND_DOWN))}
        return caps

    def _books_by_pair(self, last: dict) -> Dict[str, tuple]:
        """trading pair -> (bids, asks) as [[price, qty], ...]: the asks are the CA bought on the DEX ("buy"), the bids sold"""
        out = {}
        books = last.get("books") or {}
        for trading_pair in self._trading_pairs:
            ca, _ = self._ca_for_pair(trading_pair)
            book = books.get(ca) or {}
            out[trading_pair] = (self._side_levels(ca, "sell", book.get("sell")),
                                 self._side_levels(ca, "buy", book.get("buy")))
        return out

    def _side_levels(self, ca: str, side: str, book: Optional[dict]) -> List[list]:
        """a book side's levels, or none while the wallet can't pay that side's input (USDT to buy, the CA to sell) into its
        route's executor: a side on show is a side that can execute, so a strategy never fires a CEX leg against a DEX leg
        the connector would refuse. Applies to a trading connector only; the approvals are read with the balances"""
        if not book:
            return []
        if self.is_trading_required:
            ex = executor(book.get("record") or {})
            if not ex or not self._approvals.get((CONSTANTS.USDT if side == "buy" else ca, ex["router"])):
                return []
        return merged(book.get("levels", []))

    async def _update_trading_rules(self):
        self._reload_pools()

    async def _fetch_trading_rule_for_pair(self, trading_pair: str):
        self._quoted_pairs = None                  # a runtime-added pair: set the quoter up again
        self._reload_pools()

    async def _initialize_trading_pair_symbol_map(self):
        self._set_trading_pair_symbol_map(bidict({tp: tp for tp in self._trading_pairs}))

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        return

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        # set, not mutated: an empty map is answered with a fresh bidict (ExchangeBase.trading_pair_symbol_map)
        self._set_trading_pair_symbol_map(bidict({tp: tp for tp in set(self._trading_pairs) | {trading_pair}}))

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        return []

    # ---------------------------------------------------------------- balances
    def _wallet_plan(self) -> dict:
        """the wallet's reads: its BNB, USDT and traded tokens, and whether each token it may pay is approved for its router.
        A started connector has them answered inside each book poll's one eth_call, at the books' block (_poll_books; Pavel
        2026-10-15: no request of their own); `connect bsc` reads them once (_update_balances)"""
        tokens = {CONSTANTS.QUOTE: (CONSTANTS.USDT, CONSTANTS.USDT_DECIMALS)}
        for trading_pair in self._trading_pairs:
            ca, _ = self._ca_for_pair(trading_pair)
            if ca:
                tokens[trading_pair.split("-")[0].upper()] = (ca, int(self._pool_list.cas[ca]["decimals"]))
        calls = [(CONSTANTS.MULTICALL3, bytes.fromhex(CONSTANTS.SEL_ETH_BALANCE) + adr(self._wallet))]
        calls += [(address, bytes.fromhex(CONSTANTS.SEL_BALANCE_OF) + adr(self._wallet)) for address, _ in tokens.values()]
        approvals = [(key, ex, allowance_calls(self._wallet, key[0], ex)) for key, ex in self._approval_pairs().items()]
        return {"tokens": tokens, "approvals": approvals, "calls": calls + [c for _, _, cs in approvals for c in cs]}

    async def _update_balances(self):
        """one wallet read of its own, the block with it: `connect bsc`'s key check, where no poll runs. A started connector
        never calls it: its wallet rides in every book poll (_poll_books)"""
        if self._key_problem:
            raise ValueError(self._key_problem)       # `connect bsc` shows it and keeps no bad key
        if not self._wallet:
            return
        plan = self._wallet_plan()
        res = await self._quoter._call(plan["calls"] + [(CONSTANTS.MULTICALL3, bytes.fromhex(CONSTANTS.SEL_BLOCK_NUMBER))])
        ok, ret = res[-1]
        self._apply_wallet(plan, res[:-1], word(ret, 0) if ok and len(ret) >= 32 else None)

    def _after_wallet_read(self):
        """after each poll's wallet read: the approvals it found missing go out (_maybe_approve); and the nonce, while it is
        unknown and the wallet idle, is read in the background, never inside the poll (the books don't wait for it), so the
        run's first order finds it"""
        self._maybe_approve()
        if (self._nonce is None and self._busy is None and self._account is not None and CONSTANTS.TRADING_ENABLED
                and (self._nonce_task is None or self._nonce_task.done())):
            self._nonce_task = safe_ensure_future(self._read_nonce())

    def _apply_wallet(self, plan: dict, res: list, block: Optional[int]):
        """one read's answers onto the balances and the approvals; an asset whose call failed keeps its last value (said
        once, not every poll). A read older than a receipt already booked on the balances (a node behind the one that
        served the receipt) is dropped, as it would undo the fill"""
        if block is not None and block < self._balances_block:
            return
        tokens = plan["tokens"]
        read = [(CONSTANTS.NATIVE_ASSET, 18, res[0])] + [(asset, dec, r) for (asset, (_, dec)), r in zip(tokens.items(), res[1:])]
        for asset, decimals, (ok, ret) in read:
            if not ok or len(ret) < 32:
                self._report(f"balance {asset}", "not read: its last value kept")
                continue
            if f"balance {asset}" in self._reported:
                self._report(f"balance {asset}", "read again")
            amount = Decimal(word(ret, 0)).scaleb(-decimals)
            self._account_balances[asset] = amount
            self._account_available_balances[asset] = amount
        i, now, missing = 1 + len(tokens), int(time.time()), []
        for key, ex, cs in plan["approvals"]:
            answers, i = res[i:i + len(cs)], i + len(cs)
            need = missing_approvals(self._wallet, key[0], ex, answers, CONSTANTS.APPROVED_AT_LEAST, now)
            self._report(f"approval {key[0]} -> {key[1]}", "approved" if not need else
                         "MISSING: the wallet can't pay this token into this router (one-time approval transactions)")
            self._approvals[key] = not need
            missing += [c for c in need if c not in missing]            # a token -> Permit2 approve serves both routers
        self._missing_approvals = missing
        if block is not None:
            self._balances_block = block
        self._balances_read = True
        if not self._gas_ok() or "gas" in self._reported:          # said when short, and once when it is covered again
            self._report("gas", "BNB covers a swap's gas again" if self._gas_ok() else
                         f"BNB {self._account_balances.get(CONSTANTS.NATIVE_ASSET, Decimal(0))} can't pay one swap's gas "
                         f"(up to {CONSTANTS.SWAP_GAS_MAX_BNB}): nothing is available to trade until the wallet is topped up")

    def _approval_pairs(self) -> Dict[tuple, dict]:
        """(token, router) -> executor, for every token the traded CAs' swaps may pay: USDT (a buy) and the CA (a sell), into
        the executor of each of the CA's tradable pools"""
        pairs = {}
        for trading_pair in self._trading_pairs:
            ca, _ = self._ca_for_pair(trading_pair)
            for pool in self._pool_list.pools(ca) if ca else []:
                ex = executor(pool)
                if ex:
                    for token in (CONSTANTS.USDT, ca):
                        pairs[(token, ex["router"])] = ex
        return pairs

    def _maybe_approve(self):
        """after a wallet read: the approvals it found missing go out (_approve), when trading is on, the wallet has a key
        and gas, and no earlier batch is still running. One that was included this run is not sent again, even if a read
        from a node behind still shows it missing; one that failed waits out its back-off (_approve_back_off)"""
        now = time.time()
        todo = [c for c in self._missing_approvals
                if c not in self._approved_sent and self._approve_retry.get(c, (0.0, 0.0))[0] <= now]
        if (todo and CONSTANTS.TRADING_ENABLED and self._account is not None and self._gas_ok()
                and (self._approve_task is None or self._approve_task.done())):
            self._approve_task = safe_ensure_future(self._approve(todo))

    def _approve_back_off(self, call: Tuple[str, bytes]) -> float:
        """an approval that failed (reverted, not included, or not sent) is tried again only after a back-off: APPROVE_RETRY_S
        the first time, doubled each time after, at most APPROVE_RETRY_MAX_S (Pavel 2026-10-15). One that can never land then
        can't hold the wallet's turn at every balance read. -> the back-off, in s"""
        last = self._approve_retry.get(call, (0.0, 0.0))[1]
        delay = min(last * 2, CONSTANTS.APPROVE_RETRY_MAX_S) if last else CONSTANTS.APPROVE_RETRY_S
        self._approve_retry[call] = (time.time() + delay, delay)
        return delay

    async def _approve(self, calls: List[Tuple[str, bytes]]):
        """the missing one-time approvals, sent by the connector itself (Pavel 2026-10-14), one at a time in the wallet's turn,
        each a bundle like a swap. Every spender is a pinned executor's contract (bsc_swap.EXECUTORS, PINNED_V2_ROUTERS),
        never one from data. One that doesn't land is tried again after its back-off (_approve_back_off). The next poll's
        wallet read (within POLL_S) shows the approved sides"""
        for to, data in calls:
            while self._busy is not None:              # the wallet's one turn: after the swap (or approval) out
                await asyncio.sleep(CONSTANTS.RECEIPT_POLL_S)
            turn = f"approval:{to}:{data[:36].hex()}"
            self._busy = turn
            what, record = self._approval_name(to, data), {}
            try:
                self.logger().info(f"bsc: sending a one-time approval: {what}")
                await self._send_tx(record, to, data, 0, CONSTANTS.APPROVE_GAS_LIMIT)
                ended, receipt = False, None
                while not ended:
                    await asyncio.sleep(CONSTANTS.RECEIPT_POLL_S)
                    try:
                        ended, receipt = await self._landed(record)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self._warn_swap(record, f"bsc: approval {record['tx_hash']}: its receipt could not be read: "
                                                f"{self._mask(e)}")
                if receipt is not None and int(receipt.get("status") or "0x0", 16) == 1:
                    self._approved_sent.add((to, data))
                    self._approve_retry.pop((to, data), None)
                    self.logger().info(f"bsc: approved in block {int(receipt['blockNumber'], 16)}: {what}")
                else:
                    if receipt is None:
                        await self._nonce_lost()       # it never landed: its nonce is free, read anew in this turn
                    delay = self._approve_back_off((to, data))
                    self.logger().warning(f"bsc: the approval {what} did not land ("
                                          f"{'reverted' if receipt else 'not included by its last block'}); it is tried again "
                                          f"in {delay / 60:.0f} min")
            except BscOrderRefused as e:
                delay = self._approve_back_off((to, data))
                self.logger().warning(f"bsc: the approval {what} was not sent: {self._mask(e)}; it is tried again in "
                                      f"{delay / 60:.0f} min")
            finally:
                if self._busy == turn:
                    self._busy = None

    def _approval_name(self, to: str, data: bytes) -> str:
        """an approval call in words: '<token> -> <spender>' (the pinned contracts and the traded tokens by name)"""
        names = {CONSTANTS.USDT: CONSTANTS.QUOTE}
        for fam, ex in EXECUTORS.items():
            names[ex["router"]] = f"{fam} Universal Router"
            names[ex["permit2"]] = f"{fam} Permit2"
        for ca, meta in self._pool_list.cas.items():
            names.setdefault(ca, meta.get("ticker") or ca)
        name = (lambda a: names.get(a.lower(), a.lower()))
        first, second = "0x" + data[16:36].hex(), "0x" + data[48:68].hex()
        return f"{name(first)} -> {name(second)} (via {name(to)})" if to in names and "Permit2" in name(to) else \
            f"{name(to)} -> {name(first)}"

    # ---------------------------------------------------------------- orders: one swap each, one at a time
    def _get_fee(self, base_currency: str, quote_currency: str, order_type: OrderType, order_side: TradeType,
                 amount: Decimal, price: Decimal = s_decimal_NaN, is_maker: Optional[bool] = None) -> TradeFeeBase:
        """the pools' fees and the trade's tax are in the book's price; the gas is booked from the receipt"""
        return AddedToCostTradeFee(percent=Decimal("0"))

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT, price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        return self._take_turn(super().buy(trading_pair, amount, order_type, price, **kwargs))

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT, price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        return self._take_turn(super().sell(trading_pair, amount, order_type, price, **kwargs))

    def _take_turn(self, order_id: str) -> str:
        """ONE swap at a time per wallet (Pavel 2026-10-14): its nonces are sequential, so an order waits until the one before
        it has ended. The order placed while none is out takes the wallet's turn here, synchronously, before its creation
        runs, so a second order in the same tick already finds the turn taken (and _place_order refuses it). The turn is
        freed when the order ends: its receipt or drop (_settle), or a refusal (_update_order_after_failure)"""
        if self._busy is None:
            self._busy = order_id
        return order_id

    def _free_turn(self, order_id: str):
        if self._busy == order_id:
            self._busy = None

    def _gas_ok(self) -> bool:
        return self._account_balances.get(CONSTANTS.NATIVE_ASSET, Decimal(0)) >= CONSTANTS.SWAP_GAS_MAX_BNB

    def get_available_balance(self, currency: str) -> Decimal:
        """0 while a swap is out (the wallet's turn is taken) or while the BNB can't pay one swap's gas, so the strategies,
        which size by it, place nothing here meanwhile; otherwise the balance. The TOTAL (get_balance) is never held back:
        the hold band and the balancer's position read it"""
        if self._busy is not None or not self._gas_ok():
            return Decimal(0)
        return super().get_available_balance(currency)

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        """the limit as given: a swap has no price grid (its limit is the worst average price accepted), and flooring it to
        the trading rule's step would refuse an order priced exactly at its book's worst level"""
        return price

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        try:
            await super()._create_order(trade_type, order_id, trading_pair, amount, order_type, price, **kwargs)
        finally:
            if self._order_tracker.fetch_order(client_order_id=order_id) is None:
                self._free_turn(order_id)            # an error before it was tracked: no event will ever end it

    def _update_order_after_failure(self, order_id: str, trading_pair: str, exception: Optional[Exception] = None):
        """an order refused before or at its send (the framework's checks, _place_order): it ends here"""
        super()._update_order_after_failure(order_id, trading_pair, exception)
        self._free_turn(order_id)

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """the connector's own refusals are one WARNING line, the reason in it; any other error is the framework's to log"""
        if not isinstance(exception, (BscOrderRefused, BscTradingDisabled)):
            return super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception,
                                             **kwargs)
        self.logger().warning(f"bsc: {trade_type.name.lower()} {amount} {trading_pair} at {price} refused: "
                              f"{self._mask(exception)}")
        self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)

    def _book_route(self, ca: str, side: str, usd: float) -> Tuple[dict, dict, List[dict]]:
        """the current (ca, side) book, its CA pool's record and its hop route at the smallest priced rung of at least `usd`
        (else the largest): the swap runs exactly where its book was priced. A book whose block is STALE_S old is no book,
        the rule that empties the books on show"""
        last = self._quoter.last
        if not last or time.time() - (last.get("ts") or 0) > CONSTANTS.STALE_S:
            raise BscOrderRefused("no fresh book")
        book = (last.get("books") or {}).get(ca, {}).get(side)
        if not book:
            raise BscOrderRefused(f"no {side} book")
        rungs = sorted(r for r, hops in (book.get("routes") or {}).items() if hops is not None)
        if not rungs:
            raise BscOrderRefused("the book has no priced route")
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

    def _swap_for(self, trading_pair: str, amount: Decimal, trade_type: TradeType, price: Decimal) -> dict:
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
        ca, why = self._ca_for_pair(trading_pair)
        if not ca:
            raise BscOrderRefused(why)
        dec = int(self._pool_list.cas[ca]["decimals"])
        side = "buy" if trade_type == TradeType.BUY else "sell"
        book, record, hops = self._book_route(ca, side, float(amount * price))
        last = self._quoter.last                                         # fresh: _book_route refuses a stale one
        book_block, book_at = last["block"], last["at"]                  # the block that book was priced at; its poll's send
        # the chain's head now: that block plus the blocks since (Pavel 2026-10-15: the send reads nothing). A node behind
        # makes it low by its lag, and the bundle's window (BUNDLE_BLOCKS) then ends that many blocks sooner
        head = book_block + int(max(time.time() - book_at, 0.0) / CONSTANTS.BLOCK_S)
        with localcontext() as ctx:
            ctx.prec = 60
            qty = int((amount * Decimal(10) ** dec).to_integral_value(rounding=ROUND_DOWN))
            at_limit = amount * price
            if qty <= 0 or at_limit <= 0:
                raise BscOrderRefused(f"{amount} at {price} is nothing to swap")
            value = self._book_value(book["levels"], amount, price)      # the book's cost (buy) / proceeds (sell) of `amount`
            slack = at_limit * CONSTANTS.PRICE_SLACK
            room = f"{CONSTANTS.SWAP_SLIPPAGE * 100:g}%"
            if side == "buy":
                worst_px = price * (1 + CONSTANTS.SWAP_SLIPPAGE)              # the worst average price accepted
                if value > amount * worst_px + slack:
                    raise BscOrderRefused(f"the book moved past the limit and its {room} room: {amount} costs {value:.8g} > "
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
                    raise BscOrderRefused(f"the book moved past the limit and its {room} room: {amount} brings {value:.8g} < "
                                          f"{amount * worst_px:.8g} USDT")
                amount_in = qty
                min_out = int((min(amount * worst_px, value) * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(
                    rounding=ROUND_DOWN))
                expected_out = int((value * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(rounding=ROUND_DOWN))
        if amount_in <= 0 or min_out <= 0:
            raise BscOrderRefused(f"{amount} at {price} is too small to swap protected (its input or minimum output is 0)")
        try:
            swap = build_swap(record, hops, side, amount_in, min_out, self._wallet)
        except ValueError as e:
            raise BscOrderRefused(str(e)) from None
        return dict(swap, side=side, ca=ca, ca_decimals=dec, amount_in=amount_in, min_out=min_out, pool=record["pool"],
                    book_block=book_block, book_at=book_at, head=head, expected_out=expected_out)

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        """build, sign and send one swap -> (its transaction hash = the exchange order id, the time). Off while
        CONSTANTS.TRADING_ENABLED is False. The swap goes as a bundle to the builders (_send_tx), its only request, and is
        followed on chain from here (_receipt_loop) whatever they answered, unless nothing went out (BscOrderRefused)"""
        if not CONSTANTS.TRADING_ENABLED:
            raise BscTradingDisabled("bsc: orders are off (bsc_constants.TRADING_ENABLED)")
        if self._busy != order_id:
            raise BscOrderRefused("one swap at a time: it was placed while another transaction was out")
        if self._account is None:
            raise BscOrderRefused(self._key_problem or "no wallet key: connect bsc with bsc_private_key")
        swap = self._swap_for(trading_pair, amount, trade_type, price)
        if not self._approvals.get((swap["token_in"], swap["executor"]["router"])):
            raise BscOrderRefused(f"{swap['token_in']} is not approved for {swap['executor']['router']}")
        record = dict(swap, trading_pair=trading_pair)
        try:
            await self._send_tx(record, swap["to"], swap["data"], swap["value"], CONSTANTS.SWAP_GAS_LIMIT, head=swap["head"])
        finally:
            if "tx_hash" in record:                   # it may be out (even if this task was cancelled): followed to its end
                self._swaps[order_id] = record
                self._swap_sent.set()
        return record["tx_hash"], time.time()

    async def _send_tx(self, record: dict, to: str, data: bytes, value: int, gas: int, head: Optional[int] = None):
        """sign the wallet's next transaction and send it as a bundle to every builder, valid for BUNDLE_BLOCKS blocks after
        the head; its hash, nonce, last block and send time go into `record` (what the lifecycle follows). A swap gives the
        head (its book's, _swap_for) and finds the nonce known (_read_nonce), so its bundle is its one request; an approval
        has no book and reads the head. The nonce is read here only while it is still unknown.
        Raises BscOrderRefused when nothing went out: the nonce or the head unreadable, or every builder refused it
        (BscSendRefused: the nonce is read anew in this turn, and `record` gets no hash). Any other error is an unknown
        outcome: it may be out, so it is followed."""
        try:
            if self._nonce is None:
                self._nonce = int(await self._read("eth_getTransactionCount", [self._wallet, "pending"]), 16)
            if head is None:
                head = int(await self._read("eth_blockNumber", []), 16)
            last_block = head + CONSTANTS.BUNDLE_BLOCKS
            nonce = self._nonce
            raw, tx_hash = sign(self._account, build_tx(to, data, value, nonce, gas, CONSTANTS.GAS_PRICE_WEI))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise BscOrderRefused(f"not sent: {self._mask(e)}") from None
        self._nonce = nonce + 1                       # taken before the send: the transaction may be out even if the call fails
        record.update(tx_hash=tx_hash, nonce=nonce, last_block=last_block, sent=time.time())
        try:
            await self._send_bundle(raw, last_block)
        except BscSendRefused:
            for k in ("tx_hash", "nonce", "last_block", "sent"):
                record.pop(k, None)                   # nothing to follow
            await self._nonce_lost()                  # nothing went out: its nonce is free again, read anew in this turn
            raise
        except Exception as e:
            self.logger().warning(f"bsc: {tx_hash}: no builder took it for sure ({self._mask(e)}); following it on chain")

    async def _read_nonce(self):
        """the wallet's next nonce, read OFF the order path (Pavel 2026-10-15): after a wallet read while it is unknown and
        the wallet idle (so the run's first order finds it), and the moment a transaction ends without landing (_nonce_lost).
        Kept only if no send took a nonce meanwhile; a read that fails leaves it unknown, for the next send to read"""
        try:
            nonce = int(await self._read("eth_getTransactionCount", [self._wallet, "pending"]), 16)
        except asyncio.CancelledError:
            raise
        except Exception:
            return
        if self._nonce is None:
            self._nonce = nonce

    async def _nonce_lost(self):
        """a transaction that never went out, or never landed, leaves its nonce free: it is forgotten and read anew at once,
        in the wallet's turn (no send can race the read), so the next order doesn't read it on its way out"""
        self._nonce = None
        await self._read_nonce()

    async def _send_bundle(self, raw: str, last_block: int):
        """the transaction as a one-transaction bundle to every builder at once, valid up to `last_block`. Returns as soon as
        one builder takes it. Raises BscSendRefused when every builder refused it (each answered with an error: nothing is
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
        for answer in asyncio.as_completed([asyncio.ensure_future(ask(url)) for url in CONSTANTS.BUILDERS]):
            url, ans, err = await answer
            host = url.split("//", 1)[-1]
            if err is not None or not isinstance(ans, dict):
                silent.append(f"{host}: {err or 'not JSON'}")
            elif "error" not in ans:
                return                                # taken: anything but an error (the other's answer finishes on its own)
            else:
                refused.append(f"{host}: {ans['error']}")
        if refused and not silent:
            raise BscSendRefused("every builder refused it: " + "; ".join(refused))
        raise RuntimeError("no builder took it: " + "; ".join(silent + refused))

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

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """False, quietly: a sent swap can't be called back. It ends on its own within seconds (its receipt, or its
        deadline), and the framework leaves the order as it is meanwhile"""
        return False

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """a swap can't be cancelled, but it ends on its own within seconds: wait for the orders out, up to the timeout. One
        that ended reads as done (True); one still out as not cancelled (False)"""
        waiting = [o.client_order_id for o in self.in_flight_orders.values() if not o.is_done]
        end = time.time() + timeout_seconds
        while time.time() < end and any(oid in self.in_flight_orders for oid in waiting):
            await asyncio.sleep(CONSTANTS.RECEIPT_POLL_S)
        return [CancellationResult(oid, oid not in self.in_flight_orders) for oid in waiting]

    # ---------------------------------------------------------------- the swaps out, followed on chain
    async def _receipt_loop(self):
        """every swap out has its receipt asked for every RECEIPT_POLL_S, so its order ends about a block after it is mined
        (the framework's status poll would take up to 10 s, and its fill wait 5 more); idle while none is out"""
        while True:
            if not self._swaps:
                self._swap_sent.clear()
                await self._swap_sent.wait()
            for order_id, swap in list(self._swaps.items()):
                try:
                    await self._follow(order_id, swap)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._warn_swap(swap, f"bsc: {order_id} {swap['tx_hash']}: its receipt could not be read: {self._mask(e)}")
            await asyncio.sleep(CONSTANTS.RECEIPT_POLL_S)

    def _warn_swap(self, swap: dict, text: str):
        """a transaction's trouble, said at most once per WARN_EVERY_S (the loop looks at it ten times a second)"""
        now = time.time()
        if now - swap.get("warned", 0.0) >= CONSTANTS.WARN_EVERY_S:
            swap["warned"] = now
            self.logger().warning(text)

    async def _follow(self, order_id: str, swap: dict):
        """one look at a swap out: mined, or past its last block without a receipt -> it ends (_settle)"""
        ended, receipt = await self._landed(swap)
        if ended:
            await self._settle(order_id, swap, receipt)

    async def _settle(self, order_id: str, swap: dict, receipt: Optional[dict]):
        """a swap's end (receipt None: dropped). Its receipt's moves go on the balances first, so whoever reads them at the
        fill event sees the trade (the hold band, the balancer); then the fill and FILLED with what it really got, or FAILED,
        processed in place (no 5 s wait: what the swap got IS the order); then the wallet's turn is free"""
        parsed = None
        if receipt is None:
            self._nonce = None                        # it never landed, so its nonce is free: read anew before the turn ends
            state, fill, problem = OrderState.FAILED, None, (
                "BscSwapDropped", f"not included by its last block {swap['last_block']}: no builder took it in time, or it "
                                  f"would have reverted (nothing paid)")
        else:
            parsed = parse_receipt(receipt, self._wallet, swap["token_in"], swap["token_out"])
            self._book_receipt(swap, parsed)
            state, fill, problem = self._outcome(order_id, swap, parsed)
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            order = self._order_tracker.fetch_order(client_order_id=order_id)
            if order is not None and order.executed_amount_base < order.amount:
                order.amount = order.executed_amount_base     # an exact-input buy may get less than its amount: no more comes
        try:
            line = self._swap_line(order_id, swap, receipt, parsed, problem)
        except Exception as e:                        # the line is information only: it never holds the settle up
            line = f"bsc: {order_id} {swap.get('tx_hash')}: {problem[1] if problem else 'filled'} ({type(e).__name__} in its line)"
        (self.logger().warning if problem is not None else self.logger().info)(line)
        await self._order_tracker._process_order_update(self._order_update(order_id, swap, state, problem))
        if receipt is None:
            await self._read_nonce()                  # its nonce, read anew in its turn, not on the next order's path
        self._swaps.pop(order_id, None)
        self._free_turn(order_id)

    def _swap_line(self, order_id: str, swap: dict, receipt: Optional[dict], parsed: Optional[dict],
                   problem: Optional[tuple]) -> str:
        """one line per swap's end (Pavel 2026-10-15; audit finding 7: the data the room, a gas bid and the cadence are tuned
        on): what it got against its book, where it landed against its book's block (+N, its index in the block), the
        seconds from the send, how old its book was at the send, the gas. The send reads no head, so the book's block is
        the line's reference: +N covers the book's age and the wait for inclusion"""
        book = swap["book_block"]
        where = f"its book (block {book}) {swap['sent'] - swap['book_at']:.2f} s old at the send"
        what = f"bsc: {swap['trading_pair']} {swap['side']} ({order_id})"
        if receipt is None:
            return f"{what} DROPPED: {problem[1]}; {where}; {swap['tx_hash']}"
        landed, index = int(receipt["blockNumber"], 16), receipt.get("transactionIndex")
        at = (f"landed +{landed - book} blocks after its book (block {landed}, tx #{int(index, 16) if index else '?'}), "
              f"{time.time() - swap['sent']:.2f} s after the send")
        if problem is not None:                                             # its text carries the gas
            return f"{what} {problem[0]}: {problem[1]}; {at}; {where}; {swap['tx_hash']}"
        got, expected = parsed["amount_out"], swap.get("expected_out")
        decimals, unit = ((swap["ca_decimals"], swap["trading_pair"].split("-")[0]) if swap["side"] == "buy" else
                          (CONSTANTS.USDT_DECIMALS, CONSTANTS.QUOTE))
        versus = f"{(got / expected - 1) * 1e4:+.1f} bps vs its book" if expected else "no book figure"
        return (f"{what} filled: got {got / 10 ** decimals:.8g} {unit} ({versus}, room "
                f"{CONSTANTS.SWAP_SLIPPAGE * 10000:.0f} bps); {at}; {where}; gas "
                f"{Decimal(parsed['gas_fee_wei']).scaleb(-18)} BNB ({int(receipt['gasUsed'], 16):,}); {swap['tx_hash']}")

    def _outcome(self, order_id: str, swap: dict, parsed: dict) -> Tuple[OrderState, Optional[TradeUpdate], Optional[tuple]]:
        """a mined swap -> (FILLED, its one fill, None) or (FAILED, None, (error type, why)): reverted (its gas still paid),
        or mined without moving both tokens at the wallet. The fill is what really left and reached the wallet (net of any
        tax); its fee the gas in BNB"""
        gas = Decimal(parsed["gas_fee_wei"]).scaleb(-18)
        if parsed["status"] == "reverted":
            return OrderState.FAILED, None, ("BscSwapReverted", f"reverted on chain (block {parsed['block']}); gas {gas} BNB")
        problem = receipt_problem(parsed)
        if problem:
            return OrderState.FAILED, None, ("BscSwapNoFill", f"mined without a fill: {problem}; gas {gas} BNB")
        ca_raw, usd_raw = ((parsed["amount_out"], parsed["amount_in"]) if swap["side"] == "buy"
                           else (parsed["amount_in"], parsed["amount_out"]))
        base = Decimal(ca_raw).scaleb(-swap["ca_decimals"])
        quote = Decimal(usd_raw).scaleb(-CONSTANTS.USDT_DECIMALS)
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount(CONSTANTS.NATIVE_ASSET, gas)])
        return OrderState.FILLED, TradeUpdate(trade_id=swap["tx_hash"], client_order_id=order_id,
                                              exchange_order_id=swap["tx_hash"], trading_pair=swap["trading_pair"],
                                              fill_timestamp=time.time(), fill_price=quote / base, fill_base_amount=base,
                                              fill_quote_amount=quote, fee=fee), None

    @staticmethod
    def _order_update(order_id: str, swap: dict, state: OrderState, problem: Optional[tuple] = None) -> OrderUpdate:
        return OrderUpdate(client_order_id=order_id, exchange_order_id=swap["tx_hash"], trading_pair=swap["trading_pair"],
                           update_timestamp=time.time(), new_state=state,
                           misc_updates={"error_type": problem[0], "error_message": problem[1]} if problem else None)

    def _book_receipt(self, swap: dict, parsed: dict):
        """a mined swap's moves on the balances: the gas off the BNB and, filled, its input off and its output on, as the
        receipt's transfers at the wallet say. Skipped when a balance read already saw its block (it holds them)"""
        if parsed["block"] <= self._balances_block:
            return
        asset = {CONSTANTS.USDT: (CONSTANTS.QUOTE, CONSTANTS.USDT_DECIMALS),
                 swap["ca"].lower(): (swap["trading_pair"].split("-")[0].upper(), swap["ca_decimals"])}
        moves = [(CONSTANTS.NATIVE_ASSET, -Decimal(parsed["gas_fee_wei"]).scaleb(-18))]
        if parsed["status"] == "filled":
            token_in, token_out = asset[swap["token_in"].lower()], asset[swap["token_out"].lower()]
            moves += [(token_in[0], -Decimal(parsed["amount_in"]).scaleb(-token_in[1])),
                      (token_out[0], Decimal(parsed["amount_out"]).scaleb(-token_out[1]))]
        for name, delta in moves:
            balance = self._account_balances.get(name, Decimal(0)) + delta
            self._account_balances[name] = balance
            self._account_available_balances[name] = balance
        self._balances_block = parsed["block"]

    async def _mined(self, client_order_id: str) -> Tuple[dict, Optional[dict]]:
        """(the order's swap out, its parsed receipt or None while it is not mined)"""
        swap = self._swaps.get(client_order_id)
        if swap is None:
            raise BscOrderRefused(f"no swap out for {client_order_id}")
        receipt = await self._read("eth_getTransactionReceipt", [swap["tx_hash"]])
        return swap, None if receipt is None else parse_receipt(receipt, self._wallet, swap["token_in"], swap["token_out"])

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """the framework's poll (unused: _receipt_loop follows every swap): the fill _settle books"""
        swap, parsed = await self._mined(order.client_order_id)
        fill = None if parsed is None else self._outcome(order.client_order_id, swap, parsed)[1]
        return [fill] if fill is not None else []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """the framework's poll (unused: _receipt_loop follows every swap): OPEN while not mined, then as _settle decides"""
        swap, parsed = await self._mined(tracked_order.client_order_id)
        if parsed is None:
            return self._order_update(tracked_order.client_order_id, swap, OrderState.OPEN)
        state, _, problem = self._outcome(tracked_order.client_order_id, swap, parsed)
        return self._order_update(tracked_order.client_order_id, swap, state, problem)

    async def _update_trading_fees(self):
        return

    async def _user_stream_event_listener(self):
        return

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    # ---------------------------------------------------------------- factories
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return WebAssistantsFactory(throttler=self._throttler)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BscAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BscAPIUserStreamDataSource()
