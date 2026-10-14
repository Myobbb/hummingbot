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
from hummingbot.connector.exchange.bsc.bsc_swap import build_swap, executor
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
      ~/p2_dex/<ca>.json (bsc_pool_list.PoolList). A trading pair is P1's ticker + "-USDT".
    - Balances: the wallet's BNB, USDT and traded tokens, one multicall every BALANCE_POLL_S, and each swap's receipt booked
      on them the moment it is read (a strategy sees a fill in the balances when its event fires).
    - Trading rules: from P1's records (the CA's decimals and reference price). No minimum notional: a DEX has none.
    - Orders (step 4): one exact-input swap per order along the route that priced its book (bsc_swap), signed by the wallet
      (bsc_tx); the transaction hash is the exchange order id; the receipt gives the fill (what really moved at the wallet,
      the gas in BNB) or a revert (a failed order). The send method is its own session's (`self._send`): until it is plugged
      in, and while CONSTANTS.TRADING_ENABLED is False, every order is refused and the connector never reports ready, so the
      orchestrator never ticks a strategy with a bsc leg.
    - A swap venue (`is_swap_venue`, 2026-10-14): an order is one immediate swap, never resting, never cancelled; the limit
      price is the worst average price accepted (no price grid). ONE swap at a time per wallet: an order takes the wallet's
      turn when it is placed and every available balance reads 0 until it ends, so the strategies (arb_l, the balancer),
      which size by it, place nothing here meanwhile. Each swap out is followed on chain (_receipt_loop) and ends about a
      block after it is mined: FILLED with what it really got, or FAILED (reverted, or dropped past its deadline).
    - Approvals are read with the balances. A book side the wallet can't pay into its executor is not shown, so no
      strategy fires a CEX leg against a DEX leg that would be refused.
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
        self._send = None                              # async (raw tx hex) -> None: the Sending session's method; None = off.
                                                       # Raises BscSendRefused when the node refused it (see there)
        self._nonce: Optional[int] = None              # the wallet's next nonce (read before the first swap, then counted)
        self._busy: Optional[str] = None               # the order holding the wallet's one turn (placed, not yet ended)
        self._swaps: Dict[str, dict] = {}              # client order id -> its swap out: tx hash, nonce, tokens, side, deadline
        self._swap_sent = asyncio.Event()              # wakes the receipt loop when a swap goes out
        self._receipt_task: Optional[asyncio.Task] = None
        self._balances_block = 0                       # the block the balances are known at (a read's, or a booked receipt's)
        self._approvals: Dict[tuple, bool] = {}        # (token, router) -> the wallet can pay the token into the router
        self._domain = domain
        self._trading_pairs = list(trading_pairs or [])
        self._trading_required = trading_required
        self._pool_list = PoolList()
        self._quoter = BscQuoter(self._post)
        self._quoted_pairs: Optional[tuple] = None    # the trading pairs the quoter was last set for
        self._reported: Dict[str, str] = {}           # what was last said about the payloads (a change is logged once)
        self._session: Optional[aiohttp.ClientSession] = None
        self._balance_task: Optional[asyncio.Task] = None
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
        """one JSON-RPC POST to the node: the quoter's transport and the balances'"""
        if not self._rpc_url:
            raise RuntimeError("no BSC RPC URL: connect bsc")
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=CONSTANTS.RPC_TIMEOUT_S))
        try:
            async with self._session.post(self._rpc_url, data=body, headers={"Content-Type": "application/json"}) as r:
                return await r.read()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise RuntimeError(self._mask(f"{type(e).__name__}: {e}")) from None

    # ---------------------------------------------------------------- network
    async def start_network(self):
        self._throttler.label = self.name
        await self.stop_network()
        self._reload_pools()
        self._orderbook_ds.reset()
        self.order_book_tracker.start()            # its data source runs the poll loop
        if self.is_trading_required:
            self._balance_task = safe_ensure_future(self._balance_loop())
            self._receipt_task = safe_ensure_future(self._receipt_loop())   # also picks up swaps still out from before

    async def stop_network(self):
        await super().stop_network()
        for task in (self._balance_task, self._receipt_task):
            if task is not None:
                task.cancel()
        self._balance_task = self._receipt_task = None
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def check_network(self) -> NetworkStatus:
        """a fresh poll answers by itself; otherwise one eth_blockNumber (the poll only runs after start_network)"""
        if time.time() - self._orderbook_ds.last_ok < CONSTANTS.STALE_S:
            return NetworkStatus.CONNECTED
        try:
            ans = json.loads(await self._post(b'{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'))
            return NetworkStatus.CONNECTED if "result" in ans else NetworkStatus.NOT_CONNECTED
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    # ---------------------------------------------------------------- P1's pools: quoter, symbol map, trading rules
    def _ca_for_pair(self, trading_pair: str) -> Tuple[Optional[str], str]:
        base, _, quote = trading_pair.partition("-")
        if quote.upper() != CONSTANTS.QUOTE:
            return None, f"quote {quote}: bsc books are priced in {CONSTANTS.QUOTE}"
        return self._pool_list.ca_for(base)

    def _reload_pools(self):
        """the payloads in the pool directory: the quoter, the symbol map and the trading rules follow them, for the CAs of
        this connector's trading pairs only"""
        try:
            changed = self._pool_list.load()
        except Exception as e:
            self.logger().warning(f"bsc: the pool payloads could not be read: {e}")
            return
        pairs = tuple(sorted(self._trading_pairs))
        if not changed and pairs == self._quoted_pairs:
            return
        cas = {}
        for trading_pair in pairs:
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

    async def _poll_books(self) -> dict:
        self._reload_pools()
        return await self._quoter.poll()

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
    async def _balance_loop(self):
        while True:
            try:
                await self._update_balances()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(f"bsc: balance read failed: {self._mask(e)}")
            await asyncio.sleep(CONSTANTS.BALANCE_POLL_S)

    async def _update_balances(self):
        """the wallet's BNB, USDT and traded tokens, and whether each token it may pay is approved for its router, in one
        multicall; an asset whose call fails keeps its last value. The read says which block it saw: one older than a receipt
        already booked on the balances (a node behind the one that served the receipt) is dropped, as it would undo the fill"""
        if self._key_problem:
            raise ValueError(self._key_problem)       # `connect bsc` shows it and keeps no bad key
        if not self._wallet:
            return
        tokens = {CONSTANTS.QUOTE: (CONSTANTS.USDT, CONSTANTS.USDT_DECIMALS)}
        for trading_pair in self._trading_pairs:
            ca, _ = self._ca_for_pair(trading_pair)
            if ca:
                tokens[trading_pair.split("-")[0].upper()] = (ca, int(self._pool_list.cas[ca]["decimals"]))
        calls = [(CONSTANTS.MULTICALL3, bytes.fromhex(CONSTANTS.SEL_ETH_BALANCE) + adr(self._wallet))]
        calls += [(address, bytes.fromhex(CONSTANTS.SEL_BALANCE_OF) + adr(self._wallet)) for address, _ in tokens.values()]
        approvals = [(key, ex, allowance_calls(self._wallet, key[0], ex)) for key, ex in self._approval_pairs().items()]
        res = await self._quoter._call(calls + [c for _, _, cs in approvals for c in cs]
                                       + [(CONSTANTS.MULTICALL3, bytes.fromhex(CONSTANTS.SEL_BLOCK_NUMBER))])
        ok, ret = res[-1]
        block = word(ret, 0) if ok and len(ret) >= 32 else None
        if block is not None and block < self._balances_block:
            return
        read = [(CONSTANTS.NATIVE_ASSET, 18, res[0])] + [(asset, dec, r) for (asset, (_, dec)), r in zip(tokens.items(), res[1:])]
        for asset, decimals, (ok, ret) in read:
            if not ok or len(ret) < 32:
                self.logger().warning(f"bsc: no balance for {asset} this read")
                continue
            amount = Decimal(word(ret, 0)).scaleb(-decimals)
            self._account_balances[asset] = amount
            self._account_available_balances[asset] = amount
        i, now = len(calls), int(time.time())
        for key, ex, cs in approvals:
            answers, i = res[i:i + len(cs)], i + len(cs)
            approved = not missing_approvals(self._wallet, key[0], ex, answers, CONSTANTS.APPROVED_AT_LEAST, now)
            self._report(f"approval {key[0]} -> {key[1]}", "approved" if approved else
                         "MISSING: the wallet can't pay this token into this router (one-time approval transactions)")
            self._approvals[key] = approved
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
        (else the largest): the swap runs exactly where its book was priced"""
        last = self._quoter.last
        if not last or time.time() - self._orderbook_ds.last_ok > CONSTANTS.STALE_S:
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
        """the order as one exact-input swap along its book's route. The limit price is the worst AVERAGE price accepted
        (Pavel 2026-10-14: output at least x is the protection):
          - a buy spends the book's own cost of `amount` (not amount x limit, which bought past `amount` from the curve's
            tail) and must receive at least cost / limit CA;
          - a sell spends `amount` CA and must receive at least amount x limit USDT.
        An order the current book already prices past its limit (beyond PRICE_SLACK) is refused: it could only revert. Within
        the slack the minimum is the book's own output: the limit's, uncapped, would ask for more than the book gives."""
        ca, why = self._ca_for_pair(trading_pair)
        if not ca:
            raise BscOrderRefused(why)
        dec = int(self._pool_list.cas[ca]["decimals"])
        side = "buy" if trade_type == TradeType.BUY else "sell"
        book, record, hops = self._book_route(ca, side, float(amount * price))
        with localcontext() as ctx:
            ctx.prec = 60
            qty = int((amount * Decimal(10) ** dec).to_integral_value(rounding=ROUND_DOWN))
            at_limit = amount * price
            if qty <= 0 or at_limit <= 0:
                raise BscOrderRefused(f"{amount} at {price} is nothing to swap")
            value = self._book_value(book["levels"], amount, price)      # the book's cost (buy) / proceeds (sell) of `amount`
            slack = at_limit * CONSTANTS.PRICE_SLACK
            if side == "buy":
                if value > at_limit + slack:
                    raise BscOrderRefused(f"the book moved past the limit: {amount} costs {value:.8g} > {at_limit:.8g} USDT")
                amount_in = int((value * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(rounding=ROUND_DOWN))
                min_out = int((min(value / price, amount) * Decimal(10) ** dec).to_integral_value(rounding=ROUND_DOWN))
                tax = Decimal(str((record.get("tax_pct") or {}).get("buy") or 0)) / 100
                if tax and record.get("type") != "v2":  # the router checks a v3 / v4 / Infinity pool's output before the tax
                    min_out = int((min_out / (1 - tax)).to_integral_value(rounding=ROUND_DOWN)) + 1
            else:
                if value < at_limit - slack:
                    raise BscOrderRefused(f"the book moved past the limit: {amount} brings {value:.8g} < {at_limit:.8g} USDT")
                amount_in = qty
                min_out = int((min(at_limit, value) * Decimal(10) ** CONSTANTS.USDT_DECIMALS).to_integral_value(
                    rounding=ROUND_DOWN))
        if amount_in <= 0 or min_out <= 0:
            raise BscOrderRefused(f"{amount} at {price} is too small to swap protected (its input or minimum output is 0)")
        try:
            swap = build_swap(record, hops, side, amount_in, min_out, self._wallet)
        except ValueError as e:
            raise BscOrderRefused(str(e)) from None
        return dict(swap, side=side, ca=ca, ca_decimals=dec, amount_in=amount_in, min_out=min_out, pool=record["pool"])

    async def _rpc(self, method: str, params: list):
        ans = json.loads(await self._post(json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()))
        if "error" in ans:
            raise RuntimeError(self._mask(f"{method}: {ans['error']}"))
        return ans.get("result")

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        """build, sign and send one swap -> (its transaction hash = the exchange order id, the time). Off while
        TRADING_ENABLED is False or no send method is plugged in (the Sending session's). The swap is followed on chain
        from here (_receipt_loop) whatever the send call says, unless the node refused it (BscSendRefused)"""
        if not CONSTANTS.TRADING_ENABLED or self._send is None:
            raise BscTradingDisabled("bsc: orders are off until the send method exists (its own session)")
        if self._busy != order_id:
            raise BscOrderRefused(f"one swap at a time: {self._busy} is still out")
        if self._account is None:
            raise BscOrderRefused(self._key_problem or "no wallet key: connect bsc with bsc_private_key")
        swap = self._swap_for(trading_pair, amount, trade_type, price)
        if not self._approvals.get((swap["token_in"], swap["executor"]["router"])):
            raise BscOrderRefused(f"{swap['token_in']} is not approved for {swap['executor']['router']}")
        if self._nonce is None:
            self._nonce = int(await self._rpc("eth_getTransactionCount", [self._wallet, "pending"]), 16)
        nonce = self._nonce
        tx = build_tx(swap["to"], swap["data"], swap["value"], nonce, CONSTANTS.SWAP_GAS_LIMIT, CONSTANTS.GAS_PRICE_WEI)
        raw, tx_hash = sign(self._account, tx)
        self._nonce = nonce + 1                       # taken before the send: the transaction may be out even if the call fails
        self._swaps[order_id] = dict(swap, tx_hash=tx_hash, nonce=nonce, trading_pair=trading_pair)
        try:
            await self._send(raw)
        except BscSendRefused:                        # nothing went out: the nonce is free again, read it anew
            self._swaps.pop(order_id, None)
            self._nonce = None
            raise
        except Exception as e:                        # no answer: it may be out, so it is followed like any other
            self.logger().warning(f"bsc: {order_id}: the send of {tx_hash} did not answer ({self._mask(e)}); "
                                  f"following it on chain")
        finally:
            if order_id in self._swaps:
                self._swap_sent.set()
        return tx_hash, time.time()

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
        """a swap's trouble, said once per STUCK_WARN_S (the loop looks at it four times a second)"""
        now = time.time()
        if now - swap.get("warned", 0.0) >= CONSTANTS.STUCK_WARN_S:
            swap["warned"] = now
            self.logger().warning(text)

    async def _follow(self, order_id: str, swap: dict):
        """one look at a swap out. Mined: it ends (_settle). Not mined DROP_GRACE_S past its deadline: asked for by hash; gone
        from the node, it ends as dropped (mined later, it could only revert); still pending, the wallet waits for it (its
        nonce) and says so"""
        receipt = await self._rpc("eth_getTransactionReceipt", [swap["tx_hash"]])
        if receipt is not None:
            await self._settle(order_id, swap, receipt)
            return
        late = time.time() - swap["deadline"]
        if late < CONSTANTS.DROP_GRACE_S:
            return
        tx = await self._rpc("eth_getTransactionByHash", [swap["tx_hash"]])
        if tx is None:
            await self._settle(order_id, swap, None)
        elif not tx.get("blockNumber"):               # (mined: its receipt is a node behind, the next look has it)
            self._warn_swap(swap, f"bsc: {order_id} {swap['tx_hash']} is still pending {late:.0f}s past its deadline: it "
                                  f"can only revert now, and no other swap goes out before it ends (its nonce)")

    async def _settle(self, order_id: str, swap: dict, receipt: Optional[dict]):
        """a swap's end (receipt None: dropped). Its receipt's moves go on the balances first, so whoever reads them at the
        fill event sees the trade (the hold band, the balancer); then the fill and FILLED with what it really got, or FAILED,
        processed in place (no 5 s wait: what the swap got IS the order); then the wallet's turn is free"""
        if receipt is None:
            self._nonce = None                        # its nonce may be free again: read it anew
            state, fill, problem = OrderState.FAILED, None, (
                "BscSwapDropped", f"not mined {CONSTANTS.DROP_GRACE_S:.0f} s past its deadline and gone from the node")
        else:
            parsed = parse_receipt(receipt, self._wallet, swap["token_in"], swap["token_out"])
            self._book_receipt(swap, parsed)
            state, fill, problem = self._outcome(order_id, swap, parsed)
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            order = self._order_tracker.fetch_order(client_order_id=order_id)
            if order is not None and order.executed_amount_base < order.amount:
                order.amount = order.executed_amount_base     # an exact-input buy may get less than its amount: no more comes
        if problem is not None:
            self.logger().warning(f"bsc: {order_id} {swap['tx_hash']}: {problem[1]}")
        else:
            self.logger().debug(f"bsc: {order_id} {swap['tx_hash']} filled: {fill.fill_base_amount} at {fill.fill_price}")
        await self._order_tracker._process_order_update(self._order_update(order_id, swap, state, problem))
        self._swaps.pop(order_id, None)
        self._free_turn(order_id)

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
        receipt = await self._rpc("eth_getTransactionReceipt", [swap["tx_hash"]])
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
