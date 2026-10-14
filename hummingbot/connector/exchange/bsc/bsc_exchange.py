import asyncio
import json
import time
import types
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.bsc import bsc_constants as CONSTANTS
from hummingbot.connector.exchange.bsc.bsc_api_order_book_data_source import BscAPIOrderBookDataSource
from hummingbot.connector.exchange.bsc.bsc_api_user_stream_data_source import BscAPIUserStreamDataSource
from hummingbot.connector.exchange.bsc.bsc_p1_quoter import adr, word
from hummingbot.connector.exchange.bsc.bsc_pool_list import PoolList
from hummingbot.connector.exchange.bsc.bsc_quoter import BscQuoter
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BscTradingDisabled(Exception):
    """an order on `bsc` before the transaction path (step 4) and the send method (its own session) exist"""


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
    - Balances: the wallet's BNB, USDT and traded tokens, one multicall every BALANCE_POLL_S.
    - Trading rules: from P1's records (the CA's decimals and reference price).
    - Orders: OFF in step 3. Every order is refused and the connector never reports ready (CONSTANTS.TRADING_ENABLED), so
      the orchestrator never ticks a strategy with a bsc leg.
    The node URL carries its key: it never reaches a log (_mask)."""

    web_utils = None

    def __init__(self,
                 bsc_rpc_url: str,
                 bsc_wallet_address: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        self._rpc_url = (bsc_rpc_url or "").strip()
        self._wallet = (bsc_wallet_address or "").strip().lower()
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
            "trading_enabled": self.trading_enabled,          # step 3: books and balances only (bsc_constants)
        }

    # ---------------------------------------------------------------- the node
    def _mask(self, text: Any) -> str:
        text = str(text)
        if self._rpc_url:
            text = text.replace(self._rpc_url, "<rpc>")
            key = self._rpc_url.rstrip("/").rsplit("/", 1)[-1]
            if len(key) >= 16:
                text = text.replace(key, "<key>")
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

    async def stop_network(self):
        await super().stop_network()
        if self._balance_task is not None:
            self._balance_task.cancel()
            self._balance_task = None
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
        return TradingRule(trading_pair=trading_pair, min_order_size=step, min_price_increment=price_step,
                           min_base_amount_increment=step, min_notional_size=Decimal("1"))

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
            out[trading_pair] = (merged((book.get("sell") or {}).get("levels", [])),
                                 merged((book.get("buy") or {}).get("levels", [])))
        return out

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
        """the wallet's BNB, USDT and traded tokens in one multicall; an asset whose call fails keeps its last value"""
        if not self._wallet:
            return
        tokens = {CONSTANTS.QUOTE: (CONSTANTS.USDT, CONSTANTS.USDT_DECIMALS)}
        for trading_pair in self._trading_pairs:
            ca, _ = self._ca_for_pair(trading_pair)
            if ca:
                tokens[trading_pair.split("-")[0].upper()] = (ca, int(self._pool_list.cas[ca]["decimals"]))
        calls = [(CONSTANTS.MULTICALL3, bytes.fromhex(CONSTANTS.SEL_ETH_BALANCE) + adr(self._wallet))]
        calls += [(address, bytes.fromhex(CONSTANTS.SEL_BALANCE_OF) + adr(self._wallet)) for address, _ in tokens.values()]
        res = await self._quoter._call(calls)
        read = [(CONSTANTS.NATIVE_ASSET, 18, res[0])] + [(asset, dec, r) for (asset, (_, dec)), r in zip(tokens.items(), res[1:])]
        for asset, decimals, (ok, ret) in read:
            if not ok or len(ret) < 32:
                self.logger().warning(f"bsc: no balance for {asset} this read")
                continue
            amount = Decimal(word(ret, 0)).scaleb(-decimals)
            self._account_balances[asset] = amount
            self._account_available_balances[asset] = amount
        self._balances_read = True

    # ---------------------------------------------------------------- orders: off in step 3
    def _get_fee(self, base_currency: str, quote_currency: str, order_type: OrderType, order_side: TradeType,
                 amount: Decimal, price: Decimal = s_decimal_NaN, is_maker: Optional[bool] = None) -> TradeFeeBase:
        return AddedToCostTradeFee(percent=Decimal("0"))

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        raise BscTradingDisabled("bsc: orders are off until the transaction path (step 4) and the send method exist")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        raise BscTradingDisabled("bsc: there is nothing to cancel, orders are off")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        raise BscTradingDisabled("bsc: orders are off")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        return []

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
