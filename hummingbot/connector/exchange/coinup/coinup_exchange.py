import asyncio
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.exchange.coinup import coinup_constants as CONSTANTS
from hummingbot.connector.exchange.coinup import coinup_web_utils as web_utils
from hummingbot.connector.exchange.coinup.coinup_api_order_book_data_source import CoinupAPIOrderBookDataSource
from hummingbot.connector.exchange.coinup.coinup_api_user_stream_data_source import CoinupAPIUserStreamDataSource
from hummingbot.connector.exchange.coinup.coinup_auth import CoinupAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.utils.async_utils import safe_gather

# CoinUp spot (ChainUp white-label), P2 connector, built 2026-11-27 from the live probes (hmb_local_tools/coinup/README),
# P1's LIVE adapter and the docs mirror. NOT yet shipped: the private half's live shapes (the first order's own statuses,
# the fill linkage, the balance timing) are phase-3/6 work and audit-logged ([CU-AUDIT]) rather than guessed.
#
# What the venue gives and what stands in for the rest:
#   - NO private push: orders, fills and balances are polled over REST. While an order is open every 2 s; idle every 10 s;
#     balances every 30 s (the account read is fast and includes zero rows).
#   - Client order ids: `newClientOrderId` is documented and echoed (probe: order/test answered {}; the live placement's
#     echo is phase-6 evidence). The connector sends its HBOT id and keeps it.
#   - Fills: GET /sapi/v1/myTrades?symbol (per-fill rows with fee/feeCoin/time/isMaker) joined to the order by
#     bidId (a buy) / askId (a sell). The fill row carries NO client id — the order does.
#   - Status: GET /sapi/v1/order?orderId&symbol; a made-up id answers {} (probe-verified) — the not-found shape.
#   - Cancel: POST /sapi/v1/cancel answers PENDING_CANCEL with no fill quantity -> is_cancel_request_in_exchange_synchronous
#     False; the status poll settles it.
#   - Books: the kline-api socket (see the data source); the isShow switch from get_allticker.
#
# Every request goes through curl_cffi behind Cloudflare (web_utils); the transport paces itself and retries across
# browser profiles. `curl-cffi` must be installed in HMB's environment before this package can trade.


class CoinupExchange(ExchangePyBase):
    web_utils = web_utils

    def __init__(
        self,
        coinup_api_key: str,
        coinup_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature must match ConnectorSetting.conn_init_parameters in this fork (balance_asset_limit and
        rate_limits_share_pct, no config map)."""
        self._api_key = coinup_api_key
        self._secret_key = coinup_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._symbols_cache: Optional[Tuple[float, List[Dict[str, Any]]]] = None
        self._symbols_lock = asyncio.Lock()
        self._tickers_cache: Optional[Tuple[float, Dict[str, Dict[str, Any]]]] = None
        self._tickers_lock = asyncio.Lock()
        self._last_private_read = 0.0
        self._last_time_sync = 0.0
        self._last_balance_read = 0.0
        self._trades_cache: Dict[str, List[Dict[str, Any]]] = {}   # symbol -> rows, per poll
        self._detail_cache: Dict[str, Dict[str, Any]] = {}          # client id -> order row, per poll
        self._audit_seen: Dict[str, None] = {}
        self._status_log_at: float = 0.0
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinupAuth:
        return CoinupAuth(api_key=self._api_key, secret_key=self._secret_key)

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.ORDER_ID_MAX_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.SYMBOLS_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.SYMBOLS_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return CONSTANTS.IS_CANCEL_REQUEST_IN_EXCHANGE_SYNCHRONOUS

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def last_private_read_time(self) -> float:
        return self._last_private_read

    @property
    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def server_time_ms(self) -> float:
        return self._time_synchronizer.time() * 1e3

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self.authenticator,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinupAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinupAPIUserStreamDataSource(self)

    async def check_network(self) -> NetworkStatus:
        """The network checker calls start_network() only once this returns CONNECTED (NetworkIterator's loop). The
        base check hits the PUBLIC /sapi/v1/ping, which Cloudflare challenges from myserver, so the connector never
        started (LEO, 2026-11-28: every status key false, no balance read, no book). The SIGNED account route passes
        through the challenge window — use it as the health check."""
        try:
            await self._api_get(path_url=CONSTANTS.ACCOUNT_PATH, is_auth_required=True, limit_id="coinup_query")
            self._last_private_read = time.time()
            return NetworkStatus.CONNECTED
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    @property
    def status_dict(self) -> Dict[str, bool]:
        """[CU-BOOT] log which readiness key is false (once per 10 s while not ready): `ready` is the AND of
        symbols_mapping_initialized, order_books_initialized, account_balance, trading_rule_initialized and
        user_stream_initialized — the strategy pauses on any false key."""
        status = super().status_dict
        if not all(status.values()) and time.monotonic() - self._status_log_at > 10.0:
            self._status_log_at = time.monotonic()
            self.logger().info(f"[CU-BOOT] status not ready: {status}")
        return status

    async def start_network(self):
        """[CU-BOOT] instrumentation: the CoinUp connector built but its order book never started at boot
        (2026-11-28, "waiting for connector to initialize"); these two lines show whether start_network runs and
        whether the base's tracker start raises."""
        self.logger().info("[CU-BOOT] start_network entered")
        try:
            await super().start_network()
        except Exception as e:
            self.logger().error(f"[CU-BOOT] start_network FAILED: {type(e).__name__}: {e}", exc_info=True)
            raise
        self.logger().info("[CU-BOOT] start_network done")

    # ------------------------------------------------------------------ public reads (shared caches)

    async def _symbols(self, max_age: float = 300.0) -> List[Dict[str, Any]]:
        async with self._symbols_lock:
            if self._symbols_cache is not None and time.monotonic() - self._symbols_cache[0] < max_age:
                return self._symbols_cache[1]
            payload = await self._api_get(path_url=CONSTANTS.SYMBOLS_PATH, limit_id="coinup_bulk")
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(rows, list) or not rows:
                raise IOError(f"CoinUp's {CONSTANTS.SYMBOLS_PATH} answered no rows")
            self._symbols_cache = (time.monotonic(), rows)
            return rows

    async def market_list(self, max_age: float = 0.0) -> Dict[str, Dict[str, Any]]:
        """{base_coin: symbols row} — the venue's market list (one call, 318 rows), cached max_age s."""
        rows = await self._symbols(max_age=max_age or 0.0)
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            code = str(row.get("base_coin") or "").strip().upper()
            if not code:
                continue
            prev = result.get(code)
            if prev is None or (str(prev.get("count_coin") or "").upper() != "USDT"
                                and str(row.get("count_coin") or "").upper() == "USDT"):
                result[code] = row
        return result

    async def all_tickers(self, max_age: float = 0.0) -> Dict[str, Dict[str, Any]]:
        """{symbol: ticker row} from the single bulk call: isShow, last/buy/sell, amount = 24 h QUOTE turnover."""
        async with self._tickers_lock:
            if self._tickers_cache is not None and time.monotonic() - self._tickers_cache[0] < max_age:
                return self._tickers_cache[1]
            payload = await self._api_get(path_url=CONSTANTS.ALL_TICKER_PATH, limit_id="coinup_bulk")
            data = payload.get("data") if isinstance(payload, dict) else None
            rows = data.get("ticker") if isinstance(data, dict) else None
            if not isinstance(rows, list) or not rows:
                raise IOError(f"CoinUp's {CONSTANTS.ALL_TICKER_PATH} answered no rows")
            result = {str(r.get("symbol") or "").strip().lower(): r for r in rows if r.get("symbol")}
            self._tickers_cache = (time.monotonic(), result)
            return result

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        tickers = await self.all_tickers(max_age=CONSTANTS.ALL_TICKER_SHARE_SECONDS)
        prices: Dict[str, float] = {}
        for pair in trading_pairs:
            try:
                row = tickers.get(await self.exchange_symbol_associated_to_pair(pair))
            except Exception:
                row = None
            if row and row.get("last") is not None:
                prices[pair] = float(row["last"])
        return prices

    # ------------------------------------------------------------------ trading rules / symbol map

    def _format_decimal(self, value: Decimal) -> str:
        return format(value, "f")

    def _wire(self, trading_pair: str) -> str:
        base, quote = trading_pair.split("-")
        return f"{base}{quote}".lower()

    async def _make_trading_rules_request(self) -> Any:
        return await self._symbols(max_age=0.0)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._symbols(max_age=0.0)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any) -> None:
        rows = self._rows(exchange_info)
        mapping: bidict = bidict()
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().lower()
            base = str(row.get("base_coin") or "").strip().upper()
            quote = str(row.get("count_coin") or "").strip().upper()
            if symbol and base and quote:
                mapping[symbol] = f"{base}-{quote}"
        self._set_trading_pair_symbol_map(mapping)

    @staticmethod
    def _rows(exchange_info: Any) -> List[Dict[str, Any]]:
        if isinstance(exchange_info, list):
            return exchange_info
        if isinstance(exchange_info, dict):
            data = exchange_info.get("data")
            if isinstance(data, list):
                return data
        return []

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for row in self._rows(exchange_info_dict):
            base = str(row.get("base_coin") or "").strip().upper()
            quote = str(row.get("count_coin") or "").strip().upper()
            if not base or not quote:
                continue
            try:
                price_precision = int(row.get("price_precision") or 0)
                amount_precision = int(row.get("amount_precision") or 0)
                rules.append(TradingRule(
                    trading_pair=f"{base}-{quote}",
                    min_order_size=Decimal(str(row.get("limit_volume_min") or "0")),
                    min_price_increment=Decimal(1).scaleb(-price_precision),
                    min_base_amount_increment=Decimal(1).scaleb(-amount_precision),
                ))
            except Exception as e:
                self.logger().debug(f"CoinUp: no trading rule for {base}-{quote}: {e}")
        return rules

    async def _update_trading_fees(self) -> None:
        """No fee route: the connector's DEFAULT_FEES stand; each fill carries its own fee/feeCoin."""

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        payload = await self._api_get(path_url=CONSTANTS.ACCOUNT_PATH, is_auth_required=True,
                                       limit_id="coinup_query")
        rows = payload.get("balances") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise IOError(f"CoinUp's {CONSTANTS.ACCOUNT_PATH} answered no balances")
        self._last_private_read = time.time()
        balances: Dict[str, Decimal] = {}
        available: Dict[str, Decimal] = {}
        for row in rows:
            asset = str(row.get("asset") or "").strip().upper()
            if not asset:
                continue
            free = Decimal(str(row.get("free") or "0"))
            locked = Decimal(str(row.get("locked") or "0"))
            balances[asset] = free + locked
            available[asset] = free
        self._account_balances = balances
        self._account_available_balances = available

    # ------------------------------------------------------------------ orders

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        body: Dict[str, Any] = {
            "symbol": symbol,
            "volume": self._format_decimal(amount),
            "side": trade_type.name,
            "type": order_type.name,
            "newClientOrderId": order_id,
        }
        if order_type is OrderType.LIMIT:
            body["price"] = self._format_decimal(price)
        response = await self._api_post(path_url=CONSTANTS.CREATE_ORDER_PATH, data=body,
                                        is_auth_required=True, limit_id="coinup_place")
        self._last_private_read = time.time()
        self._audit_once("placement", symbol=symbol, side=trade_type.name, type=order_type.name,
                         qty=body["volume"], price=body.get("price"), response=response)
        if web_utils.is_error(response):
            raise IOError(f"CoinUp refused the order: {response}")
        order_id_value = response.get("orderId")
        if isinstance(order_id_value, list):
            order_id_value = order_id_value[0] if order_id_value else None
        if order_id_value in (None, ""):
            raise IOError(f"CoinUp's placement answer carried no orderId: {response}")
        transact_time = response.get("transactTime")
        timestamp = float(transact_time) / 1e3 if transact_time else time.time()
        return str(order_id_value), timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        symbol = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                        data={"orderId": str(tracked_order.exchange_order_id), "symbol": symbol},
                                        is_auth_required=True, limit_id="coinup_cancel")
        self._last_private_read = time.time()
        self._audit_once("cancel", symbol=symbol, order=str(tracked_order.exchange_order_id), response=response)
        if web_utils.is_error(response):
            raise IOError(f"CoinUp refused the cancel: {response}")
        return True

    async def _order_row(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        """One order read per poll: GET /sapi/v1/order?orderId&symbol. {} is CoinUp's not-found (probe-verified)."""
        cached = self._detail_cache.get(tracked_order.client_order_id)
        if cached is not None:
            return cached
        symbol = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)
        row = await self._api_get(path_url=CONSTANTS.CREATE_ORDER_PATH,
                                  params={"orderId": str(tracked_order.exchange_order_id), "symbol": symbol},
                                  is_auth_required=True, limit_id="coinup_query")
        self._last_private_read = time.time()
        if not isinstance(row, dict):
            row = {}
        if web_utils.is_error(row):
            raise IOError(f"CoinUp's order read refused: {row}")
        self._detail_cache[tracked_order.client_order_id] = row
        return row

    @staticmethod
    def _order_state(status: Any, executed: Decimal, original: Decimal) -> Optional[OrderState]:
        key = str(status or "").strip().lower()
        if key in ("partially filled", "partially_filled") and executed > 0 and original > 0 and executed >= original:
            return OrderState.FILLED
        return CONSTANTS.ORDER_STATE_MAP.get(key)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        row = await self._order_row(tracked_order)
        if not row:
            raise CoinupOrderNotFound(tracked_order.client_order_id)
        executed = Decimal(str(row.get("executedQty") or "0"))
        original = Decimal(str(row.get("origQty") or "0")) or tracked_order.amount
        new_state = self._order_state(row.get("status"), executed, original)
        if new_state is None:
            self._audit_once(f"status:{row.get('status')}", order=tracked_order.client_order_id,
                             status=row.get("status"))
            new_state = tracked_order.current_state
        update_timestamp = float(row.get("transactTime") or row.get("time") or 0) / 1e3 or time.time()
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
        )

    async def _trades_for_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        if symbol in self._trades_cache:
            return self._trades_cache[symbol]
        payload = await self._api_get(path_url=CONSTANTS.MY_TRADES_PATH,
                                      params={"symbol": symbol, "limit": CONSTANTS.TRADES_LIMIT},
                                      is_auth_required=True, limit_id="coinup_query")
        self._last_private_read = time.time()
        rows = payload if isinstance(payload, list) else []
        if web_utils.is_error(payload):
            raise IOError(f"CoinUp's trade history refused: {payload}")
        self._trades_cache[symbol] = rows
        return rows

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if not order.exchange_order_id:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(order.trading_pair)
        rows = await self._trades_for_symbol(symbol)
        updates: List[TradeUpdate] = []
        for row in rows:
            if order.trade_type is TradeType.BUY:
                linked = str(row.get("bidId") or "") == str(order.exchange_order_id)
            else:
                linked = str(row.get("askId") or "") == str(order.exchange_order_id)
            if not linked:
                continue
            price = Decimal(str(row.get("price") or "0"))
            amount = Decimal(str(row.get("qty") or "0"))
            fee_amount = Decimal(str(row.get("fee") or "0"))
            fee_coin = str(row.get("feeCoin") or "").upper()
            fee = AddedToCostTradeFee(percent=Decimal("0"),
                                      flat_fees=[TokenAmount(asset=fee_coin, amount=fee_amount)] if fee_amount else [])
            updates.append(TradeUpdate(
                trade_id=str(row.get("id")),
                client_order_id=order.client_order_id,
                exchange_order_id=str(order.exchange_order_id),
                trading_pair=order.trading_pair,
                fill_timestamp=float(row.get("time") or 0) / 1e3 or time.time(),
                fill_price=price,
                fill_base_amount=amount,
                fill_quote_amount=price * amount,
                fee=fee,
                is_taker=not bool(row.get("isMaker")),
            ))
        return updates

    def _get_fee(self, base_currency: str, quote_currency: str, order_type: OrderType, order_side: TradeType,
                 amount: Decimal, price: Decimal = Decimal("NaN"), is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = bool(is_maker) or order_type is OrderType.LIMIT_MAKER
        trading_pair = f"{base_currency}-{quote_currency}"
        fee_schema: Optional[TradeFeeSchema] = self._trading_fees.get(trading_pair)
        if fee_schema is not None:
            return AddedToCostTradeFee(percent=fee_schema.maker_percent_fee_decimal if is_maker
                                       else fee_schema.taker_percent_fee_decimal)
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    # ------------------------------------------------------------------ polling

    def _get_poll_interval(self, timestamp: float) -> float:
        return CONSTANTS.ORDER_POLL_INTERVAL if self.in_flight_orders else CONSTANTS.SHORT_POLL_INTERVAL

    async def _status_polling_loop(self) -> None:
        """The base syncs time every poll (a Cloudflare call each time): here every 5 min."""
        while True:
            try:
                await self._poll_notifier.wait()
                now = time.monotonic()
                if now - self._last_time_sync > 300.0:
                    await self._update_time_synchronizer(pass_on_non_cancelled_error=True)
                    self._last_time_sync = now
                await self._status_polling_loop_fetch_updates()
                self._last_poll_timestamp = self.current_timestamp
                self._poll_notifier = asyncio.Event()
            except asyncio.CancelledError:
                raise
            except NotImplementedError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching account updates.", exc_info=True,
                    app_warning_msg=f"Could not fetch account updates from {self.name_cap}. "
                                    "Check API key and network connection.")
                await self._sleep(1.0)

    async def _status_polling_loop_fetch_updates(self) -> None:
        """Balances every 30 s; orders/fills every poll. One fill read per symbol and one order read per order a poll."""
        self._trades_cache.clear()
        self._detail_cache.clear()
        if time.monotonic() - self._last_balance_read >= CONSTANTS.BALANCE_POLL_INTERVAL:
            self._last_balance_read = time.monotonic()
            await safe_gather(self._update_all_balances(), self._update_order_status())
        else:
            await self._update_order_status()

    async def _user_stream_event_listener(self) -> None:
        """No private stream: nothing ever arrives here."""
        while True:
            await asyncio.sleep(3600)

    # ------------------------------------------------------------------ not-found vs a transport error

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        """CoinUp answers a stale timestamp in an HTTP 200 body {"code": ...}; the transport never raises for it, and a
        genuine transport error is not a time problem. Never re-sync from an exception."""
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return isinstance(status_update_exception, CoinupOrderNotFound)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return isinstance(cancelation_exception, CoinupOrderNotFound)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception) -> None:
        """Only the venue's own not-found ({}) may strike an order; a Cloudflare challenge or a timeout must not (it
        would fail a live order after four polls)."""
        if isinstance(error, CoinupOrderNotFound):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        self.logger().warning(f"CoinUp status read for {order.client_order_id} failed; it stays tracked: {error}")

    # ------------------------------------------------------------------ helpers

    def _audit_once(self, key: str, **fields: Any) -> None:
        if CONSTANTS.LIVE_AUDIT_LOGGING and key not in self._audit_seen:
            self._audit_seen[key] = None
            self.logger().info(f"[CU-AUDIT] {key}: {fields}")


class CoinupOrderNotFound(IOError):
    """CoinUp's not-found order: GET /sapi/v1/order answers {} (probe-verified 2026-11-27)."""

    def __init__(self, client_order_id: str) -> None:
        super().__init__(f"CoinUp order {client_order_id} not found")
        self.client_order_id = client_order_id