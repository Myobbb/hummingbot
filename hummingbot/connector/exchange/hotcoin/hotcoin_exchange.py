import asyncio
import json
import time
from decimal import Decimal
from itertools import chain
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.hotcoin import (
    hotcoin_constants as CONSTANTS,
    hotcoin_utils as utils,
    hotcoin_web_utils as web_utils,
)
from hummingbot.connector.exchange.hotcoin.hotcoin_api_order_book_data_source import HotcoinAPIOrderBookDataSource
from hummingbot.connector.exchange.hotcoin.hotcoin_api_user_stream_data_source import HotcoinAPIUserStreamDataSource
from hummingbot.connector.exchange.hotcoin.hotcoin_auth import HotcoinAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class HotcoinBusinessError(IOError):
    """A request Hotcoin answered with a code other than 200: a definite refusal (the code and msg say why), never a
    transport failure. IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[int], msg: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.msg = msg


class HotcoinExchange(ExchangePyBase):
    """
    Hotcoin spot connector (REST v1 + the gzip WebSocket; Huobi/HTX lineage).

    Reference: the spot docs (offline mirror VS_code_projects/MDs/hotcoin-api/, the Chinese page being the complete
    one), the official Python SDK, live probes of 2026-10-05, and the wiki page trading/exchanges/hotcoin-api. Every
    Hotcoin-specific choice cites one of them; what they could not settle is audit-logged ([HC-AUDIT]) rather than
    guessed.

    Scope: LIMIT orders (matchType 0) only, which is all arb_l and the position balancer send.

    Fills. Hotcoin never sends a trade id with a fill: the order push (eventType created / trade / canceled) and
    GET /v1/order/detailById both carry the order's CUMULATIVE filled quantity, value and fee. Each new cumulative
    total becomes one fill of the difference, with an id built from the order id and that total
    (_fill_from_cumulative). The push and the REST poll therefore produce the same fills, a lost push is made up by
    the next push or poll, and an order can never be filled twice or past its size.
    """

    web_utils = web_utils

    def __init__(
        self,
        hotcoin_api_key: str,
        hotcoin_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """
        The signature must match ConnectorSetting.conn_init_parameters in this fork, which always passes
        `balance_asset_limit` (and `rate_limits_share_pct`) and no config map. An upstream-style `client_config_map`
        argument imports fine and then crashes `balance` (CoinEx, 2026-09-15).
        """
        self._api_key = hotcoin_api_key
        self._secret_key = hotcoin_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # exchange order id -> [(received_at, order push)] for pushes that beat the placement answer home and carry
        # no clientOrderId to match on
        self._pending_pushes: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}
        # exchange order id -> (monotonic time, detailById data), shared by the fills poll and the status poll
        self._detail_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        # exchange AND client order ids that got at least one order push (bounded), for _expect_order_push
        self._pushed_order_ids: Dict[str, None] = {}
        self._missing_pushes = 0
        self._last_push_alarm = 0.0
        self._balance_pushes_audited = 0
        # asset -> net base change of every fill this process booked (_note_fill_booked, _balance_shows_fills)
        self._booked_base: Dict[str, Decimal] = {}
        # client order id -> (base asset total balance, _booked_base of that asset) when the order was sent
        self._hold_baseline: Dict[str, Tuple[Decimal, Decimal]] = {}
        # client order ids whose terminal update is waiting for the balance (one waiter each)
        self._settling: set = set()
        # asset -> monotonic time of its last balance push: an older REST snapshot doesn't overwrite it
        self._balance_pushed_at: Dict[str, float] = {}
        # exchange order ids an orphan alarm (and cancel) was raised for, once each (_check_orphan_push)
        self._orphan_ids: Dict[str, None] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # WS-authoritative until the fill test proves otherwise (runbook §6.3); one switch.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> HotcoinAuth:
        return HotcoinAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        return CONSTANTS.SERVER_TIME_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # POST /v1/order/cancel answers {"data": null} and "is asynchronous" (docs): the order push settles it.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    # ------------------------------------------------------------------ factories

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return HotcoinAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return HotcoinAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [HC-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[HC-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [HC-ALARM] WARNING, logged whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[HC-ALARM] {tag} {rendered}")

    # ------------------------------------------------------------------ requests and errors

    async def _api_request(
        self,
        path_url,
        overwrite_url: Optional[str] = None,
        method: RESTMethod = RESTMethod.GET,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        return_err: bool = False,
        limit_id: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Hotcoin answers every request with HTTP 200, so the base class's resync-and-retry (which runs only on a
        transport IOError) never sees a stale timestamp. Here: numbers are parsed exactly (Decimal), a placement can
        carry its own timeout, and code 1000 "Timestamp out of range" re-syncs server time and repeats the request
        once. The timestamp is checked before anything is executed (live: it is refused even before the key is),
        so the repeat is safe for an order placement. The old clock samples are dropped first: TimeSynchronizer
        blends its last 5, so one fresh sample next to 4 stale ones would still leave ~0.8 of the jump.
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)
        for attempt in range(2):
            response = await rest_assistant.execute_request_and_get_response(
                url=url,
                params=params,
                data=data,
                method=method,
                is_auth_required=is_auth_required,
                return_err=return_err,
                throttler_limit_id=limit_id if limit_id else path_url,
                headers=headers,
                timeout=timeout,
            )
            result = web_utils.loads(await response.text())
            if attempt == 0 and is_auth_required and web_utils.is_timestamp_error(result):
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer()
                continue
            return result
        return result

    async def _make_trading_rules_request(self) -> Any:
        return self._market_list(await self._api_get(path_url=self.trading_rules_request_path))

    async def _make_trading_pairs_request(self) -> Any:
        return self._market_list(await self._api_get(path_url=self.trading_pairs_request_path))

    def _market_list(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """/v1/common/symbols, or an exception. Hotcoin's refusals arrive as data (HTTP 200), and an empty or refused
        list would otherwise become an empty symbol map and no trading rules until the next poll, 30 min later, with
        every order failing as "no trading rule". Raising keeps the last map and rules; the base retries in 0.5 s."""
        self._raise_on_error(response, f"Error reading Hotcoin's market list ({CONSTANTS.SYMBOLS_PATH})")
        if not isinstance(response.get("data"), list) or not response["data"]:
            raise IOError(f"Hotcoin's market list came back empty | Hotcoin response: {self._raw(response)}")
        return response

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.SERVER_TIME_PATH, params=dict(CONSTANTS.SERVER_TIME_PARAMS),
                                       limit_id=CONSTANTS.SERVER_TIME_PATH)
        if not isinstance(response, dict) or response.get("time") is None:
            raise IOError(f"Unexpected Hotcoin answer to the network check: {response}")

    @staticmethod
    def _raw(response: Any) -> str:
        """Hotcoin's response body as JSON, complete, for logs and error messages."""
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    def _raise_on_error(self, response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if not web_utils.is_ok(response):
            code = web_utils.error_code(response)
            msg = response.get("msg") if isinstance(response, dict) else None
            self._audit_once(f"error-code:{code}", context=context, response=self._raw(response))
            raise HotcoinBusinessError(f"{context}: code {code} ({msg}) | Hotcoin response: {self._raw(response)}",
                                       code, msg)
        return response

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """
        The base treats only HTTP 4xx as an exchange rejection and logs anything else as a network error with a
        traceback. Hotcoin refuses orders with HTTP 200 and a code, so a refusal would look like an outage. It is
        logged as a refusal, with the request and Hotcoin's complete response; transport failures take the base path.
        """
        if isinstance(exception, HotcoinBusinessError):
            self.logger().warning(f"Hotcoin rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return CONSTANTS.MSG_TIMESTAMP_OUT_OF_RANGE.lower() in str(request_exception).lower()

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        """Hotcoin's own "this order does not exist" (40010)."""
        return isinstance(error, HotcoinBusinessError) and error.code in CONSTANTS.ORDER_NOT_FOUND_CODES

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found(cancelation_exception)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """
        This fork's base counts EVERY failed status read toward failing the order (the 4th strike, never reset) and
        runs no lost-order recovery: a FAILED order is forgotten while it may still rest on Hotcoin, and its later
        fills are never booked. At the 10 s poll a 40 s REST outage would fail every open order. So only "the order
        does not exist" counts: our own client-id lookup coming back empty (CODE_NOT_IN_ORDER_LIST) or 40010 for an
        order with fills (one without is resolved as CANCELED before it gets here). Anything else is a WARNING and
        the order stays tracked: the push or the next poll settles it.
        """
        if isinstance(error, HotcoinBusinessError) and (error.code == CONSTANTS.CODE_NOT_IN_ORDER_LIST
                                                         or self._is_not_found(error)):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        self.logger().warning(f"Hotcoin status read for {order.client_order_id} failed, the order stays tracked: "
                              f"{error!r}")

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        # Plain positional notation: str(Decimal) gives "1E-8" (CoinEx's scientific-notation bug).
        return format(value, "f")

    @staticmethod
    def _dec(value: Any, default: str = "0") -> Decimal:
        try:
            return Decimal(str(value)) if value is not None and value != "" else Decimal(default)
        except Exception:
            return Decimal(default)

    def _now(self) -> float:
        """The clock's time, or the wall clock before the first tick (current_timestamp is NaN until then: a
        connector built at runtime can get pushes before the clock reaches it)."""
        timestamp = self.current_timestamp
        return time.time() if timestamp is None or timestamp != timestamp else timestamp

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """
        The base reads self._trading_rules[trading_pair] before it tracks the order. With no rule it stops there with
        a bare KeyError in a background task (XT B2-USDT, 2026-09-23): nothing is sent, the order is never tracked,
        no failure event fires, and the strategy keeps cancelling an order that does not exist. Here the order is
        tracked and failed the way the base fails its own pre-send checks, with a message saying it was not sent.
        """
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to Hotcoin: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from GET {CONSTANTS.SYMBOLS_PATH}).")
            self.logger().error(message)
            self.start_tracking_order(order_id=order_id, exchange_order_id=None, trading_pair=trading_pair,
                                      order_type=order_type, trade_type=trade_type, price=price, amount=amount,
                                      **kwargs)
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair,
                                             exception=ValueError(message))
            return
        await super()._create_order(trade_type=trade_type, order_id=order_id, trading_pair=trading_pair,
                                    amount=amount, order_type=order_type, price=price, **kwargs)

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        # Every parameter rides in the signed query string, as the SDK sends it (POST_SIGN_URL). clientOrderId is in
        # the Chinese docs and the SDK only; whether Hotcoin echoes it is audit-logged.
        params = {
            "symbol": symbol,
            "type": CONSTANTS.TRADE_TYPES[trade_type],
            "tradeAmount": self._format_decimal(amount),
            "tradePrice": self._format_decimal(price),
            "matchType": CONSTANTS.MATCH_TYPE_LIMIT,
            "clientOrderId": order_id,
        }
        try:
            response = await self._api_post(path_url=CONSTANTS.PLACE_ORDER_PATH, params=params, is_auth_required=True,
                                            limit_id=CONSTANTS.PLACE_ORDER_PATH, timeout=CONSTANTS.PLACE_ORDER_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as transport_error:
            # No answer (timeout, dropped connection): Hotcoin may still have accepted the order. The base would mark
            # it FAILED and stop tracking it, leaving a live order nobody watches. Ask Hotcoin by client id first;
            # only a confirmed absence lets the failure stand.
            exchange_order_id = await self._find_order_by_client_id(order_id, symbol)
            self._alarm("place-order-unanswered", client_id=order_id, error=repr(transport_error),
                        found_on_exchange=exchange_order_id)
            if exchange_order_id is not None:
                return exchange_order_id, self._now()
            raise
        self._audit("place-order", request=self._raw(params), response=self._raw(response))
        self._raise_on_error(response, f"Order {order_id} refused, request {self._raw(params)}")
        data = response.get("data") or {}
        exchange_order_id = data.get("ID", data.get("id")) if isinstance(data, dict) else None
        if exchange_order_id is None:
            raise IOError(f"Error submitting order {order_id}: Hotcoin returned no order ID | "
                          f"Hotcoin response: {self._raw(response)}")
        self._audit_once("place-client-id-echo", sent=order_id, echoed=data.get("clientOrderId"))
        return str(exchange_order_id), self._now()

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        # The baseline the order's terminal update compares the balance with (_balance_shows_fills).
        asset = order.base_asset
        self._hold_baseline[order.client_order_id] = (self._account_balances.get(asset, Decimal("0")),
                                                      self._booked_base.get(asset, Decimal("0")))
        if len(self._hold_baseline) > 2000:
            for client_order_id in list(self._hold_baseline)[:1000]:
                del self._hold_baseline[client_order_id]
        exchange_order_id = await super()._place_order_and_process_update(order, **kwargs)
        if order.exchange_order_id is not None and str(order.exchange_order_id) != str(exchange_order_id):
            # A push beat this answer and gave the order its id first; the base never overwrites an id. If the two
            # differ, cancels and status reads run with the push's id: the docs' examples are 19 vs 8 digits.
            self._alarm("order-id-mismatch", client_id=order.client_order_id, push_id=order.exchange_order_id,
                        placement_id=str(exchange_order_id))
        # The order now has its exchange id: order pushes parked for lack of it are applied.
        self._replay_pending_pushes(str(exchange_order_id), order)
        safe_ensure_future(self._expect_order_push(order.client_order_id, str(exchange_order_id)))
        return exchange_order_id

    async def _expect_order_push(self, client_order_id: str, exchange_order_id: str) -> None:
        """Hotcoin pushes `created` for every accepted order. None within ORDER_PUSH_EXPECTED_WITHIN means the private
        stream isn't delivering: poll orders and balances over REST now, and say so (rate-limited [HC-ALARM])."""
        await asyncio.sleep(CONSTANTS.ORDER_PUSH_EXPECTED_WITHIN)
        if (exchange_order_id in self._pushed_order_ids or client_order_id in self._pushed_order_ids
                or client_order_id not in self._order_tracker.all_fillable_orders):
            return
        self._poll_notifier.set()
        self._missing_pushes += 1
        now = time.time()
        if now - self._last_push_alarm >= CONSTANTS.ORDER_PUSH_ALARM_INTERVAL:
            self._last_push_alarm = now
            self._alarm("order-push-missing", client_id=client_order_id, order_id=exchange_order_id,
                        within_s=CONSTANTS.ORDER_PUSH_EXPECTED_WITHIN, missing_since_last_alarm=self._missing_pushes)
            self._missing_pushes = 0

    def _get_poll_interval(self, timestamp: float) -> float:
        """Order pushes can't be relied on (live 2026-10-06: created/trade pushes for 2 of 7 orders), and the server's
        pings keep the stream looking alive, so the base stays on its 60 s poll. While any order is open the status
        poll runs every SHORT_POLL_INTERVAL (10 s): a resting order's fill or cancel is seen within 10 s, not 60."""
        if self.in_flight_orders:
            return self.SHORT_POLL_INTERVAL
        return super()._get_poll_interval(timestamp=timestamp)

    # ------------------------------------------------------------------ terminal updates wait for the balance

    @staticmethod
    def _base_delta(order: InFlightOrder, fills: List[TradeUpdate]) -> Decimal:
        """What the fills change the base asset's balance by: + quantity on a buy, - on a sell, less any fee charged
        in the base asset (a Hotcoin buy's fee is; a sell's is in the quote asset)."""
        delta = Decimal("0")
        for fill in fills:
            base_fee = sum((fee.amount for fee in fill.fee.flat_fees if fee.token == order.base_asset), Decimal("0"))
            delta += (fill.fill_base_amount if order.trade_type is TradeType.BUY else -fill.fill_base_amount) - base_fee
        return delta

    def _note_fill_booked(self, order: InFlightOrder, fill: TradeUpdate) -> None:
        """Every fill this process books, summed per asset, so the hold can tell an order's own balance change from
        other orders' (_balance_shows_fills)."""
        asset = order.base_asset
        self._booked_base[asset] = self._booked_base.get(asset, Decimal("0")) + self._base_delta(order, [fill])

    def _balance_shows_fills(self, order: InFlightOrder) -> bool:
        """
        True once the base asset's total balance carries the order's own fills. Since the order was sent the balance
        has moved by its own fills and by other orders' fills booked meanwhile (landed or not): the others' are taken
        off, and what is left must reach 99% of the order's own (the slack absorbs Hotcoin's fee rounding). So another
        order's fill can't stand in for this one's, a fill that landed long ago passes at once however much the asset
        traded since (the false holds of UP, 2026-10-06 08:07 and 09:17), and a lost balance push keeps holding until
        the waiter reads REST. The one case left: a fill booked BEFORE this order was sent but landing after it can
        still pass it early. An order without fills, or with no baseline (sent before this process), needs no wait.
        """
        baseline = self._hold_baseline.get(order.client_order_id)
        if baseline is None or order.executed_amount_base <= 0:
            return True
        own = self._base_delta(order, list(order.order_fills.values()))
        if own == 0:
            return True
        balance_then, booked_then = baseline
        asset = order.base_asset
        others = self._booked_base.get(asset, Decimal("0")) - booked_then - own
        moved_own = self._account_balances.get(asset, Decimal("0")) - balance_then - others
        return moved_own / own >= Decimal("0.99")

    def _forget_settled(self, order: InFlightOrder) -> None:
        self._hold_baseline.pop(order.client_order_id, None)

    async def _await_balance_showing_fills(self, order: InFlightOrder, source: str) -> None:
        """
        Holds a terminal update until the balance shows the order's fills, at most FILL_BALANCE_WAIT_SECONDS; then
        reads REST balances (bounded by the same time) and lets it go anyway, with an [HC-ALARM]. Hotcoin pushes a
        fill before the balance that holds it, and a strategy reads balances on completion: reported first, the
        hold-band read AEON as 0 and bought it twice (2026-10-06).
        """
        if self._balance_shows_fills(order):
            return
        started = time.monotonic()
        while time.monotonic() - started < CONSTANTS.FILL_BALANCE_WAIT_SECONDS:
            await asyncio.sleep(0.05)
            if self._balance_shows_fills(order):
                self._audit("fill-balance-wait", client_id=order.client_order_id, source=source,
                            waited_s=round(time.monotonic() - started, 3))
                return
        try:
            await asyncio.wait_for(self._update_balances(), timeout=CONSTANTS.FILL_BALANCE_WAIT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Hotcoin balance read for {order.client_order_id} failed: {e!r}")
        self._alarm("fill-balance-late", client_id=order.client_order_id, source=source,
                    waited_s=CONSTANTS.FILL_BALANCE_WAIT_SECONDS,
                    shows_fills_after_rest=self._balance_shows_fills(order))

    def _process_terminal_update(self, order: InFlightOrder, update: OrderUpdate, source: str = "push") -> None:
        """An order update on its way to the tracker. A terminal one is held until the balance shows the order's
        fills, by ONE waiter per order: the push and the status poll can both bring the same update."""
        if update.new_state not in CONSTANTS.TERMINAL_STATES:
            self._order_tracker.process_order_update(update)
            return
        if self._balance_shows_fills(order):
            self._forget_settled(order)
            self._order_tracker.process_order_update(update)
            return
        if order.client_order_id in self._settling:
            return  # its waiter reports it
        self._settling.add(order.client_order_id)
        safe_ensure_future(self._report_when_balance_shows_fills(order, update, source))

    async def _report_when_balance_shows_fills(self, order: InFlightOrder, update: OrderUpdate, source: str) -> None:
        try:
            await self._await_balance_showing_fills(order, source)
            self._order_tracker.process_order_update(update)
        finally:
            self._settling.discard(order.client_order_id)
            self._forget_settled(order)

    async def _lookup_client_id(self, client_order_id: str, symbol: str) -> Optional[str]:
        """Exchange order id for a client id from GET /v1/order/entrust (current and history, 100 newest), or None.
        The lists are spelled `entrutsCur` / `entrutsHis` in the docs; the conventional spelling is read too."""
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_LIST_PATH,
            params={"symbol": symbol, "type": 0, "page": 1, "count": 100},
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_LIST_PATH,
        )
        self._raise_on_error(response, f"Error listing Hotcoin orders on {symbol}")
        data = response.get("data") or {}
        if not isinstance(data, dict):
            return None
        self._audit_once("entrust-shape", keys=sorted(data.keys()))
        rows = chain(data.get("entrutsCur") or [], data.get("entrutsHis") or [],
                     data.get("entrustCur") or [], data.get("entrustHis") or [])
        for row in rows:
            if (isinstance(row, dict) and str(row.get("clientOrderId") or "") == client_order_id
                    and row.get("id") is not None):
                return str(row["id"])
        return None

    async def _find_order_by_client_id(self, client_order_id: str, symbol: str) -> Optional[str]:
        """The placement guard: the order's exchange id, or None once two looks (the second allows for an order not
        yet listed) have not found it. Any failed look is None too: the caller then keeps the original failure."""
        for delay in CONSTANTS.PLACEMENT_LOOKUP_DELAYS:
            await asyncio.sleep(delay)
            try:
                exchange_order_id = await self._lookup_client_id(client_order_id, symbol)
            except asyncio.CancelledError:
                raise
            except Exception:
                return None
            if exchange_order_id is not None:
                return exchange_order_id
        return None

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        # Hotcoin cancels by exchange order id only; this waits for the placement answer if needed.
        exchange_order_id = await tracked_order.get_exchange_order_id()
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, params={"id": exchange_order_id},
                                        is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    response=self._raw(response))
        self._raise_on_error(response, f"Hotcoin refused to cancel order {order_id} ({exchange_order_id})")
        # A detail read just before the cancel would put the order back to OPEN over PENDING_CANCEL.
        self._detail_cache.pop(str(exchange_order_id), None)
        return True

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """
        The base's cancel path, except:
          - a Hotcoin refusal -- 40010 included (the order is gone) -- is a WARNING with Hotcoin's answer (no
            traceback), and the order's status is read at once: a refused cancel usually means the order already
            filled or was cancelled, and for an order cancelled without a fill the read confirms it (40010);
          - a timeout counts toward failing the order only when it is the wait for an exchange id that never came;
            an HTTP timeout on the cancel itself reads the order's status instead (the cancel may have landed).
        """
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if order.exchange_order_id is None:
                self.logger().warning(f"Failed to cancel the order {order.client_order_id} because it does not have "
                                      f"an exchange order id yet")
                await self._order_tracker.process_order_not_found(order.client_order_id)
            else:
                self.logger().warning(f"Hotcoin did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except HotcoinBusinessError as refusal:
            self.logger().warning(f"Hotcoin refused to cancel {order.client_order_id}: {refusal}. Reading its status "
                                  f"now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        # A fresh read: the cached answer may predate the fill or cancel that made Hotcoin refuse.
        if order.exchange_order_id is not None:
            self._detail_cache.pop(str(order.exchange_order_id), None)
        try:
            order_update = await self._request_order_status(tracked_order=order)
            self._order_tracker.process_order_update(order_update)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Hotcoin status read for {order.client_order_id} after its cancel failed; the "
                                  f"next poll reads it again: {e!r}")

    def _order_state(self, status_code: Any, executed: Decimal, original: Decimal) -> OrderState:
        try:
            state = CONSTANTS.ORDER_STATE.get(int(status_code))
        except (TypeError, ValueError):
            state = None
        if state is not None:
            return state
        # Not in the documented map: derive from the amounts rather than guess a terminal state.
        self._audit_once(f"unknown-status:{status_code}", status_code=status_code, executed=str(executed),
                         original=str(original))
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    # ------------------------------------------------------------------ order detail (REST) and fills

    async def _fetch_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """GET /v1/order/detailById for the order, reused for ORDER_DETAIL_CACHE_SECONDS: the status poll runs the
        fills poll and the status read back to back for each order."""
        exchange_order_id = order.exchange_order_id
        if exchange_order_id is None:
            # The placement answer never arrived; Hotcoin can be asked by client id through the order list.
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            exchange_order_id = await self._lookup_client_id(order.client_order_id, symbol)
            if exchange_order_id is None:
                raise HotcoinBusinessError(f"Order {order.client_order_id} is not in Hotcoin's order list "
                                           f"(looked up by clientOrderId)", CONSTANTS.CODE_NOT_IN_ORDER_LIST)
        cached = self._detail_cache.get(exchange_order_id)
        if cached is not None and time.monotonic() - cached[0] <= CONSTANTS.ORDER_DETAIL_CACHE_SECONDS:
            return cached[1]
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ORDER_DETAIL_PATH, params={"id": exchange_order_id},
                                is_auth_required=True, limit_id=CONSTANTS.ORDER_DETAIL_PATH),
            f"Error fetching status of order {order.client_order_id} ({exchange_order_id})",
        )
        detail = response.get("data")
        if not isinstance(detail, dict) or not detail:
            # Code 200 with no data: our own code, so S4 names this case rather than "code 200".
            raise HotcoinBusinessError(f"Error fetching status of order {order.client_order_id}: code 200 with empty "
                                       f"data | Hotcoin response: {self._raw(response)}", CONSTANTS.CODE_EMPTY_ANSWER)
        self._audit_once("detail-key-set", keys=sorted(detail.keys()))
        self._detail_cache[exchange_order_id] = (time.monotonic(), detail)
        if len(self._detail_cache) > 500:
            for key in sorted(self._detail_cache, key=lambda k: self._detail_cache[k][0])[:250]:
                del self._detail_cache[key]
        return detail

    def _detail_cumulative(self, detail: Dict[str, Any]) -> Tuple[Decimal, Decimal, Decimal]:
        """(filled quantity, filled value, fee) from detailById: count - leftcount, successamount, fees (docs: count =
        order quantity, leftcount = unfilled, successamount = total traded value, fees = fee). A `successcount`
        field, as on the push, is preferred if Hotcoin sends one.
        Known, left as is (Pavel, 2026-10-06): detailById's `fees` is cut to 4 decimals (0.3993 for Hotcoin's
        0.399375 USDT), so a fill settled over REST records its fee up to 0.0001 short. The pushed fees seen so far
        (buys, 2026-10-06) matched Hotcoin's exactly. Balances always come from Hotcoin, so only the recorded fee is
        short."""
        if detail.get("successcount") is not None:
            filled = self._dec(detail.get("successcount"))
        else:
            filled = self._dec(detail.get("count")) - self._dec(detail.get("leftcount"))
        if filled > 0:
            # One sample per statusCode, so a partially-filled-then-cancelled order (8) is captured too.
            self._audit_once(f"detail-fill-fields:{detail.get('statusCode')}", count=detail.get("count"),
                             leftcount=detail.get("leftcount"), successamount=detail.get("successamount"),
                             fees=detail.get("fees"), price=detail.get("price"), last=detail.get("last"))
        return filled, self._dec(detail.get("successamount")), self._dec(detail.get("fees"))

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        try:
            detail = await self._fetch_order_detail(order)
        except HotcoinBusinessError as e:
            if self._is_not_found(e) and order.executed_amount_base <= 0:
                # Hotcoin drops an order cancelled without a fill (40010 from the moment of the cancel), and the fills
                # poll still reads it for 30 s after (cached orders stay fillable): three WARNING tracebacks per
                # timed-out order at the 10 s poll. No fill, nothing to fetch. An order WITH fills that answers this
                # still raises.
                return []
            raise
        filled, value, fee = self._detail_cumulative(detail)
        fill = self._fill_from_cumulative(order, filled, value, fee, fill_time=self._now(), source="rest")
        if fill is None:
            return []
        self._note_fill_booked(order, fill)   # the base books it right after this returns
        return [fill]

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        try:
            detail = await self._fetch_order_detail(tracked_order)
        except HotcoinBusinessError as e:
            if (self._is_not_found(e) and tracked_order.exchange_order_id is not None
                    and tracked_order.executed_amount_base <= 0):
                # Hotcoin deletes an order cancelled without a fill: 40010 from the moment of the cancel (5 of 5,
                # 2026-10-06). The docs say to confirm a cancel with detailById, so this answer IS the confirmation;
                # it settles an order whose `canceled` push was lost. Left to the base's not-found count, the order
                # would be FAILED instead -- read by arb_l as a placement failure, with its cooldown.
                self._forget_settled(tracked_order)
                return self._order_update(tracked_order, OrderState.CANCELED)
            raise
        if tracked_order.exchange_order_id is None and detail.get("id") is not None:
            # Found by client id: the order gets its exchange id before a fill is named after it.
            tracked_order.update_exchange_order_id(str(detail["id"]))
        filled, value, fee = self._detail_cumulative(detail)
        # Fills first: a FILLED state with fills still missing would complete the order short.
        fill = self._fill_from_cumulative(tracked_order, filled, value, fee, fill_time=self._now(),
                                          source="rest")
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(tracked_order, fill)
        status_code = detail.get("statusCode")
        new_state = self._order_state(status_code, filled, self._dec(detail.get("count")))
        self._audit_once(f"detail-status:{status_code}", status=detail.get("status"), mapped=str(new_state))
        update = self._order_update(tracked_order, new_state, exchange_order_id=detail.get("id"))
        if new_state in CONSTANTS.TERMINAL_STATES:
            if self._balance_shows_fills(tracked_order):
                self._forget_settled(tracked_order)
                return update
            # The caller reports what this returns at once, and reads its orders one after another: a terminal update
            # the balance doesn't show yet goes to the shared waiter instead, and this read reports the state the
            # order already has (a no-op for the tracker).
            self._process_terminal_update(tracked_order, update, source="rest")
            return self._order_update(tracked_order, tracked_order.current_state)
        return update

    def _order_update(self, order: InFlightOrder, state: OrderState, exchange_order_id: Any = None) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id or order.exchange_order_id),
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    def _received_asset(self, order: InFlightOrder) -> str:
        return order.base_asset if order.trade_type is TradeType.BUY else order.quote_asset

    @staticmethod
    def _fees_held(order: InFlightOrder) -> Decimal:
        total = Decimal("0")
        for trade_update in order.order_fills.values():
            for flat_fee in trade_update.fee.flat_fees:
                total += flat_fee.amount
        return total

    def _fill_from_cumulative(
        self,
        order: InFlightOrder,
        filled: Decimal,
        value: Decimal,
        fee: Decimal,
        fill_time: float,
        source: str,
        trade: Optional[Tuple[Decimal, Decimal, Decimal]] = None,
    ) -> Optional[TradeUpdate]:
        """
        One fill for the difference between Hotcoin's cumulative totals (filled quantity, value, fee) and what the
        order already holds, or None when there is none. `trade` = (quantity, price, value) of the trade a push
        reports; when it is the whole difference its own price and value are used, otherwise the difference's
        average. The id is the order id plus the cumulative quantity, so the push and the poll name a fill alike.
        Guards, each an [HC-ALARM]: never past the order's size; never a quantity without value (the two fields
        disagree, e.g. a `leftcount` that stopped meaning "unfilled": booking it could invent a fill, so the next
        push or poll decides); a price implausibly far from the limit (a field read wrong) is refused on the REST
        path and booked at the limit price on the push path; a falling cumulative fee books 0 (except REST's
        4-decimal rounding below a push's exact fee, which is silent).
        """
        tolerance = CONSTANTS.FILL_AMOUNT_TOLERANCE
        held = order.executed_amount_base
        if filled <= held + tolerance or order.exchange_order_id is None:
            return None  # nothing new (or no id to name it by yet: the next push or poll counts it)
        if filled > order.amount + tolerance:
            self._alarm("fill-over-amount", client_id=order.client_order_id, source=source, cumulative=str(filled),
                        amount=str(order.amount))
            return None
        base = filled - held
        quote = value - order.executed_amount_quote
        if quote <= 0:
            self._alarm("fill-without-value", client_id=order.client_order_id, source=source, cumulative=str(filled),
                        value=str(value), held_base=str(held), held_value=str(order.executed_amount_quote))
            return None
        if trade is not None and abs(trade[0] - base) <= tolerance and trade[1] > 0:
            price, quote = trade[1], (trade[2] if trade[2] > 0 else trade[1] * base)
        else:
            price = quote / base
        limit = order.price if order.price is not None and not order.price.is_nan() else Decimal("0")
        if limit > 0 and not (limit / 2 <= price <= limit * 2):
            self._alarm("fill-price-implausible", client_id=order.client_order_id, source=source, price=str(price),
                        limit=str(limit), cumulative=str(filled), value=str(value),
                        action="refused" if source == "rest" else "booked at the limit price")
            if source == "rest":
                # REST's quantity is derived (count - leftcount): a price this far off says the quantity may be wrong
                # (e.g. a leftcount that reads 0 after a cancel). Strict: the push or the next poll decides.
                return None
            # The push's quantity is explicit (successcount): book it, at the limit price, rather than lose it.
            price, quote = limit, limit * base
        fee_amount = fee - self._fees_held(order)
        if fee_amount < 0:
            if not (source == "rest" and fee_amount >= -CONSTANTS.REST_FEE_ROUNDING):
                self._alarm("fill-fee-decreased", client_id=order.client_order_id, source=source,
                            cumulative_fee=str(fee), held=str(self._fees_held(order)))
            fee_amount = Decimal("0")
        self._audit_once(f"fee-asset:{order.trade_type.name}", fee=str(fee), filled=str(filled), value=str(value),
                         per_filled=str(fee / filled) if filled > 0 else None,
                         per_value=str(fee / value) if value > 0 else None)
        trade_fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            flat_fees=[TokenAmount(amount=fee_amount, token=self._received_asset(order))],
        )
        return TradeUpdate(
            trade_id=f"{order.exchange_order_id}-{self._format_decimal(filled.normalize())}",
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=trade_fee,
            fill_base_amount=base,
            fill_quote_amount=quote,
            fill_price=price,
            fill_timestamp=fill_time,
            # Hotcoin's push doesn't say, and resting legs do fill as makers (UP 2026-10-06); nothing downstream
            # reads it. The fee above is Hotcoin's own figure either way.
            is_taker=True,
        )

    # ------------------------------------------------------------------ fees

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        trading_pair = combine_to_hb_trading_pair(base=base_currency, quote=quote_currency)
        fee_schema: Optional[TradeFeeSchema] = self._trading_fees.get(trading_pair)
        if fee_schema is not None:
            return TradeFeeBase.new_spot_fee(
                fee_schema=fee_schema,
                trade_type=order_side,
                percent=fee_schema.maker_percent_fee_decimal if is_maker else fee_schema.taker_percent_fee_decimal,
            )
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self) -> None:
        """Hotcoin's API has no fee endpoint. The site's public market list prices all 364 markets at 0.2% / 0.2%
        (2026-10-05), which is DEFAULT_FEES; the fee actually charged arrives with every fill."""
        return

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        """GET /v3/balance: data.assets[] = {currency, currencyId, free, frozen, total} as strings (Chinese docs, SDK).
        The English page's /v1/balance calls `total` "Available", which is ambiguous; v3 names all three.
        An asset whose balance push arrived after this request was sent keeps the push: the snapshot is older."""
        requested_at = time.monotonic()
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.BALANCE_PATH, is_auth_required=True,
                                limit_id=CONSTANTS.BALANCE_PATH),
            "Error fetching Hotcoin balances",
        )
        data = response.get("data") or {}
        assets = data.get("assets") if isinstance(data, dict) else None
        if assets is None:
            raise IOError(f"Hotcoin balance answer carried no assets | Hotcoin response: {self._raw(response)}")
        self._audit_once("balance-rest-shape", assets=len(assets), keys=sorted(assets[0].keys()) if assets else [])
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for entry in assets:
            asset = str(entry.get("currency") or "").upper()
            if not asset:
                continue
            remote_asset_names.add(asset)
            if self._balance_pushed_at.get(asset, 0.0) > requested_at:
                continue
            available = self._dec(entry.get("free"))
            total = (self._dec(entry.get("total")) if entry.get("total") is not None
                     else available + self._dec(entry.get("frozen")))
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total
        for asset_name in local_asset_names.difference(remote_asset_names):
            if self._balance_pushed_at.get(asset_name, 0.0) > requested_at:
                continue
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for info in exchange_info.get("data") or []:
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                mapping[str(info["symbol"])] = combine_to_hb_trading_pair(
                    base=str(info["baseCurrency"]).upper(), quote=str(info["quoteCurrency"]).upper())
            except Exception as exception:
                self.logger().error(f"Error parsing Hotcoin symbol {info.get('symbol')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """
        Runtime add of a pair that is not in the startup map, i.e. not among the markets GET /v1/common/symbols
        listed at startup (every listed market is mapped). The base builds f"{base}{quote}" ("BTCUSDT"), which Hotcoin
        does not recognise, so the pair would subscribe and trade under a dead symbol with no error (the OKX
        'hyphen poison' class). Hotcoin's form is lowercase base_quote (all 364 listed markets, 2026-10-05).
        """
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.lower()}_{quote.lower()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(
            f"Hotcoin {trading_pair} was not among the markets GET {CONSTANTS.SYMBOLS_PATH} listed at startup; "
            f"mapped to {exchange_symbol}. Hotcoin's answers to its requests will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Live 2026-10-05 (364 markets): pricePrecision 1-11 and amountPrecision 0-10 decimals give the increments;
        minOrderAmount is the minimum order value (15 USDT on 210 markets, 1 on 96, 0 = none on 46); minOrderCount /
        maxOrderCount are 0 (= no limit) on all but 7 markets, which carry a minimum quantity (100 ... 1,000,000);
        min/maxOrderPrice are 0 everywhere.
        """
        rules: List[TradingRule] = []
        for info in exchange_info_dict.get("data") or []:
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(info["symbol"]))
                price_increment = Decimal(1).scaleb(-int(info["pricePrecision"]))
                amount_increment = Decimal(1).scaleb(-int(info["amountPrecision"]))
                min_quantity = self._dec(info.get("minOrderCount"))
                max_quantity = self._dec(info.get("maxOrderCount"))
                kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=max(min_quantity, amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_notional_size=self._dec(info.get("minOrderAmount")),
                )
                if max_quantity > 0:
                    kwargs["max_order_size"] = max_quantity
                rules.append(TradingRule(**kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the Hotcoin trading rule {info.get('symbol')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        One GET /v1/market/ticker (no symbol = every market) instead of one call per pair: the order book tracker
        re-reads the REST last price of every quiet book every 5 s, and per-pair calls hit the venue's limit on every
        pass (XT: 1,842 throttler warnings in 28 h). Every requested pair gets a value, NaN when Hotcoin has none:
        the tracker re-asks a pair it got nothing for at once, without pausing.
        """
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        symbol_to_pair: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                symbol_to_pair[await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)] = trading_pair
            except KeyError:
                continue  # not a market Hotcoin lists: stays NaN
        if not symbol_to_pair:
            return prices
        response = self._raise_on_error(await self._api_get(path_url=CONSTANTS.TICKER_PATH,
                                                            limit_id=CONSTANTS.TICKER_PATH),
                                        "Error fetching the last Hotcoin prices")
        for entry in response.get("ticker") or []:
            if not isinstance(entry, dict):
                continue
            trading_pair = symbol_to_pair.get(str(entry.get("symbol")))
            if trading_pair is not None and entry.get("last") not in (None, ""):
                prices[trading_pair] = float(entry["last"])
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                channel = event_message.get("ch")
                data = event_message.get("data")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if channel == CONSTANTS.WS_TOPIC_ORDERS:
                        self._process_order_push(item)
                    elif channel == CONSTANTS.WS_TOPIC_BALANCE:
                        self._process_balance_push(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the Hotcoin user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        order = tracker.all_fillable_orders.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id is not None:
            order = tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        return order

    def _process_order_push(self, data: Dict[str, Any]) -> None:
        """
        Order push (WebSocket Account and Order, market.trade.entrust.change): id = exchange order id (a string),
        clientOrderId (if any), eventType created / trade / canceled, statusCode, count, successcount, successamount,
        fees (cumulative), and for a trade tradecount / tradeprice / tradeamount / tradetime. Pushes for orders HMB
        did not place (source WEB, APP ...) or no longer tracks are not applied, only audit-logged with their age.
        """
        self._audit_once("order-push-key-set", keys=sorted(data.keys()))
        client_order_id = str(data.get("clientOrderId") or "")
        exchange_order_id = str(data["id"]) if data.get("id") is not None else None
        for seen in (exchange_order_id, client_order_id):
            if seen:
                self._pushed_order_ids[seen] = None
        if len(self._pushed_order_ids) > 4000:
            for seen in list(self._pushed_order_ids)[:2000]:
                del self._pushed_order_ids[seen]
        order = self._locate_order(client_order_id, exchange_order_id)
        if order is None:
            if exchange_order_id is not None and not client_order_id and str(data.get("source") or "").upper() == "API":
                # An API order with no client id to match on, before its placement answer gave it an exchange id.
                self._pending_pushes.setdefault(exchange_order_id, []).append((time.time(), data))
                self._audit("push-parked", order_id=exchange_order_id, event=data.get("eventType"))
                self._prune_pending_pushes()
                safe_ensure_future(self._retry_parked_pushes(exchange_order_id))
            else:
                # No order this connector tracks: one finished (and gone from the tracker) before its push came, or
                # one HMB didn't place. The age tells a late push from a missing one (2026-10-06: created/trade pushes
                # came for 2 of 7 orders).
                self._audit("order-push-unmatched", client_id=client_order_id or None, order_id=exchange_order_id,
                            event=data.get("eventType"), status_code=data.get("statusCode"), source=data.get("source"),
                            age_s=self._push_age(data))
                if (client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) and self._is_open_push(data)
                        and exchange_order_id is not None and exchange_order_id not in self._orphan_ids):
                    # One of ours, open on Hotcoin, that nothing tracks: its fills would never be booked. Said, not
                    # cancelled -- it may be another process's (Pavel's call).
                    self._orphan_ids[exchange_order_id] = None
                    self._alarm("untracked-order-open", client_id=client_order_id, order_id=exchange_order_id,
                                status_code=data.get("statusCode"))
            return
        self._apply_order_push(order, data)

    @staticmethod
    def _is_open_push(data: Dict[str, Any]) -> bool:
        return str(data.get("statusCode")) in ("1", "2")   # open, partially filled

    def _push_age(self, data: Dict[str, Any]) -> Optional[float]:
        """Seconds from a push's eventTime (Hotcoin's clock, ms) to its arrival, on the synchronized clock."""
        try:
            return round(self._time_synchronizer.time() - int(data["eventTime"]) * 1e-3, 3)
        except (KeyError, TypeError, ValueError):
            return None

    def _apply_order_push(self, order: InFlightOrder, data: Dict[str, Any]) -> None:
        exchange_order_id = str(data["id"]) if data.get("id") is not None else order.exchange_order_id
        event = str(data.get("eventType") or "")
        status_code = data.get("statusCode")
        filled = self._dec(data.get("successcount"))
        trade = None
        if event == "trade" and data.get("tradecount") is not None:
            trade = (self._dec(data.get("tradecount")), self._dec(data.get("tradeprice")),
                     self._dec(data.get("tradeamount")))
        self._audit("order-push", client_id=order.client_order_id, order_id=exchange_order_id, event=event,
                    status_code=status_code, successcount=data.get("successcount"),
                    successamount=data.get("successamount"), fees=data.get("fees"), tradecount=data.get("tradecount"),
                    tradeprice=data.get("tradeprice"), age_s=self._push_age(data))
        if order.exchange_order_id is None and exchange_order_id is not None:
            # The push beat the placement answer: give the order its id before a fill is named after it.
            order.update_exchange_order_id(exchange_order_id)
        elif exchange_order_id is not None:
            # The docs' examples show a 19-digit push id and an 8-digit placement ID: settle that they are one id.
            self._audit_once("push-id-vs-placement-id", push_id=exchange_order_id,
                             placement_id=order.exchange_order_id, same=exchange_order_id == order.exchange_order_id)
        event_time = data.get("tradetime") or data.get("eventTime")
        fill_time = int(event_time) * 1e-3 if event_time else self._now()
        fill = self._fill_from_cumulative(order, filled, self._dec(data.get("successamount")),
                                          self._dec(data.get("fees")), fill_time=fill_time, source="push", trade=trade)
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(order, fill)
        if order.is_failure and self._is_open_push(data) and exchange_order_id is not None:
            self._cancel_failed_but_open(order, exchange_order_id, status_code)
            return
        if order.client_order_id not in self._order_tracker.all_updatable_orders:
            return  # already final: a late push may still carry a fill (above), never a state
        count = self._dec(data.get("count"), default=str(order.amount))
        new_state = self._order_state(status_code, filled, count)
        if new_state is OrderState.FILLED and order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < count:
            # A guard refused (part of) the fill this FILLED push reports: let a REST read settle it rather than
            # complete the order short.
            self._poll_notifier.set()
            return
        self._process_terminal_update(order, OrderUpdate(
            trading_pair=order.trading_pair,
            update_timestamp=int(data["eventTime"]) * 1e-3 if data.get("eventTime") else self._now(),
            new_state=new_state,
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
        ))

    def _cancel_failed_but_open(self, order: InFlightOrder, exchange_order_id: str, status_code: Any) -> None:
        """
        HMB gave the order up (FAILED: its placement answer never came and the lookups missed it), yet Hotcoin reports
        it open. Nothing tracks it once its 30 s in the cache are over, so it is cancelled now, with an [HC-ALARM].
        A fill it already had was booked from this push; one after the cancel can't happen.
        """
        if exchange_order_id in self._orphan_ids:
            return
        self._orphan_ids[exchange_order_id] = None
        self._alarm("failed-order-alive", client_id=order.client_order_id, order_id=exchange_order_id,
                    status_code=status_code, action="cancelling")
        safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id))

    async def _cancel_by_exchange_id(self, exchange_order_id: str) -> None:
        try:
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, params={"id": exchange_order_id},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-untracked", order_id=exchange_order_id, response=self._raw(response))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Hotcoin cancel of the untracked order {exchange_order_id} failed: {e!r}")

    async def _retry_parked_pushes(self, exchange_order_id: str) -> None:
        for _ in range(5):
            await asyncio.sleep(0.2)
            if exchange_order_id not in self._pending_pushes:
                return
            self._replay_pending_pushes(exchange_order_id)
        # Still unclaimed: a last look once its TTL has passed, then it expires with its [HC-ALARM] (an order that
        # failed its placement yet lives on the venue), without waiting for another push to be parked.
        await asyncio.sleep(CONSTANTS.PENDING_PUSH_TTL_SECONDS)
        if exchange_order_id in self._pending_pushes:
            self._replay_pending_pushes(exchange_order_id)
            self._prune_pending_pushes()

    def _replay_pending_pushes(self, exchange_order_id: str, order: Optional[InFlightOrder] = None) -> None:
        parked = self._pending_pushes.pop(exchange_order_id, None)
        if not parked:
            return
        order = order or self._locate_order("", exchange_order_id)
        if order is None:
            self._pending_pushes[exchange_order_id] = parked
            return
        for _, data in parked:
            self._apply_order_push(order, data)
        self._audit("push-replayed", order_id=exchange_order_id, count=len(parked))

    def _prune_pending_pushes(self) -> None:
        cutoff = time.time() - CONSTANTS.PENDING_PUSH_TTL_SECONDS
        for exchange_order_id in list(self._pending_pushes):
            kept = [(ts, d) for ts, d in self._pending_pushes[exchange_order_id] if ts >= cutoff]
            if len(kept) != len(self._pending_pushes[exchange_order_id]):
                self._alarm("push-expired", order_id=exchange_order_id,
                            dropped=len(self._pending_pushes[exchange_order_id]) - len(kept))
            if kept:
                self._pending_pushes[exchange_order_id] = kept
            else:
                del self._pending_pushes[exchange_order_id]

    def _process_balance_push(self, data: Dict[str, Any]) -> None:
        """Asset push (market.trade.asset.balance): data.assets[] = {currency, free, frozen, total, price, fullName}."""
        assets = data.get("assets") or []
        self._audit_once("balance-push-key-set", keys=sorted(data.keys()),
                         asset_keys=sorted(assets[0].keys()) if assets and isinstance(assets[0], dict) else [])
        for entry in assets:
            if not isinstance(entry, dict):
                continue
            asset = str(entry.get("currency") or "").upper()
            if not asset:
                continue
            available = self._dec(entry.get("free"))
            total = (self._dec(entry.get("total")) if entry.get("total") is not None
                     else available + self._dec(entry.get("frozen")))
            self._account_balances[asset] = total
            self._account_available_balances[asset] = available
            self._balance_pushed_at[asset] = time.monotonic()
            if CONSTANTS.LIVE_AUDIT_LOGGING and self._balance_pushes_audited < CONSTANTS.BALANCE_AUDIT_PUSHES:
                self._balance_pushes_audited += 1
                safe_ensure_future(self._audit_balance_push(asset, entry))

    async def _audit_balance_push(self, asset: str, push: Dict[str, Any]) -> None:
        """AUDIT: the first few pushes next to Hotcoin's REST balance for the same asset, which settles whether the
        push carries every change (runbook §6.3)."""
        try:
            response = await self._api_get(path_url=CONSTANTS.BALANCE_PATH, params={"currency": asset},
                                           is_auth_required=True, limit_id=CONSTANTS.BALANCE_PATH)
            rest = (response.get("data") or {}).get("assets") if web_utils.is_ok(response) else response
        except Exception as e:
            rest = f"REST error {e}"
        self._audit("balance-push-vs-rest", asset=asset, push_free=push.get("free"), push_frozen=push.get("frozen"),
                    push_total=push.get("total"), rest=self._raw(rest))
