import asyncio
import json
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.lbank import (
    lbank_constants as CONSTANTS,
    lbank_utils as utils,
    lbank_web_utils as web_utils,
)
from hummingbot.connector.exchange.lbank.lbank_api_order_book_data_source import LbankAPIOrderBookDataSource
from hummingbot.connector.exchange.lbank.lbank_api_user_stream_data_source import LbankAPIUserStreamDataSource
from hummingbot.connector.exchange.lbank.lbank_auth import LbankAuth
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


class LbankBusinessError(IOError):
    """A request LBank answered with result false / an error_code: a definite refusal (the code and msg say why),
    never a transport failure. IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[int], msg: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.msg = msg


class _Trade(NamedTuple):
    """The latest trade an order push reports: quantity, price, value."""
    quantity: Decimal
    price: Decimal
    value: Decimal


class LbankExchange(ExchangePyBase):
    """
    LBank spot connector (REST v2 + the V2 WebSocket, host api.lbkex.com). DISABLED, kept as reference: LBank's API
    trading is institutional-only (LBank support, 2026-10-07); see __init__.py.

    Reference: the docs (offline mirror VS_code_projects/MDs/lbank-api/, English and Chinese, which match), the four
    official SDKs and CCXT (MDs/lbank-api/sdk/), live probes from myserver (2026-10-07) and P1's (2026-10-04/05), and
    the wiki page trading/exchanges/lbank-api. Every LBank-specific choice cites one of them; what they could not
    settle is audit-logged ([LB-AUDIT]) rather than guessed.

    Scope: LIMIT orders (type buy / sell) only, which is all arb_l and the position balancer send.

    Fills. No LBank fill carries a fee, and the fills are reported two ways: the order push (orderUpdate) carries the
    order's CUMULATIVE filled quantity (accAmt) plus its latest trade (txUuid, amount, price, volumePrice, role); the
    REST order query carries the cumulative quantity and value (executedQty, cummulativeQuoteQty). Each new cumulative
    total becomes one fill of the difference, named after the order id and that total (_fill_from_cumulative), so the
    push and the poll name a fill alike and can never count it twice, nor past the order's size. A push whose latest
    trade is the whole difference gives the exact price and value; any other difference (a push was lost) is valued
    by a REST read, never estimated. The fee = the account's own rate for the pair (maker / taker by the push's role,
    taker when unknown) on the received asset; the first live orders compare it with LBank's per-trade commission.
    """

    web_utils = web_utils

    def __init__(
        self,
        lbank_api_key: str,
        lbank_secret_key: str,
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
        self._api_key = lbank_api_key
        self._secret_key = lbank_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # exchange order id -> [(received_at, order push)] for pushes that beat the placement answer home and carry
        # no client id to match on
        self._pending_pushes: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}
        # exchange order id -> (monotonic time, order query data), shared by the fills poll and the status poll
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
        # asset -> (monotonic time, LBank's `time` ms) of its last balance push: an older one, or an older REST
        # snapshot, doesn't overwrite it
        self._balance_pushed_at: Dict[str, Tuple[float, int]] = {}
        # exchange order ids an orphan alarm was raised for, once each
        self._orphan_ids: Dict[str, None] = {}
        # exchange order id -> the cumulative quantity this process booked for it (bounded): tells a late push of an
        # order already finished from fills nothing booked (_check_orphan_push)
        self._exchange_executed: Dict[str, Decimal] = {}
        # client order ids whose booked fees were compared with LBank's commission (the first FEE_CHECK_ORDERS)
        self._fee_checked: Dict[str, None] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # WS-authoritative until the fill test proves otherwise (runbook §6.3); one switch.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> LbankAuth:
        return LbankAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        return CONSTANTS.RULES_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.RULES_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # The cancel answer carries a status whose meaning (before or after the cancel) is undocumented: the push or
        # the status read settles the order ([LB-AUDIT] cancel-answer).
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
        return LbankAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return LbankAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [LB-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[LB-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [LB-ALARM] WARNING, logged whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[LB-ALARM] {tag} {rendered}")

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
        LBank answers every request with HTTP 200, so the base class's resync-and-retry (which runs only on a
        transport IOError) never sees a stale timestamp. Here: numbers are parsed exactly (Decimal), a placement can
        carry its own timeout, and 10600 (the timestamp is out of LBank's window) re-syncs server time and repeats the
        request once. The timestamp is checked before anything else (live 2026-10-07: a stale timestamp with a made-up
        key answers 10600, not the key's 10005), so the repeat is safe for an order placement. The old clock samples
        are dropped first: TimeSynchronizer blends its last 5, so one fresh sample next to 4 stale ones would still
        leave ~0.8 of the jump. A body that is not JSON (the gateway's HTML 404) is an IOError naming the path.
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)
        result: Any = None
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
            text = await response.text()
            try:
                result = web_utils.loads(text)
            except ValueError:
                raise IOError(f"LBank answered {path_url} with a non-JSON body (HTTP {response.status}): "
                              f"{text[:200]!r}") from None
            if attempt == 0 and is_auth_required and web_utils.error_code(result) == CONSTANTS.CODE_TIMESTAMP:
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer()
                continue
            return result
        return result

    async def _make_trading_rules_request(self) -> Any:
        return self._rules_list(await self._api_get(path_url=self.trading_rules_request_path))

    async def _make_trading_pairs_request(self) -> Any:
        return self._rules_list(await self._api_get(path_url=self.trading_pairs_request_path))

    def _rules_list(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """/v2/accuracy.do (every pair, one call), or an exception. A refused or empty list would otherwise become an
        empty symbol map and no trading rules until the next poll, 30 min later, with every order failing "no trading
        rule". Raising keeps the last map and rules; the base retries in 0.5 s."""
        self._raise_on_error(response, f"Error reading LBank's pair rules ({CONSTANTS.RULES_PATH})")
        if not isinstance(response.get("data"), list) or not response["data"]:
            raise IOError(f"LBank's pair rules came back empty | LBank response: {self._raw(response)}")
        return response

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.SERVER_TIME_PATH, limit_id=CONSTANTS.SERVER_TIME_PATH)
        if not web_utils.is_ok(response) or response.get("data") is None:
            raise IOError(f"Unexpected LBank answer to the network check: {response}")

    @staticmethod
    def _raw(response: Any) -> str:
        """LBank's response body as JSON, complete, for logs and error messages."""
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    def _raise_on_error(self, response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if not web_utils.is_ok(response):
            code = web_utils.error_code(response)
            msg = response.get("msg") if isinstance(response, dict) else None
            self._audit_once(f"error-code:{code}", context=context, response=self._raw(response))
            raise LbankBusinessError(f"{context}: code {code} ({msg}) | LBank response: {self._raw(response)}",
                                     code, msg)
        return response

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """
        The base treats only HTTP 4xx as an exchange rejection and logs anything else as a network error with a
        traceback. LBank refuses orders with HTTP 200 and a code, so a refusal would look like an outage. It is logged
        as a refusal, with the request and LBank's complete response; transport failures take the base path.
        """
        if isinstance(exception, LbankBusinessError):
            self.logger().warning(f"LBank rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return f"code {CONSTANTS.CODE_TIMESTAMP}" in str(request_exception)

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        """LBank's own "this order does not exist"."""
        return isinstance(error, LbankBusinessError) and error.code in CONSTANTS.ORDER_NOT_FOUND_CODES

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found(cancelation_exception)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """
        This fork's base counts EVERY failed status read toward failing the order (the 4th strike, never reset) and
        runs no lost-order recovery: a FAILED order is forgotten while it may still rest on LBank, and its later fills
        are never booked. At the 10 s poll a 40 s REST outage would fail every open order (Hotcoin's audit,
        2026-10-06). So only LBank's own "the order does not exist" counts; anything else is a WARNING and the order
        stays tracked: the push or the next poll settles it.
        """
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        self.logger().warning(f"LBank status read for {order.client_order_id} failed, the order stays tracked: "
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

    @classmethod
    def _is_zero(cls, value: Any) -> bool:
        """LBank's zero amount: "0" on the fast path, any other spelling of zero through Decimal."""
        if value in ("0", 0, None, ""):
            return True
        return cls._dec(value, default="NaN") == 0

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
                       f"was NOT sent to LBank: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from GET {CONSTANTS.RULES_PATH}).")
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
        # type buy / sell = a limit order (docs). custom_id carries the client id; whether LBank echoes it is
        # audit-logged.
        data = {
            "symbol": symbol,
            "type": CONSTANTS.TRADE_TYPES[trade_type],
            "price": self._format_decimal(price),
            "amount": self._format_decimal(amount),
            "custom_id": order_id,
        }
        try:
            response = await self._api_post(path_url=CONSTANTS.PLACE_ORDER_PATH, data=data, is_auth_required=True,
                                            limit_id=CONSTANTS.PLACE_ORDER_PATH, timeout=CONSTANTS.PLACE_ORDER_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as transport_error:
            # No answer (timeout, dropped connection): LBank may still have accepted the order. The base would mark it
            # FAILED and stop tracking it, leaving a live order nobody watches. Ask LBank by client id first; only a
            # confirmed absence lets the failure stand.
            exchange_order_id = await self._find_order_by_client_id(order_id, symbol)
            found_by = "lookup" if exchange_order_id is not None else None
            if exchange_order_id is None:
                # A push carrying our custom_id may have given the order its exchange id meanwhile: LBank has it, even
                # if the lookups failed (e.g. 10004). Failing it would orphan a live order.
                tracked = self._order_tracker.fetch_tracked_order(order_id)
                if tracked is not None and tracked.exchange_order_id is not None:
                    exchange_order_id, found_by = str(tracked.exchange_order_id), "push"
            self._alarm("place-order-unanswered", client_id=order_id, error=repr(transport_error),
                        found_on_exchange=exchange_order_id, found_by=found_by)
            if exchange_order_id is not None:
                return exchange_order_id, self._now()
            raise
        self._audit("place-order", request=self._raw(data), response=self._raw(response))
        self._raise_on_error(response, f"Order {order_id} refused, request {self._raw(data)}")
        result = response.get("data") or {}
        exchange_order_id = result.get("order_id") if isinstance(result, dict) else None
        if exchange_order_id is None:
            raise IOError(f"Error submitting order {order_id}: LBank returned no order_id | "
                          f"LBank response: {self._raw(response)}")
        self._audit_once("place-client-id-echo", sent=order_id, echoed=result.get("custom_id"))
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
        self._remember_order(str(exchange_order_id))
        if order.exchange_order_id is not None and str(order.exchange_order_id) != str(exchange_order_id):
            # A push beat this answer and gave the order its id first; the base never overwrites an id.
            self._alarm("order-id-mismatch", client_id=order.client_order_id, push_id=order.exchange_order_id,
                        placement_id=str(exchange_order_id))
        # The order now has its exchange id: order pushes parked for lack of it are applied.
        self._replay_pending_pushes(str(exchange_order_id), order)
        safe_ensure_future(self._expect_order_push(order.client_order_id, str(exchange_order_id)))
        return exchange_order_id

    async def _expect_order_push(self, client_order_id: str, exchange_order_id: str) -> None:
        """Every accepted order should get a push. None within ORDER_PUSH_EXPECTED_WITHIN means the private stream isn't
        delivering (a socket that is connected proves nothing: the subscribe has no acknowledgement): poll orders and
        balances over REST now, and say so (rate-limited [LB-ALARM])."""
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
        """The base stays on its 60 s poll while any private-stream message arrives, and LBank's pings keep the stream
        looking alive whether or not order pushes come (Hotcoin's lesson, 2026-10-06). While any order is open the
        status poll runs every SHORT_POLL_INTERVAL (10 s): a resting order's fill or cancel is seen within 10 s."""
        if self.in_flight_orders:
            return self.SHORT_POLL_INTERVAL
        return super()._get_poll_interval(timestamp=timestamp)

    # ------------------------------------------------------------------ terminal updates wait for the balance

    @staticmethod
    def _base_delta(order: InFlightOrder, fills: List[TradeUpdate]) -> Decimal:
        """What the fills change the base asset's balance by: + quantity on a buy, - on a sell, less any fee charged
        in the base asset (a buy's is; a sell's is in the quote asset)."""
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
        off, and what is left must reach 99% of the order's own (the slack absorbs fee rounding and an estimated fee).
        An order without fills, or with no baseline (sent before this process), needs no wait. Hotcoin's rule
        (2026-10-06); if LBank's balance push comes before the order push, no terminal update ever waits.
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
        reads REST balances (bounded by the same time) and lets it go anyway, with an [LB-ALARM]. A strategy reads
        balances on completion: reported before the balance, the hold-band would read the old total and buy (or sell)
        again (Hotcoin AEON, 2026-10-06).
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
            self.logger().warning(f"LBank balance read for {order.client_order_id} failed: {e!r}")
        self._alarm("fill-balance-late", client_id=order.client_order_id, source=source,
                    waited_s=CONSTANTS.FILL_BALANCE_WAIT_SECONDS,
                    shows_fills_after_rest=self._balance_shows_fills(order))

    def _process_terminal_update(self, order: InFlightOrder, update: OrderUpdate, source: str = "push") -> None:
        """An order update on its way to the tracker. A terminal one is held until the balance shows the order's
        fills, by ONE waiter per order: the push and the status poll can both bring the same update."""
        if update.new_state not in CONSTANTS.TERMINAL_STATES:
            self._order_tracker.process_order_update(update)
            return
        if order.executed_amount_base > 0:
            safe_ensure_future(self._fee_check(order))
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

    # ------------------------------------------------------------------ order query (REST)

    async def _query_order(self, symbol: str, exchange_order_id: Optional[str] = None,
                           client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """POST /v2/spot/trade/orders_info.do by orderId, or by origClientOrderId (docs: one of the two). The data is
        one order object; a list (the old endpoint's shape) is searched for the order. Raises LbankBusinessError on
        LBank's refusal (its not-found included) and on an ok answer with no order (CODE_EMPTY_ANSWER)."""
        params: Dict[str, Any] = {"symbol": symbol}
        if exchange_order_id is not None:
            params["orderId"] = exchange_order_id
        else:
            params["origClientOrderId"] = client_order_id
        response = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.ORDER_QUERY_PATH, data=params, is_auth_required=True,
                                 limit_id=CONSTANTS.ORDER_QUERY_PATH),
            f"Error fetching status of order {client_order_id or exchange_order_id}",
        )
        detail = response.get("data")
        if isinstance(detail, list):
            wanted = exchange_order_id or client_order_id
            detail = next((row for row in detail if isinstance(row, dict)
                           and wanted in (str(row.get("orderId")), str(row.get("clientOrderId")))), None)
        if not isinstance(detail, dict) or not detail:
            raise LbankBusinessError(f"Error fetching status of order {client_order_id or exchange_order_id}: ok "
                                     f"with no order data | LBank response: {self._raw(response)}",
                                     CONSTANTS.CODE_EMPTY_ANSWER)
        self._audit_once("order-query-key-set", keys=sorted(detail.keys()))
        return detail

    async def _find_order_by_client_id(self, client_order_id: str, symbol: str) -> Optional[str]:
        """The placement guard: the order's exchange id, or None once two looks (the second allows for an order not
        yet queryable) have not found it. Any failed look is None too: the caller then keeps the original failure."""
        for delay in CONSTANTS.PLACEMENT_LOOKUP_DELAYS:
            await asyncio.sleep(delay)
            try:
                detail = await self._query_order(symbol, client_order_id=client_order_id)
            except asyncio.CancelledError:
                raise
            except LbankBusinessError as e:
                self._audit("placement-lookup", client_id=client_order_id, code=e.code)
                if self._is_not_found(e):
                    continue
                return None
            except Exception:
                return None
            if detail.get("orderId") is not None:
                return str(detail["orderId"])
        return None

    async def _fetch_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """The order query for the order, reused for ORDER_DETAIL_CACHE_SECONDS: the status poll runs the fills poll
        and the status read back to back for each order. An order whose placement answer never came is asked by its
        client id."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        exchange_order_id = order.exchange_order_id
        cache_key = exchange_order_id or f"client:{order.client_order_id}"
        cached = self._detail_cache.get(cache_key)
        if cached is not None and time.monotonic() - cached[0] <= CONSTANTS.ORDER_DETAIL_CACHE_SECONDS:
            return cached[1]
        detail = await self._query_order(symbol, exchange_order_id=exchange_order_id,
                                         client_order_id=order.client_order_id)
        self._detail_cache[cache_key] = (time.monotonic(), detail)
        if len(self._detail_cache) > 500:
            for key in sorted(self._detail_cache, key=lambda k: self._detail_cache[k][0])[:250]:
                del self._detail_cache[key]
        return detail

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        # Cancel by exchange order id; this waits for the placement answer if needed (a cancel by client id sent
        # before LBank has the order would answer "not found" and leave the order to land untracked).
        exchange_order_id = await tracked_order.get_exchange_order_id()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                        data={"symbol": symbol, "orderId": exchange_order_id},
                                        is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    response=self._raw(response))
        self._raise_on_error(response, f"LBank refused to cancel order {order_id} ({exchange_order_id})")
        result = response.get("data")
        if isinstance(result, dict):
            # What the answer's status means (before or after the cancel) is undocumented: one sample per value.
            self._audit_once(f"cancel-answer:{result.get('status')}", answer=self._raw(result))
        # A detail read just before the cancel would put the order back to OPEN over PENDING_CANCEL.
        self._detail_cache.pop(str(exchange_order_id), None)
        return True

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """
        The base's cancel path, except:
          - an LBank refusal (already filled, already cancelled, cancelling, not found ...) is a WARNING with LBank's
            answer (no traceback), and the order's status is read at once: a refused cancel usually means the order
            already ended;
          - a timeout counts toward failing the order only when it is the wait for an exchange id that never came; an
            HTTP timeout on the cancel itself reads the order's status instead (the cancel may have landed).
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
                self.logger().warning(f"LBank did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except LbankBusinessError as refusal:
            self.logger().warning(f"LBank refused to cancel {order.client_order_id}: {refusal}. Reading its status "
                                  f"now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        # A fresh read: the cached answer may predate the fill or cancel that made LBank refuse.
        self._detail_cache.pop(str(order.exchange_order_id), None)
        self._detail_cache.pop(f"client:{order.client_order_id}", None)
        try:
            order_update = await self._request_order_status(tracked_order=order)
            self._order_tracker.process_order_update(order_update)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"LBank status read for {order.client_order_id} failed; the next poll reads it "
                                  f"again: {e!r}")

    def _order_state(self, status: Any, executed: Decimal, original: Decimal) -> OrderState:
        try:
            state = CONSTANTS.ORDER_STATE.get(int(status))
        except (TypeError, ValueError):
            state = None
        if state is not None:
            return state
        # Not in the documented map: derive from the amounts rather than guess a terminal state.
        self._audit_once(f"unknown-status:{status}", status=status, executed=str(executed), original=str(original))
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        detail = await self._fetch_order_detail(order)
        fill = self._fill_from_cumulative(order, self._dec(detail.get("executedQty")),
                                          self._dec(detail.get("cummulativeQuoteQty")), fill_time=self._now(),
                                          source="rest")
        if fill is None:
            return []
        self._note_fill_booked(order, fill)   # the base books it right after this returns
        return [fill]

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        detail = await self._fetch_order_detail(tracked_order)
        if tracked_order.exchange_order_id is None and detail.get("orderId") is not None:
            # Found by client id: the order gets its exchange id before a fill is named after it.
            tracked_order.update_exchange_order_id(str(detail["orderId"]))
        filled = self._dec(detail.get("executedQty"))
        # Fills first: a FILLED state with fills still missing would complete the order short.
        fill = self._fill_from_cumulative(tracked_order, filled, self._dec(detail.get("cummulativeQuoteQty")),
                                          fill_time=self._now(), source="rest")
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(tracked_order, fill)
        status = detail.get("status")
        new_state = self._order_state(status, filled, self._dec(detail.get("origQty")))
        self._audit_once(f"order-status:{status}", mapped=str(new_state), executed=str(filled),
                         value=str(detail.get("cummulativeQuoteQty")))
        update = self._order_update(tracked_order, new_state, exchange_order_id=detail.get("orderId"))
        if new_state in CONSTANTS.TERMINAL_STATES:
            if tracked_order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # A guard refused (part of) the fill LBank reports, with its own [LB-ALARM]. The order still ends:
                # held open it would never complete, since every later read reports the same.
                self._alarm("completed-short", client_id=tracked_order.client_order_id, state=str(new_state),
                            lbank_filled=str(filled), booked=str(tracked_order.executed_amount_base))
            if self._balance_shows_fills(tracked_order):
                self._forget_settled(tracked_order)
                if tracked_order.executed_amount_base > 0:
                    safe_ensure_future(self._fee_check(tracked_order))
                return update
            # The caller reports what this returns at once, and reads its orders one after another: a terminal update
            # the balance doesn't show yet goes to the shared waiter instead, and this read reports the state the
            # order already has (a no-op for the tracker).
            self._process_terminal_update(tracked_order, update, source="rest")
            return self._order_update(tracked_order, tracked_order.current_state)
        return update

    def _order_update(self, order: InFlightOrder, state: OrderState, exchange_order_id: Any = None) -> OrderUpdate:
        exchange_order_id = exchange_order_id or order.exchange_order_id
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id) if exchange_order_id is not None else None,
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    # ------------------------------------------------------------------ fills and fees

    def _received_asset(self, order: InFlightOrder) -> str:
        return order.base_asset if order.trade_type is TradeType.BUY else order.quote_asset

    def _fee_rate(self, trading_pair: str, role: Optional[str]) -> Decimal:
        schema = self._trading_fees.get(trading_pair) or utils.DEFAULT_FEES
        if str(role or "").lower() == "maker":
            return schema.maker_percent_fee_decimal
        return schema.taker_percent_fee_decimal

    def _fill_from_cumulative(
        self,
        order: InFlightOrder,
        filled: Decimal,
        value: Optional[Decimal],
        fill_time: float,
        source: str,
        trade: Optional[_Trade] = None,
        role: Optional[str] = None,
    ) -> Optional[TradeUpdate]:
        """
        One fill for the difference between LBank's cumulative filled quantity and what the order already holds, or
        None when there is none. `value` = the cumulative filled value (REST's cummulativeQuoteQty; the push has none).
        `trade` = the latest trade a push reports; when it is the whole difference its own price and value are used.
        A difference no reported trade covers and no cumulative value prices (a push was lost) is not booked here: a
        REST read is asked for, and its exact value books it. The id is the order id plus the cumulative quantity, so
        the push and the poll name a fill alike.
        Guards, each an [LB-ALARM]: never past the order's size; never a quantity without value; a price implausibly
        far from the limit (½x-2x: a field read wrong) is refused on the REST path and booked at the limit price on the
        push path (its quantity is LBank's own cumulative).
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
        if trade is not None and abs(trade.quantity - base) <= tolerance and trade.price > 0:
            price = trade.price
            quote = trade.value if trade.value > 0 else trade.price * base
        elif value is not None:
            quote = value - order.executed_amount_quote
            if quote <= 0:
                self._alarm("fill-without-value", client_id=order.client_order_id, source=source,
                            cumulative=str(filled), value=str(value), held_base=str(held),
                            held_value=str(order.executed_amount_quote))
                return None
            price = quote / base
        else:
            # A push reporting more than its own trade: a push was lost. REST's cumulative value books it exactly.
            self._audit("fill-gap-to-rest", client_id=order.client_order_id, cumulative=str(filled), held=str(held),
                        trade=None if trade is None else str(trade.quantity))
            safe_ensure_future(self._refresh_order(order))
            return None
        limit = order.price if order.price is not None and not order.price.is_nan() else Decimal("0")
        if limit > 0 and not (limit / 2 <= price <= limit * 2):
            self._alarm("fill-price-implausible", client_id=order.client_order_id, source=source, price=str(price),
                        limit=str(limit), cumulative=str(filled), value=str(value),
                        action="refused" if source == "rest" else "booked at the limit price")
            if source == "rest":
                return None
            price, quote = limit, limit * base
        rate = self._fee_rate(order.trading_pair, role)
        received = base if order.trade_type is TradeType.BUY else quote
        trade_fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            flat_fees=[TokenAmount(amount=received * rate, token=self._received_asset(order))],
        )
        self._audit_once(f"fee-estimate:{order.trade_type.name}", rate=str(rate), role=role, source=source,
                         token=self._received_asset(order))
        self._remember_order(str(order.exchange_order_id), filled)
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
            is_taker=str(role or "").lower() != "maker",
        )

    async def _fee_check(self, order: InFlightOrder) -> None:
        """AUDIT, the first FEE_CHECK_ORDERS orders with fills: the fees booked (estimated from the rate) next to
        LBank's own per-trade commission and trade ids (/v2/supplement/transaction_history.do, the order's symbol,
        from a minute before it was created). Settles the fee's unit, asset and role, and whether the push's txUuid
        is the REST trade id. Never affects an order."""
        if (not CONSTANTS.LIVE_AUDIT_LOGGING or order.client_order_id in self._fee_checked
                or len(self._fee_checked) >= CONSTANTS.FEE_CHECK_ORDERS):
            return
        self._fee_checked[order.client_order_id] = None
        await asyncio.sleep(2.0)
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            start = datetime.fromtimestamp((order.creation_timestamp or self._now()) - 60, CONSTANTS.SERVER_TZ)
            response = await self._api_post(
                path_url=CONSTANTS.TRADE_HISTORY_PATH,
                data={"symbol": symbol, "startTime": start.strftime("%Y-%m-%d %H:%M:%S"), "limit": 100},
                is_auth_required=True, limit_id=CONSTANTS.TRADE_HISTORY_PATH)
            rows = response.get("data") if web_utils.is_ok(response) else None
            mine = [row for row in rows or [] if isinstance(row, dict)
                    and str(row.get("orderId")) == str(order.exchange_order_id)]
            booked = [(str(f.fill_base_amount), [(str(a.amount), a.token) for a in f.fee.flat_fees])
                      for f in order.order_fills.values()]
            self._audit("fee-check", client_id=order.client_order_id, side=order.trade_type.name, booked=booked,
                        lbank=[{k: row.get(k) for k in ("id", "qty", "price", "quoteQty", "commission", "isMaker",
                                                        "isBuyer", "time")} for row in mine],
                        row_keys=sorted(mine[0].keys()) if mine else None,
                        answer=None if rows is not None else self._raw(response)[:300])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._audit("fee-check", client_id=order.client_order_id, error=repr(e))

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
        """The account's per-pair rates (POST /v2/supplement/customer_trade_fee.do, every pair in one call:
        {symbol, makerCommission, takerCommission}), fractions: btc_usdt reads "0.001" live (FEE_RATE_IS_PERCENT).
        The first fills are checked against LBank's own commission ([LB-AUDIT] fee-check). A rate that reads outside
        0-1% is not used for that pair (DEFAULT_FEES stays), with an [LB-ALARM]."""
        response = await self._api_post(path_url=CONSTANTS.FEE_RATE_PATH, is_auth_required=True,
                                        limit_id=CONSTANTS.FEE_RATE_PATH)
        if not web_utils.is_ok(response):
            self.logger().warning(f"Could not read LBank trading fees: {self._raw(response)[:300]}")
            return
        rows = response.get("data") or []
        if rows:
            self._audit_once("fee-rate-row", row=self._raw(rows[0]), rows=len(rows))
        divisor = Decimal("100") if CONSTANTS.FEE_RATE_IS_PERCENT else Decimal("1")
        implausible = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(row.get("symbol")))
            except KeyError:
                continue  # a delisted pair LBank still prices
            maker = self._dec(row.get("makerCommission"), default="NaN") / divisor
            taker = self._dec(row.get("takerCommission"), default="NaN") / divisor
            # A blank or non-numeric rate is NaN, and an ordering comparison with NaN raises: finite first.
            if not (maker.is_finite() and taker.is_finite()
                    and Decimal("0") <= maker <= Decimal("0.01") and Decimal("0") <= taker <= Decimal("0.01")):
                implausible.append((row.get("symbol"), row.get("makerCommission"), row.get("takerCommission")))
                continue
            self._trading_fees[trading_pair] = TradeFeeSchema(maker_percent_fee_decimal=maker,
                                                              taker_percent_fee_decimal=taker)
        if implausible:
            self._alarm("fee-rate-implausible", pairs=len(implausible), first=implausible[:3],
                        read_as_percent=CONSTANTS.FEE_RATE_IS_PERCENT)

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        """POST /v2/supplement/user_info_account.do: data.balances[] = {asset, free, locked}. total = free + locked.
        LBank lists EVERY asset, held or not (5,654 rows on 2026-10-07): a zero row is skipped before its numbers are
        parsed and counts as absent, which reads 0 like any asset the connector doesn't hold. An asset whose balance push
        arrived after this request was sent keeps the push: the snapshot is older."""
        requested_at = time.monotonic()
        response = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.ACCOUNT_PATH, is_auth_required=True,
                                 limit_id=CONSTANTS.ACCOUNT_PATH),
            "Error fetching LBank balances",
        )
        data = response.get("data") or {}
        balances = data.get("balances") if isinstance(data, dict) else None
        if balances is None:
            raise IOError(f"LBank's account answer carried no balances | LBank response: {self._raw(response)}")
        self._audit_once("balance-rest-shape", assets=len(balances), keys=sorted(balances[0].keys()) if balances else [],
                         can_trade=data.get("canTrade"))
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for entry in balances:
            asset = str(entry.get("asset") or "").upper()
            if not asset or (self._is_zero(entry.get("free")) and self._is_zero(entry.get("locked"))):
                continue
            remote_asset_names.add(asset)
            if self._balance_pushed_at.get(asset, (0.0, 0))[0] > requested_at:
                continue
            available = self._dec(entry.get("free"))
            self._account_available_balances[asset] = available
            self._account_balances[asset] = available + self._dec(entry.get("locked"))
        for asset_name in local_asset_names.difference(remote_asset_names):
            if self._balance_pushed_at.get(asset_name, (0.0, 0))[0] > requested_at:
                continue
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)

    # ------------------------------------------------------------------ symbols / rules

    @staticmethod
    def _split_symbol(symbol: str) -> Tuple[str, str]:
        """base_quote, split at the LAST underscore (a base may hold one, e.g. vet_erc20)."""
        base, quote = symbol.rsplit("_", 1)
        return base.upper(), quote.upper()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for rule in exchange_info.get("data") or []:
            if not utils.is_exchange_information_valid(rule):
                continue
            symbol = str(rule["symbol"])
            try:
                base, quote = self._split_symbol(symbol)
                mapping[symbol] = combine_to_hb_trading_pair(base=base, quote=quote)
            except Exception as exception:
                self.logger().error(f"Error parsing LBank symbol {symbol}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """
        Runtime add of a pair that is not in the startup map, i.e. not among the pairs GET /v2/accuracy.do listed at
        startup (every listed pair is mapped). The base builds f"{base}{quote}" ("BTCUSDT"), which LBank does not
        recognise, so the pair would subscribe and trade under a dead symbol with no error (the OKX 'hyphen poison'
        class). LBank's form is lowercase base_quote.
        """
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.lower()}_{quote.lower()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(
            f"LBank {trading_pair} was not among the pairs GET {CONSTANTS.RULES_PATH} listed at startup; mapped to "
            f"{exchange_symbol}. LBank's answers to its requests will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        /v2/accuracy.do, every pair in one call: priceAccuracy / quantityAccuracy = decimals of the price and the
        quantity; minTranQua = the minimum quantity; minOrderAmount = the minimum order VALUE in the quote asset: "1"
        on 1,372 of 1,378 pairs whatever their price (live 2026-10-07), while minTranQua x price is ~$0.001 at the
        median. Errors 10013 / 10126 name the two. No maximum is published.
        """
        rules: List[TradingRule] = []
        for rule in exchange_info_dict.get("data") or []:
            if not utils.is_exchange_information_valid(rule):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(rule["symbol"]))
                price_increment = Decimal(1).scaleb(-int(rule["priceAccuracy"]))
                amount_increment = Decimal(1).scaleb(-int(rule["quantityAccuracy"]))
                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(rule.get("minTranQua")), amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_notional_size=self._dec(rule.get("minOrderAmount")),
                ))
            except Exception:
                self.logger().exception(f"Error parsing the LBank trading rule {rule.get('symbol')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        One GET /v2/supplement/ticker/price.do with no symbol (every pair, 1,378 rows) instead of one call per pair:
        the order book tracker re-reads the REST last price of every quiet book every 5 s, and per-pair calls hit the
        venue's limit on every pass (XT: 1,842 throttler warnings in 28 h). Every requested pair gets a value, NaN
        when LBank has none: the tracker re-asks a pair it got nothing for at once, without pausing.
        """
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        symbol_to_pair: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                symbol_to_pair[await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)] = trading_pair
            except KeyError:
                continue  # not a pair LBank lists: stays NaN
        if not symbol_to_pair:
            return prices
        response = self._raise_on_error(await self._api_get(path_url=CONSTANTS.PRICE_PATH,
                                                            limit_id=CONSTANTS.PRICE_PATH),
                                        "Error fetching the last LBank prices")
        for entry in response.get("data") or []:
            if not isinstance(entry, dict):
                continue
            trading_pair = symbol_to_pair.get(str(entry.get("symbol")))
            if trading_pair is not None and entry.get("price") not in (None, ""):
                prices[trading_pair] = float(entry["price"])
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                message_type = event_message.get("type")
                if message_type == "orderUpdate":
                    self._process_order_push(event_message.get("orderUpdate") or {}, event_message)
                elif message_type == "assetUpdate":
                    self._process_balance_push(event_message.get("data") or {})
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the LBank user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        order = tracker.all_fillable_orders.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id is not None:
            order = tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        return order

    def _push_age(self, envelope: Dict[str, Any]) -> Optional[float]:
        """Seconds from a push's TS (LBank's clock) to its arrival, on the synchronized clock."""
        ts = web_utils.ts_ms(envelope.get("TS"))
        return None if ts is None else round(self._time_synchronizer.time() - ts * 1e-3, 3)

    def _process_order_push(self, data: Dict[str, Any], envelope: Dict[str, Any]) -> None:
        """
        Order push (orderUpdate, docs "Update subscribed orders"): uuid = exchange order id, customerID = our client
        id, orderStatus, orderAmt, orderPrice, accAmt (cumulative filled quantity), avgPrice, remainAmt, and the latest
        trade: txUuid, amount (its quantity), volumePrice (its value), price (its price while the status is 1 or 2),
        role (maker | taker). Pushes for orders HMB did not place (the website ...) or no longer tracks are not
        applied, only audit-logged with their age.
        """
        self._audit_once("order-push-key-set", keys=sorted(data.keys()))
        client_order_id = str(data.get("customerID") or "")
        exchange_order_id = str(data["uuid"]) if data.get("uuid") is not None else None
        for seen in (exchange_order_id, client_order_id):
            if seen:
                self._pushed_order_ids[seen] = None
        if len(self._pushed_order_ids) > 4000:
            for seen in list(self._pushed_order_ids)[:2000]:
                del self._pushed_order_ids[seen]
        order = self._locate_order(client_order_id, exchange_order_id)
        if order is None:
            known = self._exchange_executed.get(exchange_order_id) if exchange_order_id is not None else None
            if exchange_order_id is not None and not client_order_id and known is None:
                # No client id to match on: possibly ours, before its placement answer gave it an exchange id (if LBank
                # doesn't echo customerID), or an order placed elsewhere. Parked for the placement answer.
                self._pending_pushes.setdefault(exchange_order_id, []).append((time.time(), envelope))
                self._audit("push-parked", order_id=exchange_order_id, status=data.get("orderStatus"))
                self._prune_pending_pushes()
                safe_ensure_future(self._retry_parked_pushes(exchange_order_id))
            else:
                self._audit("order-push-unmatched", client_id=client_order_id or None, order_id=exchange_order_id,
                            status=data.get("orderStatus"), accAmt=data.get("accAmt"), age_s=self._push_age(envelope))
                self._check_orphan_push(client_order_id, exchange_order_id, data, known)
            return
        self._apply_order_push(order, data, envelope)

    def _remember_order(self, exchange_order_id: str, filled: Optional[Decimal] = None) -> None:
        """The cumulative quantity this process booked for an order (0 until a fill), bounded."""
        if filled is None:
            self._exchange_executed.setdefault(exchange_order_id, Decimal("0"))
        else:
            self._exchange_executed[exchange_order_id] = max(filled, self._exchange_executed.get(exchange_order_id,
                                                                                                  Decimal("0")))
        if len(self._exchange_executed) > 4000:
            for key in list(self._exchange_executed)[:2000]:
                del self._exchange_executed[key]

    def _check_orphan_push(self, client_order_id: str, exchange_order_id: Optional[str], data: Dict[str, Any],
                           known: Optional[Decimal]) -> None:
        """A push for one of our orders that nothing tracks any more (its 30 s in the cache are over, or it was failed):
        an [LB-ALARM] once per order when it is open on LBank (untracked-order-open) or reports fills beyond what this
        process booked (untracked-order-fill: they will never be booked). Said, not cancelled: it may be another
        process's. An order this process finished, pushed late, is silent."""
        if exchange_order_id is None or exchange_order_id in self._orphan_ids:
            return
        if known is None and not client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX):
            return  # placed outside HMB
        filled = self._dec(data.get("accAmt"))
        unbooked = filled > (known or Decimal("0")) + CONSTANTS.FILL_AMOUNT_TOLERANCE
        if not (unbooked or self._is_open_push(data)):
            return
        self._orphan_ids[exchange_order_id] = None
        self._alarm("untracked-order-fill" if unbooked else "untracked-order-open", client_id=client_order_id or None,
                    order_id=exchange_order_id, status=data.get("orderStatus"), lbank_filled=str(filled),
                    booked=None if known is None else str(known))

    @staticmethod
    def _is_open_push(data: Dict[str, Any]) -> bool:
        return str(data.get("orderStatus")) in ("0", "1")   # unfilled, partially filled

    def _apply_order_push(self, order: InFlightOrder, data: Dict[str, Any], envelope: Dict[str, Any]) -> None:
        exchange_order_id = str(data["uuid"]) if data.get("uuid") is not None else order.exchange_order_id
        status = data.get("orderStatus")
        filled = self._dec(data.get("accAmt"))
        trade = None
        amount, value = self._dec(data.get("amount")), self._dec(data.get("volumePrice"))
        if data.get("txUuid") and amount > 0:
            # `price` is the trade's price only while the status is 1 or 2 (docs); otherwise it is the order's, and the
            # trade's price comes from its value.
            price = (self._dec(data.get("price")) if str(status) in ("1", "2")
                     else (value / amount if value > 0 else Decimal("0")))
            if price > 0:
                trade = _Trade(amount, price, value)
        self._audit("order-push", client_id=order.client_order_id, order_id=exchange_order_id, status=status,
                    accAmt=data.get("accAmt"), amount=data.get("amount"), price=data.get("price"),
                    volumePrice=data.get("volumePrice"), avgPrice=data.get("avgPrice"), remainAmt=data.get("remainAmt"),
                    role=data.get("role"), txUuid=data.get("txUuid"), age_s=self._push_age(envelope))
        if exchange_order_id is not None:
            self._remember_order(exchange_order_id)
        if order.exchange_order_id is None and exchange_order_id is not None:
            # The push beat the placement answer: give the order its id before a fill is named after it.
            order.update_exchange_order_id(exchange_order_id)
        elif exchange_order_id is not None:
            self._audit_once("push-id-vs-placement-id", push_id=exchange_order_id,
                             placement_id=order.exchange_order_id, same=exchange_order_id == order.exchange_order_id)
        update_time = data.get("updateTime")
        fill_time = int(update_time) * 1e-3 if update_time else self._now()
        fill = self._fill_from_cumulative(order, filled, None, fill_time=fill_time, source="push", trade=trade,
                                          role=data.get("role"))
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(order, fill)
        if order.is_failure and self._is_open_push(data) and exchange_order_id is not None:
            self._cancel_failed_but_open(order, exchange_order_id, status)
            return
        if order.client_order_id not in self._order_tracker.all_updatable_orders:
            return  # already final: a late push may still carry a fill (above), never a state
        new_state = self._order_state(status, filled, self._dec(data.get("orderAmt"), default=str(order.amount)))
        if new_state in CONSTANTS.TERMINAL_STATES and order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
            # Part of the fills this terminal push reports is not booked yet (a guard, or a gap a REST read is
            # valuing): the REST read completes the order, never short.
            self._poll_notifier.set()
            return
        self._process_terminal_update(order, OrderUpdate(
            trading_pair=order.trading_pair,
            update_timestamp=fill_time,
            new_state=new_state,
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
        ))

    def _cancel_failed_but_open(self, order: InFlightOrder, exchange_order_id: str, status: Any) -> None:
        """
        HMB gave the order up (FAILED: its placement answer never came and the lookups missed it), yet LBank reports it
        open. Nothing tracks it once its 30 s in the cache are over, so it is cancelled now, with an [LB-ALARM]. A fill
        it already had was booked from this push.
        """
        if exchange_order_id in self._orphan_ids:
            return
        self._orphan_ids[exchange_order_id] = None
        self._alarm("failed-order-alive", client_id=order.client_order_id, order_id=exchange_order_id, status=status,
                    action="cancelling")
        safe_ensure_future(self._cancel_by_exchange_id(order.trading_pair, exchange_order_id))

    async def _cancel_by_exchange_id(self, trading_pair: str, exchange_order_id: str) -> None:
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                            data={"symbol": symbol, "orderId": exchange_order_id},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-untracked", order_id=exchange_order_id, response=self._raw(response))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"LBank cancel of the untracked order {exchange_order_id} failed: {e!r}")

    async def _retry_parked_pushes(self, exchange_order_id: str) -> None:
        for _ in range(5):
            await asyncio.sleep(0.2)
            if exchange_order_id not in self._pending_pushes:
                return
            self._replay_pending_pushes(exchange_order_id)
        # Still unclaimed after its TTL: an order placed outside HMB, or ours with its placement unanswered (the
        # placement lookup and the REST poll cover that one).
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
        for _, envelope in parked:
            self._apply_order_push(order, envelope.get("orderUpdate") or {}, envelope)
        self._audit("push-replayed", order_id=exchange_order_id, count=len(parked))

    def _prune_pending_pushes(self) -> None:
        cutoff = time.time() - CONSTANTS.PENDING_PUSH_TTL_SECONDS
        for exchange_order_id in list(self._pending_pushes):
            kept = [(ts, d) for ts, d in self._pending_pushes[exchange_order_id] if ts >= cutoff]
            if len(kept) != len(self._pending_pushes[exchange_order_id]):
                self._audit("push-expired", order_id=exchange_order_id,
                            dropped=len(self._pending_pushes[exchange_order_id]) - len(kept))
            if kept:
                self._pending_pushes[exchange_order_id] = kept
            else:
                del self._pending_pushes[exchange_order_id]

    def _process_balance_push(self, data: Dict[str, Any]) -> None:
        """Asset push (assetUpdate): {asset = the total, assetCode, free, freeze, time (ms), type DEPOSIT | WITHDRAW |
        ORDER_CREATE | ORDER_DEAL | ORDER_CANCEL}. A push older (by its `time`) than the last one applied is
        ignored."""
        self._audit_once("balance-push-key-set", keys=sorted(data.keys()))
        asset = str(data.get("assetCode") or "").upper()
        if not asset:
            return
        try:
            push_time = int(data.get("time") or 0)
        except (TypeError, ValueError):
            push_time = 0
        last = self._balance_pushed_at.get(asset)
        if last is not None and push_time and push_time < last[1]:
            self._audit("balance-push-out-of-order", asset=asset, time=push_time, last=last[1])
            return
        available = self._dec(data.get("free"))
        total = (self._dec(data.get("asset")) if data.get("asset") not in (None, "")
                 else available + self._dec(data.get("freeze")))
        self._account_balances[asset] = total
        self._account_available_balances[asset] = available
        self._balance_pushed_at[asset] = (time.monotonic(), push_time)
        if CONSTANTS.LIVE_AUDIT_LOGGING and self._balance_pushes_audited < CONSTANTS.BALANCE_AUDIT_PUSHES:
            self._balance_pushes_audited += 1
            safe_ensure_future(self._audit_balance_push(asset, data))

    async def _audit_balance_push(self, asset: str, push: Dict[str, Any]) -> None:
        """AUDIT: the first few pushes next to LBank's REST balance for the same asset, which settles whether the push
        carries every change (runbook §6.3)."""
        try:
            response = await self._api_post(path_url=CONSTANTS.ACCOUNT_PATH, is_auth_required=True,
                                            limit_id=CONSTANTS.ACCOUNT_PATH)
            balances = (response.get("data") or {}).get("balances") if web_utils.is_ok(response) else None
            rest = ([b for b in balances or [] if str(b.get("asset") or "").upper() == asset]
                    if balances is not None else self._raw(response)[:300])
        except Exception as e:
            rest = f"REST error {e}"
        self._audit("balance-push-vs-rest", asset=asset, push_total=push.get("asset"), push_free=push.get("free"),
                    push_freeze=push.get("freeze"), push_type=push.get("type"), rest=self._raw(rest))
