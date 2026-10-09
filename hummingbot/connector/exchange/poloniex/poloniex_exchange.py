import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.poloniex import (
    poloniex_constants as CONSTANTS,
    poloniex_utils as utils,
    poloniex_web_utils as web_utils,
)
from hummingbot.connector.exchange.poloniex.poloniex_api_order_book_data_source import PoloniexAPIOrderBookDataSource
from hummingbot.connector.exchange.poloniex.poloniex_api_user_stream_data_source import PoloniexAPIUserStreamDataSource
from hummingbot.connector.exchange.poloniex.poloniex_auth import PoloniexAuth
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


class PoloniexBusinessError(IOError):
    """A request Poloniex refused (HTTP 4xx with {"code", "message"}): a definite answer, never a transport failure.
    IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[str], status: int, body: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.body = body


class PoloniexStatusDeferred(Exception):
    """Not a status: the poll leaves this order alone for now (its placement awaits its answer, or its terminal state
    waits for Poloniex's fill rows). Raised instead of returning a snapshot of the current state, which the base would
    queue and could apply after a newer state (a second created event, a finished order set back)."""


class PoloniexPlacementUnknown(IOError):
    """A placement got no answer and the client-id lookup could not tell whether Poloniex has the order. The order is
    NOT failed (it may rest on Poloniex): it stays PENDING_CREATE, and the status poll reads it by its client id until
    Poloniex answers either way."""


class PoloniexExchange(ExchangePyBase):
    """
    Poloniex spot connector.

    Reference: the SPOT docs (offline mirror VS_code_projects/MDs/poloniex-api/), the official SDK polo-sdk-python, and
    the phase-0 probes (wiki trading/exchanges/poloniex-api, "P2 + P3 onboarding"). Every Poloniex-specific choice
    below cites its source; open questions are audit-logged ([PLX-AUDIT]) rather than guessed, and money-guard firings
    always log as [PLX-ALARM].

    Scope: LIMIT (GTC) orders, which is what arb_l and the position balancer send (LIMIT_MAKER is offered too).
    Templates (runbook §0.4, by API shape): the local book and the guards from `xt`, the order-tracking checklist from
    `hotcoin` (only Poloniex's own not-found counts toward failing an order).
    """

    web_utils = web_utils

    def __init__(
        self,
        poloniex_api_key: str,
        poloniex_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature matches ConnectorSetting.conn_init_parameters in this fork, which always passes
        `balance_asset_limit` (and `rate_limits_share_pct`) and no config map (CoinEx, 2026-09-15)."""
        self._api_key = poloniex_api_key
        self._secret_key = poloniex_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # trade ids first recorded from REST, so a later push of the same fill under another id is recognised
        self._rest_fill_ids: Dict[str, float] = {}
        # per-currency balance push `version`: an older push is never applied over a newer one
        self._balance_versions: Dict[str, int] = {}
        self._balance_pushes_audited = 0
        # order ids (exchange and client) a push was seen for: the push-missing alarm
        self._pushed_order_ids: Dict[str, None] = {}
        self._missing_pushes = 0
        self._last_push_alarm = 0.0
        # exchange ids alarmed once: an untracked open order of ours, a failed order still open
        self._orphan_ids: Dict[str, None] = {}
        # client ids whose terminal update waits for a REST backfill (one waiter per order)
        self._settling: set = set()
        # client ids whose placement has not returned yet: the status poll leaves them alone
        self._placing: set = set()
        # client id -> when its terminal state was first held back for missing fill rows (TERMINAL_HOLD_MAX_S)
        self._short_since: Dict[str, float] = {}
        # per asset: monotonic time of its last balance push; a REST read sent before it never overwrites it
        self._balance_pushed_at: Dict[str, float] = {}
        # REST trade id -> the pushed fill (another id) it was matched to: one fill, counted once
        self._rest_aliases: Dict[str, str] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> PoloniexAuth:
        return PoloniexAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        return CONSTANTS.MARKETS_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.MARKETS_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # DELETE /orders/{id} answers state PENDING_CANCEL; the `canceled` push settles it.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    # ------------------------------------------------------------------ factories

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return PoloniexAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return PoloniexAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [PLX-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[PLX-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [PLX-ALARM] WARNING, logged whatever LIVE_AUDIT_LOGGING says."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[PLX-ALARM] {tag} {rendered}")

    # ------------------------------------------------------------------ requests and errors

    @staticmethod
    def _raw(response: Any) -> str:
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            return repr(response)

    async def _call(self, method: RESTMethod, path_url: str, *, params: Optional[Dict[str, Any]] = None,
                    data: Optional[Dict[str, Any]] = None, signed: bool = True,
                    limit_id: Optional[str] = None) -> Any:
        """
        One request, with Poloniex's error model applied:
          2xx -> the parsed body
          4xx -> PoloniexBusinessError (a refusal: code, status, the complete body). 408 (recvWindow, never sent here)
                 counts as unknown, like a 5xx.
          5xx / an unparseable 2xx / transport -> IOError or the transport's own exception (an UNKNOWN outcome)
        A timestamp Poloniex calls expired (HTTP 401 "Signature has expired") was refused at authentication: the clock
        is re-synced and the request sent once more (safe even for a placement).
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.private_rest_url(path_url) if signed else web_utils.public_rest_url(path_url)
        for attempt in range(2):
            response = await rest_assistant.execute_request_and_get_response(
                url=url, throttler_limit_id=limit_id or path_url, params=params, data=data, method=method,
                is_auth_required=signed, return_err=True)
            status = response.status
            text = await response.text()
            if status == 401 and CONSTANTS.MSG_SIGNATURE_EXPIRED in (text or "").lower() and attempt == 0:
                await self._update_time_synchronizer()
                continue
            if 200 <= status < 300:
                try:
                    return json.loads(text)
                except (TypeError, ValueError):
                    raise IOError(f"Poloniex {method.value} {path_url}: an unparseable {status} answer: {text[:300]}")
            if 400 <= status < 500 and status != 408:
                parsed = web_utils.parse_error_text(text)
                code = str(parsed.get("code")) if parsed.get("code") not in (None, "") else None
                raise PoloniexBusinessError(
                    f"Poloniex refused {method.value} {path_url}: HTTP {status} code {code} ({parsed.get('message')})"
                    f" | Poloniex response: {text[:500]}", code, status, text)
            raise IOError(f"Poloniex {method.value} {path_url}: HTTP {status} (outcome unknown): {text[:300]}")
        raise IOError(f"Poloniex {method.value} {path_url}: the timestamp was refused twice")

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        if tracked is not None and (tracked.exchange_order_id is not None
                                    or tracked.current_state is not OrderState.PENDING_CREATE):
            # A push already gave the order its id or moved it: Poloniex has it. Never failed on top of that.
            self._alarm("placement-error-but-order-exists", client_id=order_id, order_id=tracked.exchange_order_id,
                        state=str(tracked.current_state), error=repr(exception))
            return
        if isinstance(exception, PoloniexPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll reads it by "
                                  f"its client id and fails it only once Poloniex says it doesn't exist.")
            return
        if isinstance(exception, PoloniexBusinessError):
            meaning = CONSTANTS.REFUSAL_CODES.get(str(exception.code))
            self.logger().warning(f"Poloniex rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}"
                                  f"{' (' + meaning + ')' if meaning else ''}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return CONSTANTS.MSG_SIGNATURE_EXPIRED in str(request_exception).lower()

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        return isinstance(error, PoloniexBusinessError) and error.code == str(CONSTANTS.CODE_ORDER_NOT_FOUND)

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found(cancelation_exception)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (the 4th strike, never reset) and
        runs no lost-order recovery: a FAILED order is forgotten while it may still rest on Poloniex. Only Poloniex's
        own "Order not exists" (21301) counts; anything else is a WARNING and the order stays tracked (Hotcoin's
        lesson, 2026-10-06). A deferred read (PoloniexStatusDeferred) is no error at all."""
        if isinstance(error, PoloniexStatusDeferred):
            return
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        self.logger().warning(f"Poloniex status read for {order.client_order_id} failed, the order stays tracked: "
                              f"{error!r}")

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        return format(value, "f")    # never "1E-8" (CoinEx's scientific-notation bug)

    @staticmethod
    def _dec(value: Any, default: str = "0") -> Decimal:
        try:
            return Decimal(str(value)) if value is not None and value != "" else Decimal(default)
        except Exception:
            return Decimal(default)

    def _now(self) -> float:
        """The clock's time, or the wall clock before the first tick (current_timestamp is NaN until then)."""
        timestamp = self.current_timestamp
        return time.time() if timestamp is None or timestamp != timestamp else timestamp

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """
        The base reads self._trading_rules[trading_pair] before it tracks the order. With no rule it stops there with a
        bare KeyError (XT B2-USDT, 2026-09-23): nothing is sent, the order is never tracked, no failure event fires.
        Here the order is tracked and failed the way the base fails its own pre-send checks.
        """
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to Poloniex: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from GET {CONSTANTS.MARKETS_PATH}).")
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
        body = {
            "symbol": symbol,
            "side": CONSTANTS.TRADE_TYPES[trade_type],
            "type": CONSTANTS.ORDER_TYPE_LIMIT_MAKER if order_type is OrderType.LIMIT_MAKER else CONSTANTS.ORDER_TYPE_LIMIT,
            "timeInForce": CONSTANTS.TIME_IN_FORCE_GTC,
            "price": self._format_decimal(price),
            "quantity": self._format_decimal(amount),
            "clientOrderId": order_id,
        }
        self._placing.add(order_id)
        try:
            return await self._send_placement(order_id, body)
        finally:
            self._placing.discard(order_id)

    async def _send_placement(self, order_id: str, body: Dict[str, Any]) -> Tuple[str, float]:
        try:
            response = await self._call(RESTMethod.POST, CONSTANTS.ORDERS_PATH, data=body,
                                        limit_id=CONSTANTS.PLACE_ORDER_LIMIT_ID)
        except PoloniexBusinessError as refusal:
            # AUDIT: every refusal with Poloniex's complete answer.
            self._audit("place-order-refused", request=self._raw(body), status=refusal.status, response=refusal.body)
            raise
        except asyncio.CancelledError:
            raise
        except Exception as transport_error:
            # No answer (timeout, dropped connection, 5xx): Poloniex may still have accepted the order. The base would
            # mark it FAILED and stop tracking it, leaving a live order nobody watches. Ask Poloniex by client id first.
            exchange_order_id, verdict = await self._lookup_placement(order_id)
            self._alarm("place-order-unanswered", client_id=order_id, error=repr(transport_error),
                        found_on_exchange=exchange_order_id, verdict=verdict)
            if exchange_order_id is not None:
                return exchange_order_id, self._now()
            # Even "absent" (two 'Order not exists') is no proof: Poloniex may book an order it hadn't answered for
            # later. The order stays pending; the status poll's cid: reads fail it only after repeated not-founds
            # (the Bitunix rule: never a blind FAILED).
            raise PoloniexPlacementUnknown(f"Poloniex did not answer the placement ({transport_error!r}) and the "
                                           f"client-id lookup could not tell ({verdict})") from transport_error
        self._audit("place-order", request=self._raw(body), response=self._raw(response))
        exchange_order_id = response.get("id") if isinstance(response, dict) else None
        if exchange_order_id in (None, ""):
            # A 2xx without an id: the order may exist. Looked up the same way as an unanswered placement.
            found, verdict = await self._lookup_placement(order_id)
            self._alarm("place-order-no-id", client_id=order_id, response=self._raw(response), found_on_exchange=found,
                        verdict=verdict)
            if found is not None:
                return found, self._now()
            raise PoloniexPlacementUnknown(f"Poloniex answered the placement without an id ({self._raw(response)}); "
                                           f"lookup: {verdict}")
        echoed = response.get("clientOrderId")
        if echoed not in (None, "", order_id):
            self._alarm("client-id-not-kept", client_id=order_id, echoed=echoed, exchange_order_id=exchange_order_id)
        return str(exchange_order_id), self._now()

    async def _lookup_placement(self, client_order_id: str) -> Tuple[Optional[str], str]:
        """The placement guard: (exchange id, "found") if Poloniex has an order with our client id; (None, "absent")
        once two looks answered "Order not exists" (21301); (None, "inconclusive") if a look failed otherwise."""
        verdict = "absent"
        for delay in CONSTANTS.PLACEMENT_LOOKUP_DELAYS:
            await asyncio.sleep(delay)
            try:
                order = await self._call(RESTMethod.GET, CONSTANTS.ORDER_PATH.format(order_id=f"cid:{client_order_id}"),
                                         limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._is_not_found(e):
                    continue
                verdict = f"inconclusive: {e!r}"
                continue
            if isinstance(order, dict) and order.get("id") not in (None, ""):
                return str(order["id"]), "found"
        return None, verdict

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        """
        The base's sequence, except its OPEN update is applied only while the order is still PENDING_CREATE. Poloniex's
        pushes carry the client id and are applied at once, so a taker order's `trade` / FILLED push can complete the
        order before this answer arrives; this fork's update_with_order_update sets any state it is given, and an OPEN
        then would set the finished order back to open.
        """
        exchange_order_id, update_timestamp = await self._place_order(
            order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            amount=order.amount,
            trade_type=order.trade_type,
            order_type=order.order_type,
            price=order.price,
            **kwargs,
        )
        if order.exchange_order_id is not None and str(order.exchange_order_id) != str(exchange_order_id):
            # A push beat this answer and gave the order its id first; the base never overwrites an id.
            self._alarm("order-id-mismatch", client_id=order.client_order_id, push_id=order.exchange_order_id,
                        placement_id=str(exchange_order_id))
        if order.current_state is OrderState.PENDING_CREATE:
            await self._order_tracker._process_order_update(OrderUpdate(
                client_order_id=order.client_order_id,
                exchange_order_id=str(exchange_order_id),
                trading_pair=order.trading_pair,
                update_timestamp=update_timestamp,
                new_state=OrderState.OPEN,
            ))
        safe_ensure_future(self._expect_order_push(order.client_order_id, str(exchange_order_id)))
        return exchange_order_id

    async def _expect_order_push(self, client_order_id: str, exchange_order_id: str) -> None:
        """Poloniex pushes `place` for every accepted order. None within ORDER_PUSH_EXPECTED_WITHIN means the private
        stream isn't delivering: poll orders and balances over REST now, and say so (rate-limited [PLX-ALARM])."""
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
        """While any order is open, the status poll runs every SHORT_POLL_INTERVAL (10 s) whatever the private stream
        does: a lost push is settled within 10 s, not 60 (Hotcoin's checklist, 2026-10-06)."""
        if self.in_flight_orders:
            return self.SHORT_POLL_INTERVAL
        return super()._get_poll_interval(timestamp=timestamp)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        exchange_order_id = tracked_order.exchange_order_id
        target = str(exchange_order_id) if exchange_order_id else f"cid:{order_id}"
        response = await self._call(RESTMethod.DELETE, CONSTANTS.ORDER_PATH.format(order_id=target),
                                    limit_id=CONSTANTS.CANCEL_ORDER_LIMIT_ID)
        self._audit("cancel-order", client_id=order_id, target=target, response=self._raw(response))
        if isinstance(response, dict) and response.get("code") not in (None, 200, "200"):
            code = str(response.get("code"))
            raise PoloniexBusinessError(f"Poloniex refused to cancel {order_id} ({target}): code {code} "
                                        f"({response.get('message')}) | Poloniex response: {self._raw(response)}",
                                        code, 200, self._raw(response))
        return True

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        """The base's, except PENDING_CANCEL is applied only to an order still live: a fill or the `canceled` push
        may have finished it while the DELETE was in flight, and this fork's update_with_order_update would set a
        finished order back."""
        cancelled = await self._place_cancel(order.client_order_id, order)
        if cancelled and not order.is_done and order.client_order_id in self._order_tracker.all_updatable_orders:
            self._order_tracker.process_order_update(OrderUpdate(
                client_order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                update_timestamp=self._now(),
                new_state=OrderState.PENDING_CANCEL,
            ))
        return cancelled

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """
        The base's cancel path, except:
          - a refusal -- 21301 included (the order is gone or done) -- is a WARNING with Poloniex's answer, and the
            order's status is read at once: a refused cancel usually means it already filled or was cancelled;
          - a timeout counts toward failing the order only when it is the wait for an exchange id that never came;
            an HTTP timeout on the cancel itself reads the order's status instead (the cancel may have landed).
        """
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except PoloniexBusinessError as refusal:
            self.logger().warning(f"Poloniex refused to cancel {order.client_order_id}: {refusal}. Reading its status "
                                  f"now.")
            safe_ensure_future(self._refresh_order(order))
        except (asyncio.TimeoutError, IOError) as e:
            self.logger().warning(f"Poloniex did not answer the cancel of {order.client_order_id} ({e!r}). Reading its "
                                  f"status now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        try:
            # A terminal answer books the order's missing fills before it is returned (_request_order_status).
            self._order_tracker.process_order_update(await self._request_order_status(tracked_order=order))
        except asyncio.CancelledError:
            raise
        except PoloniexStatusDeferred:
            return
        except Exception as e:
            if self._is_not_found(e):
                await self._order_tracker.process_order_not_found(order.client_order_id)
            self.logger().warning(f"Poloniex status read for {order.client_order_id} after its cancel failed; the "
                                  f"next poll reads it again: {e!r}")

    def _order_state(self, raw_state: Any, executed: Decimal, original: Decimal) -> OrderState:
        state = CONSTANTS.ORDER_STATE.get(str(raw_state).upper()) if raw_state is not None else None
        if state is not None:
            return state
        # Not in the documented enum: derive from the amounts rather than guess a terminal state.
        self._audit_once(f"unknown-state:{raw_state}", state=raw_state, executed=str(executed), original=str(original))
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id is None and tracked_order.client_order_id in self._placing:
            # Its placement is still waiting for Poloniex's answer (or looking itself up): a `cid:` read now could
            # land before Poloniex has the order and count a 21301 strike against a live order. Left as it is.
            raise PoloniexStatusDeferred(f"{tracked_order.client_order_id}: its placement awaits Poloniex's answer")
        target = (str(tracked_order.exchange_order_id) if tracked_order.exchange_order_id is not None
                  else f"cid:{tracked_order.client_order_id}")
        order = await self._call(RESTMethod.GET, CONSTANTS.ORDER_PATH.format(order_id=target),
                                 limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID)
        if not isinstance(order, dict) or order.get("id") in (None, ""):
            raise IOError(f"Unexpected Poloniex answer for order {tracked_order.client_order_id}: {self._raw(order)}")
        raw_state = order.get("state")
        new_state = self._order_state(raw_state, self._dec(order.get("filledQuantity")), self._dec(order.get("quantity")))
        self._audit_once(f"order-status:{raw_state}", state=raw_state, mapped=str(new_state), keys=sorted(order.keys()),
                         cancel_reason=order.get("cancelReason"))
        if new_state in CONSTANTS.TERMINAL_STATES and new_state is not OrderState.FAILED:
            filled = self._dec(order.get("filledQuantity"))
            if tracked_order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # The poll reads fills before states, but a fill that landed between the two reads must not be
                # reported short: book it first.
                for trade_update in await self._all_trade_updates_for_order(tracked_order):
                    self._order_tracker.process_trade_update(trade_update)
            if tracked_order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # Poloniex's trade rows are behind its own state: closing the order now would report it short.
                if len(self._short_since) > 2000:
                    self._short_since.clear()
                held_since = self._short_since.setdefault(tracked_order.client_order_id, time.time())
                held_s = round(time.time() - held_since, 1)
                if held_s < CONSTANTS.TERMINAL_HOLD_MAX_S:
                    self._audit("terminal-held", client_id=tracked_order.client_order_id, state=raw_state,
                                reported=str(filled), booked=str(tracked_order.executed_amount_base), held_s=held_s)
                    raise PoloniexStatusDeferred(f"{tracked_order.client_order_id}: {raw_state} held for its fill rows")
                self._alarm("terminal-forwarded-short", client_id=tracked_order.client_order_id, state=raw_state,
                            reported=str(filled), booked=str(tracked_order.executed_amount_base), held_s=held_s)
            self._short_since.pop(tracked_order.client_order_id, None)
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order.get("id")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(order.get("updateTime") or self._now() * 1e3) * 1e-3,
            new_state=new_state,
        )

    # ------------------------------------------------------------------ fills

    def _trade_update(self, order: InFlightOrder, *, trade_id: str, price: Decimal, quantity: Decimal,
                      quote: Optional[Decimal], fee: Optional[Decimal], fee_token: str, role: str,
                      fill_ms: Any) -> TradeUpdate:
        if fee is not None and fee_token:
            flat_fee = TokenAmount(amount=fee, token=str(fee_token).upper())
        else:
            # No fee on the row (never seen): the account's rate on the received asset, audit-logged.
            schema = self._trading_fees.get(order.trading_pair) or utils.DEFAULT_FEES
            rate = schema.taker_percent_fee_decimal if role != "MAKER" else schema.maker_percent_fee_decimal
            received = quantity if order.trade_type is TradeType.BUY else (quote or price * quantity)
            flat_fee = TokenAmount(amount=received * rate, token=self._received_asset(order))
            self._audit_once("fee-estimated-from-rate", rate=str(rate), token=flat_fee.token)
        return TradeUpdate(
            trade_id=str(trade_id),
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=TradeFeeBase.new_spot_fee(fee_schema=self.trade_fee_schema(), trade_type=order.trade_type,
                                          flat_fees=[flat_fee]),
            fill_base_amount=quantity,
            fill_quote_amount=quote if quote is not None and quote > 0 else price * quantity,
            fill_price=price,
            fill_timestamp=int(fill_ms or self._now() * 1e3) * 1e-3,
            is_taker=role != "MAKER",
        )

    def _trade_update_from_rest(self, row: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        """One row of GET /orders/{id}/trades: id, price, quantity, amount, feeAmount, feeCurrency, matchRole, createTime."""
        return self._trade_update(
            order, trade_id=row["id"], price=self._dec(row.get("price")), quantity=self._dec(row.get("quantity")),
            quote=self._dec(row.get("amount"), default="0") or None,
            fee=self._dec(row.get("feeAmount")) if row.get("feeAmount") not in (None, "") else None,
            fee_token=str(row.get("feeCurrency") or ""), role=str(row.get("matchRole") or "").upper(),
            fill_ms=row.get("createTime"))

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """REST backstop for fills (GET /orders/{id}/trades: an order's fills, exchange id only)."""
        if order.exchange_order_id is None:
            return []
        rows = await self._call(RESTMethod.GET, CONSTANTS.ORDER_TRADES_PATH.format(order_id=order.exchange_order_id),
                                limit_id=CONSTANTS.ORDER_TRADES_LIMIT_ID)
        if not isinstance(rows, list):
            raise IOError(f"Unexpected Poloniex fills answer for {order.client_order_id}: {self._raw(rows)}")
        updates: Dict[str, TradeUpdate] = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("id") in (None, ""):
                continue
            if str(row.get("orderId", order.exchange_order_id)) != str(order.exchange_order_id):
                continue
            if rows:
                self._audit_once("rest-trade-key-set", keys=sorted(row.keys()), fee=row.get("feeAmount"),
                                 fee_ccy=row.get("feeCurrency"), role=row.get("matchRole"))
            updates[str(row["id"])] = self._trade_update_from_rest(row, order)
        return self._fills_not_yet_counted(order, list(updates.values()))

    def _fills_not_yet_counted(self, order: InFlightOrder, rest_fills: List[TradeUpdate]) -> List[TradeUpdate]:
        """
        The REST rows for one order are Poloniex's complete record of its fills, so their total is the true executed
        quantity. Hummingbot de-duplicates fills by trade id only and never caps an order's executed amount. So a REST
        row with an unseen id is applied only while the order's executed amount is still below Poloniex's total: exact
        when the push and REST ids agree, a cap when they do not (XT's guard; whether they agree is audit-logged).
        """
        known = order.order_fills
        if known and rest_fills:
            self._audit_once("fill-ids-match", ws_ids=sorted(known)[:3], rest_ids=sorted(f.trade_id for f in rest_fills)[:3],
                             match=any(f.trade_id in known for f in rest_fills))
        # Pushed fills not yet matched to a REST row: a REST row with an unseen id that equals one of them (quantity,
        # price, within FILL_MATCH_SECONDS) is that fill under REST's id — matched one-to-one BEFORE the cap, so the
        # cap never spends its room on a duplicate (and leaves a real fill out).
        matched = set(self._rest_aliases.values())
        pushed = [f for tid, f in known.items() if tid not in self._rest_fill_ids and tid not in matched]
        room = sum((f.fill_base_amount for f in rest_fills), Decimal("0")) - order.executed_amount_base
        counted: List[TradeUpdate] = []
        for fill in sorted(rest_fills, key=lambda f: f.fill_timestamp):
            if fill.trade_id in known or fill.trade_id in self._rest_aliases:
                continue
            twin = next((p for p in pushed if p.fill_base_amount == fill.fill_base_amount
                         and p.fill_price == fill.fill_price
                         and abs(p.fill_timestamp - fill.fill_timestamp) <= CONSTANTS.FILL_MATCH_SECONDS), None)
            if twin is not None:
                pushed.remove(twin)
                self._rest_aliases[fill.trade_id] = twin.trade_id
                self._audit_once("fill-matched-across-ids", client_id=order.client_order_id, push_id=twin.trade_id,
                                 rest_id=fill.trade_id)
                continue
            if fill.fill_base_amount > room + CONSTANTS.FILL_AMOUNT_TOLERANCE:
                self._alarm("fill-capped", client_id=order.client_order_id, trade_id=fill.trade_id,
                            amount=str(fill.fill_base_amount), room=str(room))
                continue
            counted.append(fill)
            room -= fill.fill_base_amount
        now = time.time()
        for fill in counted:
            self._rest_fill_ids[fill.trade_id] = now
        if len(self._rest_fill_ids) > 5000:
            for trade_id in sorted(self._rest_fill_ids, key=self._rest_fill_ids.get)[:2500]:
                del self._rest_fill_ids[trade_id]
        if len(self._rest_aliases) > 5000:
            for trade_id in list(self._rest_aliases)[:2500]:
                del self._rest_aliases[trade_id]
        return counted

    def _received_asset(self, order: InFlightOrder) -> str:
        return order.base_asset if order.trade_type is TradeType.BUY else order.quote_asset

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
        fee_schema = self._trading_fees.get(trading_pair)
        if fee_schema is not None:
            return TradeFeeBase.new_spot_fee(
                fee_schema=fee_schema,
                trade_type=order_side,
                percent=fee_schema.maker_percent_fee_decimal if is_maker else fee_schema.taker_percent_fee_decimal,
            )
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self) -> None:
        """The account's rates (GET /feeinfo: makerRate / takerRate) for every market, with specialFeeRates per symbol
        on top (WSTUSDT_USDT 0 / 0 on 2026-10-09). The first row is audit-logged: LBank's fees were fractions read as
        percents (2026-10-07), so the unit is checked against the live answer."""
        info = await self._call(RESTMethod.GET, CONSTANTS.FEE_INFO_PATH, limit_id=CONSTANTS.FEE_INFO_PATH)
        if not isinstance(info, dict) or info.get("makerRate") in (None, ""):
            self.logger().warning(f"Unexpected Poloniex fee answer: {self._raw(info)}")
            return
        self._audit_once("fee-rate-row", maker=info.get("makerRate"), taker=info.get("takerRate"),
                         trx_discount=info.get("trxDiscount"), special=len(info.get("specialFeeRates") or []))
        default = TradeFeeSchema(maker_percent_fee_decimal=self._dec(info.get("makerRate")),
                                 taker_percent_fee_decimal=self._dec(info.get("takerRate")))
        special = {str(r.get("symbol")): r for r in info.get("specialFeeRates") or [] if isinstance(r, dict)}
        mapping = await self.trading_pair_symbol_map()
        for symbol, trading_pair in mapping.items():
            row = special.get(symbol)
            self._trading_fees[trading_pair] = default if row is None else TradeFeeSchema(
                maker_percent_fee_decimal=self._dec(row.get("makerRate")),
                taker_percent_fee_decimal=self._dec(row.get("takerRate")))

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        """GET /accounts/balances. An asset whose balance push arrived after this request was sent keeps the push:
        the snapshot is older (runbook §1.5; Hotcoin's rule). Its deletion is skipped the same way."""
        requested_at = time.monotonic()
        response = await self._call(RESTMethod.GET, CONSTANTS.BALANCES_PATH, params={"accountType": "SPOT"},
                                    limit_id=CONSTANTS.BALANCES_PATH)
        accounts = [a for a in response if isinstance(a, dict)] if isinstance(response, list) else []
        spot = [a for a in accounts if str(a.get("accountType") or "").upper() == "SPOT"]
        if len(spot) != 1 or not isinstance(spot[0].get("balances"), list):
            raise IOError(f"Unexpected Poloniex balances answer: {self._raw(response)}")
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for row in spot[0]["balances"]:
            asset = str(row.get("currency") or "").upper()
            if not asset:
                continue
            remote_asset_names.add(asset)
            if self._balance_pushed_at.get(asset, 0.0) > requested_at:
                continue
            available, hold = self._dec(row.get("available")), self._dec(row.get("hold"))
            self._account_available_balances[asset] = available
            self._account_balances[asset] = available + hold
        # Poloniex lists held currencies only (the empty account answered balances: []): one it drops holds nothing.
        for asset_name in local_asset_names.difference(remote_asset_names):
            if self._balance_pushed_at.get(asset_name, 0.0) > requested_at:
                continue
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any) -> None:
        if not isinstance(exchange_info, list) or not exchange_info:
            # A 2xx that isn't the market list: keep the map we have (this fork's rules refresh sets the map first).
            self.logger().warning(f"Poloniex GET {CONSTANTS.MARKETS_PATH} answered no market list; the symbol map is "
                                  f"kept: {self._raw(exchange_info)[:200]}")
            return
        mapping = bidict()
        for info in exchange_info if isinstance(exchange_info, list) else []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                mapping[info["symbol"]] = combine_to_hb_trading_pair(
                    base=str(info["baseCurrencyName"]).upper(), quote=str(info["quoteCurrencyName"]).upper())
            except Exception as exception:
                self.logger().error(f"Error parsing Poloniex symbol {info.get('symbol')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair that is not in the startup map. The base builds f"{base}{quote}" ("BTCUSDT"), which
        Poloniex does not recognise; its form is BASE_QUOTE (every one of the 863 markets, 2026-10-09)."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.upper()}_{quote.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(f"Poloniex {trading_pair} was not among the markets GET {CONSTANTS.MARKETS_PATH} listed "
                              f"at startup; mapped to {exchange_symbol}. Poloniex's answers will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        """symbolTradeLimit: priceScale / quantityScale / amountScale (decimals), minQuantity, minAmount (the minimum
        notional, 1 USDT on 831 of 832 USDT markets), maxQuantity (0 = none). A 2xx that isn't the market list
        raises, so the base keeps the rules it has (it clears them only after this returns)."""
        if not isinstance(exchange_info_dict, list) or not exchange_info_dict:
            raise IOError(f"Poloniex GET {CONSTANTS.MARKETS_PATH} answered no market list: "
                          f"{self._raw(exchange_info_dict)[:200]}")
        rules: List[TradingRule] = []
        for info in exchange_info_dict if isinstance(exchange_info_dict, list) else []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=info["symbol"])
                limit = info.get("symbolTradeLimit") or {}
                price_increment = Decimal(1).scaleb(-int(limit["priceScale"]))
                amount_increment = Decimal(1).scaleb(-int(limit["quantityScale"]))
                kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(limit.get("minQuantity")), amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_quote_amount_increment=Decimal(1).scaleb(-int(limit["amountScale"])),
                    min_notional_size=self._dec(limit.get("minAmount")),
                )
                if self._dec(limit.get("maxQuantity")) > 0:
                    kwargs["max_order_size"] = self._dec(limit.get("maxQuantity"))
                rules.append(TradingRule(**kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the Poloniex trading rule {info.get('symbol')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        One GET /markets/price for every market, not one call per pair: the order book tracker re-reads the REST price
        of every quiet book every 5 s (XT's 1,842 throttler warnings in 28 h). Every requested pair gets a value, NaN
        when Poloniex has none (the tracker re-asks a pair it got nothing for at once).
        """
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        symbol_to_pair: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                symbol_to_pair[await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)] = trading_pair
            except KeyError:
                continue
        rows = await self._call(RESTMethod.GET, CONSTANTS.PRICES_PATH, signed=False, limit_id=CONSTANTS.PRICES_PATH)
        for row in rows if isinstance(rows, list) else []:
            trading_pair = symbol_to_pair.get((row or {}).get("symbol"))
            price = self._dec((row or {}).get("price"))
            if trading_pair is not None and price > 0:
                prices[trading_pair] = float(price)
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                channel = event_message.get("channel")
                for item in event_message.get("data") or []:
                    if not isinstance(item, dict):
                        continue
                    if channel == CONSTANTS.WS_ORDERS_CHANNEL:
                        self._process_order_event(item)
                    elif channel == CONSTANTS.WS_BALANCES_CHANNEL:
                        self._process_balance_event(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the Poloniex user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        order = tracker.all_fillable_orders.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id:
            order = tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        return order

    def _push_age(self, data: Dict[str, Any]) -> Optional[float]:
        try:
            return round(self._time_synchronizer.time() - int(data["ts"]) * 1e-3, 3)
        except (KeyError, TypeError, ValueError):
            return None

    def _process_order_event(self, data: Dict[str, Any]) -> None:
        """
        The `orders` push (Spot WebSocket API/Orders): orderId, clientOrderId, eventType place / trade / canceled, state,
        cumulative filledQuantity / filledAmount, and on a trade the fill itself: tradeId, tradeQty, tradePrice,
        tradeAmount, tradeFee, feeCurrency, matchRole, tradeTime. The client id rides on every event, so a fill that
        beats the placement answer is matched by it (no parking by exchange id, unlike XT).
        """
        self._audit_once("order-push-key-set", keys=sorted(data.keys()))
        client_order_id = str(data.get("clientOrderId") or "")
        exchange_order_id = str(data["orderId"]) if data.get("orderId") not in (None, "") else None
        for seen in (exchange_order_id, client_order_id):
            if seen:
                self._pushed_order_ids[seen] = None
        if len(self._pushed_order_ids) > 4000:
            for seen in list(self._pushed_order_ids)[:2000]:
                del self._pushed_order_ids[seen]
        event = str(data.get("eventType") or "").lower()
        state = str(data.get("state") or "").upper()
        order = self._locate_order(client_order_id, exchange_order_id)
        if order is None:
            self._audit("order-push-unmatched", client_id=client_order_id or None, order_id=exchange_order_id,
                        event=event, state=state, source=data.get("source"), age_s=self._push_age(data))
            if (client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) and state in ("NEW", "PARTIALLY_FILLED")
                    and exchange_order_id is not None and exchange_order_id not in self._orphan_ids):
                # One of ours, open on Poloniex, that nothing tracks: its fills would never be booked. Said, not
                # cancelled -- it may be another process's.
                self._orphan_ids[exchange_order_id] = None
                self._alarm("untracked-order-open", client_id=client_order_id, order_id=exchange_order_id, state=state)
            return
        self._audit("order-push", client_id=order.client_order_id, order_id=exchange_order_id, event=event, state=state,
                    filled=data.get("filledQuantity"), trade_id=data.get("tradeId"), trade_qty=data.get("tradeQty"),
                    fee=data.get("tradeFee"), fee_ccy=data.get("feeCurrency"), age_s=self._push_age(data))
        if order.exchange_order_id is None and exchange_order_id is not None:
            # The push beat the placement answer: give the order its id before a fill is named after it.
            order.update_exchange_order_id(exchange_order_id)
        if event == "trade" and str(data.get("tradeId") or "0") != "0":
            self._apply_push_fill(data, order)
        if order.is_failure and state in ("NEW", "PARTIALLY_FILLED") and exchange_order_id is not None:
            self._cancel_failed_but_open(order, exchange_order_id, state)
            return
        if order.client_order_id not in self._order_tracker.all_updatable_orders:
            return  # already final: a late push may still carry a fill (above), never a state
        filled = self._dec(data.get("filledQuantity"))
        new_state = self._order_state(state, filled, self._dec(data.get("quantity"), default=str(order.amount)))
        update = OrderUpdate(
            trading_pair=order.trading_pair,
            update_timestamp=int(data.get("ts") or self._now() * 1e3) * 1e-3,
            new_state=new_state,
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id or order.exchange_order_id,
        )
        if (new_state in CONSTANTS.TERMINAL_STATES
                and order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled):
            # A terminal state whose fills are not all booked yet (a lost or refused trade push): never forwarded short.
            if order.client_order_id not in self._settling:
                self._settling.add(order.client_order_id)
                safe_ensure_future(self._settle_after_backfill(order, update, filled))
            return
        self._order_tracker.process_order_update(update)

    async def _settle_after_backfill(self, order: InFlightOrder, update: OrderUpdate, filled: Decimal) -> None:
        try:
            await asyncio.sleep(CONSTANTS.FILL_BACKFILL_GRACE)   # the trade push gets its chance first
            if order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                self._alarm("fill-backfill", client_id=order.client_order_id, reported=str(filled),
                            held=str(order.executed_amount_base))
                try:
                    for trade_update in await self._all_trade_updates_for_order(order):
                        self._order_tracker.process_trade_update(trade_update)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().warning(f"Poloniex fill backfill for {order.client_order_id} failed: {e!r}")
            if order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # Still short: the status poll (which books fills first) settles it, never a short terminal update here.
                self._poll_notifier.set()
                return
            if order.client_order_id not in self._order_tracker.all_updatable_orders:
                return   # finished meanwhile (the poll booked the fill and closed it): nothing to forward
            self._order_tracker.process_order_update(update)
        finally:
            self._settling.discard(order.client_order_id)

    def _apply_push_fill(self, data: Dict[str, Any], order: InFlightOrder) -> None:
        """
        One pushed fill onto its order, with the two checks Hummingbot does not make:
          - an order is never filled past its size: a push that would do that is a duplicate
          - a fill already recorded from REST (the backfill ran first) is not counted again when its push arrives, even
            under another id: same price, same quantity, within FILL_MATCH_SECONDS
        """
        trade_update = self._trade_update(
            order, trade_id=str(data.get("tradeId")), price=self._dec(data.get("tradePrice")),
            quantity=self._dec(data.get("tradeQty")), quote=self._dec(data.get("tradeAmount"), default="0") or None,
            fee=self._dec(data.get("tradeFee")) if data.get("tradeFee") not in (None, "") else None,
            fee_token=str(data.get("feeCurrency") or ""), role=str(data.get("matchRole") or "").upper(),
            fill_ms=data.get("tradeTime"))
        if trade_update.trade_id in order.order_fills:
            return
        if trade_update.fill_base_amount <= 0:
            self._alarm("fill-empty", client_id=order.client_order_id, trade_id=trade_update.trade_id)
            return
        if order.executed_amount_base + trade_update.fill_base_amount > order.amount + CONSTANTS.FILL_AMOUNT_TOLERANCE:
            self._alarm("fill-over-amount", client_id=order.client_order_id, trade_id=trade_update.trade_id,
                        executed=str(order.executed_amount_base), fill=str(trade_update.fill_base_amount),
                        amount=str(order.amount))
            return
        for known_id, known in order.order_fills.items():
            if (known_id in self._rest_fill_ids
                    and known.fill_base_amount == trade_update.fill_base_amount
                    and known.fill_price == trade_update.fill_price
                    and abs(known.fill_timestamp - trade_update.fill_timestamp) <= CONSTANTS.FILL_MATCH_SECONDS):
                self._alarm("fill-duplicate-of-rest", client_id=order.client_order_id, ws_id=trade_update.trade_id,
                            rest_id=known_id)
                return
        self._order_tracker.process_trade_update(trade_update)

    def _cancel_failed_but_open(self, order: InFlightOrder, exchange_order_id: str, state: str) -> None:
        """HMB gave the order up (FAILED), yet Poloniex reports it open: nothing would track it once its time in the
        tracker's cache is over, so it is cancelled now, with a [PLX-ALARM]. A fill it already had was booked above."""
        if exchange_order_id in self._orphan_ids:
            return
        self._orphan_ids[exchange_order_id] = None
        if len(self._orphan_ids) > 2000:
            for seen in list(self._orphan_ids)[:1000]:
                del self._orphan_ids[seen]
        self._alarm("failed-order-alive", client_id=order.client_order_id, order_id=exchange_order_id, state=state,
                    action="cancelling")
        safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id))

    async def _cancel_by_exchange_id(self, exchange_order_id: str) -> None:
        try:
            response = await self._call(RESTMethod.DELETE, CONSTANTS.ORDER_PATH.format(order_id=exchange_order_id),
                                        limit_id=CONSTANTS.CANCEL_ORDER_LIMIT_ID)
            self._audit("cancel-untracked", order_id=exchange_order_id, response=self._raw(response))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Poloniex cancel of the untracked order {exchange_order_id} failed: {e!r}")

    def _process_balance_event(self, data: Dict[str, Any]) -> None:
        """The `balances` push: available + hold after each change, eventType, and a per-currency `version` that orders
        them (an older one is dropped)."""
        self._audit_once("balance-push-key-set", keys=sorted(data.keys()))
        if str(data.get("accountType") or "SPOT").upper() != "SPOT":
            return
        asset = str(data.get("currency") or "").upper()
        if not asset:
            return
        try:
            version = int(data.get("version") or 0)
        except (TypeError, ValueError):
            version = 0
        if version and version < self._balance_versions.get(asset, 0):
            self._audit("balance-push-out-of-order", asset=asset, version=version, last=self._balance_versions[asset])
            return
        if version:
            self._balance_versions[asset] = version
        available, hold = self._dec(data.get("available")), self._dec(data.get("hold"))
        self._account_available_balances[asset] = available
        self._account_balances[asset] = available + hold
        self._balance_pushed_at[asset] = time.monotonic()
        if CONSTANTS.LIVE_AUDIT_LOGGING and self._balance_pushes_audited < CONSTANTS.BALANCE_AUDIT_PUSHES:
            self._balance_pushes_audited += 1
            safe_ensure_future(self._audit_balance_push(asset, data))

    async def _audit_balance_push(self, asset: str, push: Dict[str, Any]) -> None:
        """The first few balance pushes next to Poloniex's REST balance (balance-push-vs-rest): whether `available` +
        `hold` are the totals after the change, as the docs say."""
        try:
            response = await self._call(RESTMethod.GET, CONSTANTS.BALANCES_PATH, params={"accountType": "SPOT"},
                                        limit_id=CONSTANTS.BALANCES_PATH)
            rows = (response[0].get("balances") if isinstance(response, list) and response else []) or []
            rest = next((r for r in rows if str(r.get("currency") or "").upper() == asset), "absent")
        except Exception as e:
            rest = f"REST error {e!r}"
        self._audit("balance-push-vs-rest", asset=asset, event=push.get("eventType"), push_available=push.get("available"),
                    push_hold=push.get("hold"), version=push.get("version"), rest=rest)
