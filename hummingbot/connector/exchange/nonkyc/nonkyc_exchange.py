import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.nonkyc import (
    nonkyc_constants as CONSTANTS,
    nonkyc_utils as utils,
    nonkyc_web_utils as web_utils,
)
from hummingbot.connector.exchange.nonkyc.nonkyc_api_order_book_data_source import (
    NonkycAPIOrderBookDataSource,
    _iso_to_epoch,
)
from hummingbot.connector.exchange.nonkyc.nonkyc_api_user_stream_data_source import NonkycAPIUserStreamDataSource
from hummingbot.connector.exchange.nonkyc.nonkyc_auth import NonkycAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class NonkycBusinessError(IOError):
    """A request NonKYC refused (HTTP 4xx with {"error": {"code", ...}}): a definite answer, never a transport
    failure. IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[int], status: int, body: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.body = body


class NonkycStatusDeferred(Exception):
    """Not a status: the poll leaves this order alone for now (its placement awaits its answer, or its terminal state
    waits for the venue's fill rows). Raised instead of returning a snapshot of the current state, which the base would
    queue and could apply after a newer state."""


class NonkycPlacementUnknown(IOError):
    """A placement got no answer and the client-id lookup could not tell whether NonKYC has the order. The order is
    NOT failed (it may rest on NonKYC): it stays PENDING_CREATE, and the status poll reads it by its client id until
    NonKYC answers either way."""


class NonkycExchange(ExchangePyBase):
    """
    NonKYC spot connector.

    Reference: the OpenAPI 1.3.0 + WS API 1.1.0 mirror (VS_code_projects/MDs/nonkyc-api/), NonKYC's own Hummingbot
    fork (same mirror, hb_nonkyc/), the live probes of 2026-11-17 from myserver
    (VS_code_projects/p2p3_nonkyc_onboarding_2026-11-17/probes/), and P1's tracker (Tracker_cex_cex, live since
    2026-11-15). Wiki: trading/exchanges/nonkyc-api. Every venue-specific choice below cites its source; open
    questions are audit-logged ([NKYC-AUDIT]) rather than guessed, and money-guard firings always log as
    [NKYC-ALARM].

    Templates (runbook §0.4, chosen per component by API shape):
      - the order path and its guards: `poloniex` (pushes carry the client id, a fill can beat the placement answer,
        the status route takes the client id, not-found is a code);
      - the local book: `xt` (snapshot + sequenced deltas kept locally, top-N snapshots only to the tracker, a
        reorder buffer, a per-market repair);
      - the runtime-add / reconnect bookkeeping: `bexc` and `xt`.

    Scope: LIMIT and LIMIT_MAKER orders, which is what arb_l and the position balancer send.
    """

    web_utils = web_utils

    def __init__(
        self,
        nonkyc_api_key: str,
        nonkyc_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature matches ConnectorSetting.conn_init_parameters in this fork, which always passes
        `balance_asset_limit` (and `rate_limits_share_pct`) and no config map (CoinEx, 2026-09-15)."""
        self._api_key = nonkyc_api_key
        self._secret_key = nonkyc_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # trade ids first recorded from REST, so a later push of the same fill under another id is recognised
        self._rest_fill_ids: Dict[str, float] = {}
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
        # REST trade id -> the pushed fill (another id) it was matched to: one fill, counted once
        self._rest_aliases: Dict[str, str] = {}
        # symbol -> the venue's own trading switch, from the last market list (refreshed with the trading rules)
        self._market_flags: Dict[str, bool] = {}
        self._balance_pushes_audited = 0
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> NonkycAuth:
        return NonkycAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        # The cancel's answer is {"success": true, "id": ...}: no state and no filled amount. The `cancelled` report
        # settles it (the venue's own WS docs: "the authoritative confirmation is the report frame").
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def is_market_paused(self, symbol: str) -> bool:
        """The venue's own trading switch for a market, from the cached market list. Unknown (before the first rules
        refresh) counts as trading: a connector never drops a market (non-negotiable 3)."""
        return self._market_flags.get(symbol) is False

    # ------------------------------------------------------------------ factories

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return NonkycAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return NonkycAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [NKYC-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[NKYC-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [NKYC-ALARM] WARNING, logged whatever LIVE_AUDIT_LOGGING says."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[NKYC-ALARM] {tag} {rendered}")

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
        One request, with NonKYC's error model applied:
          2xx -> the parsed body
          4xx -> NonkycBusinessError (a refusal: code, status, the complete body). The venue answers 400 with
                 {"error": {"code", "message", "description", "errorType"}}; a 401 is the plain text "Not Authorized"
                 (a bad signature or a clock skew) and gets one clock re-sync and one retry first.
          5xx / an unparseable 2xx / transport -> IOError or the transport's own exception (an UNKNOWN outcome)
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.private_rest_url(path_url) if signed else web_utils.public_rest_url(path_url)
        for attempt in range(2):
            response = await rest_assistant.execute_request_and_get_response(
                url=url, throttler_limit_id=limit_id or path_url, params=params, data=data, method=method,
                is_auth_required=signed, return_err=True)
            status = response.status
            text = await response.text()
            if status == 401 and attempt == 0:
                # A 401 is a bad signature or a clock outside the venue's window (no code distinguishes them): one
                # re-sync, one retry. Safe for a placement: a refused signature creates nothing.
                await self._update_time_synchronizer()
                continue
            if 200 <= status < 300:
                try:
                    return json.loads(text)
                except (TypeError, ValueError):
                    raise IOError(f"NonKYC {method.value} {path_url}: an unparseable {status} answer: {text[:300]}")
            parsed = web_utils.parse_error_body(text)
            if 400 <= status < 500 and status != 408:
                code = web_utils.error_code(parsed)
                error = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
                raise NonkycBusinessError(
                    f"NonKYC refused {method.value} {path_url}: HTTP {status} code {code} "
                    f"({error.get('message')}: {error.get('description')})"
                    f" | NonKYC response: {text[:500]}", code, status, text)
            raise IOError(f"NonKYC {method.value} {path_url}: HTTP {status} (outcome unknown): {text[:300]}")
        raise IOError(f"NonKYC {method.value} {path_url}: the signature was refused twice")

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        if tracked is not None and (tracked.exchange_order_id is not None
                                    or tracked.current_state is not OrderState.PENDING_CREATE):
            # A push already gave the order its id or moved it: NonKYC has it. Never failed on top of that.
            self._alarm("placement-error-but-order-exists", client_id=order_id, order_id=tracked.exchange_order_id,
                        state=str(tracked.current_state), error=repr(exception))
            return
        if isinstance(exception, NonkycPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll reads it by "
                                  f"its client id and fails it only once NonKYC says it doesn't exist.")
            return
        if isinstance(exception, NonkycBusinessError):
            error = web_utils.parse_error_body(exception.body).get("error") or {}
            meaning = error.get("description") or error.get("message")
            self.logger().warning(f"NonKYC rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}"
                                  f"{' (' + str(meaning) + ')' if meaning else ''}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return isinstance(request_exception, NonkycBusinessError) and request_exception.status == CONSTANTS.CODE_AUTH

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        return isinstance(error, NonkycBusinessError) and error.code == CONSTANTS.CODE_ORDER_NOT_FOUND

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found(cancelation_exception)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (the 4th strike, never reset) and
        runs no lost-order recovery: a FAILED order is forgotten while it may still rest on NonKYC. Only NonKYC's own
        20002 counts; anything else is a WARNING and the order stays tracked (Hotcoin's lesson, 2026-10-06). A
        deferred read (NonkycStatusDeferred) is no error at all."""
        if isinstance(error, NonkycStatusDeferred):
            return
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        self.logger().warning(f"NonKYC status read for {order.client_order_id} failed, the order stays tracked: "
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
        The base reads self._trading_rules[trading_pair] before it tracks the order. With no rule it stops there with
        a bare KeyError (XT B2-USDT, 2026-09-23): nothing is sent, the order is never tracked, no failure event fires.
        Here the order is tracked and failed the way the base fails its own pre-send checks.
        """
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to NonKYC: the connector has no trading rule for {trading_pair} "
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
        # The documented body (OpenAPI /createorder). `type` is documented lowercase; the venue's own fork sends
        # "LIMIT" and both pass validation (probed 2026-11-17: either spelling reaches the funds check).
        body: Dict[str, Any] = {
            "symbol": symbol,
            "side": CONSTANTS.SIDE[trade_type],
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
            "quantity": self._format_decimal(amount),
            "userProvidedId": order_id,
        }
        if order_type is not OrderType.MARKET and price is not None and price == price:
            body["price"] = self._format_decimal(price)
        self._placing.add(order_id)
        try:
            return await self._send_placement(order_id, body)
        finally:
            self._placing.discard(order_id)

    async def _send_placement(self, order_id: str, body: Dict[str, Any]) -> Tuple[str, float]:
        try:
            response = await self._call(RESTMethod.POST, CONSTANTS.PLACE_ORDER_PATH, data=body,
                                        limit_id=CONSTANTS.PLACE_ORDER_PATH)
        except NonkycBusinessError as refusal:
            # AUDIT: every refusal with NonKYC's complete answer.
            self._audit("place-order-refused", request=self._raw(body), status=refusal.status, response=refusal.body)
            raise
        except asyncio.CancelledError:
            raise
        except Exception as transport_error:
            # No answer (timeout, dropped connection, 5xx): NonKYC may still have accepted the order. The base would
            # mark it FAILED and stop tracking it, leaving a live order nobody watches. Ask by client id first.
            exchange_order_id, verdict = await self._lookup_placement(order_id)
            self._alarm("place-order-unanswered", client_id=order_id, error=repr(transport_error),
                        found_on_exchange=exchange_order_id, verdict=verdict)
            if exchange_order_id is not None:
                return exchange_order_id, self._now()
            raise NonkycPlacementUnknown(f"NonKYC did not answer the placement ({transport_error!r}) and the "
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
            raise NonkycPlacementUnknown(f"NonKYC answered the placement without an id ({self._raw(response)}); "
                                         f"lookup: {verdict}")
        echoed = response.get("userProvidedId")
        if echoed not in (None, "", order_id):
            self._alarm("client-id-not-kept", client_id=order_id, echoed=echoed, exchange_order_id=exchange_order_id)
        created_at = response.get("createdAt")
        update_timestamp = (int(created_at) * 1e-3 if isinstance(created_at, (int, float)) and created_at > 1e11
                            else self._now())
        return str(exchange_order_id), update_timestamp

    async def _lookup_placement(self, client_order_id: str) -> Tuple[Optional[str], str]:
        """The placement guard: (exchange id, "found") if NonKYC has an order with our client id; (None, "absent")
        once two looks answered 20002; (None, "inconclusive") if a look failed otherwise."""
        verdict = "absent"
        for delay in CONSTANTS.PLACEMENT_LOOKUP_DELAYS:
            await asyncio.sleep(delay)
            try:
                order = await self._call(RESTMethod.GET, CONSTANTS.ORDER_PATH.format(order_id=client_order_id),
                                         limit_id=CONSTANTS.ORDER_PATH)
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
        The base's sequence, except its OPEN update is applied only while the order is still PENDING_CREATE. NonKYC's
        reports carry the client id and are applied at once, so a taker order's trade / Filled report can complete the
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
        """NonKYC reports every accepted order. None within ORDER_PUSH_EXPECTED_WITHIN means the private stream isn't
        delivering: poll orders and balances over REST now, and say so (rate-limited [NKYC-ALARM])."""
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
        if exchange_order_id is None:
            # The REST cancel names the EXCHANGE id only (OpenAPI /cancelorder: {"id"}). Without one yet, find it by
            # the client id first -- never cancel by a client id the venue's REST route does not accept.
            found, verdict = await self._lookup_placement(order_id)
            if found is None:
                self._audit("cancel-without-exchange-id", client_id=order_id, verdict=verdict)
                return False
            tracked_order.update_exchange_order_id(found)
            exchange_order_id = found
        response = await self._call(RESTMethod.POST, CONSTANTS.CANCEL_ORDER_PATH,
                                    data={"id": str(exchange_order_id)},
                                    limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, order_id=str(exchange_order_id),
                    response=self._raw(response))
        return bool(response.get("success")) if isinstance(response, dict) else False

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        """The base's, except PENDING_CANCEL is applied only to an order still live: a fill or the `cancelled` report
        may have finished it while the cancel was in flight."""
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
          - a refusal -- 20002 included (the order is gone or done) -- is a WARNING with NonKYC's answer, and the
            order's status is read at once: a refused cancel usually means it already filled or was cancelled;
          - a timeout on the cancel itself reads the order's status instead (the cancel may have landed).
        """
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except NonkycBusinessError as refusal:
            self.logger().warning(f"NonKYC refused to cancel {order.client_order_id}: {refusal}. Reading its status "
                                  f"now.")
            safe_ensure_future(self._refresh_order(order))
        except (asyncio.TimeoutError, IOError) as e:
            self.logger().warning(f"NonKYC did not answer the cancel of {order.client_order_id} ({e!r}). Reading its "
                                  f"status now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        try:
            self._order_tracker.process_order_update(await self._request_order_status(tracked_order=order))
        except asyncio.CancelledError:
            raise
        except NonkycStatusDeferred:
            return
        except Exception as e:
            if self._is_not_found(e):
                await self._order_tracker.process_order_not_found(order.client_order_id)
            self.logger().warning(f"NonKYC status read for {order.client_order_id} after its cancel failed; the "
                                  f"next poll reads it again: {e!r}")

    def _order_state(self, raw_state: Any, executed: Decimal, original: Decimal) -> OrderState:
        state = CONSTANTS.ORDER_STATE.get(str(raw_state).upper().strip()) if raw_state is not None else None
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
            # Its placement is still waiting for NonKYC's answer (or looking itself up): a read now could land
            # before the venue has the order and count a 20002 strike against a live order. Left as it is.
            raise NonkycStatusDeferred(f"{tracked_order.client_order_id}: its placement awaits NonKYC's answer")
        target = (str(tracked_order.exchange_order_id) if tracked_order.exchange_order_id is not None
                  else tracked_order.client_order_id)
        order = await self._call(RESTMethod.GET, CONSTANTS.ORDER_PATH.format(order_id=target),
                                 limit_id=CONSTANTS.ORDER_PATH)
        if not isinstance(order, dict) or order.get("id") in (None, ""):
            raise IOError(f"Unexpected NonKYC answer for order {tracked_order.client_order_id}: {self._raw(order)}")
        raw_state = order.get("status")
        new_state = self._order_state(raw_state, self._dec(order.get("executedQuantity")),
                                      self._dec(order.get("quantity")))
        self._audit_once(f"order-status:{raw_state}", state=raw_state, mapped=str(new_state), keys=sorted(order.keys()),
                         executed=order.get("executedQuantity"))
        if new_state in CONSTANTS.TERMINAL_STATES and new_state is not OrderState.FAILED:
            filled = self._dec(order.get("executedQuantity"))
            if tracked_order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # A fill that landed between the poll's reads must not be reported short: book it first.
                for trade_update in await self._all_trade_updates_for_order(tracked_order):
                    self._order_tracker.process_trade_update(trade_update)
            if tracked_order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # NonKYC's trade rows are behind its own state: closing the order now would report it short.
                if len(self._short_since) > 2000:
                    self._short_since.clear()
                held_since = self._short_since.setdefault(tracked_order.client_order_id, time.time())
                held_s = round(time.time() - held_since, 1)
                if held_s < CONSTANTS.TERMINAL_HOLD_MAX_S:
                    self._audit("terminal-held", client_id=tracked_order.client_order_id, state=raw_state,
                                reported=str(filled), booked=str(tracked_order.executed_amount_base), held_s=held_s)
                    raise NonkycStatusDeferred(f"{tracked_order.client_order_id}: {raw_state} held for its fill rows")
                self._alarm("terminal-forwarded-short", client_id=tracked_order.client_order_id, state=raw_state,
                            reported=str(filled), booked=str(tracked_order.executed_amount_base), held_s=held_s)
            self._short_since.pop(tracked_order.client_order_id, None)
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order.get("id")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._dec(order.get("updatedAt")) / Decimal(1000) if order.get("updatedAt") else self._now(),
            new_state=new_state,
        )

    # ------------------------------------------------------------------ fills

    def _trade_update(self, order: InFlightOrder, *, trade_id: str, price: Decimal, quantity: Decimal,
                      quote: Optional[Decimal], fee: Optional[Decimal], fee_token: str, fill_ms: Any) -> TradeUpdate:
        if fee is not None and fee_token:
            flat_fee = TokenAmount(amount=fee, token=str(fee_token).upper())
        else:
            # No fee on the row (never seen): the standard rate on the RECEIVED asset, audit-logged.
            rate = utils.DEFAULT_FEES.taker_percent_fee_decimal
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
        )

    def _trade_update_from_rest(self, row: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        """One row of GET /gettrades: id, orderid, price, quantity, fee (quote), alternateFee / alternateFeeAsset
        (when a platform token paid it), createdAt / updatedAt (ms)."""
        fee = self._dec(row.get("fee")) if row.get("fee") not in (None, "") else None
        fee_token = order.quote_asset
        alternate = self._dec(row.get("alternateFee")) if row.get("alternateFee") not in (None, "") else Decimal("0")
        if alternate > 0 and row.get("alternateFeeAsset"):
            # The platform-token fee is the one charged; `fee` is then nominal.
            fee, fee_token = alternate, str(row["alternateFeeAsset"])
            self._audit_once("alternate-fee", asset=fee_token, amount=str(alternate), nominal_quote_fee=str(fee))
        return self._trade_update(
            order, trade_id=row["id"], price=self._dec(row.get("price")), quantity=self._dec(row.get("quantity")),
            quote=None, fee=fee, fee_token=fee_token, fill_ms=row.get("createdAt") or row.get("updatedAt"))

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """REST backstop for fills: the account's trades on the order's market, filtered by the venue's order id."""
        if order.exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        rows = await self._call(RESTMethod.GET, CONSTANTS.TRADES_PATH,
                                params={"symbol": symbol.replace("/", "_")}, limit_id=CONSTANTS.TRADES_PATH)
        if not isinstance(rows, list):
            raise IOError(f"Unexpected NonKYC fills answer for {order.client_order_id}: {self._raw(rows)}")
        updates: Dict[str, TradeUpdate] = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("id") in (None, ""):
                continue
            if str(row.get("orderid", order.exchange_order_id)) != str(order.exchange_order_id):
                continue
            if not updates:
                self._audit_once("rest-trade-key-set", keys=sorted(row.keys()), fee=row.get("fee"),
                                 alternate=row.get("alternateFee"), alternate_asset=row.get("alternateFeeAsset"))
            updates[str(row["id"])] = self._trade_update_from_rest(row, order)
        return self._fills_not_yet_counted(order, list(updates.values()))

    def _fills_not_yet_counted(self, order: InFlightOrder, rest_fills: List[TradeUpdate]) -> List[TradeUpdate]:
        """
        The REST rows for one order are NonKYC's complete record of its fills, so their total is the true executed
        quantity. Hummingbot de-duplicates fills by trade id only and never caps an order's executed amount. So a REST
        row with an unseen id is applied only while the order's executed amount is still below the venue's total, and
        a REST row that IS a pushed fill under another id (same quantity, price, time) is matched one-to-one first, so
        the cap never spends its room on a duplicate.
        """
        known = order.order_fills
        if known and rest_fills:
            self._audit_once("fill-ids-match", ws_ids=sorted(known)[:3],
                             rest_ids=sorted(f.trade_id for f in rest_fills)[:3],
                             match=any(f.trade_id in known for f in rest_fills))
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
        # NonKYC exposes no fee route: the standard rate is the default and the fee actually charged arrives with
        # every fill (the push's tradeFee, the REST row's fee / alternateFee).
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self) -> None:
        """NonKYC exposes no fee route (no account/tier endpoint in the OpenAPI, none on the market list), so every
        market carries the standard rate and the fee actually charged arrives with each fill. The unit is settled on
        the first live fill against DEFAULT_FEES (LBank's fractions-read-as-percents trap, 2026-10-07)."""
        self._audit_once("fee-rate-source", source="DEFAULT_FEES (no fee route on the venue)",
                         maker=str(utils.DEFAULT_FEES.maker_percent_fee_decimal),
                         taker=str(utils.DEFAULT_FEES.taker_percent_fee_decimal))
        mapping = await self.trading_pair_symbol_map()
        for trading_pair in mapping.values():
            self._trading_fees[trading_pair] = utils.DEFAULT_FEES

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        """GET /balances: every asset, zeros included (probed 2026-11-17: 262 rows), so nothing needs zeroing.
        available + held = total; `pending` is a PENDING DEPOSIT, not part of the balance (it is the venue's own
        "Balance of Pending Deposits" field) and is left out on purpose."""
        response = await self._call(RESTMethod.GET, CONSTANTS.BALANCES_PATH, limit_id=CONSTANTS.BALANCES_PATH)
        if not isinstance(response, list):
            raise IOError(f"Unexpected NonKYC balances answer: {self._raw(response)[:300]}")
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for row in response:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("asset") or "").upper()
            if not asset:
                continue
            remote_asset_names.add(asset)
            available, held = self._dec(row.get("available")), self._dec(row.get("held"))
            self._account_available_balances[asset] = available
            self._account_balances[asset] = available + held
        for asset_name in local_asset_names.difference(remote_asset_names):
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any) -> None:
        if not isinstance(exchange_info, list) or not exchange_info:
            # A 2xx that isn't the market list: keep the map we have (this fork's rules refresh sets the map first).
            self.logger().warning(f"NonKYC GET {CONSTANTS.MARKETS_PATH} answered no market list; the symbol map is "
                                  f"kept: {self._raw(exchange_info)[:200]}")
            return
        mapping = bidict()
        for info in exchange_info:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                symbol = str(info["symbol"])
                quote = symbol.split("/")[1] if "/" in symbol else None
                if not quote:
                    continue
                mapping[symbol] = combine_to_hb_trading_pair(
                    base=str(info["primaryTicker"]).upper(), quote=str(quote).upper())
            except Exception as exception:
                self.logger().error(f"Error parsing NonKYC symbol {info.get('symbol')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair that is not in the startup map. The base builds f"{base}{quote}" ("BTCUSDT"), which
        NonKYC does not recognise; its form is BASE/QUOTE (every listed market)."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.upper()}/{quote.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(f"NonKYC {trading_pair} was not among the markets GET {CONSTANTS.MARKETS_PATH} listed "
                              f"at startup; mapped to {exchange_symbol}. NonKYC's answers will show whether it "
                              f"exists.")

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        """priceDecimals / quantityDecimals (decimals), minimumQuantity (base units), minQuote (the minimum notional,
        1 USDT where minQuoteActive) and isActive / isPaused (the venue's switch, kept for the data source; never used
        to drop a market). A 2xx that isn't the market list raises, so the base keeps the rules it has."""
        if not isinstance(exchange_info_dict, list) or not exchange_info_dict:
            raise IOError(f"NonKYC GET {CONSTANTS.MARKETS_PATH} answered no market list: "
                          f"{self._raw(exchange_info_dict)[:200]}")
        rules: List[TradingRule] = []
        flags: Dict[str, bool] = {}
        for info in exchange_info_dict:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            symbol = str(info.get("symbol"))
            flags[symbol] = utils.is_market_tradable(info)
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=symbol)
                price_increment = Decimal(1).scaleb(-int(info.get("priceDecimals") or 0))
                amount_increment = Decimal(1).scaleb(-int(info.get("quantityDecimals") or 0))
                kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(info.get("minimumQuantity")), amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                )
                min_quote = self._dec(info.get("minQuote"))
                if min_quote > 0 and (info.get("minQuoteActive") or info.get("isMinQuoteActive")):
                    kwargs["min_notional_size"] = min_quote
                rules.append(TradingRule(**kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the NonKYC trading rule {info.get('symbol')}. Skipping.")
        self._market_flags = flags
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        One GET /tickers for every market, not one call per pair: the order book tracker re-reads the REST price of
        every quiet book every 5 s, and NonKYC's markets are mostly quiet (XT's 1,842 throttler warnings in 28 h).
        Every requested pair gets a value, NaN when NonKYC has none (the tracker re-asks a pair it got nothing for).
        """
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        symbol_to_pair: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                symbol_to_pair[await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)] = trading_pair
            except KeyError:
                continue
        rows = await self._call(RESTMethod.GET, CONSTANTS.TICKERS_PATH, signed=False,
                                limit_id=CONSTANTS.TICKERS_PATH)
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            # /tickers rows carry ticker_id like "BTC/USDT" (or "BTC_USDT" on some routes); accept both.
            key = str(row.get("ticker_id") or row.get("symbol") or "").replace("_", "/")
            trading_pair = symbol_to_pair.get(key)
            price = self._dec(row.get("last_price"))
            if trading_pair is not None and price > 0:
                prices[trading_pair] = float(price)
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                method = event_message.get("method")
                if method == CONSTANTS.WS_FRAME_REPORT:
                    self._process_order_event(event_message.get("params") or {})
                elif method == CONSTANTS.WS_FRAME_ACTIVE_ORDERS:
                    for row in event_message.get("result") or ():
                        if isinstance(row, dict):
                            self._process_order_event(row, snapshot=True)
                elif method == CONSTANTS.WS_FRAME_CURRENT_BALANCES:
                    for row in event_message.get("result") or ():
                        if isinstance(row, dict):
                            self._process_balance_event(row)
                elif method == CONSTANTS.WS_FRAME_BALANCE_UPDATE:
                    self._process_balance_event(event_message.get("params") or {})
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the NonKYC user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        order = tracker.all_fillable_orders.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id:
            order = tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        return order

    def _process_order_event(self, data: Dict[str, Any], snapshot: bool = False) -> None:
        """
        A `report` frame (or one row of the `activeOrders` snapshot): id (exchange), userProvidedId (ours), symbol,
        side, status, quantity, executedQuantity, updatedAt; a trade report adds tradeId, tradePrice, tradeQuantity,
        tradeFee, tradeTime. The client id rides on every frame, so a fill that beats the placement answer is matched
        by it (no parking by exchange id, unlike XT).
        """
        if not snapshot:
            self._audit_once("order-push-key-set", keys=sorted(data.keys()))
        client_order_id = str(data.get("userProvidedId") or "")
        exchange_order_id = str(data["id"]) if data.get("id") not in (None, "") else None
        for seen in (exchange_order_id, client_order_id):
            if seen:
                self._pushed_order_ids[seen] = None
        if len(self._pushed_order_ids) > 4000:
            for seen in list(self._pushed_order_ids)[:2000]:
                del self._pushed_order_ids[seen]
        report_type = str(data.get("reportType") or "").lower()
        state = str(data.get("status") or "").upper()
        order = self._locate_order(client_order_id, exchange_order_id)
        if order is None:
            if not snapshot:
                self._audit("order-push-unmatched", client_id=client_order_id or None, order_id=exchange_order_id,
                            report=report_type, state=state)
            if (client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) and state in ("NEW", "ACTIVE", "PARTLY FILLED",
                                                                                        "PARTIALLY FILLED")
                    and exchange_order_id is not None and exchange_order_id not in self._orphan_ids):
                # One of ours, open on NonKYC, that nothing tracks: its fills would never be booked. Said, not
                # cancelled -- it may be another process's.
                self._orphan_ids[exchange_order_id] = None
                self._alarm("untracked-order-open", client_id=client_order_id, order_id=exchange_order_id, state=state)
            return
        if not snapshot:
            self._audit("order-push", client_id=order.client_order_id, order_id=exchange_order_id,
                        report=report_type, state=state, executed=data.get("executedQuantity"),
                        trade_id=data.get("tradeId"), trade_qty=data.get("tradeQuantity"),
                        fee=data.get("tradeFee"), age_s=self._push_age(data))
        if order.exchange_order_id is None and exchange_order_id is not None:
            # The push beat the placement answer: give the order its id before a fill is named after it.
            order.update_exchange_order_id(exchange_order_id)
        if report_type == "trade" and data.get("tradeId") not in (None, ""):
            self._apply_push_fill(data, order)
        if order.is_failure and state in ("NEW", "ACTIVE", "PARTLY FILLED", "PARTIALLY FILLED") \
                and exchange_order_id is not None:
            self._cancel_failed_but_open(order, exchange_order_id, state)
            return
        if order.client_order_id not in self._order_tracker.all_updatable_orders:
            return  # already final: a late push may still carry a fill (above), never a state
        filled = self._dec(data.get("executedQuantity"))
        new_state = self._order_state(state, filled, self._dec(data.get("quantity"), default=str(order.amount)))
        update = OrderUpdate(
            trading_pair=order.trading_pair,
            update_timestamp=self._dec(data.get("updatedAt")) / Decimal(1000) if data.get("updatedAt") else self._now(),
            new_state=new_state,
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id or order.exchange_order_id,
        )
        if (new_state in CONSTANTS.TERMINAL_STATES
                and order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled):
            # A terminal state whose fills are not all booked yet (a lost or refused trade report): never forwarded
            # short.
            if order.client_order_id not in self._settling:
                self._settling.add(order.client_order_id)
                safe_ensure_future(self._settle_after_backfill(order, update, filled))
            return
        self._order_tracker.process_order_update(update)

    def _push_age(self, data: Dict[str, Any]) -> Optional[float]:
        try:
            return round(self._time_synchronizer.time() - int(data["updatedAt"]) * 1e-3, 3)
        except (KeyError, TypeError, ValueError):
            return None

    async def _settle_after_backfill(self, order: InFlightOrder, update: OrderUpdate, filled: Decimal) -> None:
        try:
            await asyncio.sleep(CONSTANTS.FILL_BACKFILL_GRACE)   # the trade report gets its chance first
            if order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                self._alarm("fill-backfill", client_id=order.client_order_id, reported=str(filled),
                            held=str(order.executed_amount_base))
                try:
                    for trade_update in await self._all_trade_updates_for_order(order):
                        self._order_tracker.process_trade_update(trade_update)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().warning(f"NonKYC fill backfill for {order.client_order_id} failed: {e!r}")
            if order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE < filled:
                # Still short: the status poll (which books fills first) settles it, never a short terminal update.
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
        # The report's own fill time (tradeTime, ISO) beats updatedAt: a terminal frame can carry the order's last
        # update, not the fill's.
        fill_ms = None
        if data.get("tradeTime") not in (None, ""):
            epoch = _iso_to_epoch(data.get("tradeTime"))
            if epoch is not None:
                fill_ms = int(epoch * 1e3)
        if fill_ms is None:
            fill_ms = data.get("updatedAt")
        trade_update = self._trade_update(
            order, trade_id=str(data.get("tradeId")), price=self._dec(data.get("tradePrice")),
            quantity=self._dec(data.get("tradeQuantity")), quote=None,
            fee=self._dec(data.get("tradeFee")) if data.get("tradeFee") not in (None, "") else None,
            fee_token=order.quote_asset, fill_ms=fill_ms)
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
        """HMB gave the order up (FAILED), yet NonKYC reports it open: nothing would track it once its time in the
        tracker's cache is over, so it is cancelled now, with a [NKYC-ALARM]."""
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
            response = await self._call(RESTMethod.POST, CONSTANTS.CANCEL_ORDER_PATH,
                                        data={"id": str(exchange_order_id)},
                                        limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-untracked", order_id=exchange_order_id, response=self._raw(response))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"NonKYC cancel of the untracked order {exchange_order_id} failed: {e!r}")

    def _process_balance_event(self, data: Dict[str, Any]) -> None:
        """A `balanceUpdate` push or one row of the `currentBalances` snapshot: ticker, available, held. available +
        held = total; the venue pushes on every change (order placed, filled, cancelled, deposit, withdrawal)."""
        self._audit_once("balance-push-key-set", keys=sorted(data.keys()))
        asset = str(data.get("ticker") or data.get("asset") or "").upper()
        if not asset:
            return
        available, held = self._dec(data.get("available")), self._dec(data.get("held"))
        self._account_available_balances[asset] = available
        self._account_balances[asset] = available + held
        if CONSTANTS.LIVE_AUDIT_LOGGING and self._balance_pushes_audited < CONSTANTS.BALANCE_AUDIT_PUSHES:
            self._balance_pushes_audited += 1
            safe_ensure_future(self._audit_balance_push(asset, data))

    async def _audit_balance_push(self, asset: str, push: Dict[str, Any]) -> None:
        """The first few balance pushes next to NonKYC's REST balance (balance-push-vs-rest): whether available + held
        are the totals after the change, as the docs say."""
        try:
            response = await self._call(RESTMethod.GET, CONSTANTS.BALANCES_PATH, limit_id=CONSTANTS.BALANCES_PATH)
            rows = response if isinstance(response, list) else []
            rest = next((r for r in rows if str(r.get("asset") or "").upper() == asset), "absent")
        except Exception as e:
            rest = f"REST error {e!r}"
        self._audit("balance-push-vs-rest", asset=asset, push_available=push.get("available"),
                    push_held=push.get("held"), rest=rest)
