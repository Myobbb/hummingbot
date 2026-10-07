import asyncio
import copy
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.bitunix import (
    bitunix_constants as CONSTANTS,
    bitunix_utils as utils,
    bitunix_web_utils as web_utils,
)
from hummingbot.connector.exchange.bitunix.bitunix_api_order_book_data_source import BitunixAPIOrderBookDataSource
from hummingbot.connector.exchange.bitunix.bitunix_api_user_stream_data_source import BitunixAPIUserStreamDataSource
from hummingbot.connector.exchange.bitunix.bitunix_auth import BitunixAuth
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


class BitunixBusinessError(IOError):
    """A request Bitunix answered with a code other than "0" (a definite refusal), or one of our own codes for an
    answer that is not what it should be (CODE_ORDER_NOT_FOUND, CODE_EMPTY_ANSWER). IOError keeps every existing
    `except IOError` path working."""

    def __init__(self, message: str, code: Optional[str], msg: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.msg = msg


class BitunixPlacementUnknown(IOError):
    """A placement Bitunix may have accepted (no answer, or "0" without an order id) that the lookup did not find: the
    order is NOT failed — it may rest on Bitunix. It stays PENDING_CREATE and the status poll keeps looking for it;
    only the poll's age-gated not-found verdict fails it."""


class BitunixEmptyBalanceUnconfirmed(IOError):
    """Bitunix answered an empty account while balances are held, and the answer is not believed yet
    (BitunixExchange._update_balances): the last balances stay."""


class _LookupInconclusive(Exception):
    """The placement lookup could not tell: a list read failed."""


class BitunixExchange(ExchangePyBase):
    """
    Bitunix spot connector: REST /api/spot/v1 for everything private, the website's market socket for books.

    Reference: the spot docs (offline mirror VS_code_projects/MDs/bitunix-api-spot/), live probes of 2026-10-07 from
    myserver, and the wiki page trading/exchanges/bitunix-api. What the docs could not settle is audit-logged
    ([BU-AUDIT]) rather than guessed; money guards log as [BU-ALARM].

    Scope: LIMIT orders only, which is all arb_l and the position balancer send.

    What Bitunix does not have, and what stands in for it:
      - No private push. While any order is open, orders and balances are polled every ORDER_POLL_INTERVAL (1 s).
      - Client ids are undocumented but real (live 2026-10-07): place_order keeps a `clientId` and every order read
        returns it, but it is no key — nothing reads or cancels by it, and a cancel by clientId stalls the order ~60 s.
        Every order is placed with its Hummingbot client id; a placement that got no answer is found by that id in
        the open orders and the recent history (_find_placed_order); cancels go by orderId only.
      - Silent not-found: an unknown order's detail answers code 0 with data null, and a cancel of an unknown id
        succeeds. "Not found" is decided here (_resolve_missing_order), never from a code.
      - Fills: order/deal/list gives each fill with Bitunix's own id (the trade id), quantity, price, fee and fee coin;
        it is read when the order's cumulative filled quantity (detail dealVolume) grows.
    """

    web_utils = web_utils

    def __init__(
        self,
        bitunix_api_key: str,
        bitunix_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature must match ConnectorSetting.conn_init_parameters in this fork (balance_asset_limit and
        rate_limits_share_pct, no config map): an upstream-style client_config_map crashes `balance` (CoinEx)."""
        self._api_key = bitunix_api_key
        self._secret_key = bitunix_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: Dict[str, None] = {}
        self._alarm_seen: Dict[str, None] = {}
        self._detail_cache: Dict[str, Dict[str, Any]] = {}   # exchange id -> detail, for one poll (both passes)
        self._depth_steps: Dict[str, str] = {}               # exchange symbol -> precisions[0] (the finest step)
        self._last_private_read: float = 0.0                 # wall time of the last successful signed read
        self._last_time_sync: float = 0.0
        self._empty_since: Optional[float] = None            # monotonic time of the first empty answer while holding
        self._empty_reads: int = 0
        self._market_list_cache: Optional[Tuple[float, Dict[str, Any]]] = None
        self._market_list_lock = asyncio.Lock()
        # Per-order state; entries of orders no longer tracked are dropped every poll (_purge_order_state).
        self._placing: set = set()                           # client ids whose placement awaits Bitunix's answer
        self._volume_checked: set = set()                    # client ids whose quantity Bitunix confirmed
        self._sent_at_ms: Dict[str, int] = {}                # client id -> server ms just before its placement
        self._cancel_sent: set = set()                       # client ids we asked Bitunix to cancel
        self._fills_pending_since: Dict[str, float] = {}     # client id -> monotonic time it first waited for fills
        self._hold_baseline: Dict[str, Tuple[Decimal, Decimal]] = {}
        self._settling: set = set()
        # Base-asset deltas of every fill booked (per asset), and that total as it stood when the last balance read
        # went out: _balance_shows_fills compares the two.
        self._booked_base: Dict[str, Decimal] = {}
        self._booked_base_at_read: Dict[str, Decimal] = {}
        self._noted_fills: Dict[str, None] = {}              # Bitunix fill ids already counted in _booked_base
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # No balance push exists: the REST poll is authoritative and the base's in-flight snapshot keeps `available`
        # right between polls.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BitunixAuth:
        return BitunixAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        return CONSTANTS.PAIRS_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.PAIRS_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.LAST_PRICE_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # A cancel answers data null — and success even for an unknown order (live): the next status read settles it.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def last_private_read_time(self) -> float:
        return self._last_private_read

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    def depth_step(self, symbol: str) -> Optional[str]:
        """The market's finest price step (coin_pair/list precisions[0]): the depth channel's name needs it."""
        return self._depth_steps.get(symbol)

    # ------------------------------------------------------------------ factories

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BitunixAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BitunixAPIUserStreamDataSource(connector=self)

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [BU-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[BU-AUDIT] {tag} {rendered}")

    @staticmethod
    def _first_time(seen: Dict[str, None], key: str) -> bool:
        """True the first time `key` is seen. Bounded: the oldest half is forgotten past 5000 keys (per-order keys)."""
        if key in seen:
            return False
        seen[key] = None
        if len(seen) > 5000:
            for old in list(seen)[:2500]:
                del seen[old]
        return True

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if CONSTANTS.LIVE_AUDIT_LOGGING and self._first_time(self._audit_seen, tag):
            self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [BU-ALARM] WARNING, whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[BU-ALARM] {tag} {rendered}")

    def _alarm_once(self, key: str, tag: str, **fields: Any) -> None:
        """An alarm about a state the poll meets again every second (a refused fill row, an order kept open): once."""
        if self._first_time(self._alarm_seen, key):
            self._alarm(tag, **fields)

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
        Bitunix answers every request with HTTP 200, so the base class's resync-and-retry (which runs only on a
        transport IOError) never sees an expired timestamp. Here: numbers are parsed exactly (Decimal), a placement can
        carry its own timeout, and 100008 re-syncs the clock and repeats the request once (the timestamp is checked
        before anything is executed — live, it is refused even for a read — so the repeat can't place twice). A signed
        request that Bitunix answered "0" marks private REST as working (BitunixAPIUserStreamDataSource).
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
            result = web_utils.loads(await response.text())
            if attempt == 0 and is_auth_required and web_utils.error_code(result) == CONSTANTS.CODE_REQUEST_EXPIRED:
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer(force=True)
                continue
            if is_auth_required and web_utils.is_ok(result):
                self._last_private_read = time.time()
            return result
        return result

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False, force: bool = False):
        """The base re-reads the server time on every status poll — every second while orders are open. Bitunix's
        clock comes from an HTTP Date header at 1 s resolution against a 60 s window, so it is re-read every
        TIME_SYNC_INTERVAL, or at once on a 100008."""
        now = time.monotonic()
        if not force and self._last_time_sync and now - self._last_time_sync < CONSTANTS.TIME_SYNC_INTERVAL:
            return
        self._last_time_sync = now
        await super()._update_time_synchronizer(pass_on_non_cancelled_error=pass_on_non_cancelled_error)

    async def _make_trading_rules_request(self) -> Any:
        return await self.market_list(max_age=CONSTANTS.MARKET_LIST_SHARE_SECONDS)

    async def _make_trading_pairs_request(self) -> Any:
        return await self.market_list(max_age=CONSTANTS.MARKET_LIST_SHARE_SECONDS)

    async def market_list(self, max_age: float = 0.0) -> Dict[str, Any]:
        """coin_pair/list (~577 KB: every market with its rules and trading switch), read by the symbol map, the trading
        rules, the book's trading switch and runtime adds. A read younger than max_age (s) is shared instead of read
        again, and callers that ask while a read is under way wait for it."""
        async with self._market_list_lock:
            cached = self._market_list_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            response = self._market_list(await self._api_get(path_url=CONSTANTS.PAIRS_PATH,
                                                             limit_id=CONSTANTS.PAIRS_PATH))
            self._market_list_cache = (time.monotonic(), response)
            return response

    def _market_list(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """coin_pair/list, or an exception: an empty or refused list would otherwise become an empty symbol map and no
        trading rules until the next poll, with every order failing "no rule". Raising keeps the last map and rules."""
        self._raise_on_error(response, f"Error reading Bitunix's market list ({CONSTANTS.PAIRS_PATH})")
        if not isinstance(response.get("data"), list) or not response["data"]:
            raise IOError(f"Bitunix's market list came back empty | Bitunix response: {self._raw(response)}")
        return response

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.LAST_PRICE_PATH, params=dict(CONSTANTS.SERVER_TIME_PARAMS),
                                       limit_id=CONSTANTS.LAST_PRICE_PATH)
        if not web_utils.is_ok(response):
            raise IOError(f"Unexpected Bitunix answer to the network check: {response}")

    @staticmethod
    def _raw(response: Any) -> str:
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    def _raise_on_error(self, response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if not web_utils.is_ok(response):
            code = web_utils.error_code(response)
            msg = response.get("msg") if isinstance(response, dict) else None
            self._audit_once(f"error-code:{code}", context=context, response=self._raw(response))
            raise BitunixBusinessError(f"{context}: code {code} ({msg}) | Bitunix response: {self._raw(response)}",
                                       code, msg)
        return response

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """Bitunix refuses with HTTP 200 and a code, which the base would log as a network error with a traceback: a
        refusal is logged as one, with the request and Bitunix's complete answer."""
        if isinstance(exception, BitunixPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll looks for it "
                                  f"on Bitunix and fails it only once it is not there.")
            return
        if isinstance(exception, BitunixBusinessError):
            self.logger().warning(f"Bitunix rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return CONSTANTS.CODE_REQUEST_EXPIRED in str(request_exception)

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        return isinstance(error, BitunixBusinessError) and error.code == CONSTANTS.CODE_ORDER_NOT_FOUND

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # A cancel of an unknown order succeeds (live): it never says "not found".
        return False

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (and forgets a FAILED order that may
        still rest on the venue). Only our own verdict — null detail, absent from the open orders and the history, and
        old enough — counts; anything else is a WARNING and the order stays tracked (Hotcoin's lesson, 2026-10-06)."""
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        if isinstance(error, BitunixBusinessError) and error.code == CONSTANTS.CODE_EMPTY_ANSWER:
            # Not readable yet (a placement awaiting its answer, a just-placed order): expected, every poll until then.
            self.logger().debug(f"Bitunix status of {order.client_order_id} not readable yet: {error}")
            return
        self.logger().warning(f"Bitunix status read for {order.client_order_id} failed, the order stays tracked: "
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
        timestamp = self.current_timestamp
        return time.time() if timestamp is None or timestamp != timestamp else timestamp

    def _server_ms(self) -> int:
        return int(self._time_synchronizer.time() * 1e3)

    def _get_poll_interval(self, timestamp: float) -> float:
        """No private push: while any order is open the status poll runs every ORDER_POLL_INTERVAL (1 s), so a fill or a
        cancel is seen within about a second; otherwise every SHORT_POLL_INTERVAL (10 s) for the balances."""
        if self.in_flight_orders:
            return CONSTANTS.ORDER_POLL_INTERVAL
        return self.SHORT_POLL_INTERVAL

    async def _user_stream_event_listener(self) -> None:
        # Bitunix has no private stream: nothing ever arrives here.
        while True:
            await asyncio.sleep(3600)

    async def _update_order_status(self) -> None:
        """One poll (the base's fills pass, then its status pass), reading each order's detail once for both passes;
        first the per-order state of orders no longer tracked is dropped."""
        self._detail_cache.clear()
        self._purge_order_state()
        await super()._update_order_status()

    def _purge_order_state(self) -> None:
        live = set(self._order_tracker.all_fillable_orders) | set(self._placing)
        for registry in (self._sent_at_ms, self._hold_baseline, self._fills_pending_since):
            for client_order_id in [c for c in registry if c not in live]:
                del registry[client_order_id]
        self._volume_checked &= live
        self._cancel_sent &= live

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """With no trading rule the base stops at a bare KeyError before tracking the order (XT B2-USDT, 2026-09-23):
        here the order is tracked and failed the way the base fails its own pre-send checks."""
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to Bitunix: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from GET {CONSTANTS.PAIRS_PATH}).")
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
            "side": CONSTANTS.SIDE[trade_type],
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
            # LIMIT_VOLUME_IS_BASE: see the constant; the first status read checks Bitunix's own quantity.
            "volume": self._format_decimal(amount if CONSTANTS.LIMIT_VOLUME_IS_BASE else amount * price),
            "price": self._format_decimal(price),
            "symbol": symbol,
            # Undocumented, kept by Bitunix (live 2026-10-07): the placement guard finds an unanswered order by it, and
            # the asset_manager tells Hummingbot's fills by it. Never used to cancel (that stalls the order ~60 s).
            "clientId": order_id,
        }
        sent_at_ms = self._server_ms()
        self._sent_at_ms[order_id] = sent_at_ms
        # While the answer is awaited only this placement looks the order up, not the status poll (review 2026-10-07).
        # Discarded on return, and the base gives the order its id with no await in between
        # (_place_order_and_process_update).
        self._placing.add(order_id)
        try:
            try:
                response = await self._api_post(path_url=CONSTANTS.PLACE_ORDER_PATH, data=body, is_auth_required=True,
                                                limit_id=CONSTANTS.PLACE_ORDER_PATH,
                                                timeout=CONSTANTS.PLACE_ORDER_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception as transport_error:
                # No answer: Bitunix may still have accepted the order (the base would mark it FAILED and stop tracking
                # a live order). Bitunix can't be asked by client id, but its order lists carry it: looked up there.
                return await self._locate_unconfirmed_placement(order_id, symbol, sent_at_ms,
                                                                repr(transport_error)), self._now()
            self._audit("place-order", client_id=order_id, request=self._raw(body), response=self._raw(response))
            self._raise_on_error(response, f"Order {order_id} refused, request {self._raw(body)}")
            data = response.get("data") or {}
            exchange_order_id = data.get("orderId") if isinstance(data, dict) else None
            if exchange_order_id in (None, ""):
                # "0" (accepted) without an order id: Bitunix may have the order; looked up the same way.
                cause = f"code 0 without an order id | Bitunix response: {self._raw(response)}"
                return await self._locate_unconfirmed_placement(order_id, symbol, sent_at_ms, cause), self._now()
            place_status = str(data.get("placeStatus"))
            if place_status not in ("None", "1"):
                # Docs: placeStatus "whether the order was successful, 1 success". Never seen otherwise; the order id
                # says Bitunix has one, so it is tracked and its status read decides.
                self._alarm("place-status-not-1", client_id=order_id, exchange_order_id=exchange_order_id,
                            response=self._raw(response))
            return str(exchange_order_id), self._now()
        finally:
            self._placing.discard(order_id)

    async def _locate_unconfirmed_placement(self, order_id: str, symbol: str, sent_at_ms: int, cause: str) -> str:
        """A placement Bitunix may have accepted without telling us the order's id. Found on Bitunix -> its id. Not
        found, or the lookup could not tell -> BitunixPlacementUnknown: the order is NOT failed (it may rest on
        Bitunix); it stays PENDING_CREATE and the status poll keeps looking (_fetch_order_detail), failing it only by
        its age-gated not-found verdict."""
        found, verdict = None, "not on Bitunix yet"
        try:
            found = await self._find_placed_order(order_id, symbol, sent_at_ms, CONSTANTS.PLACEMENT_LOOKUP_DELAYS)
        except _LookupInconclusive as e:
            verdict = f"lookup inconclusive: {e}"
        self._alarm("place-order-unconfirmed", client_id=order_id, cause=cause, found_on_exchange=found,
                    verdict=None if found is not None else verdict)
        if found is not None:
            return found
        raise BitunixPlacementUnknown(f"Bitunix did not confirm the placement ({cause}); {verdict}")

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        asset = order.base_asset
        # The balance and the fills booked as they stood at one moment, the last balance read: the fills booked by now
        # may include one that balance doesn't show yet, which would release this order's hold early (review
        # 2026-10-07, the double-buy class).
        self._hold_baseline[order.client_order_id] = (self._account_balances.get(asset, Decimal("0")),
                                                      self._booked_base_at_read.get(asset, Decimal("0")))
        # Registered from here, not only in _place_order: the symbol lookup before the request is an await too.
        self._placing.add(order.client_order_id)
        try:
            exchange_order_id = await super()._place_order_and_process_update(order, **kwargs)
        finally:
            self._placing.discard(order.client_order_id)
        if order.exchange_order_id is not None and str(order.exchange_order_id) != str(exchange_order_id):
            # The base never overwrites an id: cancels and status reads would use the other one. _placing keeps the
            # poll's lookup away while the placement is awaited, so this should never fire.
            self._alarm("order-id-mismatch", client_id=order.client_order_id, tracked_id=order.exchange_order_id,
                        placement_id=str(exchange_order_id))
        # The first status read checks the quantity Bitunix booked (the volume guard): do it now, not at the next tick.
        self._poll_notifier.set()
        return exchange_order_id

    async def _list_orders(self, symbol: str, since_ms: int) -> List[Dict[str, Any]]:
        """The market's open orders and its orders created since since_ms (both routes require the symbol). null is an
        empty list (Bitunix's way, as for an empty account); any other answer that is not a list raises: read as "no
        orders" it would feed a not-found verdict."""
        pending = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.ORDER_PENDING_PATH, data={"symbol": symbol},
                                 is_auth_required=True, limit_id=CONSTANTS.ORDER_PENDING_PATH),
            f"Error listing Bitunix open orders on {symbol}")
        history = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.ORDER_HISTORY_PATH,
                                 data={"symbol": symbol, "page": 1, "pageSize": 100,
                                       "startTime": web_utils.iso_utc(since_ms)},
                                 is_auth_required=True, limit_id=CONSTANTS.ORDER_HISTORY_PATH),
            f"Error listing Bitunix order history on {symbol}")
        open_rows = pending.get("data")
        hist = history.get("data")
        hist_rows = hist.get("data") if isinstance(hist, dict) else hist
        open_rows = [] if open_rows is None else open_rows
        hist_rows = [] if hist_rows is None else hist_rows
        if not isinstance(open_rows, list) or not isinstance(hist_rows, list):
            raise IOError(f"Unexpected Bitunix order lists on {symbol} | open orders: {self._raw(pending)} | "
                          f"history: {self._raw(history)}")
        rows: Dict[str, Dict[str, Any]] = {}
        for row in hist_rows + open_rows:
            if isinstance(row, dict) and row.get("orderId") not in (None, ""):
                rows[str(row["orderId"])] = row
        return list(rows.values())

    def _order_base_quantity(self, row: Dict[str, Any]) -> Optional[Decimal]:
        """Bitunix's own base quantity of an order: dealVolume + leftVolume (both documented as base on every order
        page; `volume` is documented twice with two meanings, so it is not used). None if either is missing."""
        if row.get("dealVolume") in (None, "") or row.get("leftVolume") in (None, ""):
            return None
        return self._dec(row.get("dealVolume")) + self._dec(row.get("leftVolume"))

    async def _find_placed_order(self, client_order_id: str, symbol: str, sent_at_ms: int,
                                 delays: Tuple[float, ...]) -> Optional[str]:
        """The placement guard: the order carrying our client id in the market's open orders or in its history since
        the request went out (5 s clock slack), looked for after each delay (s). Exactly one -> it is ours. None -> none
        (yet), or two or more under one id (never seen): every one still open is cancelled ([BU-ALARM]
        placement-ambiguous). _LookupInconclusive -> a list read failed, so it could not tell. An order that merely
        looks like ours (side, price, size) is never adopted: only the id counts."""
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            own = self._order_tracker.fetch_order(client_order_id=client_order_id)
            if own is not None and own.exchange_order_id is not None:
                return str(own.exchange_order_id)         # it got its id meanwhile
            try:
                rows = await self._list_orders(symbol, sent_at_ms - 5_000)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                raise _LookupInconclusive(f"listing {symbol}'s orders failed: {e!r}")
            matches = [row for row in rows if str(row.get("clientId") or "") == client_order_id]
            if len(matches) == 1:
                return str(matches[0]["orderId"])
            if len(matches) > 1:
                resting = [str(r["orderId"]) for r in matches if str(r.get("status")) not in ("2", "4", "7")]
                self._alarm_once(f"placement-ambiguous:{client_order_id}", "placement-ambiguous",
                                 client_id=client_order_id, symbol=symbol,
                                 candidates=[str(r["orderId"]) for r in matches],
                                 action=f"cancelling every one still open: {resting}")
                for exchange_order_id in resting:
                    safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id, symbol))
                return None
        return None

    async def _cancel_by_exchange_id(self, exchange_order_id: str, symbol: str) -> None:
        try:
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                            data={"orderIdList": [{"orderId": exchange_order_id, "symbol": symbol}]},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-by-exchange-id", exchange_order_id=exchange_order_id, response=self._raw(response))
        except Exception as e:
            self._alarm("cancel-by-exchange-id-failed", exchange_order_id=exchange_order_id, error=repr(e))

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        # By orderId only: a cancel by clientId answers success, does nothing, and stalls the order ~60 s against the
        # cancels that follow (live 2026-10-07).
        body = {"orderIdList": [{"orderId": str(exchange_order_id), "symbol": symbol}]}
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, data=body, is_auth_required=True,
                                        limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    response=self._raw(response))
        self._raise_on_error(response, f"Bitunix refused to cancel order {order_id} ({exchange_order_id})")
        self._cancel_sent.add(order_id)
        # A detail read just before the cancel would put the order back to OPEN over PENDING_CANCEL; read it next poll.
        self._detail_cache.pop(str(exchange_order_id), None)
        self._poll_notifier.set()
        return True

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """The base's cancel path, except that a refusal is a WARNING with Bitunix's answer (no traceback) and reads the
        order's status at once, an HTTP timeout on the cancel reads the status too (the cancel may have landed), and an
        order still without an exchange id counts no not-found strike."""
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if order.exchange_order_id is None:
                # The base counts this as a not-found strike, with no look at Bitunix: an order whose placement went
                # unconfirmed may rest there. Only the status poll's lookup decides (_fetch_order_detail).
                self.logger().warning(f"Bitunix cancel of {order.client_order_id} not sent: the order has no exchange "
                                      f"id yet. The status poll is looking for it on Bitunix.")
            else:
                self.logger().warning(f"Bitunix did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except BitunixBusinessError as refusal:
            self.logger().warning(f"Bitunix refused to cancel {order.client_order_id}: {refusal}. Reading its status "
                                  f"now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        if order.exchange_order_id is not None:
            self._detail_cache.pop(str(order.exchange_order_id), None)
        try:
            order_update = await self._request_order_status(tracked_order=order)
            self._order_tracker.process_order_update(order_update)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Bitunix status read for {order.client_order_id} after its cancel failed; the next "
                                  f"poll reads it again: {e!r}")

    def _order_state(self, status: Any, executed: Decimal, original: Decimal) -> OrderState:
        try:
            state = CONSTANTS.ORDER_STATE.get(int(str(status)))
        except (TypeError, ValueError):
            state = None
        if state is not None:
            return state
        self._audit_once(f"unknown-status:{status}", status=status, executed=str(executed), original=str(original))
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    # ------------------------------------------------------------------ order detail (REST) and fills

    def _age(self, order: InFlightOrder) -> float:
        return max(0.0, self._now() - float(order.creation_timestamp or self._now()))

    async def _resolve_missing_order(self, order: InFlightOrder, symbol: str) -> Dict[str, Any]:
        """detail answered data null (Bitunix's silent "not found", live). The order's row from the open orders or the
        recent history stands in; failing both, it is not found — but only once it is old enough to have been
        readable (a just-placed order may lag)."""
        since = int((float(order.creation_timestamp or self._now()) * 1e3)) - CONSTANTS.LOOKUP_WINDOW_MS
        rows = await self._list_orders(symbol, since)
        for row in rows:
            if str(row.get("orderId")) == str(order.exchange_order_id):
                self._audit_once("detail-null-but-listed", exchange_order_id=order.exchange_order_id,
                                 status=row.get("status"))
                return row
        if self._age(order) < CONSTANTS.NOT_FOUND_MIN_AGE:
            raise BitunixBusinessError(f"Order {order.client_order_id} ({order.exchange_order_id}) not readable yet "
                                       f"(age {self._age(order):.1f} s)", CONSTANTS.CODE_EMPTY_ANSWER)
        raise BitunixBusinessError(f"Order {order.client_order_id} ({order.exchange_order_id}) is not on Bitunix: "
                                   f"detail null, not open, not in the history since {web_utils.iso_utc(since)}",
                                   CONSTANTS.CODE_ORDER_NOT_FOUND)

    async def _fetch_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """GET order/detail, read once per poll: the fills pass and the status pass share it (_update_order_status
        empties the cache as a poll starts, and a cancel drops its order's entry). An order without an exchange id is
        looked up on Bitunix first (_adopt_placed_order)."""
        exchange_order_id = order.exchange_order_id
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        if exchange_order_id is None:
            exchange_order_id = await self._adopt_placed_order(order, symbol)
        exchange_order_id = str(exchange_order_id)
        cached = self._detail_cache.get(exchange_order_id)
        if cached is not None:
            return cached
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ORDER_DETAIL_PATH, params={"orderId": exchange_order_id},
                                is_auth_required=True, limit_id=CONSTANTS.ORDER_DETAIL_PATH),
            f"Error fetching status of order {order.client_order_id} ({exchange_order_id})",
        )
        detail = response.get("data")
        if not isinstance(detail, dict) or not detail:
            detail = await self._resolve_missing_order(order, symbol)
        self._audit_once("detail-fields", exchange_order_id=exchange_order_id, detail=self._raw(detail))
        self._detail_cache[exchange_order_id] = detail
        return detail

    async def _adopt_placed_order(self, order: InFlightOrder, symbol: str) -> str:
        """The poll's side of an unconfirmed placement: one look for the order on Bitunix, no waits. Found -> the order
        gets that id. Not readable yet (CODE_EMPTY_ANSWER, no strike): its placement still awaits the answer (only the
        placement looks then), the lookup could not tell, or the order is younger than NOT_FOUND_MIN_AGE. Otherwise
        CODE_ORDER_NOT_FOUND: a strike toward FAILED (the tracker's lost-order count)."""
        client_order_id = order.client_order_id
        if client_order_id in self._placing:
            raise BitunixBusinessError(f"Order {client_order_id}: its placement still awaits Bitunix's answer",
                                       CONSTANTS.CODE_EMPTY_ANSWER)
        sent_at_ms = self._sent_at_ms.get(client_order_id) or int(float(order.creation_timestamp) * 1e3)
        try:
            found = await self._find_placed_order(client_order_id, symbol, sent_at_ms, (0.0,))
        except _LookupInconclusive as e:
            raise BitunixBusinessError(f"Order {client_order_id} has no exchange id yet: {e}",
                                       CONSTANTS.CODE_EMPTY_ANSWER)
        if found is None:
            if self._age(order) < CONSTANTS.NOT_FOUND_MIN_AGE:
                raise BitunixBusinessError(f"Order {client_order_id} has no exchange id yet",
                                           CONSTANTS.CODE_EMPTY_ANSWER)
            raise BitunixBusinessError(f"Order {client_order_id} is not on Bitunix (looked up by side, price, quantity "
                                       f"and time)", CONSTANTS.CODE_ORDER_NOT_FOUND)
        self._alarm("placement-found", client_id=client_order_id, exchange_order_id=found)
        order.update_exchange_order_id(found)
        return found

    def _check_volume(self, order: InFlightOrder, detail: Dict[str, Any]) -> bool:
        """The volume guard, a safety net since LIMIT_VOLUME_IS_BASE was verified live (2026-10-07): Bitunix's own base
        quantity must be ours. A mismatch — e.g. Bitunix reading `volume` as a quote amount, which on a cheap token is
        an order hundreds of times too big or small — cancels the order at once. True if the order may proceed. The
        first reads also check that Bitunix kept the client id (_check_client_id)."""
        if order.client_order_id in self._volume_checked:
            return True
        self._check_client_id(order, detail)
        status = str(detail.get("status"))
        quantity = self._order_base_quantity(detail)
        if status == "2" and detail.get("dealVolume") not in (None, ""):
            quantity = self._dec(detail.get("dealVolume"))   # filled: dealVolume is the whole order
        if quantity is None or status in ("4", "7"):
            return True      # can't tell from this read; the next one may
        self._audit_once("volume-semantics", client_id=order.client_order_id, sent=str(order.amount),
                         bitunix_base_quantity=str(quantity), status=status, detail=self._raw(detail))
        if abs(quantity - order.amount) <= order.amount * CONSTANTS.VOLUME_MISMATCH_TOLERANCE:
            self._volume_checked.add(order.client_order_id)
            return True
        self._alarm("volume-semantics", client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                    sent_base=str(order.amount), bitunix_base_quantity=str(quantity),
                    action="cancelling; LIMIT_VOLUME_IS_BASE in bitunix_constants.py must be checked")
        safe_ensure_future(self._cancel_by_exchange_id(str(order.exchange_order_id),
                                                       f"{order.base_asset}{order.quote_asset}"))
        self._volume_checked.add(order.client_order_id)
        return False

    def _check_client_id(self, order: InFlightOrder, detail: Dict[str, Any]) -> None:
        """Bitunix keeps the client id sent with an order (live 2026-10-07); the placement guard finds unanswered orders
        by it, and the asset_manager tells Hummingbot's fills by it. An order read back without it means both are
        blind: one [BU-ALARM] per session."""
        kept = str(detail.get("clientId") or "")
        if kept != order.client_order_id:
            self._alarm_once("client-id-not-kept", "client-id-not-kept", client_id=order.client_order_id,
                             exchange_order_id=order.exchange_order_id, bitunix_client_id=kept,
                             action="unanswered placements can't be found by client id; check Bitunix's clientId")

    async def _fills_from_deals(self, order: InFlightOrder, cumulative: Decimal) -> List[TradeUpdate]:
        """Bitunix's own fill rows for the order, each a TradeUpdate named by Bitunix's fill id (the AM keys its ledger
        rows by the same id), booked once, never past the order's cumulative filled quantity or its size."""
        tolerance = Decimal("1e-12")
        held = order.executed_amount_base
        if cumulative <= held + tolerance or order.exchange_order_id is None:
            return []
        if cumulative > order.amount * (1 + CONSTANTS.VOLUME_MISMATCH_TOLERANCE):
            self._alarm_once(f"fill-over-amount:{order.client_order_id}:{cumulative}", "fill-over-amount",
                             client_id=order.client_order_id, cumulative=str(cumulative), amount=str(order.amount))
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        response = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.ORDER_DEALS_PATH,
                                 data={"orderId": str(order.exchange_order_id), "symbol": symbol},
                                 is_auth_required=True, limit_id=CONSTANTS.ORDER_DEALS_PATH),
            f"Error fetching fills of order {order.client_order_id} ({order.exchange_order_id})")
        rows = response.get("data")
        if isinstance(rows, dict):
            rows = [rows]
        rows = [r for r in rows or [] if isinstance(r, dict)]
        self._audit_once("deal-fields", exchange_order_id=order.exchange_order_id, rows=self._raw(rows[:3]))
        fills: List[TradeUpdate] = []
        booked = held
        for row in sorted(rows, key=lambda r: (web_utils.to_ms(r.get("ctime")) or 0, str(r.get("id")))):
            trade_id = str(row.get("id") or "")
            if not trade_id or trade_id in order.order_fills:
                continue
            quantity, price = self._dec(row.get("volume")), self._dec(row.get("price"))
            if quantity <= 0 or price <= 0:
                self._alarm_once(f"fill-unreadable:{order.client_order_id}:{trade_id}", "fill-unreadable",
                                 client_id=order.client_order_id, row=self._raw(row))
                continue
            if booked + quantity > cumulative + tolerance:
                # Never past the order's own filled quantity: the deal list ran ahead of the detail, or repeats a fill.
                self._alarm_once(f"fill-capped:{order.client_order_id}:{trade_id}:{cumulative}", "fill-capped",
                                 client_id=order.client_order_id, trade_id=trade_id, booked=str(booked),
                                 fill=str(quantity), cumulative=str(cumulative))
                break
            limit = order.price if order.price is not None and not order.price.is_nan() else Decimal("0")
            if limit > 0 and not (limit / 2 <= price <= limit * 2):
                self._alarm_once(f"fill-price-implausible:{order.client_order_id}:{trade_id}", "fill-price-implausible",
                                 client_id=order.client_order_id, trade_id=trade_id, price=str(price),
                                 limit=str(limit), action="refused")
                continue
            fee_coin = str(row.get("feeCoin") or "").upper() or (order.base_asset if order.trade_type is TradeType.BUY
                                                                  else order.quote_asset)
            fee_amount = self._dec(row.get("fee"))
            self._audit_once(f"fee-rate:{order.trade_type.name}", fee=str(fee_amount), fee_coin=fee_coin,
                             quantity=str(quantity), price=str(price), role=row.get("role"),
                             per_quote=str(fee_amount / (quantity * price)) if quantity * price > 0 else None,
                             per_base=str(fee_amount / quantity) if quantity > 0 else None)
            fills.append(TradeUpdate(
                trade_id=trade_id,
                client_order_id=order.client_order_id,
                exchange_order_id=str(order.exchange_order_id),
                trading_pair=order.trading_pair,
                fee=TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_coin)],
                ),
                fill_base_amount=quantity,
                fill_quote_amount=quantity * price,
                fill_price=price,
                fill_timestamp=(web_utils.to_ms(row.get("ctime")) or int(self._now() * 1e3)) / 1e3,
                is_taker=str(row.get("role")) != "1",
            ))
            booked += quantity
        return fills

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """The base's fills pass runs over active AND recently done orders (its 30 s cache). A done order is skipped: a
        terminal state is reported only once Bitunix's deal list showed every fill (_request_order_status), so there
        is nothing left to read (review 2026-10-07: ~30 detail reads per completed order otherwise)."""
        if order.exchange_order_id is None or order.is_done:
            return []
        try:
            detail = await self._fetch_order_detail(order)
        except BitunixBusinessError as e:
            if e.code in (CONSTANTS.CODE_ORDER_NOT_FOUND, CONSTANTS.CODE_EMPTY_ANSWER) and order.executed_amount_base <= 0:
                return []
            raise
        if not self._check_volume(order, detail):
            return []
        fills = await self._fills_from_deals(order, self._dec(detail.get("dealVolume")))
        for fill in fills:
            self._note_fill_booked(order, fill)
        return fills

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        try:
            detail = await self._fetch_order_detail(tracked_order)
        except BitunixBusinessError as e:
            if (self._is_not_found(e) and tracked_order.client_order_id in self._cancel_sent
                    and tracked_order.executed_amount_base <= 0):
                # We asked Bitunix to cancel it, it never filled, and it is gone from every list: cancelled (a venue that
                # drops an order cancelled without a fill, as Hotcoin does, answers exactly this).
                self._audit_once("cancelled-order-gone", client_id=tracked_order.client_order_id)
                self._forget_settled(tracked_order)
                return self._order_update(tracked_order, OrderState.CANCELED)
            raise
        if not self._check_volume(tracked_order, detail):
            return self._order_update(tracked_order, OrderState.PENDING_CANCEL)
        cumulative = self._dec(detail.get("dealVolume"))
        # Fills first: a FILLED state with fills still missing would complete the order short.
        for fill in await self._fills_from_deals(tracked_order, cumulative):
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(tracked_order, fill)
        status = detail.get("status")
        new_state = self._order_state(status, cumulative, tracked_order.amount)
        self._audit_once(f"detail-status:{status}", mapped=str(new_state), deal_volume=str(cumulative),
                         left_volume=detail.get("leftVolume"))
        client_order_id = tracked_order.client_order_id
        if new_state in CONSTANTS.TERMINAL_STATES and tracked_order.executed_amount_base + Decimal("1e-12") < cumulative:
            # Bitunix says done but its deal list hasn't shown every fill yet, or a guard refused a fill row: nothing
            # new is reported until every fill is booked. Strict on purpose — done with fills missing, the strategy
            # would hedge the wrong size. Past FILLS_MISSING_ALARM_SECONDS one [BU-ALARM] asks for a look at Bitunix.
            first = self._fills_pending_since.setdefault(client_order_id, time.monotonic())
            self._audit_once(f"fills-pending:{client_order_id}", client_id=client_order_id,
                             cumulative=str(cumulative), booked=str(tracked_order.executed_amount_base))
            if time.monotonic() - first >= CONSTANTS.FILLS_MISSING_ALARM_SECONDS:
                self._alarm_once(f"fills-missing:{client_order_id}", "fills-missing", client_id=client_order_id,
                                 exchange_order_id=tracked_order.exchange_order_id, bitunix_state=new_state.name,
                                 cumulative=str(cumulative), booked=str(tracked_order.executed_amount_base),
                                 waited_s=round(time.monotonic() - first, 1),
                                 action="kept open in Hummingbot until Bitunix's deal list shows every fill: check "
                                        "the order on Bitunix")
            return self._order_update(tracked_order, tracked_order.current_state)
        self._fills_pending_since.pop(client_order_id, None)
        update = self._order_update(tracked_order, new_state)
        if new_state in CONSTANTS.TERMINAL_STATES:
            self._cancel_sent.discard(tracked_order.client_order_id)
            if self._balance_shows_fills(tracked_order):
                self._forget_settled(tracked_order)
                return update
            self._process_terminal_update(tracked_order, update, source="rest")
            return self._order_update(tracked_order, tracked_order.current_state)
        return update

    def _order_update(self, order: InFlightOrder, state: OrderState) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id) if order.exchange_order_id is not None else None,
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    # ------------------------------------------------------------------ terminal updates wait for the balance

    @staticmethod
    def _base_delta(order: InFlightOrder, fills: List[TradeUpdate]) -> Decimal:
        delta = Decimal("0")
        for fill in fills:
            base_fee = sum((fee.amount for fee in fill.fee.flat_fees if fee.token == order.base_asset), Decimal("0"))
            delta += (fill.fill_base_amount if order.trade_type is TradeType.BUY else -fill.fill_base_amount) - base_fee
        return delta

    def _note_fill_booked(self, order: InFlightOrder, fill: TradeUpdate) -> None:
        """Counted once per Bitunix fill id: a status read after a cancel can race the poll over the same deal row,
        and a double count looks like another order's fill to _balance_shows_fills (a 5 s hold, a false alarm)."""
        if not self._first_time(self._noted_fills, f"{fill.exchange_order_id}:{fill.trade_id}"):
            return
        asset = order.base_asset
        self._booked_base[asset] = self._booked_base.get(asset, Decimal("0")) + self._base_delta(order, [fill])

    def _balance_shows_fills(self, order: InFlightOrder) -> bool:
        """True once the base asset's total balance carries the order's own fills (other orders' fills booked since it
        was sent are taken off; 99% slack for fee rounding). Hotcoin's design (2026-10-06): reported before the balance
        shows the fill, the hold-band reads the old total and buys twice."""
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
        if self._balance_shows_fills(order):
            return
        started = time.monotonic()
        while time.monotonic() - started < CONSTANTS.FILL_BALANCE_WAIT_SECONDS:
            await asyncio.sleep(0.25)
            if self._balance_shows_fills(order):
                self._audit("fill-balance-wait", client_id=order.client_order_id, source=source,
                            waited_s=round(time.monotonic() - started, 3))
                return
        try:
            # _update_all_balances, not _update_balances: the in-flight snapshot must move with the balance it pairs
            # with, or get_available_balance counts the fills and locks since the old snapshot twice.
            await asyncio.wait_for(self._update_all_balances(), timeout=CONSTANTS.FILL_BALANCE_WAIT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Bitunix balance read for {order.client_order_id} failed: {e!r}")
        self._alarm("fill-balance-late", client_id=order.client_order_id, source=source,
                    waited_s=CONSTANTS.FILL_BALANCE_WAIT_SECONDS,
                    shows_fills_after_rest=self._balance_shows_fills(order))

    def _process_terminal_update(self, order: InFlightOrder, update: OrderUpdate, source: str = "rest") -> None:
        if update.new_state not in CONSTANTS.TERMINAL_STATES:
            self._order_tracker.process_order_update(update)
            return
        if self._balance_shows_fills(order):
            self._forget_settled(order)
            self._order_tracker.process_order_update(update)
            return
        if order.client_order_id in self._settling:
            return
        self._settling.add(order.client_order_id)
        safe_ensure_future(self._report_when_balance_shows_fills(order, update, source))

    async def _report_when_balance_shows_fills(self, order: InFlightOrder, update: OrderUpdate, source: str) -> None:
        try:
            await self._await_balance_showing_fills(order, source)
            self._order_tracker.process_order_update(update)
        finally:
            self._settling.discard(order.client_order_id)
            self._forget_settled(order)

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
        """No fee endpoint (probed 2026-10-07): DEFAULT_FEES estimates; each fill books Bitunix's own fee."""
        return

    # ------------------------------------------------------------------ balances

    async def _update_all_balances(self) -> None:
        """The base's, except that an empty answer not believed yet (BitunixEmptyBalanceUnconfirmed) is no traceback
        every poll: _update_balances says it once. Any failed read keeps the last balances AND the in-flight snapshot
        they pair with (a new snapshot over old balances would count fills and locks twice)."""
        try:
            await self._update_balances()
        except asyncio.CancelledError:
            raise
        except BitunixEmptyBalanceUnconfirmed:
            return
        except Exception as request_error:
            self.logger().warning(f"Failed to update balances. Error: {request_error}", exc_info=request_error)
            return
        if not self.real_time_balance_update:
            self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self.in_flight_orders.items()}
            self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    async def _update_balances(self) -> None:
        """GET user/account: [{coin, balance, balanceLocked}] (numbers). An EMPTY account answers data null (live).
        While we hold balances, null is believed only once it has held for EMPTY_BALANCE_CONFIRM_SECONDS over at least
        EMPTY_BALANCE_CONFIRM_READS reads with no order open: a stray null would drop every balance, which a running
        hold-band reads as a sell-out and buys back (review 2026-10-07). Until then BitunixEmptyBalanceUnconfirmed."""
        booked = dict(self._booked_base)      # the fills booked as the read goes out (_place_order_and_process_update)
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ACCOUNT_PATH, is_auth_required=True,
                                limit_id=CONSTANTS.ACCOUNT_PATH),
            "Error fetching Bitunix balances",
        )
        rows = response.get("data")
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise IOError(f"Bitunix balance answer is not a list | Bitunix response: {self._raw(response)}")
        if rows or not any(total > 0 for total in self._account_balances.values()):
            self._empty_since, self._empty_reads = None, 0
        else:
            now = time.monotonic()
            if self._empty_since is None:
                self._empty_since, self._empty_reads = now, 0
                self.logger().warning(
                    f"Bitunix answered an empty account while {len(self._account_balances)} balances are held: the "
                    f"last balances stay until it holds for {CONSTANTS.EMPTY_BALANCE_CONFIRM_SECONDS:.0f} s with no "
                    f"order open.")
            self._empty_reads += 1
            if (self.in_flight_orders or self._empty_reads < CONSTANTS.EMPTY_BALANCE_CONFIRM_READS
                    or now - self._empty_since < CONSTANTS.EMPTY_BALANCE_CONFIRM_SECONDS):
                raise BitunixEmptyBalanceUnconfirmed(
                    f"Bitunix answered an empty account ({self._empty_reads} reads over {now - self._empty_since:.0f} "
                    f"s, {len(self.in_flight_orders)} orders open): not believed yet")
            self._alarm("balances-empty", dropped=sorted(self._account_balances), reads=self._empty_reads,
                        empty_for_s=round(now - self._empty_since, 1))
            self._empty_since, self._empty_reads = None, 0
        self._audit_once("balance-rest-shape", assets=len(rows), sample=self._raw(rows[:3]))
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for entry in rows:
            asset = str(entry.get("coin") or "").upper()
            if not asset or entry.get("balance") is None:
                continue
            available = self._dec(entry.get("balance"))
            total = available + self._dec(entry.get("balanceLocked"))
            if self.in_flight_orders and self._dec(entry.get("balanceLocked")) > 0:
                # Is `balance` the available or the total amount? Settled by a read with an order open.
                self._audit_once(f"balance-semantics:{asset}", balance=str(entry.get("balance")),
                                 balance_locked=str(entry.get("balanceLocked")))
            remote_asset_names.add(asset)
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total
        for asset_name in local_asset_names.difference(remote_asset_names):
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)
        self._booked_base_at_read = booked

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for info in exchange_info.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                base, quote = str(info["base"]).upper(), str(info["quote"]).upper()
                symbol = f"{base}{quote}"
                mapping[symbol] = combine_to_hb_trading_pair(base=base, quote=quote)
                steps = info.get("precisions") or []
                if steps:
                    self._depth_steps[symbol] = str(steps[0])
            except Exception as exception:
                self.logger().error(f"Error parsing Bitunix pair {info.get('symbol')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair the startup list did not have. Bitunix's form is BASEQUOTE (the base's own default);
        its depth channel also needs the market's price step, which only coin_pair/list gives — re-read here."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.upper()}{quote.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        try:
            # A burst of runtime adds shares one read of the ~577 KB list.
            response = await self.market_list(max_age=CONSTANTS.MARKET_LIST_FRESH_SECONDS)
            for info in response.get("data") or []:
                if f"{info.get('base')}{info.get('quote')}".upper() == exchange_symbol and info.get("precisions"):
                    self._depth_steps[exchange_symbol] = str(info["precisions"][0])
        except Exception as e:
            self.logger().warning(f"Bitunix {trading_pair}: could not read its price step ({e}); its book stays empty "
                                  f"until the next market-list read.")
        self.logger().warning(f"Bitunix {trading_pair} was not among the markets listed at startup; mapped to "
                              f"{exchange_symbol}. Bitunix's answers to its requests will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """coin_pair/list (live 2026-10-07, fields beyond the docs included): precisions[0] = the finest price step,
        basePrecision = the quantity's decimals (10058 "volume precision error" otherwise), minVolume = the minimum
        quantity, minPrice = the minimum order VALUE in the quote (BTC "10": the docs call it "minimum trading
        amount"). minBuyPriceOffset / maxSellPriceOffset bound the price (-0.8 / 50); an order outside is refused and
        logged in full."""
        rules: List[TradingRule] = []
        for info in exchange_info_dict.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                    symbol=f"{info['base']}{info['quote']}".upper())
                steps = info.get("precisions") or []
                price_increment = Decimal(str(steps[0])) if steps else Decimal(1).scaleb(-int(info["quotePrecision"]))
                amount_increment = Decimal(1).scaleb(-int(info["basePrecision"]))
                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(info.get("minVolume")), amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_notional_size=self._dec(info.get("minPrice")),
                ))
            except Exception:
                self.logger().exception(f"Error parsing the Bitunix trading rule {info.get('symbol')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """The socket's ticker channel (close, ~1/s) answers first; a market it has no fresh close for is read over REST,
        a few at a time (there is no bulk ticker). Every requested pair gets a value, NaN when there is none: the
        tracker re-asks a pair it got nothing for at once."""
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        source = self.order_book_tracker.data_source if self.order_book_tracker is not None else None
        missing: List[Tuple[str, str]] = []
        for trading_pair in trading_pairs:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            except KeyError:
                continue
            close = source.last_price(symbol) if isinstance(source, BitunixAPIOrderBookDataSource) else None
            if close is not None:
                prices[trading_pair] = close
            else:
                missing.append((trading_pair, symbol))
        semaphore = asyncio.Semaphore(4)

        async def one(trading_pair: str, symbol: str) -> None:
            async with semaphore:
                try:
                    response = await self._api_get(path_url=CONSTANTS.LAST_PRICE_PATH, params={"symbol": symbol},
                                                   limit_id=CONSTANTS.LAST_PRICE_PATH)
                except Exception:
                    return
                if web_utils.is_ok(response) and response.get("data") not in (None, ""):
                    prices[trading_pair] = float(response["data"])

        await asyncio.gather(*(one(pair, symbol) for pair, symbol in missing))
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]
