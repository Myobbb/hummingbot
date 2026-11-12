import asyncio
import copy
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.blofin import (
    blofin_constants as CONSTANTS,
    blofin_utils as utils,
    blofin_web_utils as web_utils,
)
from hummingbot.connector.exchange.blofin.blofin_api_order_book_data_source import BlofinAPIOrderBookDataSource
from hummingbot.connector.exchange.blofin.blofin_api_user_stream_data_source import BlofinAPIUserStreamDataSource
from hummingbot.connector.exchange.blofin.blofin_auth import BlofinAuth
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


class BlofinBusinessError(IOError):
    """A request BloFin answered with a code other than "0" (a definite refusal), or one of our own codes for an answer
    that is not what it should be (CODE_ORDER_NOT_FOUND, CODE_EMPTY_ANSWER). IOError keeps every existing
    `except IOError` path working."""

    def __init__(self, message: str, code: Optional[str], msg: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.msg = msg


class BlofinPlacementUnknown(IOError):
    """A placement BloFin may have accepted (no answer, or "0" without an order id) that the lookup did not find: the
    order is NOT failed — it may rest on BloFin. It stays PENDING_CREATE and the status poll keeps looking for it; only
    the poll's age-gated not-found verdict fails it."""


class _LookupInconclusive(Exception):
    """The placement lookup could not tell: a read failed."""


class BlofinExchange(ExchangePyBase):
    """
    BloFin spot connector: the undocumented spot REST namespace (/api/v1/spot/*, instType=SPOT on every call) for
    everything private, the website's spot socket for books.

    Reference: the docs mirror (VS_code_projects/MDs/blofin-api/, futures-only), live probes of 2026-11-11 from myserver
    and the wiki page trading/exchanges/blofin-api. What the docs could not settle is audit-logged ([BF-AUDIT]) rather
    than guessed; money guards log as [BF-ALARM].

    Scope: LIMIT orders only, which is all arb_l and the position balancer send.

    What BloFin does not have, and what stands in for it:
      - No spot push for an API key. While any order is open the poll runs every ORDER_POLL_INTERVAL (1 s): ONE
        orders-pending read for every open order (on the idle poll too: it feeds the orphan guard), order-detail only
        for an order the list doesn't show (it closed, or isn't listed yet), fills-history only when an order's filled
        quantity grew (backing off while the rows lag), one balance read.
      - Silent not-found: order-detail of an unknown id answers code 0 with no data (live). "Not found" is decided
        here, never from a code, and only for a placement BloFin never confirmed (_adopt_placed_order): an order BloFin
        issued an id for stays readable, cancelled too, so it is never failed for an empty read (_resolve_missing_order).
        A cancel of an order that is not open answers 106080 for a made-up id and a finished one alike, so it proves
        nothing either: the next status read settles it.
      - Client ids are real: order-detail and cancel-order both take clientOrderId (live, made-up ids). A placement
        that got no answer is looked up by it (_find_placed_order); cancels go by orderId.
      - Fills: fills-history (documented under Tax) gives each fill's tradeId, fillSize, fillPrice, fee, feeCurrency;
        it carries no client id (joined by orderId).
    """

    web_utils = web_utils

    def __init__(
        self,
        blofin_api_key: str,
        blofin_secret_key: str,
        blofin_passphrase: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature must match ConnectorSetting.conn_init_parameters in this fork (balance_asset_limit and
        rate_limits_share_pct, no config map): an upstream-style client_config_map crashes `balance` (CoinEx)."""
        self._api_key = blofin_api_key
        self._secret_key = blofin_secret_key
        self._passphrase = blofin_passphrase
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: Dict[str, None] = {}
        self._alarm_seen: Dict[str, None] = {}
        self._detail_cache: Dict[str, Dict[str, Any]] = {}   # exchange id -> row, for one poll (both passes)
        self._last_private_read: float = 0.0                 # wall time of the last successful signed read
        self._last_time_sync: float = 0.0
        self._market_list_cache: Optional[Tuple[float, Dict[str, Any]]] = None
        self._market_list_lock = asyncio.Lock()
        self._ticker_cache: Optional[Tuple[float, Dict[str, float]]] = None
        # Per-order state, by client id; entries of orders no longer tracked are dropped every poll (_purge_order_state).
        self._placing: Set[str] = set()                      # placements awaiting BloFin's answer
        self._size_checked: Set[str] = set()                 # orders whose quantity BloFin confirmed
        self._sent_at_ms: Dict[str, int] = {}                # server ms just before the placement
        self._cancel_sent: Dict[str, float] = {}             # monotonic time BloFin took (or answered 106080 to) our cancel
        self._short_since: Dict[str, float] = {}             # monotonic time its terminal state was first held for fills
        self._fills_retry: Dict[str, Tuple[float, float, Tuple[Decimal, bool]]] = {}  # next read, interval, what it saw
        self._unreadable_until: Dict[str, float] = {}        # no order-detail read before this (monotonic)
        self._final_rows: Dict[str, Dict[str, Any]] = {}     # its terminal row: final, never read again
        self._fill_booked_at: Dict[str, int] = {}            # _balance_reads_sent when its last fill was booked
        self._settling: Set[str] = set()
        # The completion gate (_balance_shows_fills): balance reads are numbered as they go out, and the balances are
        # from read number _balance_applied (an older answer arriving later is dropped).
        self._balance_reads_sent = 0
        self._balance_applied = 0
        # The orphan guard (_check_orphans): client ids this process sent to BloFin; the orders it has alarmed about.
        self._placed_ids: Dict[str, None] = {}
        self._orphan_ids: Dict[str, None] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # No balance push exists: the REST poll is authoritative and the base's in-flight snapshot keeps `available`
        # right between polls.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BlofinAuth:
        return BlofinAuth(api_key=self._api_key, secret_key=self._secret_key, passphrase=self._passphrase,
                          time_provider=self._time_synchronizer)

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
        return CONSTANTS.INSTRUMENTS_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.INSTRUMENTS_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.TICKERS_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # A cancel's answer carries no state (docs: orderId, clientOrderId, code, msg): the next status read settles it.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def last_private_read_time(self) -> float:
        return self._last_private_read

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
        return BlofinAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BlofinAPIUserStreamDataSource(connector=self)

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [BF-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[BF-AUDIT] {tag} {rendered}")

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
        """A money guard fired: one [BF-ALARM] WARNING, whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[BF-ALARM] {tag} {rendered}")

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
        BloFin answers every request with HTTP 200, so the base class's resync-and-retry (which runs only on a transport
        IOError) never sees an expired timestamp. Here: numbers are parsed exactly (Decimal), every request has a timeout
        (REQUEST_TIMEOUT unless given: a placement's is PLACE_ORDER_TIMEOUT), and 152405 re-syncs the clock and repeats
        the request once (the timestamp is checked before anything else — live, before even the passphrase — so the
        repeat can't place twice). A signed request BloFin answered "0" marks private REST as working
        (BlofinAPIUserStreamDataSource).
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
                timeout=timeout if timeout is not None else CONSTANTS.REQUEST_TIMEOUT,
            )
            result = web_utils.loads(await response.text())
            if attempt == 0 and is_auth_required and web_utils.error_code(result) == CONSTANTS.CODE_TIMESTAMP_EXPIRED:
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer(force=True)
                continue
            if is_auth_required and web_utils.is_ok(result):
                self._last_private_read = time.time()
            if is_auth_required and web_utils.error_code(result) in CONSTANTS.AUTH_CODES:
                self._alarm_once(f"auth-refused:{web_utils.error_code(result)}", "auth-refused",
                                 path=path_url, response=self._raw(result),
                                 action="every signed BloFin call fails: check the key (connect blofin)")
            return result
        return result

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False, force: bool = False):
        """The base re-reads the server time on every status poll — every second while orders are open. BloFin's clock
        comes from an HTTP Date header at 1 s resolution against a 60 s window, so it is re-read every
        TIME_SYNC_INTERVAL, or at once on a 152405."""
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
        """The spot instrument list (every market with its rules and state), read by the symbol map, the trading rules,
        the book's trading switch and runtime adds. A read younger than max_age (s) is shared instead of read again,
        and callers that ask while a read is under way wait for it."""
        async with self._market_list_lock:
            cached = self._market_list_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            response = await self._api_get(path_url=CONSTANTS.INSTRUMENTS_PATH, params={"instType": CONSTANTS.INST_TYPE},
                                           limit_id=CONSTANTS.INSTRUMENTS_PATH)
            self._raise_on_error(response, f"Error reading BloFin's spot instruments ({CONSTANTS.INSTRUMENTS_PATH})")
            # An empty or refused list would become an empty symbol map and no trading rules until the next poll, with
            # every order failing "no rule": raising keeps the last map and rules.
            if not isinstance(response.get("data"), list) or not response["data"]:
                raise IOError(f"BloFin's spot instrument list came back empty | BloFin response: {self._raw(response)}")
            self._market_list_cache = (time.monotonic(), response)
            return response

    async def markets_switched_off(self) -> Set[str]:
        """The instIds whose instrument state is not `live` (the book shows them empty; their rules stay)."""
        response = await self.market_list(max_age=CONSTANTS.TRADING_SWITCH_INTERVAL / 2)
        off: Set[str] = set()
        for info in response.get("data") or []:
            if isinstance(info, dict) and info.get("instId") and str(info.get("state") or "").lower() != "live":
                off.add(str(info["instId"]))
                self._audit_once(f"instrument-state:{info.get('state')}", inst_id=info.get("instId"))
        return off

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.TICKERS_PATH, params=dict(CONSTANTS.SERVER_TIME_PARAMS),
                                       limit_id=CONSTANTS.TICKERS_PATH)
        if not web_utils.is_ok(response):
            raise IOError(f"Unexpected BloFin answer to the network check: {response}")

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
            raise BlofinBusinessError(f"{context}: code {code} ({msg}) | BloFin response: {self._raw(response)}",
                                      code, msg)
        return response

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """BloFin refuses with HTTP 200 and a code, which the base would log as a network error with a traceback: a
        refusal is logged as one, with the request and BloFin's complete answer."""
        if isinstance(exception, BlofinPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll looks for it "
                                  f"on BloFin and fails it only once it is not there.")
            return
        if isinstance(exception, BlofinBusinessError):
            self.logger().warning(f"BloFin rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return CONSTANTS.CODE_TIMESTAMP_EXPIRED in str(request_exception)

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        return isinstance(error, BlofinBusinessError) and error.code == CONSTANTS.CODE_ORDER_NOT_FOUND

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # 106080 answers a made-up id and a filled or cancelled order alike: it never proves "not found".
        return False

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (and forgets a FAILED order that may
        still rest on the venue). Only our own verdict counts — an unconfirmed placement not found by its client id, old
        enough (_adopt_placed_order); anything else is a WARNING and the order stays tracked (Hotcoin's lesson,
        2026-10-06). CODE_EMPTY_ANSWER is the poll's sentinel: nothing to report now."""
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        if isinstance(error, BlofinBusinessError) and error.code == CONSTANTS.CODE_EMPTY_ANSWER:
            # Not readable (yet), or held (fills, balance, a pending cancel): expected, every poll until it resolves.
            self.logger().debug(f"BloFin status of {order.client_order_id}: nothing to report now: {error}")
            return
        self.logger().warning(f"BloFin status read for {order.client_order_id} failed, the order stays tracked: "
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
        # BloFin has no spot private stream for an API key: nothing ever arrives here.
        while True:
            await asyncio.sleep(3600)

    @staticmethod
    def _rows(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = response.get("data")
        if data is None:
            return []
        if isinstance(data, dict):
            return [data]
        if not isinstance(data, list):
            raise IOError(f"Unexpected BloFin answer shape: {data!r:.200}")
        return [row for row in data if isinstance(row, dict)]

    async def _update_order_status(self) -> None:
        """One poll (the base's fills pass, then its status pass): ONE orders-pending read covers every open order and
        is shared by both passes; order-detail is read only for an order it doesn't list (_fetch_order_detail). The same
        read feeds the orphan guard, so it is made on the idle poll too (every SHORT_POLL_INTERVAL). First the per-order
        state of orders no longer tracked is dropped."""
        self._detail_cache.clear()
        self._purge_order_state()
        rows: Optional[List[Dict[str, Any]]] = None
        try:
            rows = await self._open_orders()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._order_tracker.active_orders:
                # Each open order is then read on its own this poll (more requests, same answers).
                self.logger().warning(f"BloFin open-orders read failed; reading each order's detail this poll: {e!r}")
            else:
                self.logger().debug(f"BloFin open-orders read failed (no order open): {e!r}")
        if rows is not None:
            for row in rows:
                if row.get("orderId") not in (None, ""):
                    self._detail_cache[str(row["orderId"])] = row
            self._check_orphans(rows)
        await super()._update_order_status()

    async def _open_orders(self) -> List[Dict[str, Any]]:
        """Every open spot order (one page of PENDING_PAGE_LIMIT; a second page only if the first was full)."""
        params = {"instType": CONSTANTS.INST_TYPE, "limit": str(CONSTANTS.PENDING_PAGE_LIMIT)}
        rows: List[Dict[str, Any]] = []
        for _ in range(2):
            response = self._raise_on_error(
                await self._api_get(path_url=CONSTANTS.ORDERS_PENDING_PATH, params=params, is_auth_required=True,
                                    limit_id=CONSTANTS.ORDERS_PENDING_PATH),
                "Error listing BloFin open orders")
            page = self._rows(response)
            self._audit_once("pending-fields", rows=self._raw(page[:2]))
            rows.extend(page)
            if len(page) < CONSTANTS.PENDING_PAGE_LIMIT:
                return rows
            params = dict(params, after=str(page[-1].get("orderId")))
        self._alarm_once("pending-pages", "pending-pages", rows=len(rows),
                         action="more open orders than two pages: the rest are read one by one")
        return rows

    def _purge_order_state(self) -> None:
        live = set(self._order_tracker.all_fillable_orders) | set(self._placing)
        for registry in (self._sent_at_ms, self._cancel_sent, self._short_since, self._fills_retry,
                         self._unreadable_until, self._final_rows, self._fill_booked_at):
            for client_order_id in [c for c in registry if c not in live]:
                del registry[client_order_id]
        self._size_checked &= live

    def _check_orphans(self, rows: List[Dict[str, Any]]) -> None:
        """The orphan guard (runbook §1.5), on every poll's open-order list. A row carrying our client-id prefix that no
        active order holds:
          - sent by this process, and Hummingbot gave it up (FAILED; this fork's tracker forgets an order it fails for
            not-found at once) or forgot it -> cancelled by its exchange id, [BF-ALARM] failed-order-alive. Its fills
            are not booked to Hummingbot (it was failed); the alarm carries BloFin's filledSize, and P3 books them from
            BloFin's own fill list;
          - not sent by this process (another process's, or an earlier run's) -> [BF-ALARM] untracked-order-open, NOT
            cancelled (Pavel's call, as for Hotcoin and Poloniex);
          - finished in Hummingbot a moment ago (filled or cancelled, still in the tracker's 30 s cache) -> nothing:
            this list was read before that. Should BloFin still list it after the cache, it is cancelled as above.
        Each order once."""
        tracker = self._order_tracker
        active = tracker.active_orders
        for row in rows:
            client_order_id = str(row.get("clientOrderId") or "")
            exchange_order_id = str(row.get("orderId") or "")
            if (not client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) or not exchange_order_id
                    or client_order_id in active or client_order_id in self._placing
                    or exchange_order_id in self._orphan_ids):
                continue
            order = tracker.all_fillable_orders.get(client_order_id)
            if order is not None and not order.is_failure:
                continue
            self._first_time(self._orphan_ids, exchange_order_id)
            fields = dict(client_id=client_order_id, order_id=exchange_order_id, inst_id=row.get("instId"),
                          state=row.get("state"), side=row.get("side"), size=row.get("size"), price=row.get("price"),
                          filled=row.get("filledSize"))
            if order is None and client_order_id not in self._placed_ids:
                self._alarm("untracked-order-open", **fields, action="not cancelled: not placed by this process")
                continue
            self._alarm("failed-order-alive", **fields,
                        hmb_state=order.current_state.name if order is not None else "no longer tracked",
                        action="cancelling")
            safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id, symbol=str(row.get("instId") or "")))

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """With no trading rule the base stops at a bare KeyError before tracking the order (XT B2-USDT, 2026-09-23):
        here the order is tracked and failed the way the base fails its own pre-send checks."""
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to BloFin: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from GET {CONSTANTS.INSTRUMENTS_PATH}).")
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
            "instType": CONSTANTS.INST_TYPE,
            "instId": symbol,
            "side": CONSTANTS.SIDE[trade_type],
            "orderType": CONSTANTS.ORDER_TYPE_LIMIT,
            "price": self._format_decimal(price),
            "size": self._format_decimal(amount),    # the BASE quantity (minSize / lotSize are base units)
            "clientOrderId": order_id,
        }
        sent_at_ms = self._server_ms()
        self._sent_at_ms[order_id] = sent_at_ms
        self._first_time(self._placed_ids, order_id)      # the orphan guard tells our own orders by it
        # While the answer is awaited only this placement looks the order up, not the status poll (Bitunix review,
        # 2026-10-07). Discarded on return, and the base gives the order its id with no await in between
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
                # No answer: BloFin may still have accepted the order (the base would mark it FAILED and stop tracking a
                # live order). Looked up by its client id.
                return await self._locate_unconfirmed_placement(order_id, symbol, repr(transport_error)), self._now()
            self._audit("place-order", client_id=order_id, request=self._raw(body), response=self._raw(response))
            try:
                rows = self._rows(response) if isinstance(response, dict) else []
            except IOError:
                rows = []        # an answer of an unexpected shape: looked up below, like one without an order id
            row = rows[0] if rows else {}
            # The answer's code: the per-order one inside data when it isn't "0" (a refusal, live: top-level
            # {"code":"1","msg":"All operations failed"} with e.g. 106089 "Order minimum buy price is 14,886.29 USDT"
            # per order), else the top-level one.
            item_code = row.get("code")
            if item_code not in (None, "", CONSTANTS.CODE_OK, 0):
                code: Optional[str] = str(item_code)
            elif not web_utils.is_ok(response):
                code = web_utils.error_code(response) or "?"
            else:
                code = None
            if code is not None:
                msg = row.get("msg") or (response.get("msg") if isinstance(response, dict) else None)
                if self._is_definite_refusal(code):
                    raise BlofinBusinessError(
                        f"Order {order_id} refused, request {self._raw(body)}: code {code} ({msg}) | "
                        f"BloFin response: {self._raw(response)}", code, msg)
                # Not a known refusal (an internal error, a duplicate client id, a code never seen): it proves no more
                # than a timeout, and BloFin may have the order. Looked up by its client id.
                cause = f"code {code} ({msg}), not a known refusal | BloFin response: {self._raw(response)}"
                return await self._locate_unconfirmed_placement(order_id, symbol, cause), self._now()
            exchange_order_id = row.get("orderId")
            if exchange_order_id in (None, ""):
                # "0" (accepted) without an order id: BloFin may have the order; looked up the same way.
                cause = f"code 0 without an order id | BloFin response: {self._raw(response)}"
                return await self._locate_unconfirmed_placement(order_id, symbol, cause), self._now()
            if str(row.get("clientOrderId") or order_id) != order_id:
                self._alarm("place-client-id-mismatch", client_id=order_id, response=self._raw(response))
            return str(exchange_order_id), self._now()
        finally:
            self._placing.discard(order_id)

    @staticmethod
    def _is_definite_refusal(code: str) -> bool:
        """A placement code that proves nothing was placed (PLACE_REFUSED_CODES / PLACE_REFUSED_PREFIXES)."""
        return code in CONSTANTS.PLACE_REFUSED_CODES or code.startswith(CONSTANTS.PLACE_REFUSED_PREFIXES)

    async def _locate_unconfirmed_placement(self, order_id: str, symbol: str, cause: str) -> str:
        """A placement BloFin may have accepted without telling us the order's id. Found on BloFin -> its id. Not found,
        or the lookup could not tell -> BlofinPlacementUnknown: the order is NOT failed (it may rest on BloFin); it stays
        PENDING_CREATE and the status poll keeps looking (_fetch_order_detail), failing it only by its age-gated
        not-found verdict."""
        found, verdict = None, "not on BloFin yet"
        try:
            found = await self._find_placed_order(order_id, symbol, CONSTANTS.PLACEMENT_LOOKUP_DELAYS)
        except _LookupInconclusive as e:
            verdict = f"lookup inconclusive: {e}"
        self._alarm("place-order-unconfirmed", client_id=order_id, cause=cause, found_on_exchange=found,
                    verdict=None if found is not None else verdict)
        if found is not None:
            return found
        raise BlofinPlacementUnknown(f"BloFin did not confirm the placement ({cause}); {verdict}")

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
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
        # The first status read checks the quantity BloFin booked (the size guard): do it now, not at the next tick.
        self._poll_notifier.set()
        return exchange_order_id

    async def _detail_by(self, symbol: str, **ids: str) -> Optional[Dict[str, Any]]:
        """order-detail by orderId or clientOrderId: the row, or None when BloFin answers code 0 with no data (its silent
        "not found", live). Any refusal raises."""
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ORDER_DETAIL_PATH,
                                params={"instType": CONSTANTS.INST_TYPE, "instId": symbol, **ids},
                                is_auth_required=True, limit_id=CONSTANTS.ORDER_DETAIL_PATH),
            f"Error reading BloFin order {ids}")
        rows = self._rows(response)
        return rows[0] if rows else None

    async def _find_placed_order(self, client_order_id: str, symbol: str, delays: Tuple[float, ...]) -> Optional[str]:
        """The placement guard: the order carrying our client id (order-detail ?clientOrderId=), looked for after each
        delay (s). Found -> its orderId. None -> not (yet). _LookupInconclusive -> the read failed, so it could not
        tell. An order that merely looks like ours (side, price, size) is never adopted: only the id counts."""
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            own = self._order_tracker.fetch_order(client_order_id=client_order_id)
            if own is not None and own.exchange_order_id is not None:
                return str(own.exchange_order_id)         # it got its id meanwhile
            try:
                row = await self._detail_by(symbol, clientOrderId=client_order_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                raise _LookupInconclusive(f"order-detail by clientOrderId failed: {e!r}")
            if row is not None and row.get("orderId") not in (None, ""):
                if str(row.get("clientOrderId") or client_order_id) != client_order_id:
                    self._alarm_once(f"lookup-client-id-mismatch:{client_order_id}", "lookup-client-id-mismatch",
                                     client_id=client_order_id, row=self._raw(row), action="not adopted")
                    return None
                return str(row["orderId"])
        return None

    async def _cancel_by_exchange_id(self, exchange_order_id: str, symbol: Optional[str] = None,
                                     trading_pair: Optional[str] = None) -> None:
        """A guard's cancel (the size guard, the orphan guard), by BloFin's order id: BloFin's own symbol, else the
        pair's from the symbol map. The answer is audit-logged; a failure is alarmed."""
        try:
            if not symbol:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                            data={"instType": CONSTANTS.INST_TYPE, "instId": symbol,
                                                  "orderId": exchange_order_id},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-by-exchange-id", exchange_order_id=exchange_order_id, response=self._raw(response))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._alarm("cancel-by-exchange-id-failed", exchange_order_id=exchange_order_id, error=repr(e))

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        body = {"instType": CONSTANTS.INST_TYPE, "instId": symbol, "orderId": str(exchange_order_id)}
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, data=body, is_auth_required=True,
                                        limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    response=self._raw(response))
        self._raise_on_error(response, f"BloFin refused to cancel order {order_id} ({exchange_order_id})")
        rows = self._rows(response)
        item_code = rows[0].get("code") if rows else None
        if item_code not in (None, "", CONSTANTS.CODE_OK, 0):
            raise BlofinBusinessError(f"BloFin refused to cancel order {order_id} ({exchange_order_id}): code "
                                      f"{item_code} ({rows[0].get('msg')}) | BloFin response: {self._raw(response)}",
                                      str(item_code), rows[0].get("msg"))
        self._cancel_sent[order_id] = time.monotonic()
        # A row read just before the cancel is not this poll's answer any more; and one still in flight can't set the
        # order back to OPEN either (_request_order_status keeps PENDING_CANCEL).
        self._detail_cache.pop(str(exchange_order_id), None)
        self._poll_notifier.set()
        return True

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        """The base's, except PENDING_CANCEL is applied only to an order still live in the tracker: the poll may have
        finished it while the cancel was in flight, and this fork's update_with_order_update would set a finished order
        back (runbook §1.5; Poloniex, 2026-10-09)."""
        cancelled = await self._place_cancel(order.client_order_id, order)
        if cancelled and not order.is_done and order.client_order_id in self._order_tracker.all_updatable_orders:
            self._order_tracker.process_order_update(self._order_update(order, OrderState.PENDING_CANCEL))
        return cancelled

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """The base's cancel path, except that a refusal is a WARNING with BloFin's answer (no traceback) and reads the
        order's status at once (106080 = filled, cancelled or unknown: the status says which), an HTTP timeout on the
        cancel reads the status too (the cancel may have landed), and an order still without an exchange id counts no
        not-found strike."""
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if order.exchange_order_id is None:
                # The base counts this as a not-found strike, with no look at BloFin: an order whose placement went
                # unconfirmed may rest there. Only the status poll's lookup decides (_fetch_order_detail).
                self.logger().warning(f"BloFin cancel of {order.client_order_id} not sent: the order has no exchange "
                                      f"id yet. The status poll is looking for it on BloFin.")
            else:
                self.logger().warning(f"BloFin did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except BlofinBusinessError as refusal:
            if refusal.code == CONSTANTS.CODE_CANCEL_NOT_OPEN:
                self._cancel_sent.setdefault(order.client_order_id, time.monotonic())
            self.logger().warning(f"BloFin refused to cancel {order.client_order_id}: {refusal}. Reading its status "
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
        except BlofinBusinessError as e:
            if e.code == CONSTANTS.CODE_EMPTY_ANSWER:
                self.logger().debug(f"BloFin status of {order.client_order_id} after its cancel: nothing to report "
                                    f"now: {e}")
                return
            self.logger().warning(f"BloFin status read for {order.client_order_id} after its cancel failed; the next "
                                  f"poll reads it again: {e!r}")
        except Exception as e:
            self.logger().warning(f"BloFin status read for {order.client_order_id} after its cancel failed; the next "
                                  f"poll reads it again: {e!r}")

    def _order_state(self, state: Any, executed: Decimal, original: Decimal) -> OrderState:
        mapped = CONSTANTS.ORDER_STATE.get(str(state or "").lower())
        if mapped is not None:
            if mapped is OrderState.FAILED and executed > 0:
                mapped = OrderState.CANCELED     # a failure after fills keeps its fills: never a placement failure
            return mapped
        self._audit_once(f"unknown-state:{state}", state=state, executed=str(executed), original=str(original))
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    # ------------------------------------------------------------------ order detail (REST) and fills

    def _age(self, order: InFlightOrder) -> float:
        return max(0.0, self._now() - float(order.creation_timestamp or self._now()))

    async def _resolve_missing_order(self, order: InFlightOrder, symbol: str) -> Dict[str, Any]:
        """order-detail answered no data (BloFin's silent "not found", live) for an id BloFin itself issued. The order's
        row from the recent order history stands in. Failing that, the order is NOT failed: BloFin keeps every order
        readable, a cancelled one included (live), so an id it issued that no read finds means its reads are empty for
        now — a degraded backend answers exactly this (code 0, no data). Failing it would forget an order that may rest
        and fill on BloFin (review, 2026-11-11). It stays tracked; once older than NOT_FOUND_MIN_AGE it is read again
        every UNREADABLE_RETRY_S, with one [BF-ALARM] order-unreadable. Only a placement BloFin never confirmed can be
        failed by not-found (_adopt_placed_order)."""
        response = await self._api_get(path_url=CONSTANTS.ORDERS_HISTORY_PATH,
                                       params={"instType": CONSTANTS.INST_TYPE, "instId": symbol,
                                               "limit": str(CONSTANTS.PENDING_PAGE_LIMIT)},
                                       is_auth_required=True, limit_id=CONSTANTS.ORDERS_HISTORY_PATH)
        for row in self._rows(self._raise_on_error(response, f"Error reading BloFin order history on {symbol}")):
            if str(row.get("orderId")) == str(order.exchange_order_id):
                self._audit_once("detail-empty-but-in-history", exchange_order_id=order.exchange_order_id,
                                 state=row.get("state"))
                return row
        age = self._age(order)
        if age >= CONSTANTS.NOT_FOUND_MIN_AGE:
            self._unreadable_until[order.client_order_id] = time.monotonic() + CONSTANTS.UNREADABLE_RETRY_S
            self._alarm_once(f"order-unreadable:{order.client_order_id}", "order-unreadable",
                             client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                             age_s=round(age, 1),
                             action=f"kept tracked, read again every {CONSTANTS.UNREADABLE_RETRY_S:.0f} s: BloFin "
                                    f"keeps every order readable, so its reads are empty for now")
        raise BlofinBusinessError(f"Order {order.client_order_id} ({order.exchange_order_id}) not readable on BloFin "
                                  f"(age {age:.1f} s): no detail, not open, not in the recent history",
                                  CONSTANTS.CODE_EMPTY_ANSWER)

    @staticmethod
    def _is_terminal_row(row: Dict[str, Any]) -> bool:
        return CONSTANTS.ORDER_STATE.get(str(row.get("state") or "").lower()) in CONSTANTS.TERMINAL_STATES

    async def _fetch_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """The order's row, cheapest first: its terminal row once read (final: never read again, and no stale open row
        can follow it), the poll's orders-pending read when it lists the order, else order-detail (read once per poll:
        the fills and status passes share it; a cancel drops its order's entry). An order without an exchange id is
        looked up on BloFin first (_adopt_placed_order); one whose reads came back empty is read again only every
        UNREADABLE_RETRY_S (_resolve_missing_order)."""
        client_order_id = order.client_order_id
        final = self._final_rows.get(client_order_id)
        if final is not None:
            return final
        exchange_order_id = order.exchange_order_id
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        if exchange_order_id is None:
            exchange_order_id = await self._adopt_placed_order(order, symbol)
        exchange_order_id = str(exchange_order_id)
        detail = self._detail_cache.get(exchange_order_id)
        if detail is None:
            if time.monotonic() < self._unreadable_until.get(client_order_id, 0.0):
                raise BlofinBusinessError(f"Order {client_order_id} ({exchange_order_id}): BloFin's reads came back "
                                          f"empty; read again shortly", CONSTANTS.CODE_EMPTY_ANSWER)
            detail = await self._detail_by(symbol, orderId=exchange_order_id)
            if not detail:
                detail = await self._resolve_missing_order(order, symbol)
            self._audit_once("detail-fields", exchange_order_id=exchange_order_id, detail=self._raw(detail))
            self._detail_cache[exchange_order_id] = detail
        self._unreadable_until.pop(client_order_id, None)
        if self._is_terminal_row(detail):
            self._final_rows[client_order_id] = detail
        return detail

    async def _adopt_placed_order(self, order: InFlightOrder, symbol: str) -> str:
        """The poll's side of an unconfirmed placement: one look for the order on BloFin, no waits. Found -> the order gets
        that id. Not readable yet (CODE_EMPTY_ANSWER, no strike): its placement still awaits the answer (only the
        placement looks then), the lookup could not tell, or the order is younger than NOT_FOUND_MIN_AGE. Otherwise
        CODE_ORDER_NOT_FOUND: a strike toward FAILED (the tracker's lost-order count)."""
        client_order_id = order.client_order_id
        if client_order_id in self._placing:
            raise BlofinBusinessError(f"Order {client_order_id}: its placement still awaits BloFin's answer",
                                      CONSTANTS.CODE_EMPTY_ANSWER)
        try:
            found = await self._find_placed_order(client_order_id, symbol, (0.0,))
        except _LookupInconclusive as e:
            raise BlofinBusinessError(f"Order {client_order_id} has no exchange id yet: {e}",
                                      CONSTANTS.CODE_EMPTY_ANSWER)
        if found is None:
            if self._age(order) < CONSTANTS.NOT_FOUND_MIN_AGE:
                raise BlofinBusinessError(f"Order {client_order_id} has no exchange id yet",
                                          CONSTANTS.CODE_EMPTY_ANSWER)
            raise BlofinBusinessError(f"Order {client_order_id} is not on BloFin (looked up by client id)",
                                      CONSTANTS.CODE_ORDER_NOT_FOUND)
        self._alarm("placement-found", client_id=client_order_id, exchange_order_id=found)
        order.update_exchange_order_id(found)
        return found

    def _check_size(self, order: InFlightOrder, detail: Dict[str, Any]) -> bool:
        """The size guard: BloFin's own `size` of the order must be ours (spot `size` is taken as the base quantity: the
        instruments' minSize and lotSize are base units; the first fills settle it). A mismatch — e.g. BloFin reading
        `size` as a quote amount, which on a cheap token is an order hundreds of times too big or small — cancels the
        order at once. True if the order may proceed. The first read also checks that BloFin kept the client id."""
        if order.client_order_id in self._size_checked:
            return True
        self._check_client_id(order, detail)
        if detail.get("size") in (None, ""):
            return True      # can't tell from this row; the next one may
        quantity = self._dec(detail.get("size"))
        self._audit_once("size-semantics", client_id=order.client_order_id, sent=str(order.amount),
                         blofin_size=str(quantity), state=detail.get("state"), detail=self._raw(detail))
        if abs(quantity - order.amount) <= order.amount * CONSTANTS.SIZE_MISMATCH_TOLERANCE:
            self._size_checked.add(order.client_order_id)
            return True
        self._alarm("size-semantics", client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                    sent_base=str(order.amount), blofin_size=str(quantity),
                    action="cancelling; spot `size` semantics in blofin_exchange._place_order must be checked")
        safe_ensure_future(self._cancel_by_exchange_id(str(order.exchange_order_id), trading_pair=order.trading_pair))
        self._size_checked.add(order.client_order_id)
        return False

    def _check_client_id(self, order: InFlightOrder, detail: Dict[str, Any]) -> None:
        """The placement guard finds unanswered orders by the client id, and the asset_manager tells Hummingbot's fills
        by it. An order read back without it means both are blind: one [BF-ALARM] per session."""
        kept = str(detail.get("clientOrderId") or "")
        if kept != order.client_order_id:
            self._alarm_once("client-id-not-kept", "client-id-not-kept", client_id=order.client_order_id,
                             exchange_order_id=order.exchange_order_id, blofin_client_id=kept,
                             action="unanswered placements can't be found by client id; check BloFin's clientOrderId")

    def _fee_token(self, order: InFlightOrder, row: Dict[str, Any]) -> str:
        """The fee's asset. BloFin's spot rows name it by ROLE, not by coin (live 2026-11-11): `feeCurrency`
        "base_currency" on the probe's taker BUY (0.1% charged in BTC: 0.00009 bought, 0.00008991 credited) and
        "quote_currency" on a sell's order row: the received asset. A coin name, should one ever come, is taken as is;
        a missing value falls back to the received asset (base on a buy, quote on a sell)."""
        token = str(row.get("feeCurrency") or "").strip()
        role = token.lower()
        if role == CONSTANTS.FEE_CURRENCY_BASE:
            return order.base_asset
        if role == CONSTANTS.FEE_CURRENCY_QUOTE:
            return order.quote_asset
        if token:
            self._audit_once(f"fee-currency-coin:{token}", row=self._raw(row))
            return token.upper()
        self._audit_once("fee-currency-missing", row=self._raw(row))
        return order.base_asset if order.trade_type is TradeType.BUY else order.quote_asset

    async def _fills_from_rows(self, order: InFlightOrder, cumulative: Decimal, created_ms: Optional[int] = None,
                               terminal: bool = False) -> List[TradeUpdate]:
        """BloFin's own fill rows for the order, each a TradeUpdate named by BloFin's tradeId (the AM keys its ledger rows
        by the same id), booked once, never past the order's cumulative filled quantity or its size. Read only when the
        row's filledSize is ahead of what is booked; after a read that left the order short, again after
        FILLS_RETRY_MIN_S, doubling to FILLS_RETRY_MAX_S while the row shows nothing new (a new filledSize or a terminal
        state reads at once): one lagging order can't eat the IP budget the placements share."""
        tolerance = Decimal("1e-12")
        held = order.executed_amount_base
        if cumulative <= held + tolerance or order.exchange_order_id is None:
            return []
        client_order_id = order.client_order_id
        if cumulative > order.amount * (1 + CONSTANTS.SIZE_MISMATCH_TOLERANCE):
            self._alarm_once(f"fill-over-amount:{client_order_id}:{cumulative}", "fill-over-amount",
                             client_id=client_order_id, cumulative=str(cumulative), amount=str(order.amount))
            return []
        seen = (cumulative, terminal)
        retry = self._fills_retry.get(client_order_id)
        if retry is not None and retry[2] == seen and time.monotonic() < retry[0]:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.FILLS_PATH,
                                params={"instType": CONSTANTS.INST_TYPE, "instId": symbol,
                                        "orderId": str(order.exchange_order_id), "limit": "100"},
                                is_auth_required=True, limit_id=CONSTANTS.FILLS_PATH),
            f"Error fetching fills of order {order.client_order_id} ({order.exchange_order_id})")
        rows = [r for r in self._rows(response) if str(r.get("orderId") or order.exchange_order_id)
                == str(order.exchange_order_id)]
        self._audit_once("fill-fields", exchange_order_id=order.exchange_order_id, rows=self._raw(rows[:3]))
        fills: List[TradeUpdate] = []
        booked = held
        for row in sorted(rows, key=lambda r: (web_utils.to_ms(r.get("ts")) or 0, str(r.get("tradeId")))):
            trade_id = str(row.get("tradeId") or "")
            if not trade_id or trade_id in order.order_fills:
                continue
            quantity, price = self._dec(row.get("fillSize")), self._dec(row.get("fillPrice"))
            if quantity <= 0 or price <= 0:
                self._alarm_once(f"fill-unreadable:{order.client_order_id}:{trade_id}", "fill-unreadable",
                                 client_id=order.client_order_id, row=self._raw(row))
                continue
            if booked + quantity > cumulative + tolerance:
                # Never past the order's own filled quantity: the fill list ran ahead of the order row, or repeats one.
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
            fee_coin = self._fee_token(order, row)
            raw_fee = self._dec(row.get("fee"))
            fee_amount = abs(raw_fee)   # the sign convention is not documented: the first fills audit it
            self._audit_once(f"fee-rate:{order.trade_type.name}", fee=str(raw_fee), fee_coin=fee_coin,
                             quantity=str(quantity), price=str(price),
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
                fill_timestamp=(web_utils.to_ms(row.get("ts")) or int(self._now() * 1e3)) / 1e3,
                # Spot fill rows carry no role: a fill within FILL_TAKER_WINDOW_MS of the order's creation matched on
                # arrival (the probe's taker buy: 9 ms), a later one rested first (maker).
                is_taker=self._is_taker(created_ms, web_utils.to_ms(row.get("ts"))),
            ))
            booked += quantity
        if booked + tolerance < cumulative:
            # fills-history trails the order row (or a guard refused a row): read again later, not every poll.
            interval = (min(retry[1] * 2, CONSTANTS.FILLS_RETRY_MAX_S) if retry is not None and retry[2] == seen
                        else CONSTANTS.FILLS_RETRY_MIN_S)
            self._fills_retry[client_order_id] = (time.monotonic() + interval, interval, seen)
            self._audit_once(f"fills-behind-state:{client_order_id}", client_id=client_order_id,
                             terminal=terminal, filled_size=str(cumulative), booked=str(booked), rows=len(rows))
        else:
            self._fills_retry.pop(client_order_id, None)
        return fills

    @staticmethod
    def _is_taker(created_ms: Optional[int], fill_ms: Optional[int]) -> bool:
        if created_ms is None or fill_ms is None:
            return True
        return fill_ms - created_ms <= CONSTANTS.FILL_TAKER_WINDOW_MS

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """The base's fills pass runs over active AND recently done orders (its 30 s cache). A done order is skipped: a
        terminal state is reported only once BloFin's fill list showed every fill (_request_order_status), so there is
        nothing left to read."""
        if order.exchange_order_id is None or order.is_done:
            return []
        try:
            detail = await self._fetch_order_detail(order)
        except BlofinBusinessError as e:
            if e.code == CONSTANTS.CODE_EMPTY_ANSWER:
                return []      # not readable now: the status pass reads it again
            raise
        if not self._check_size(order, detail):
            return []
        fills = await self._fills_from_rows(order, self._dec(detail.get("filledSize")),
                                            web_utils.to_ms(detail.get("createTime")),
                                            terminal=self._is_terminal_row(detail))
        for fill in fills:
            self._note_fill_booked(order, fill)
        return fills

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """One order's state from its row (_fetch_order_detail). The rules (runbook §1.5):
          - fills first, then the state: a terminal state is never forwarded short of its fills (FILLED = the whole
            size: a row without filledSize can't complete an order). It is held up to TERMINAL_HOLD_MAX_S for
            fills-history to list them; then the rest is booked from the row's own totals (_book_remainder);
          - a terminal state then waits for a balance read sent after the order's last fill (_balance_shows_fills);
          - an order we asked BloFin to cancel is not set back to OPEN by a row read before the cancel took;
          - whenever the poll leaves the order as it is, it raises the CODE_EMPTY_ANSWER sentinel: never a snapshot of
            the current state (queued as a task, a "no change" update can land after a newer state and roll it back)."""
        detail = await self._fetch_order_detail(tracked_order)
        client_order_id = tracked_order.client_order_id
        if not self._check_size(tracked_order, detail):
            return self._order_update(tracked_order, OrderState.PENDING_CANCEL)
        cumulative = self._dec(detail.get("filledSize"))
        state = detail.get("state")
        new_state = self._order_state(state, cumulative, tracked_order.amount)
        terminal = new_state in CONSTANTS.TERMINAL_STATES
        # Fills first: a FILLED state with fills still missing would complete the order short.
        for fill in await self._fills_from_rows(tracked_order, cumulative, web_utils.to_ms(detail.get("createTime")),
                                                terminal=terminal):
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(tracked_order, fill)
        self._audit_once(f"detail-state:{state}", mapped=str(new_state), filled_size=str(cumulative),
                         size=detail.get("size"))
        if not terminal:
            self._short_since.pop(client_order_id, None)
            if tracked_order.current_state is OrderState.PENDING_CANCEL:
                # A row read before BloFin took our cancel (~20 ms, live). Kept PENDING_CANCEL; a cancel BloFin took
                # that still hasn't taken effect after TERMINAL_HOLD_MAX_S is alarmed once.
                sent = self._cancel_sent.get(client_order_id)
                if sent is not None and time.monotonic() - sent >= CONSTANTS.TERMINAL_HOLD_MAX_S:
                    self._alarm_once(f"cancel-not-effective:{client_order_id}", "cancel-not-effective",
                                     client_id=client_order_id, exchange_order_id=tracked_order.exchange_order_id,
                                     blofin_state=state, waited_s=round(time.monotonic() - sent, 1),
                                     action="kept PENDING_CANCEL: check the order on BloFin")
                raise BlofinBusinessError(f"{client_order_id}: cancel pending; BloFin still lists it {state}",
                                          CONSTANTS.CODE_EMPTY_ANSWER)
            return self._order_update(tracked_order, new_state)
        target = max(cumulative, tracked_order.amount) if new_state is OrderState.FILLED else cumulative
        if tracked_order.executed_amount_base + Decimal("1e-12") < target:
            # BloFin says done but its fill list hasn't shown every fill yet, or a guard refused a fill row: nothing is
            # reported until every fill is booked. Strict on purpose — done with fills missing, the strategy would hedge
            # the wrong size.
            held_s = time.monotonic() - self._short_since.setdefault(client_order_id, time.monotonic())
            if held_s < CONSTANTS.TERMINAL_HOLD_MAX_S:
                self._audit_once(f"terminal-held:{client_order_id}", client_id=client_order_id, state=new_state.name,
                                 filled_size=str(cumulative), booked=str(tracked_order.executed_amount_base))
                raise BlofinBusinessError(f"{client_order_id}: {new_state.name} held for its fill rows (booked "
                                          f"{tracked_order.executed_amount_base} of {target})",
                                          CONSTANTS.CODE_EMPTY_ANSWER)
            if not self._book_remainder(tracked_order, target, cumulative, detail, held_s):
                self._alarm("terminal-forwarded-short", client_id=client_order_id, state=new_state.name,
                            filled_size=str(cumulative), booked=str(tracked_order.executed_amount_base),
                            held_s=round(held_s, 1))
        self._short_since.pop(client_order_id, None)
        self._cancel_sent.pop(client_order_id, None)
        update = self._order_update(tracked_order, new_state)
        if self._balance_shows_fills(tracked_order):
            return update
        self._process_terminal_update(tracked_order, update, source="rest")
        raise BlofinBusinessError(f"{client_order_id}: {new_state.name} held until a balance read shows its fills",
                                  CONSTANTS.CODE_EMPTY_ANSWER)

    def _book_remainder(self, order: InFlightOrder, target: Decimal, cumulative: Decimal, detail: Dict[str, Any],
                        held_s: float) -> bool:
        """What fills-history still lacks of a terminal order's fill, booked as ONE fill from the order row's own totals
        (CoinW's rule, 2026-10-15): the size target - booked; the quote filledAmount - booked quote when the row's
        filledSize covers the target (else the size x averagePrice, else x the order's price); the fee `fee` - booked
        fee, in the row's fee asset (0 when the row has none); named <orderId>:<target>. True if booked. Booked with no
        await before the terminal state that follows, so no fills read can add the same quantity again; later rows
        find no room (the cap at filledSize)."""
        target = min(target, order.amount)
        missing = target - order.executed_amount_base
        if missing <= Decimal("1e-12") or order.exchange_order_id is None:
            return False
        fills = list(order.order_fills.values())
        booked_quote = sum((f.fill_quote_amount for f in fills), Decimal("0"))
        quote_total = self._dec(detail.get("filledAmount"))
        average = self._dec(detail.get("averagePrice"))
        if cumulative >= target and quote_total > booked_quote:
            quote_amount = quote_total - booked_quote
        elif average > 0:
            quote_amount = missing * average
        else:
            quote_amount = missing * order.price
        token = self._fee_token(order, detail)
        booked_fee = sum((fee.amount for f in fills for fee in f.fee.flat_fees if fee.token == token), Decimal("0"))
        fee_known = detail.get("fee") not in (None, "")
        fee = max(abs(self._dec(detail.get("fee"))) - booked_fee, Decimal("0")) if fee_known else Decimal("0")
        update = TradeUpdate(
            trade_id=f"{order.exchange_order_id}:{self._format_decimal(target.normalize())}",
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=TradeFeeBase.new_spot_fee(fee_schema=self.trade_fee_schema(), trade_type=order.trade_type,
                                          flat_fees=[TokenAmount(amount=fee, token=token)]),
            fill_base_amount=missing,
            fill_quote_amount=quote_amount,
            fill_price=quote_amount / missing,
            fill_timestamp=self._now(),
            is_taker=True,
        )
        self._alarm("fill-from-cumulative", client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                    held_s=round(held_s, 1), booked_from_rows=str(order.executed_amount_base), added=str(missing),
                    quote=str(quote_amount), fee=str(fee), fee_coin=token, fee_known=fee_known)
        self._note_fill_booked(order, update)
        self._order_tracker.process_trade_update(update)
        return True

    def _order_update(self, order: InFlightOrder, state: OrderState) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id) if order.exchange_order_id is not None else None,
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    # ------------------------------------------------------------------ terminal updates wait for the balance

    def _note_fill_booked(self, order: InFlightOrder, fill: TradeUpdate) -> None:
        """The order's completion now waits for a balance read sent after this moment (_balance_shows_fills). A second
        call for the same fill (a status read after a cancel racing the poll) only makes that wait one read longer."""
        self._fill_booked_at[order.client_order_id] = self._balance_reads_sent

    def _balance_shows_fills(self, order: InFlightOrder) -> bool:
        """True once the balances come from a read SENT after the order's last fill was booked. BloFin credits a fill to
        the balance at once (the placement probe, 2026-11-11: a taker buy filled at its placement, and the balance read
        80 ms after the answer already held it, net of the fee), so such a read holds the fill; an earlier read may not,
        and the hold-band would read the asset without it and buy twice (Hotcoin, 2026-10-06). One rule for every
        order: other orders' fills on the asset can't confuse it, as they did the delta rule this replaces (the review,
        2026-11-11: its baseline counted a fill already in the balance a second time). An order without fills booked by
        this process needs no wait."""
        booked_at = self._fill_booked_at.get(order.client_order_id)
        return booked_at is None or order.executed_amount_base <= 0 or self._balance_applied > booked_at

    async def _await_balance_showing_fills(self, order: InFlightOrder, source: str) -> None:
        """A balance read sent now, after the fills: its answer is what the completion waits for (~50 ms, live). Should it
        fail, the poll's reads (every second while an order is open) count too, up to FILL_BALANCE_WAIT_SECONDS; then
        the completion goes anyway, with a [BF-ALARM]. _update_all_balances, not _update_balances: the in-flight
        snapshot must move with the balance it pairs with, or get_available_balance counts fills and locks twice."""
        if self._balance_shows_fills(order):
            return
        started = time.monotonic()
        await self._update_all_balances()        # never raises: a failed read is logged and keeps the last balances
        while not self._balance_shows_fills(order) and time.monotonic() - started < CONSTANTS.FILL_BALANCE_WAIT_SECONDS:
            await asyncio.sleep(0.25)
        waited_s = round(time.monotonic() - started, 3)
        if self._balance_shows_fills(order):
            self._audit("fill-balance-wait", client_id=order.client_order_id, source=source, waited_s=waited_s)
            return
        self._alarm("fill-balance-late", client_id=order.client_order_id, source=source, waited_s=waited_s)

    def _process_terminal_update(self, order: InFlightOrder, update: OrderUpdate, source: str = "rest") -> None:
        if self._balance_shows_fills(order):
            self._order_tracker.process_order_update(update)
            return
        if order.client_order_id in self._settling:
            return
        self._settling.add(order.client_order_id)
        safe_ensure_future(self._report_when_balance_shows_fills(order, update, source))

    async def _report_when_balance_shows_fills(self, order: InFlightOrder, update: OrderUpdate, source: str) -> None:
        try:
            await self._await_balance_showing_fills(order, source)
            if order.client_order_id in self._order_tracker.all_updatable_orders:
                # Only onto an order still live: the poll may have completed it meanwhile (runbook §1.5).
                self._order_tracker.process_order_update(update)
        finally:
            self._settling.discard(order.client_order_id)

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
        """No spot fee route is documented: DEFAULT_FEES estimates; each fill books BloFin's own fee."""
        return

    # ------------------------------------------------------------------ balances

    async def _update_all_balances(self) -> None:
        """The base's, except that a failed read keeps the last balances AND the in-flight snapshot they pair with (a new
        snapshot over old balances would count fills and locks twice), and so does an answer older than the balances
        already applied (dropped by _update_balances)."""
        try:
            applied = await self._update_balances()
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(f"Failed to update balances. Error: {request_error}", exc_info=request_error)
            return
        if applied is False:
            return
        if not self.real_time_balance_update:
            self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self.in_flight_orders.items()}
            self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    async def _update_balances(self) -> bool:
        """GET asset/balances?accountType=spot: every coin, zero rows included (701 rows, live), as strings: currency,
        balance, available, frozen, bonus. An EMPTY list is not believed (BloFin lists every coin): it raises and the last
        balances stay. The total is max(balance, available + frozen): `balance` is the total (live, the probe's open
        order: balance 99.99 = available 94.668 + frozen 5.322); the max only guards a change. Reads are numbered as
        they go out: an answer arriving after a newer read's was applied is dropped (False), never rolling the balances
        back (the completion gate, _balance_shows_fills, counts on it)."""
        self._balance_reads_sent += 1
        sequence = self._balance_reads_sent
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.BALANCES_PATH, params={"accountType": "spot"},
                                is_auth_required=True, limit_id=CONSTANTS.BALANCES_PATH),
            "Error fetching BloFin balances",
        )
        rows = self._rows(response)
        if not rows:
            raise IOError(f"BloFin answered an empty spot account (it lists every coin, zero rows included): not "
                          f"believed | BloFin response: {self._raw(response)}")
        self._audit_once("balance-rest-shape", assets=len(rows), sample=self._raw(rows[:2]))
        if sequence < self._balance_applied:
            self._audit_once("balance-answer-dropped", sequence=sequence, applied=self._balance_applied)
            return False
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for entry in rows:
            asset = str(entry.get("currency") or "").upper()
            if not asset or entry.get("available") in (None, ""):
                continue
            available = self._dec(entry.get("available"))
            frozen = self._dec(entry.get("frozen"))
            balance = self._dec(entry.get("balance"))
            total = max(balance, available + frozen)
            if frozen > 0:
                self._audit_once(f"balance-semantics:{asset}", balance=str(balance), available=str(available),
                                 frozen=str(frozen))
            remote_asset_names.add(asset)
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total
        for asset_name in local_asset_names.difference(remote_asset_names):
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)
        self._balance_applied = sequence
        return True

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for info in exchange_info.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                symbol = str(info["instId"])
                trading_pair = combine_to_hb_trading_pair(base=str(info["baseCurrency"]).upper(),
                                                          quote=str(info["quoteCurrency"]).upper())
                if trading_pair in mapping.inverse:
                    continue      # bidict refuses a second symbol for one pair; the first listed wins
                mapping[symbol] = trading_pair
            except Exception as exception:
                self.logger().error(f"Error parsing BloFin instrument {info.get('instId')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair the startup list did not have. BloFin's form is BASE-QUOTE, Hummingbot's own: the base
        default (f"{base}{quote}") would be wrong here."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.upper()}-{quote.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(f"BloFin {trading_pair} was not among the markets listed at startup; mapped to "
                              f"{exchange_symbol}. BloFin's answers to its requests will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """The spot instrument rows (live 2026-11-11): tickSize = the price step, lotSize = the quantity step, minSize =
        the minimum quantity (base units: BTC 0.00001, ANSEM 15), maxLimitSize = the largest LIMIT quantity. No
        minimum-notional field exists; the placement probe's minimum-size order settles whether one applies. The price
        band (order/price-range) refuses a buy more than ~1.2% over the market; refusals are logged in full."""
        rules: List[TradingRule] = []
        for info in exchange_info_dict.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(info["instId"]))
                lot = self._dec(info.get("lotSize"))
                rule_kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(info.get("minSize")), lot),
                    min_price_increment=self._dec(info.get("tickSize")),
                    min_base_amount_increment=lot,
                )
                if info.get("maxLimitSize") not in (None, ""):
                    rule_kwargs["max_order_size"] = self._dec(info.get("maxLimitSize"))
                rules.append(TradingRule(**rule_kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the BloFin trading rule {info.get('instId')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """One bulk spot ticker read (shared for TICKER_CACHE seconds) answers every pair; every requested pair gets a
        value, NaN when BloFin has none: the tracker re-asks a pair it got nothing for at once."""
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        cached = self._ticker_cache
        if cached is None or time.monotonic() - cached[0] > 2.0:
            try:
                response = await self._api_get(path_url=CONSTANTS.TICKERS_PATH,
                                               params={"instType": CONSTANTS.INST_TYPE},
                                               limit_id=CONSTANTS.TICKERS_PATH)
                lasts: Dict[str, float] = {}
                if web_utils.is_ok(response):
                    for row in self._rows(response):
                        if row.get("instId") and row.get("last") not in (None, ""):
                            lasts[str(row["instId"])] = float(row["last"])
                cached = self._ticker_cache = (time.monotonic(), lasts)
            except asyncio.CancelledError:
                raise
            except Exception:
                return prices
        for trading_pair in trading_pairs:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            except KeyError:
                continue
            if symbol in cached[1]:
                prices[trading_pair] = cached[1][symbol]
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]
