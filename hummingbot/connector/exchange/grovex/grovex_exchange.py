import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.grovex import (
    grovex_constants as CONSTANTS,
    grovex_utils as utils,
    grovex_web_utils as web_utils,
)
from hummingbot.connector.exchange.grovex.grovex_api_order_book_data_source import GrovexAPIOrderBookDataSource
from hummingbot.connector.exchange.grovex.grovex_api_user_stream_data_source import GrovexAPIUserStreamDataSource
from hummingbot.connector.exchange.grovex.grovex_auth import GrovexAuth
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

s_decimal_0 = Decimal("0")
TOLERANCE = Decimal("1e-12")


class GrovexBusinessError(IOError):
    """A request GroveX answered with a code other than "0" (a definite refusal), or one of our own codes for an answer
    that is not what it should be (CODE_ORDER_NOT_FOUND, CODE_EMPTY_ANSWER). IOError keeps every `except IOError` path
    working."""

    def __init__(self, message: str, code: Optional[str], msg: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.msg = msg


class GrovexPlacementUnknown(IOError):
    """A placement GroveX may have accepted (no answer, an answer that is not GroveX's refusal, or "0" without an
    order_id) that the lookup did not find: the order is NOT failed — it may rest on GroveX. It stays PENDING_CREATE and
    the status poll keeps looking for it; only the poll's age-gated not-found verdict fails it."""


class _LookupInconclusive(Exception):
    """The placement lookup could not tell: the order lists could not be read, or an identical placement still awaits
    its answer."""


class GrovexExchange(ExchangePyBase):
    """
    DISABLED, kept as reference (Pavel, 2026-10-09: GroveX scrapped; never connected or traded): see __init__.py.

    GroveX spot connector: the ChainUp "open/api" v1 REST for everything private (polled: there is no private push) and
    the kline-api socket for books, every request and the socket through the brr_ws relay (CONSTANTS.PROXY_URL).

    Reference: the docs mirror (VS_code_projects/MDs/grovex-api/), live probes of 2026-10-09 from myserver through the
    relay, and the wiki page trading/exchanges/grovex-api. What the docs could not settle is audit-logged ([GX-AUDIT])
    rather than guessed; money guards log as [GX-ALARM]. An independent review of the order path (2026-10-09) found 13
    points; the code below is the fixed one.

    Scope: LIMIT orders only, which is all arb_l and the position balancer send.

    What GroveX does not have, and what stands in for it:
      - No private push. While any order is open, each open order's order_info is read every ORDER_POLL_INTERVAL (1 s).
      - No client order id. A placement that got no answer is found by side, price, exact quantity and creation time in
        the open and recent orders (_find_placed_order); identical unconfirmed orders of ours are paired one-to-one with
        identical candidates by creation time; nothing is adopted while an identical placement still awaits its answer.
      - Silent not-found: order_info of an unknown order answers code 0 with nulls. "Not found" is decided here
        (_resolve_missing_order), never from a code.
      - No status in order_info (the docs' example): the shared open-order list says open or not; an order that left it
        is read once more and settled by its final deal_volume (_status_from_open_list).
      - Fills: the account's trades on the market (all_trade, newest first, paged back to the order's creation) joined to
        the order by its id (bid_id for a buy, ask_id for a sell): each with GroveX's own trade id, quantity, price, fee,
        fee coin and trade time. Read when the order's cumulative filled quantity (deal_volume) grows.
      - Balances: user/account takes ~22 s. It is read in a background task; a fill journal with GroveX's trade times
        decides which assets a read may update (_read_and_apply_balances); totals move with every fill booked.
    """

    web_utils = web_utils

    def __init__(
        self,
        grovex_api_key: str,
        grovex_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature must match ConnectorSetting.conn_init_parameters in this fork (balance_asset_limit and
        rate_limits_share_pct, no config map): an upstream-style client_config_map crashes `balance` (CoinEx)."""
        self._api_key = grovex_api_key
        self._secret_key = grovex_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: Dict[str, None] = {}
        self._alarm_seen: Dict[str, None] = {}
        # Per poll (emptied as a poll starts): order reads, the open-order list per symbol, the trade list per symbol.
        self._detail_cache: Dict[str, Dict[str, Any]] = {}
        self._open_cache: Dict[str, Tuple[float, Dict[str, Dict[str, Any]]]] = {}   # symbol -> (wall time sent, rows)
        self._open_full: Dict[str, bool] = {}                # symbol -> its last open-order read was a full page
        self._trades_cache: Dict[str, Tuple[int, List[Dict[str, Any]]]] = {}
        self._last_private_read: float = 0.0                 # wall time of the last successful signed read
        self._last_time_sync: float = 0.0
        self._symbols_cache: Optional[Tuple[float, Dict[str, Any]]] = None
        self._symbols_lock = asyncio.Lock()
        self._tickers_cache: Optional[Tuple[float, Dict[str, Dict[str, Any]]]] = None
        self._tickers_lock = asyncio.Lock()
        self._tickers_task: Optional[asyncio.Task] = None
        # Per-order state; entries of orders no longer tracked are dropped every poll (_purge_order_state).
        self._placing: Dict[str, Tuple[str, TradeType, Decimal, Decimal]] = {}   # awaiting create_order's answer
        self._sent_at_ms: Dict[str, int] = {}                # client id -> server ms just before its placement
        self._cancel_sent: set = set()                       # client ids we asked GroveX to cancel
        self._cancel_intent: set = set()                     # cancels asked for before the order had an exchange id
        self._volume_checked: set = set()                    # client ids whose quantity GroveX confirmed
        self._fills_pending_since: Dict[str, float] = {}     # client id -> monotonic time it first waited for fills
        self._noted_fills: Dict[str, None] = {}              # "<exchange order id>:<trade id>" counted in balances
        # Balances (user/account ~22 s): the background read and the fill journal that decides what a read may update.
        self._balance_task: Optional[asyncio.Task] = None
        self._balance_lock = asyncio.Lock()
        self._balance_read_done_at: float = 0.0              # monotonic end of the last read (ok or not)
        self._balance_fail_logged_at: float = 0.0
        self._balances_ready: bool = False                   # the first (baseline) read has been applied
        self._fill_journal: List[Tuple[int, Dict[str, Decimal]]] = []   # (GroveX trade ms, deltas incl. fees)
        # asset -> (window start, answer time), GroveX ms, of the read last applied to its total / start for available
        self._total_read_window: Dict[str, Tuple[int, int]] = {}
        self._read_cutoff_ms: Dict[str, int] = {}
        self._snapshot_locked: Dict[str, Decimal] = {}       # asset -> our orders' locked amount when that read applied
        self._filled_since_read: Dict[str, Decimal] = {}     # asset -> journal deltas after its read's cutoff
        self._read_applied_at: Dict[str, float] = {}         # asset -> monotonic time its last read was applied
        self._lock_activity: Dict[str, float] = {}           # asset -> monotonic time of the last placement/cancel on it
        self._drift_alarmed: set = set()
        self._warm_task: Optional[asyncio.Task] = None
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # No balance push: user/account is authoritative where it applies, and the per-asset formula
        # (apply_balance_update_since_snapshot) keeps `available` right between reads.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> GrovexAuth:
        return GrovexAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        return CONSTANTS.TICKER_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # cancel_order answers data "" (docs): the next status read settles the order.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def last_private_read_time(self) -> float:
        return self._last_private_read

    @property
    def status_dict(self) -> Dict[str, bool]:
        """The base's, except that balances count as ready only once the first (baseline) read has been applied: a
        restored order's fills booked before it would otherwise make the balances look known ~22 s early (review
        2026-10-09)."""
        status = super().status_dict
        status["account_balance"] = not self.is_trading_required or self._balances_ready
        return status

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    def server_time_ms(self) -> float:
        return self._time_synchronizer.time() * 1e3

    # ------------------------------------------------------------------ factories and network

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return GrovexAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return GrovexAPIUserStreamDataSource(connector=self)

    async def start_network(self):
        await super().start_network()
        if self._warm_task is None or self._warm_task.done():
            self._warm_task = safe_ensure_future(self._warm_loop())

    async def stop_network(self):
        for task in (self._warm_task, self._balance_task, self._tickers_task):
            if task is not None and not task.done():
                task.cancel()
        self._warm_task = self._balance_task = self._tickers_task = None
        await super().stop_network()

    async def _warm_loop(self) -> None:
        """Keep connections through the relay open: a fresh one costs ~1 s, an open one ~0.27 s (live 2026-10-09)."""
        while True:
            try:
                await web_utils.GrovexConnectionsFactory.shared().warm()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().debug(f"GroveX connection warm-up failed: {e!r}")
            await asyncio.sleep(CONSTANTS.WARM_INTERVAL)

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [GX-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[GX-AUDIT] {tag} {rendered}")

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
        """A money guard fired: one [GX-ALARM] WARNING, whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[GX-ALARM] {tag} {rendered}")

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
        GroveX answers every request with HTTP 200, so the base class's resync-and-retry (which runs only on a transport
        IOError) never sees an expired timestamp. Here: numbers are parsed exactly (Decimal), every request has a timeout
        (DEFAULT_REQUEST_TIMEOUT unless it carries its own), and 100008 re-syncs the clock and repeats the request once
        (the timestamp is checked before anything is executed, so the repeat can't place twice). A signed request GroveX
        answered "0" marks private REST as working (GrovexAPIUserStreamDataSource).
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)
        timeout = timeout if timeout is not None else CONSTANTS.DEFAULT_REQUEST_TIMEOUT
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
                raise IOError(f"GroveX answered {path_url} with no JSON (HTTP {response.status}): "
                              f"{'a challenge page' if 'Just a moment' in text else text[:200]!r}")
            if attempt == 0 and is_auth_required and web_utils.error_code(result) == CONSTANTS.CODE_REQUEST_EXPIRED:
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer(force=True)
                continue
            if is_auth_required and web_utils.is_ok(result):
                self._last_private_read = time.time()
            return result
        return result

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False, force: bool = False):
        """The base re-reads the server time on every status poll — every second while orders are open. GroveX refuses
        only a `time` older than ~60 s (live), so the clock is re-read every TIME_SYNC_INTERVAL, or at once on 100008."""
        now = time.monotonic()
        if not force and self._last_time_sync and now - self._last_time_sync < CONSTANTS.TIME_SYNC_INTERVAL:
            return
        self._last_time_sync = now
        await super()._update_time_synchronizer(pass_on_non_cancelled_error=pass_on_non_cancelled_error)

    async def _make_trading_rules_request(self) -> Any:
        return await self.market_list(max_age=60.0)

    async def _make_trading_pairs_request(self) -> Any:
        return await self.market_list(max_age=60.0)

    async def market_list(self, max_age: float = 0.0) -> Dict[str, Any]:
        """common/symbols (every market with its precisions), shared by the symbol map, the trading rules and runtime
        adds: a read younger than max_age (s) is shared. An empty or refused list raises (the last map and rules stay)."""
        async with self._symbols_lock:
            cached = self._symbols_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            response = await self._api_get(path_url=CONSTANTS.SYMBOLS_PATH, limit_id=CONSTANTS.SYMBOLS_PATH,
                                           timeout=30.0)
            self._raise_on_error(response, f"Error reading GroveX's market list ({CONSTANTS.SYMBOLS_PATH})")
            if not isinstance(response.get("data"), list) or not response["data"]:
                raise IOError(f"GroveX's market list came back empty | GroveX response: {self._raw(response)}")
            self._symbols_cache = (time.monotonic(), response)
            return response

    async def all_tickers(self, max_age: float = 0.0) -> Dict[str, Dict[str, Any]]:
        """get_allticker (6-9 s server-side): {symbol: row} with `isShow`, `last`, `buy`, `sell`. Shared by the trading
        switch and the last prices: a read younger than max_age (s) is shared, and callers that ask while a read is under
        way wait for it."""
        async with self._tickers_lock:
            cached = self._tickers_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            response = await self._api_get(path_url=CONSTANTS.ALL_TICKER_PATH, limit_id=CONSTANTS.ALL_TICKER_PATH,
                                           timeout=30.0)
            self._raise_on_error(response, f"Error reading GroveX's tickers ({CONSTANTS.ALL_TICKER_PATH})")
            data = response.get("data") if isinstance(response.get("data"), dict) else {}
            rows = {str(r.get("symbol")).lower(): r for r in data.get("ticker") or []
                    if isinstance(r, dict) and r.get("symbol")}
            if not rows:
                raise IOError(f"GroveX's ticker list came back empty | GroveX response: {self._raw(response)[:500]}")
            self._tickers_cache = (time.monotonic(), rows)
            return rows

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.TICKER_PATH, params=dict(CONSTANTS.NETWORK_CHECK_PARAMS),
                                       limit_id=CONSTANTS.TICKER_PATH)
        if not web_utils.is_ok(response):
            raise IOError(f"Unexpected GroveX answer to the network check: {response}")

    @staticmethod
    def _raw(response: Any) -> str:
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    def _raise_on_error(self, response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if not web_utils.is_ok(response):
            code = web_utils.error_code(response)
            msg = (response.get("msg") or response.get("message")) if isinstance(response, dict) else None
            self._audit_once(f"error-code:{code}", context=context, response=self._raw(response))
            raise GrovexBusinessError(f"{context}: code {code} ({msg}) | GroveX response: {self._raw(response)}",
                                      code, msg)
        return response

    @staticmethod
    def _is_refusal(response: Any) -> bool:
        """GroveX's refusal has one shape (every error seen live): the envelope with a code other than "0", `data`
        null and `success` not true. Anything else that isn't a clean "0" (no envelope, no code, a code with data) can't
        be read as "not placed"."""
        return (isinstance(response, dict) and response.get("code") not in (None, "", CONSTANTS.CODE_OK, 0)
                and response.get("data") in (None, "") and response.get("success") is not True)

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """GroveX refuses with HTTP 200 and a code, which the base would log as a network error with a traceback: a
        refusal is logged as one, with the request and GroveX's complete answer."""
        if isinstance(exception, GrovexPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll looks for it "
                                  f"on GroveX and fails it only once it is not there.")
            return
        if isinstance(exception, GrovexBusinessError):
            self.logger().warning(f"GroveX rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return CONSTANTS.CODE_REQUEST_EXPIRED in str(request_exception)

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        return isinstance(error, GrovexBusinessError) and error.code == CONSTANTS.CODE_ORDER_NOT_FOUND

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # Code 22 answers a cancel of an unknown id, and maybe of a finished one: never taken as "not found" without a
        # status read (_execute_order_cancel reads it).
        return False

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (and forgets a FAILED order that may
        still rest on the venue). Only our own verdict — null order_info, absent from the open and recent orders, and
        old enough — counts; anything else is a WARNING and the order stays tracked (Hotcoin's lesson, 2026-10-06)."""
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        if isinstance(error, GrovexBusinessError) and error.code == CONSTANTS.CODE_EMPTY_ANSWER:
            self.logger().debug(f"GroveX status of {order.client_order_id} not readable yet: {error}")
            return
        self.logger().warning(f"GroveX status read for {order.client_order_id} failed, the order stays tracked: "
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

    @staticmethod
    def _dec_or_none(value: Any) -> Optional[Decimal]:
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _now(self) -> float:
        timestamp = self.current_timestamp
        return time.time() if timestamp is None or timestamp != timestamp else timestamp

    def _server_ms(self) -> int:
        return int(self._time_synchronizer.time() * 1e3)

    def _get_poll_interval(self, timestamp: float) -> float:
        """No private push: while any order is open the status poll runs every ORDER_POLL_INTERVAL (1 s); otherwise every
        SHORT_POLL_INTERVAL (10 s), which also starts the background balance reads."""
        if self.in_flight_orders:
            return CONSTANTS.ORDER_POLL_INTERVAL
        return self.SHORT_POLL_INTERVAL

    async def _user_stream_event_listener(self) -> None:
        # GroveX has no private stream: nothing ever arrives here.
        while True:
            await asyncio.sleep(3600)

    async def _update_order_status(self) -> None:
        """One poll (the base's fills pass, then its status pass), reading each order and each market's open list and
        trade list once for both passes; first the per-order state of orders no longer tracked is dropped."""
        self._detail_cache.clear()
        self._open_cache.clear()
        self._trades_cache.clear()
        self._purge_order_state()
        await super()._update_order_status()

    def _purge_order_state(self) -> None:
        live = set(self._order_tracker.all_fillable_orders) | set(self._placing)
        for registry in (self._sent_at_ms, self._fills_pending_since):
            for client_order_id in [c for c in registry if c not in live]:
                del registry[client_order_id]
        self._volume_checked &= live
        self._cancel_sent &= live
        self._cancel_intent &= live

    def _touch_locks(self, *assets: str) -> None:
        """A placement or a cancel on these assets now: a balance read in flight may not update their `available`."""
        now = time.monotonic()
        for asset in assets:
            if asset:
                self._lock_activity[asset] = now

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """With no trading rule the base stops at a bare KeyError before tracking the order (XT B2-USDT, 2026-09-23):
        here the order is tracked and failed the way the base fails its own pre-send checks."""
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to GroveX: the connector has no trading rule for {trading_pair} "
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
        body = {
            "side": CONSTANTS.SIDE[trade_type],
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
            "volume": self._format_decimal(amount),      # type 1: volume = the BASE quantity (docs)
            "price": self._format_decimal(price),
            "symbol": symbol,
        }
        sent_at_ms = self._server_ms()
        self._sent_at_ms[order_id] = sent_at_ms
        # While the answer is awaited only this placement looks the order up, not the status poll (Bitunix review,
        # 2026-10-07). Discarded on return; the base gives the order its id with no await in between.
        self._placing[order_id] = (trading_pair, trade_type, price, amount)
        try:
            try:
                response = await self._api_post(path_url=CONSTANTS.CREATE_ORDER_PATH, data=body, is_auth_required=True,
                                                limit_id=CONSTANTS.CREATE_ORDER_PATH,
                                                timeout=CONSTANTS.PLACE_ORDER_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception as transport_error:
                # No answer: GroveX may still have accepted the order (the base would mark it FAILED and stop tracking a
                # live order). GroveX has no client id: the order is looked up by what it is.
                return await self._locate_unconfirmed_placement(order_id, symbol, sent_at_ms,
                                                                repr(transport_error)), self._now()
            self._audit("place-order", client_id=order_id, request=self._raw(body), response=self._raw(response))
            if self._is_refusal(response):
                self._raise_on_error(response, f"Order {order_id} refused, request {self._raw(body)}")
            data = response.get("data") if isinstance(response, dict) else None
            exchange_order_id = data.get("order_id") if isinstance(data, dict) else None
            if not web_utils.is_ok(response) or exchange_order_id in (None, ""):
                # Not GroveX's refusal and not a clean "0" with an id: it may have placed the order. Looked up the same
                # way (review 2026-10-09: only the refusal shape fails an order outright).
                cause = f"no order_id in GroveX's answer | GroveX response: {self._raw(response)}"
                return await self._locate_unconfirmed_placement(order_id, symbol, sent_at_ms, cause), self._now()
            return str(exchange_order_id), self._now()
        finally:
            self._placing.pop(order_id, None)

    async def _locate_unconfirmed_placement(self, order_id: str, symbol: str, sent_at_ms: int, cause: str) -> str:
        """A placement GroveX may have accepted without telling us the order's id. Found on GroveX -> its id. Not found,
        or the lookup could not tell -> GrovexPlacementUnknown: the order is NOT failed (it may rest on GroveX); it stays
        PENDING_CREATE and the status poll keeps looking (_fetch_order_detail), failing it only by its age-gated
        not-found verdict."""
        found, verdict = None, "not on GroveX yet"
        try:
            found = await self._find_placed_order(order_id, symbol, sent_at_ms, CONSTANTS.PLACEMENT_LOOKUP_DELAYS)
        except _LookupInconclusive as e:
            verdict = f"lookup inconclusive: {e}"
        self._alarm("place-order-unconfirmed", client_id=order_id, cause=cause, found_on_exchange=found,
                    verdict=None if found is not None else verdict)
        if found is not None:
            return found
        raise GrovexPlacementUnknown(f"GroveX did not confirm the placement ({cause}); {verdict}")

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        self._touch_locks(order.base_asset, order.quote_asset)
        # Registered from here, not only in _place_order: the symbol lookup before the request is an await too.
        self._placing[order.client_order_id] = (order.trading_pair, order.trade_type, order.price, order.amount)
        try:
            exchange_order_id = await super()._place_order_and_process_update(order, **kwargs)
        finally:
            self._placing.pop(order.client_order_id, None)
            self._touch_locks(order.base_asset, order.quote_asset)
        if order.exchange_order_id is not None and str(order.exchange_order_id) != str(exchange_order_id):
            # The base never overwrites an id: cancels and status reads would use the other one. _placing keeps the
            # poll's lookup away while the placement is awaited, so this should never fire.
            self._alarm("order-id-mismatch", client_id=order.client_order_id, tracked_id=order.exchange_order_id,
                        placement_id=str(exchange_order_id))
        self._check_id_collision(order, str(exchange_order_id))
        await self._cancel_if_asked(order)
        # The first status read checks the quantity GroveX booked: do it now, not at the next tick.
        self._poll_notifier.set()
        return exchange_order_id

    def _check_id_collision(self, order: InFlightOrder, exchange_order_id: str) -> None:
        """Two of our orders must never share a GroveX order: every fill would be booked twice (review 2026-10-09). The
        lookups exclude ids other orders hold; this is the alarm if one ever slips through."""
        others = [cid for cid, o in self._order_tracker.all_fillable_orders.items()
                  if cid != order.client_order_id and str(o.exchange_order_id) == exchange_order_id]
        if others:
            self._alarm("order-id-collision", client_id=order.client_order_id, exchange_order_id=exchange_order_id,
                        also_held_by=others, action="check both orders on GroveX")

    async def _cancel_if_asked(self, order: InFlightOrder) -> None:
        """A cancel asked for while the order had no exchange id (an unconfirmed placement) is sent once it has one."""
        if order.client_order_id not in self._cancel_intent or order.exchange_order_id is None:
            return
        self._cancel_intent.discard(order.client_order_id)
        self._cancel_sent.add(order.client_order_id)      # before any await: a waiting _place_cancel must not send too
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        self._audit("cancel-on-id", client_id=order.client_order_id, exchange_order_id=order.exchange_order_id)
        await self._cancel_by_exchange_id(str(order.exchange_order_id), symbol)

    # ------------------------------------------------------------------ order lists (the lookups)

    def _normalize(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """One order record (order_info's order_info, or a v2/new_order / v2/all_order row) in one shape. A field GroveX
        leaves out stays None (deal_volume included: unknown is never zero); `raw` keeps the record."""
        status = row.get("status")
        return {
            "id": None if row.get("id") in (None, "") else str(row.get("id")),
            "status": None if status in (None, "") else str(status),
            "side": str(row.get("side") or "").upper(),
            "price": self._dec(row.get("price")),
            "volume": self._dec_or_none(row.get("volume")),
            "deal_volume": self._dec_or_none(row.get("deal_volume")),
            "remain_volume": self._dec_or_none(row.get("remain_volume")),
            "created_ms": web_utils.to_ms(row.get("created_at")),
            "raw": row,
        }

    def _window(self, center_ms: int, zone_h: int) -> Dict[str, str]:
        """A v2/all_order window (wall-clock strings, <= 10 min) around center_ms, in UTC+zone_h, never past now."""
        tz = timezone(timedelta(hours=zone_h))
        fmt = "%Y-%m-%d %H:%M:%S"
        end_s = min(center_ms / 1e3 + CONSTANTS.ALL_ORDER_WINDOW_AFTER_S, self._server_ms() / 1e3 + 1)
        start_s = min(center_ms / 1e3 - CONSTANTS.ALL_ORDER_WINDOW_BEFORE_S, end_s - 1)
        return {"startDate": datetime.fromtimestamp(start_s, tz).strftime(fmt),
                "endDate": datetime.fromtimestamp(end_s, tz).strftime(fmt)}

    @staticmethod
    def _rows_of(response: Dict[str, Any], key: str, what: str) -> List[Dict[str, Any]]:
        data = response.get("data")
        listed = data.get(key) if isinstance(data, dict) else None
        listed = [] if listed is None else listed
        if not isinstance(listed, list):
            raise IOError(f"Unexpected GroveX {what}: {json.dumps(response, default=str)[:300]}")
        return [r for r in listed if isinstance(r, dict) and r.get("id") not in (None, "")]

    async def _open_orders(self, symbol: str, read_after: float = 0.0) -> Dict[str, Dict[str, Any]]:
        """The market's open orders (v2/new_order, which requires the symbol), normalized, read once per poll; a cached
        read sent before `read_after` (wall time, s) is read again. Raises if it can't be read: "not listed" must never
        come from a failed read. A full page can't say an order is not listed (_open_full)."""
        cached = self._open_cache.get(symbol)
        if cached is not None and cached[0] >= read_after:
            return cached[1]
        sent_at = time.time()
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.OPEN_ORDERS_PATH,
                                params={"symbol": symbol, "pageSize": CONSTANTS.LIST_PAGE_SIZE},
                                is_auth_required=True, limit_id=CONSTANTS.OPEN_ORDERS_PATH),
            f"Error listing GroveX open orders on {symbol}")
        listed = self._rows_of(response, "resultList", "open orders")
        self._open_full[symbol] = len(listed) >= CONSTANTS.LIST_PAGE_SIZE
        if self._open_full[symbol]:
            self._alarm_once(f"open-list-full:{symbol}", "open-list-full", symbol=symbol, rows=len(listed),
                             note="'not listed' settles no order on this market while the page is full")
        rows = {r["id"]: r for r in (self._normalize(x) for x in listed)}
        self._open_cache[symbol] = (sent_at, rows)
        return rows

    def _not_listed_is_final(self, order: InFlightOrder, symbol: str) -> bool:
        """An order missing from the open list is done (or was never placed) only if that read was sent at least
        NOT_FOUND_MIN_AGE after the order was created and was not a full page. A read cached earlier in the poll may
        predate the order (a slow poll), which would settle a live order (review of the fixes, 2026-10-09)."""
        cached = self._open_cache.get(symbol)
        created = float(order.creation_timestamp or self._now())
        return (cached is not None and cached[0] >= created + CONSTANTS.NOT_FOUND_MIN_AGE
                and not self._open_full.get(symbol, False))

    async def _list_orders(self, symbol: str, around_ms: int, read_after: float = 0.0) -> List[Dict[str, Any]]:
        """The market's open orders and its orders created around around_ms (v2/all_order windows, both zones until the
        first live order settles GroveX's), normalized. A zone GroveX refuses is skipped while another read answers;
        if none does, it raises (read as "no orders" it would feed a not-found verdict). read_after: _open_orders."""
        rows: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = []
        try:
            rows.update(await self._open_orders(symbol, read_after=read_after))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            errors.append(f"open: {e!r}")
        answered = not errors
        for zone in CONSTANTS.ALL_ORDER_ZONES_H:
            params = {"symbol": symbol, "pageSize": CONSTANTS.LIST_PAGE_SIZE}
            params.update(self._window(around_ms, zone))
            try:
                response = self._raise_on_error(
                    await self._api_get(path_url=CONSTANTS.ALL_ORDERS_PATH, params=params, is_auth_required=True,
                                        limit_id=CONSTANTS.ALL_ORDERS_PATH),
                    f"Error listing GroveX orders on {symbol} (UTC{zone:+d} window)")
                listed = [self._normalize(x) for x in self._rows_of(response, "orderList", "order list")]
            except asyncio.CancelledError:
                raise
            except Exception as e:
                errors.append(f"utc{zone:+d}: {e!r}")
                self._audit_once(f"all-order-refused:utc{zone:+d}", error=repr(e)[:300])
                continue
            answered = True
            for row in listed:
                self._audit_once(f"all-order-zone:utc{zone:+d}", symbol=symbol, order_id=row["id"],
                                 created_ms=row["created_ms"])
                rows.setdefault(row["id"], row)
        if not answered:
            raise IOError(f"GroveX order lists on {symbol} unreadable: {'; '.join(errors)}")
        return list(rows.values())

    def _placement_key(self, client_order_id: str) -> Optional[Tuple[str, TradeType, Decimal, Decimal]]:
        placing = self._placing.get(client_order_id)
        if placing is not None:
            return placing
        order = self._order_tracker.fetch_order(client_order_id=client_order_id)
        return None if order is None else (order.trading_pair, order.trade_type, order.price, order.amount)

    def _tracked_exchange_ids(self, client_order_id: str) -> set:
        """The exchange ids of every order tracked under another client id (active, recently done, lost)."""
        return {str(o.exchange_order_id) for cid, o in self._order_tracker.all_fillable_orders.items()
                if o.exchange_order_id is not None and cid != client_order_id}

    def _competing_placement(self, client_order_id: str, key: Tuple[str, TradeType, Decimal, Decimal]) -> bool:
        return any(other_id != client_order_id and other == key for other_id, other in self._placing.items())

    def _identical_unconfirmed(self, client_order_id: str, key: Tuple[str, TradeType, Decimal, Decimal]
                               ) -> List[Tuple[float, str, Optional[InFlightOrder]]]:
        """Our orders with this exact key and no exchange id yet, not awaiting a placement answer, oldest first, as
        (creation time, client id, order) — the current one included even while its own placement is the one looking,
        and even if the order tracker doesn't hold it."""
        own = [(float(o.creation_timestamp or 0), cid, o) for cid, o in self._order_tracker.all_fillable_orders.items()
               if o.exchange_order_id is None and (cid == client_order_id or cid not in self._placing)
               and (o.trading_pair, o.trade_type, o.price, o.amount) == key]
        if not any(cid == client_order_id for _, cid, _ in own):
            current = self._order_tracker.fetch_order(client_order_id=client_order_id)
            if current is None or current.exchange_order_id is None:
                created = (float(current.creation_timestamp) if current is not None and current.creation_timestamp
                           else (self._sent_at_ms.get(client_order_id) or self._server_ms()) / 1e3)
                own.append((created, client_order_id, current))
        return sorted(own, key=lambda entry: (entry[0], entry[1]))

    async def _find_placed_order(self, client_order_id: str, symbol: str, sent_at_ms: int,
                                 delays: Tuple[float, ...]) -> Optional[str]:
        """The placement guard without a client id: an order on the market with our side, price and EXACT quantity,
        created after the request went out (5 s clock slack) and tracked under no other client id, looked for after each
        delay (s) with everything re-read. Identical unconfirmed orders of ours are interchangeable: they are paired with
        the candidates one-to-one, oldest with oldest, and the others get their ids too ([GX-ALARM] placement-paired).
        More candidates than our identical orders: we can't tell which, so every one still open is cancelled and any done
        one is reported ([GX-ALARM] placement-ambiguous). _LookupInconclusive -> it could not tell: a list read failed,
        or an identical placement still awaits its answer (checked before AND after the read: the order found could be
        that one's)."""
        key = self._placement_key(client_order_id)
        if key is None:
            raise _LookupInconclusive("the order is not tracked")
        trading_pair, trade_type, price, amount = key
        side = CONSTANTS.SIDE[trade_type]
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            own = self._order_tracker.fetch_order(client_order_id=client_order_id)
            if own is not None and own.exchange_order_id is not None:
                return str(own.exchange_order_id)         # it got its id meanwhile
            if self._competing_placement(client_order_id, key):
                raise _LookupInconclusive("an identical placement still awaits GroveX's answer")
            try:
                rows = await self._list_orders(symbol, sent_at_ms, read_after=time.time())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                raise _LookupInconclusive(f"listing {symbol}'s orders failed: {e!r}")
            own = self._order_tracker.fetch_order(client_order_id=client_order_id)
            if own is not None and own.exchange_order_id is not None:
                return str(own.exchange_order_id)
            if self._competing_placement(client_order_id, key):
                raise _LookupInconclusive("an identical placement started while the lists were read")
            tracked = self._tracked_exchange_ids(client_order_id)
            matches = sorted((row for row in rows
                              if row["id"] not in tracked and row["side"] == side and row["price"] == price
                              and row["volume"] is not None and row["volume"] == amount
                              and (row["created_ms"] or 0) >= sent_at_ms - 5_000),
                             key=lambda r: (r["created_ms"] or 0, r["id"]))
            if not matches:
                continue
            siblings = self._identical_unconfirmed(client_order_id, key)
            if len(matches) <= len(siblings):
                found = None
                for (_, sibling_id, sibling), row in zip(siblings, matches):
                    if sibling_id == client_order_id:
                        found = row["id"]
                    elif sibling is not None and sibling.exchange_order_id is None:
                        sibling.update_exchange_order_id(row["id"])
                        self._alarm("placement-paired", client_id=sibling_id, exchange_order_id=row["id"],
                                    paired_with_lookup_of=client_order_id)
                        safe_ensure_future(self._cancel_if_asked(sibling))
                if len(siblings) > 1:
                    self._alarm_once(f"placement-paired:{client_order_id}", "placement-paired",
                                     client_id=client_order_id, exchange_order_id=found,
                                     siblings=[cid for _, cid, _ in siblings], candidates=[r["id"] for r in matches])
                if found is not None:
                    return found
                continue
            resting = [r["id"] for r in matches if r["status"] not in CONSTANTS.DONE_STATUSES]
            self._alarm_once(f"placement-ambiguous:{client_order_id}", "placement-ambiguous",
                             client_id=client_order_id, symbol=symbol,
                             candidates=[(r["id"], r["status"], str(r["deal_volume"])) for r in matches],
                             our_identical_orders=[cid for _, cid, _ in siblings],
                             action=f"cancelling every one still open: {resting}; any done one may hold fills of ours: "
                                    f"check GroveX")
            for exchange_order_id in resting:
                safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id, symbol))
            return None
        return None

    async def _cancel_by_exchange_id(self, exchange_order_id: str, symbol: str) -> None:
        try:
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                            data={"order_id": exchange_order_id, "symbol": symbol},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-by-exchange-id", exchange_order_id=exchange_order_id, response=self._raw(response))
        except Exception as e:
            self._alarm("cancel-by-exchange-id-failed", exchange_order_id=exchange_order_id, error=repr(e))

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        waited = tracked_order.exchange_order_id is None
        if waited:
            # An unconfirmed placement: remembered, and sent the moment the order gets its id (review 2026-10-09: the
            # base's 10 s wait for the id ran out before a lookup could finish, and the cancel was dropped).
            self._cancel_intent.add(order_id)
        exchange_order_id = await tracked_order.get_exchange_order_id()
        if waited and order_id not in self._cancel_intent:
            return True        # sent by _cancel_if_asked as the order got its id
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        self._touch_locks(tracked_order.base_asset, tracked_order.quote_asset)
        self._cancel_intent.discard(order_id)
        body = {"order_id": str(exchange_order_id), "symbol": symbol}
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, data=body, is_auth_required=True,
                                        limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    response=self._raw(response))
        self._raise_on_error(response, f"GroveX refused to cancel order {order_id} ({exchange_order_id})")
        self._cancel_sent.add(order_id)
        # A read cached just before the cancel would put the order back to OPEN over PENDING_CANCEL: read it next poll.
        self._detail_cache.pop(str(exchange_order_id), None)
        self._poll_notifier.set()
        return True

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """The base's cancel path, except that a refusal (code 22: unknown or finished) is a WARNING with GroveX's answer
        (no traceback) and reads the order's status at once, an HTTP timeout on the cancel reads the status too (the
        cancel may have landed), and an order still without an exchange id keeps the cancel for when it gets one."""
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if order.exchange_order_id is None:
                self.logger().warning(f"GroveX cancel of {order.client_order_id} kept for later: the order has no "
                                      f"exchange id yet. It is sent once the status poll finds the order on GroveX.")
            else:
                self.logger().warning(f"GroveX did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except GrovexBusinessError as refusal:
            self.logger().warning(f"GroveX refused to cancel {order.client_order_id}: {refusal}. Reading its status "
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
            self.logger().warning(f"GroveX status read for {order.client_order_id} after its cancel failed; the next "
                                  f"poll reads it again: {e!r}")

    def _order_state(self, status: Optional[str], executed: Decimal, original: Decimal) -> Optional[OrderState]:
        """GroveX's status -> a state. A value outside the map is derived from the amounts, never guessed. None when
        there is no status at all: the caller keeps the order's current state (review 2026-10-09)."""
        if status is None:
            return None
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

    # ------------------------------------------------------------------ order status (REST) and fills

    def _age(self, order: InFlightOrder) -> float:
        return max(0.0, self._now() - float(order.creation_timestamp or self._now()))

    def _created_ms(self, order: InFlightOrder) -> int:
        return self._sent_at_ms.get(order.client_order_id) or int(float(order.creation_timestamp or self._now()) * 1e3)

    async def _resolve_missing_order(self, order: InFlightOrder, symbol: str) -> Dict[str, Any]:
        """order_info answered nulls (GroveX's silent "not found", live). The order's row in the open orders or the
        orders around its creation stands in; failing both, it is not found — but only once it is old enough to have
        been readable (a just-placed order may lag)."""
        created = float(order.creation_timestamp or self._now())
        rows = await self._list_orders(symbol, self._created_ms(order),
                                       read_after=created + CONSTANTS.NOT_FOUND_MIN_AGE)
        for row in rows:
            if row["id"] == str(order.exchange_order_id):
                self._audit_once("order-info-null-but-listed", exchange_order_id=order.exchange_order_id,
                                 status=row["status"], row=self._raw(row["raw"]))
                return row
        if self._age(order) < CONSTANTS.NOT_FOUND_MIN_AGE or not self._not_listed_is_final(order, symbol):
            raise GrovexBusinessError(f"Order {order.client_order_id} ({order.exchange_order_id}) not readable yet "
                                      f"(age {self._age(order):.1f} s)", CONSTANTS.CODE_EMPTY_ANSWER)
        raise GrovexBusinessError(f"Order {order.client_order_id} ({order.exchange_order_id}) is not on GroveX: "
                                  f"order_info null, not open, not among the orders around its creation",
                                  CONSTANTS.CODE_ORDER_NOT_FOUND)

    async def _read_order_info(self, order: InFlightOrder, exchange_order_id: str,
                               symbol: str) -> Optional[Dict[str, Any]]:
        """order_info (order + its trade list), normalized; None when GroveX answers nulls (not found)."""
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ORDER_INFO_PATH,
                                params={"order_id": exchange_order_id, "symbol": symbol},
                                is_auth_required=True, limit_id=CONSTANTS.ORDER_INFO_PATH),
            f"Error fetching status of order {order.client_order_id} ({exchange_order_id})",
        )
        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        info = data.get("order_info")
        if not isinstance(info, dict) or not info:
            return None
        detail = self._normalize(info)
        trade_list = data.get("trade_list")
        detail["trade_list"] = trade_list if isinstance(trade_list, list) else None
        self._audit_once("order-info-fields", exchange_order_id=exchange_order_id, data=self._raw(data))
        return detail

    async def _status_from_open_list(self, order: InFlightOrder, symbol: str, detail: Dict[str, Any]) -> None:
        """order_info carries no `status` (the docs' example has none). The shared open-order list decides: listed ->
        its status, and the larger deal_volume of the two reads (the list is read later). Not listed, old enough -> the
        order is done: order_info is read once more (its deal_volume is final now; review 2026-10-09: settling on the
        first, older read could book a cancel short of a last fill), then FILLED if all of it filled, else CANCELED. If
        the list can't be read, or can't say "not listed" (_not_listed_is_final), the status stays unknown and the order
        keeps its state."""
        old_enough = self._age(order) >= CONSTANTS.NOT_FOUND_MIN_AGE
        created = float(order.creation_timestamp or self._now())
        try:
            rows = await self._open_orders(symbol,
                                           read_after=created + CONSTANTS.NOT_FOUND_MIN_AGE if old_enough else 0.0)
            listed = rows.get(detail.get("id") or str(order.exchange_order_id))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"GroveX open orders for {order.client_order_id}'s status failed: {e!r}")
            return
        if listed is not None:
            detail["status"] = listed["status"] or ("3" if (listed["deal_volume"] or s_decimal_0) > 0 else "1")
            if listed["deal_volume"] is not None:
                detail["deal_volume"] = max(detail["deal_volume"] or s_decimal_0, listed["deal_volume"])
            return
        if not old_enough or not self._not_listed_is_final(order, symbol):
            return
        fresh = await self._read_order_info(order, str(order.exchange_order_id), symbol)
        if fresh is not None and fresh["deal_volume"] is not None:
            detail["deal_volume"] = max(detail["deal_volume"] or s_decimal_0, fresh["deal_volume"])
            if fresh.get("trade_list") is not None:
                detail["trade_list"] = fresh["trade_list"]
        if detail["deal_volume"] is None or detail["volume"] is None:
            return
        detail["status"] = "2" if detail["deal_volume"] >= detail["volume"] else "4"
        self._audit_once("status-derived", exchange_order_id=order.exchange_order_id, status=detail["status"],
                         deal_volume=str(detail["deal_volume"]), volume=str(detail["volume"]))

    async def _fetch_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """GET order_info, read once per poll: the fills pass and the status pass share it (_update_order_status empties
        the cache as a poll starts, and a cancel drops its order's entry). An order without an exchange id is looked up on
        GroveX first (_adopt_placed_order)."""
        exchange_order_id = order.exchange_order_id
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        if exchange_order_id is None:
            exchange_order_id = await self._adopt_placed_order(order, symbol)
        exchange_order_id = str(exchange_order_id)
        cached = self._detail_cache.get(exchange_order_id)
        if cached is not None:
            return cached
        detail = await self._read_order_info(order, exchange_order_id, symbol)
        if detail is not None:
            if detail["status"] is None:
                await self._status_from_open_list(order, symbol, detail)
        else:
            detail = await self._resolve_missing_order(order, symbol)
            detail["trade_list"] = None
        self._detail_cache[exchange_order_id] = detail
        return detail

    async def _adopt_placed_order(self, order: InFlightOrder, symbol: str) -> str:
        """The poll's side of an unconfirmed placement: one look for the order on GroveX, no waits. Found -> the order
        gets that id (and a cancel asked for meanwhile is sent). Not readable yet (CODE_EMPTY_ANSWER, no strike): its
        placement still awaits the answer (only the placement looks then), the lookup could not tell, or the order is
        younger than NOT_FOUND_MIN_AGE. Otherwise CODE_ORDER_NOT_FOUND: a strike toward FAILED."""
        client_order_id = order.client_order_id
        if client_order_id in self._placing:
            raise GrovexBusinessError(f"Order {client_order_id}: its placement still awaits GroveX's answer",
                                      CONSTANTS.CODE_EMPTY_ANSWER)
        try:
            found = await self._find_placed_order(client_order_id, symbol, self._created_ms(order), (0.0,))
        except _LookupInconclusive as e:
            raise GrovexBusinessError(f"Order {client_order_id} has no exchange id yet: {e}",
                                      CONSTANTS.CODE_EMPTY_ANSWER)
        if found is None:
            if order.exchange_order_id is not None:          # paired by a sibling's lookup meanwhile
                found = str(order.exchange_order_id)
            elif self._age(order) < CONSTANTS.NOT_FOUND_MIN_AGE or not self._not_listed_is_final(order, symbol):
                raise GrovexBusinessError(f"Order {client_order_id} has no exchange id yet",
                                          CONSTANTS.CODE_EMPTY_ANSWER)
            else:
                raise GrovexBusinessError(f"Order {client_order_id} is not on GroveX (looked up by side, price, "
                                          f"quantity and time)", CONSTANTS.CODE_ORDER_NOT_FOUND)
        if order.exchange_order_id is None:
            order.update_exchange_order_id(found)
            self._alarm("placement-found", client_id=client_order_id, exchange_order_id=found)
        self._check_id_collision(order, str(found))
        await self._cancel_if_asked(order)
        return str(found)

    def _check_volume(self, order: InFlightOrder, detail: Dict[str, Any]) -> bool:
        """GroveX's own quantity of the order must be ours (type 1: `volume` = the base quantity, docs). A mismatch would
        be an order of another size: alarm and cancel it at once. True if the order may proceed."""
        if order.client_order_id in self._volume_checked:
            return True
        quantity = detail.get("volume")
        if quantity is None or quantity <= 0:
            return True      # can't tell from this read; the next one may
        self._audit_once("volume-semantics", client_id=order.client_order_id, sent=str(order.amount),
                         grovex_volume=str(quantity), status=detail.get("status"))
        self._volume_checked.add(order.client_order_id)
        if abs(quantity - order.amount) <= order.amount * CONSTANTS.VOLUME_MISMATCH_TOLERANCE:
            return True
        self._alarm("volume-semantics", client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                    sent_base=str(order.amount), grovex_volume=str(quantity), action="cancelling the order")
        safe_ensure_future(self._cancel_by_exchange_id(str(order.exchange_order_id),
                                                       f"{order.base_asset}{order.quote_asset}".lower()))
        return False

    async def _symbol_trades(self, symbol: str, since_ms: int) -> List[Dict[str, Any]]:
        """The account's trades on the market (all_trade, newest first), read page by page back to since_ms (or
        MAX_TRADE_PAGES), once per poll per market: a later order's ask needing more history reads further. Raises if the
        first page can't be read."""
        cached = self._trades_cache.get(symbol)
        if cached is not None and cached[0] <= since_ms:
            return cached[1]
        rows: List[Dict[str, Any]] = []
        covered = int(time.time() * 1e3)
        for page in range(1, CONSTANTS.MAX_TRADE_PAGES + 1):
            response = self._raise_on_error(
                await self._api_get(path_url=CONSTANTS.MY_TRADES_PATH,
                                    params={"symbol": symbol, "page": page, "pageSize": CONSTANTS.LIST_PAGE_SIZE,
                                            "sort": 1},
                                    is_auth_required=True, limit_id=CONSTANTS.MY_TRADES_PATH),
                f"Error fetching GroveX trades on {symbol} (page {page})")
            data = response.get("data")
            listed = data.get("resultList") if isinstance(data, dict) else None
            listed = [r for r in listed or [] if isinstance(r, dict)]
            rows.extend(listed)
            times = [web_utils.to_ms(r.get("ctime")) for r in listed]
            times = [t for t in times if t is not None]
            if times:
                covered = min(covered, min(times))
            if len(listed) < CONSTANTS.LIST_PAGE_SIZE or (times and min(times) < since_ms):
                covered = min(covered, since_ms)
                break
        else:
            self._alarm_once(f"trade-pages-capped:{symbol}:{since_ms // 3_600_000}", "trade-pages-capped",
                             symbol=symbol, pages=CONSTANTS.MAX_TRADE_PAGES, oldest_needed_ms=since_ms,
                             oldest_read_ms=covered)
        self._trades_cache[symbol] = (covered, rows)
        return rows

    async def _trades_for_order(self, order: InFlightOrder, symbol: str) -> List[Dict[str, Any]]:
        """The account's trades that belong to the order: bid_id for a buy, ask_id for a sell."""
        rows = await self._symbol_trades(symbol, self._created_ms(order) - 60_000)
        key = "bid_id" if order.trade_type is TradeType.BUY else "ask_id"
        own = [r for r in rows if str(r.get(key)) == str(order.exchange_order_id)]
        self._audit_once("all-trade-fields", exchange_order_id=order.exchange_order_id, rows=self._raw(own[:3]),
                         listed=len(rows))
        return own

    async def _fills_from_trades(self, order: InFlightOrder, detail: Dict[str, Any]) -> List[TradeUpdate]:
        """GroveX's own trade rows for the order, each a TradeUpdate named by GroveX's trade id, booked once, never past
        the order's cumulative filled quantity (deal_volume) or its size. deal_volume missing -> nothing is booked (it
        can't be capped) and a [GX-ALARM] says so."""
        cumulative = detail.get("deal_volume")
        if cumulative is None:
            self._alarm_once(f"deal-volume-missing:{order.client_order_id}", "deal-volume-missing",
                             client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                             detail=self._raw(detail.get("raw")), action="no fill booked until GroveX gives it")
            return []
        if cumulative <= order.executed_amount_base + TOLERANCE or order.exchange_order_id is None:
            return []
        if cumulative > order.amount * (1 + CONSTANTS.VOLUME_MISMATCH_TOLERANCE):
            self._alarm_once(f"fill-over-amount:{order.client_order_id}:{cumulative}", "fill-over-amount",
                             client_id=order.client_order_id, cumulative=str(cumulative), amount=str(order.amount))
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        if detail.get("trade_list"):              # a list or None (_read_order_info normalizes it)
            self._audit_once("trade-list-fields", exchange_order_id=order.exchange_order_id,
                             trade_list=self._raw(detail["trade_list"][:3]))
        rows = await self._trades_for_order(order, symbol)
        fills: List[TradeUpdate] = []
        booked = order.executed_amount_base          # read after the await: a concurrent read may have booked rows
        for row in sorted(rows, key=lambda r: (web_utils.to_ms(r.get("ctime")) or 0, str(r.get("id")))):
            trade_id = str(row.get("id") or "")
            if not trade_id or trade_id in order.order_fills:
                continue
            quantity, price = self._dec(row.get("volume")), self._dec(row.get("price"))
            if quantity <= 0 or price <= 0:
                self._alarm_once(f"fill-unreadable:{order.client_order_id}:{trade_id}", "fill-unreadable",
                                 client_id=order.client_order_id, row=self._raw(row))
                continue
            if booked + quantity > cumulative + TOLERANCE:
                # Never past the order's own filled quantity: the trade list ran ahead of order_info, or repeats a trade.
                self._alarm_once(f"fill-capped:{order.client_order_id}:{trade_id}:{cumulative}", "fill-capped",
                                 client_id=order.client_order_id, trade_id=trade_id, booked=str(booked),
                                 fill=str(quantity), cumulative=str(cumulative))
                break
            limit = order.price if order.price is not None and not order.price.is_nan() else s_decimal_0
            if limit > 0 and not (limit / 2 <= price <= limit * 2):
                self._alarm_once(f"fill-price-implausible:{order.client_order_id}:{trade_id}", "fill-price-implausible",
                                 client_id=order.client_order_id, trade_id=trade_id, price=str(price),
                                 limit=str(limit), action="refused")
                continue
            fee_coin = str(row.get("feeCoin") or "").upper() or (order.base_asset if order.trade_type is TradeType.BUY
                                                                  else order.quote_asset)
            fee_amount = self._dec(row.get("fee"))
            self._audit_once(f"fee-rate:{order.trade_type.name}", fee=str(fee_amount), fee_coin=fee_coin,
                             quantity=str(quantity), price=str(price),
                             per_quote=str(fee_amount / (quantity * price)) if quantity * price > 0 else None,
                             per_base=str(fee_amount / quantity) if quantity > 0 else None, row=self._raw(row))
            fills.append(TradeUpdate(
                trade_id=trade_id,
                client_order_id=order.client_order_id,
                exchange_order_id=str(order.exchange_order_id),
                trading_pair=order.trading_pair,
                fee=TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_coin)] if fee_amount > 0 else [],
                ),
                fill_base_amount=quantity,
                fill_quote_amount=quantity * price,
                fill_price=price,
                fill_timestamp=(web_utils.to_ms(row.get("ctime")) or self._server_ms()) / 1e3,
            ))
            booked += quantity
        return fills

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """The base's fills pass runs over active AND recently done orders (its 30 s cache). A done order is skipped: a
        terminal state is reported only once every trade is booked (_request_order_status), so nothing is left."""
        if order.exchange_order_id is None or order.is_done:
            return []
        try:
            detail = await self._fetch_order_detail(order)
        except GrovexBusinessError as e:
            if e.code in (CONSTANTS.CODE_ORDER_NOT_FOUND, CONSTANTS.CODE_EMPTY_ANSWER) and order.executed_amount_base <= 0:
                return []
            raise
        if not self._check_volume(order, detail):
            return []
        fills = await self._fills_from_trades(order, detail)
        for fill in fills:
            self._note_fill_booked(order, fill)
        return fills

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        try:
            detail = await self._fetch_order_detail(tracked_order)
        except GrovexBusinessError as e:
            if (self._is_not_found(e) and tracked_order.client_order_id in self._cancel_sent
                    and tracked_order.executed_amount_base <= 0):
                # We asked GroveX to cancel it, it never filled, and it is gone from every list: cancelled (a venue that
                # drops an order cancelled without a fill answers exactly this).
                self._audit_once("cancelled-order-gone", client_id=tracked_order.client_order_id)
                return self._order_update(tracked_order, OrderState.CANCELED)
            raise
        if not self._check_volume(tracked_order, detail):
            return self._order_update(tracked_order, OrderState.PENDING_CANCEL)
        # Fills first: a FILLED state with fills still missing would complete the order short.
        for fill in await self._fills_from_trades(tracked_order, detail):
            self._note_fill_booked(tracked_order, fill)
            self._order_tracker.process_trade_update(fill)
        cumulative = detail.get("deal_volume")
        status = detail.get("status")
        new_state = self._order_state(status, cumulative or s_decimal_0, tracked_order.amount)
        self._audit_once(f"order-status:{status}", mapped=str(new_state), deal_volume=str(cumulative),
                         remain_volume=str(detail.get("remain_volume")))
        client_order_id = tracked_order.client_order_id
        if new_state is None:
            return self._order_update(tracked_order, tracked_order.current_state)   # status unknown: keep the state
        if new_state in CONSTANTS.TERMINAL_STATES and (
                cumulative is None or tracked_order.executed_amount_base + TOLERANCE < cumulative):
            # GroveX says done but its trade list hasn't shown every fill yet (or deal_volume is missing, or a guard
            # refused a fill row): nothing new is reported until every fill is booked. Strict on purpose — done with
            # fills missing, the strategy would hedge the wrong size. Past FILLS_MISSING_ALARM_SECONDS one [GX-ALARM].
            first = self._fills_pending_since.setdefault(client_order_id, time.monotonic())
            self._audit_once(f"fills-pending:{client_order_id}", client_id=client_order_id,
                             cumulative=str(cumulative), booked=str(tracked_order.executed_amount_base))
            if time.monotonic() - first >= CONSTANTS.FILLS_MISSING_ALARM_SECONDS:
                self._alarm_once(f"fills-missing:{client_order_id}", "fills-missing", client_id=client_order_id,
                                 exchange_order_id=tracked_order.exchange_order_id, grovex_state=new_state.name,
                                 cumulative=str(cumulative), booked=str(tracked_order.executed_amount_base),
                                 waited_s=round(time.monotonic() - first, 1),
                                 action="kept open in Hummingbot until GroveX's trade list shows every fill: check "
                                        "the order on GroveX")
            return self._order_update(tracked_order, tracked_order.current_state)
        self._fills_pending_since.pop(client_order_id, None)
        if new_state in CONSTANTS.TERMINAL_STATES:
            self._cancel_sent.discard(client_order_id)
            self._touch_locks(tracked_order.base_asset, tracked_order.quote_asset)
        return self._order_update(tracked_order, new_state)

    def _order_update(self, order: InFlightOrder, state: OrderState) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id) if order.exchange_order_id is not None else None,
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
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
        """No fee route (2026-10-09): DEFAULT_FEES estimates; each fill books GroveX's own fee."""
        return

    # ------------------------------------------------------------------ balances (user/account ~22 s)

    def _note_fill_booked(self, order: InFlightOrder, fill: TradeUpdate) -> None:
        """A fill moves the TOTALS at once — base, quote and the fee coin — the way a venue's balance push would: the
        hold-band reads the total and must see a fill when it is booked, not ~25 s later at the next read. Counted once per
        GroveX trade id, and journaled with GroveX's trade time, which decides whether a balance read holds it:
          - timed before the window of the read last applied to the asset: that read holds it, the total stays;
          - timed after the window start: not held (fills inside the window at the read's application skip the asset,
            _read_and_apply_balances), so it moves the total, and `available` past that read's cutoff;
          - timed inside the window up to the read's answer but booked after it was applied: the read may or may not hold
            it — counted, and the asset's next read applies whatever its orders (the stale waiver)."""
        if not self._first_time(self._noted_fills, f"{fill.exchange_order_id}:{fill.trade_id}"):
            return
        base, quote = order.base_asset, order.quote_asset
        sign = Decimal(1) if order.trade_type is TradeType.BUY else Decimal(-1)
        deltas: Dict[str, Decimal] = {base: sign * fill.fill_base_amount, quote: -sign * fill.fill_quote_amount}
        for fee in fill.fee.flat_fees:
            deltas[fee.token] = deltas.get(fee.token, s_decimal_0) - fee.amount
        trade_ms = int(fill.fill_timestamp * 1e3)
        self._fill_journal.append((trade_ms, deltas))
        for asset, delta in deltas.items():
            window_start, received = self._total_read_window.get(asset, (-1, -1))
            if trade_ms > window_start:
                self._account_balances[asset] = self._account_balances.get(asset, s_decimal_0) + delta
                if trade_ms <= received:
                    self._read_applied_at.pop(asset, None)
                    self._audit_once(f"fill-booked-after-read:{asset}", asset=asset, trade_ms=trade_ms,
                                     read_window=(window_start, received))
            if trade_ms > self._read_cutoff_ms.get(asset, -1):
                self._filled_since_read[asset] = self._filled_since_read.get(asset, s_decimal_0) + delta

    def apply_balance_update_since_snapshot(self, currency: str, available_balance: Decimal) -> Decimal:
        """available = the asset's available at its last applied read + our orders' locked amount when that read was
        applied - our orders' locked amount now + every journaled fill after the read's cutoff (fees included). An asset
        never read: 0 + its fills - its locks."""
        snapshot_locked = self._snapshot_locked.get(currency, s_decimal_0)
        in_flight = self.in_flight_asset_balances(self.in_flight_orders).get(currency, s_decimal_0)
        filled = self._filled_since_read.get(currency, s_decimal_0)
        return available_balance + snapshot_locked - in_flight + filled

    async def _update_all_balances(self) -> None:
        """The poll's balance step: never waits for the ~22 s read. It starts one in the background when none is running
        and BALANCE_READ_GAP has passed since the last."""
        if self._balance_task is not None and not self._balance_task.done():
            return
        if time.monotonic() - self._balance_read_done_at < CONSTANTS.BALANCE_READ_GAP:
            return
        self._balance_task = safe_ensure_future(self._background_balance_read())

    async def _background_balance_read(self) -> None:
        try:
            await self._read_and_apply_balances()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            now = time.monotonic()
            if now - self._balance_fail_logged_at >= 300:
                self._balance_fail_logged_at = now
                self.logger().warning(f"GroveX balance read failed; the last balances stay: {e!r}")
        finally:
            self._balance_read_done_at = time.monotonic()

    async def _update_balances(self) -> None:
        """One full read (~22 s), applied by the same rule. Awaited by startup and the `balance` command."""
        await self._read_and_apply_balances()

    def _assets_with_open_orders(self) -> set:
        assets = set()
        for o in self.in_flight_orders.values():
            if not (o.is_done or o.is_failure or o.is_cancelled):
                assets.update((o.base_asset, o.quote_asset))
        return assets

    async def _read_and_apply_balances(self) -> None:
        """GET user/account (~22 s): coin_list[{coin, normal, locked}] — every coin, zeros included (live). For each
        asset, the read is applied (see BALANCE_* in grovex_constants):
          - total, unless one of our fills on the asset has a GroveX trade time inside the read's window (it can't say
            whether it holds that fill), or an order of ours on the asset was open at the read or is open now (it could
            fill unseen) — waived when the asset has gone STALE_READ_SECONDS without a read; fills after the window are
            added back on top of the read;
          - available too, unless a placement or a cancel on the asset fell inside the window.
        The first read is the baseline: applied to every asset (an ambiguous fill in it raises [GX-ALARM]
        balance-baseline-ambiguous), and the connector counts as ready only after it."""
        async with self._balance_lock:
            sent_mono = time.monotonic()
            sent_ms = self._server_ms()
            open_at_send = self._assets_with_open_orders()
            response = self._raise_on_error(
                await self._api_get(path_url=CONSTANTS.ACCOUNT_PATH, is_auth_required=True,
                                    limit_id=CONSTANTS.ACCOUNT_PATH, timeout=CONSTANTS.BALANCE_READ_TIMEOUT),
                "Error fetching GroveX balances")
            data = response.get("data")
            rows = data.get("coin_list") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                raise IOError(f"GroveX balance answer has no coin list | GroveX response: {self._raw(response)[:500]}")
            if not rows and any(total > 0 for total in self._account_balances.values()):
                raise IOError("GroveX answered an empty coin list while balances are held: not believed")
            received_mono = time.monotonic()
            received_ms = self._server_ms()
            read_s = received_mono - sent_mono
            self._audit_once("balance-rest-shape", coins=len(rows), read_s=round(read_s, 1),
                             sample=self._raw([r for r in rows if self._dec(r.get("normal")) > 0][:3]))
            if self.in_flight_orders:
                await asyncio.sleep(CONSTANTS.BALANCE_POST_SLACK_BUSY)   # fills of the window are booked first
            elif self._fill_journal:
                await asyncio.sleep(CONSTANTS.BALANCE_POST_SLACK)
            baseline = not self._balances_ready
            lo, hi = sent_ms - CONSTANTS.FILL_TIME_SLACK_MS, received_ms + CONSTANTS.FILL_TIME_SLACK_MS
            lock_lo, lock_hi = sent_mono - CONSTANTS.BALANCE_PRE_SLACK, received_mono + CONSTANTS.BALANCE_POST_SLACK
            now_mono = time.monotonic()
            open_now = self._assets_with_open_orders()
            locked_now = self.in_flight_asset_balances(self.in_flight_orders)
            applied, skipped = 0, []
            for entry in rows:
                asset = str(entry.get("coin") or "").upper()
                if not asset or entry.get("normal") is None:
                    continue
                available = self._dec(entry.get("normal"))
                total = available + self._dec(entry.get("locked"))
                in_window = [d[asset] for t, d in self._fill_journal if asset in d and lo <= t <= hi]
                stale = now_mono - self._read_applied_at.get(asset, float("-inf")) >= CONSTANTS.STALE_READ_SECONDS
                open_orders = asset in open_at_send or asset in open_now
                if not baseline and (in_window or (open_orders and not stale)):
                    skipped.append(asset)
                    self._check_drift(asset, total)
                    continue
                if baseline and in_window:
                    self._alarm("balance-baseline-ambiguous", asset=asset, fills_in_read_window=[str(x) for x in in_window],
                                note="the first read may or may not hold these fills; the next read settles it")
                after = sum((d[asset] for t, d in self._fill_journal if asset in d and t > hi), s_decimal_0)
                self._account_balances[asset] = total + after
                self._total_read_window[asset] = (lo, received_ms)
                lock_change = lock_lo <= self._lock_activity.get(asset, float("-inf")) <= lock_hi
                if baseline or not lock_change:
                    self._account_available_balances[asset] = available
                    self._read_cutoff_ms[asset] = lo
                    self._snapshot_locked[asset] = locked_now.get(asset, s_decimal_0)
                    self._filled_since_read[asset] = after
                self._read_applied_at[asset] = now_mono
                self._drift_alarmed.discard(asset)
                applied += 1
            self._balances_ready = True
            keep_after = self._server_ms() - CONSTANTS.FILL_JOURNAL_KEEP_SECONDS * 1e3
            self._fill_journal = [(t, d) for t, d in self._fill_journal if t >= keep_after]
            if skipped:
                self._audit_once("balance-read-skipped", assets=sorted(skipped)[:20], read_s=round(read_s, 1))
            self.logger().debug(f"GroveX balances: {applied} assets applied, {len(skipped)} kept running "
                                f"({sorted(skipped)[:10]}), read {read_s:.1f} s{', baseline' if baseline else ''}")

    def _check_drift(self, asset: str, rest_total: Decimal) -> None:
        running = self._account_balances.get(asset, s_decimal_0)
        scale = max(abs(running), abs(rest_total))
        if scale <= TOLERANCE or asset in self._drift_alarmed:
            return
        if abs(running - rest_total) / scale > CONSTANTS.BALANCE_DRIFT_ALARM:
            self._drift_alarmed.add(asset)
            self._alarm("balance-drift", asset=asset, running_total=str(running), read_total=str(rest_total),
                        note="the asset had order activity during the read, so the read was not applied; a read after "
                             "it goes quiet will be")

    # ------------------------------------------------------------------ symbols / rules / prices

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for info in exchange_info.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                symbol = str(info["symbol"]).lower()
                trading_pair = combine_to_hb_trading_pair(base=str(info["base_coin"]).upper(),
                                                          quote=str(info["count_coin"]).upper())
                if symbol in mapping or trading_pair in mapping.inverse:
                    continue          # common/symbols lists btcusdt twice (identical rows, live)
                mapping[symbol] = trading_pair
            except Exception as exception:
                self.logger().error(f"Error parsing GroveX market {info.get('symbol')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair the startup list did not have. GroveX's form is base + quote, lower case."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        symbol_map[f"{base}{quote}".lower()] = trading_pair
        self.logger().warning(f"GroveX {trading_pair} was not among the markets listed at startup; mapped to "
                              f"{base.lower()}{quote.lower()}. GroveX's answers to its requests will show whether it "
                              f"exists.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """common/symbols (live 2026-10-09): price_precision and amount_precision are DECIMALS (BTC 2 and 5),
        limit_volume_min the minimum quantity. No minimum order value is published: GroveX's answer to a small order is
        logged in full."""
        rules: Dict[str, TradingRule] = {}
        for info in exchange_info_dict.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(info["symbol"]).lower())
                if trading_pair in rules:
                    continue
                price_increment = Decimal(1).scaleb(-int(info["price_precision"]))
                amount_increment = Decimal(1).scaleb(-int(info["amount_precision"]))
                rules[trading_pair] = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(info.get("limit_volume_min")), amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                )
            except Exception:
                self.logger().exception(f"Error parsing the GroveX trading rule {info.get('symbol')}. Skipping.")
        return list(rules.values())

    def _refresh_tickers_in_background(self) -> None:
        if self._tickers_task is None or self._tickers_task.done():
            self._tickers_task = safe_ensure_future(self._refresh_tickers())

    async def _refresh_tickers(self) -> None:
        try:
            await self.all_tickers(max_age=CONSTANTS.ALL_TICKER_SHARE_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().debug(f"GroveX ticker refresh failed: {e!r}")

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """From get_allticker's `last` (one call for every market, but 6-9 s): the cached read answers at once and an
        older one is refreshed in the background; a pair with no value gets NaN (the tracker re-asks a pair it got
        nothing for at once)."""
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        cached = self._tickers_cache
        if cached is None or time.monotonic() - cached[0] > CONSTANTS.ALL_TICKER_SHARE_SECONDS:
            self._refresh_tickers_in_background()
        if cached is None:
            return prices
        rows = cached[1]
        for trading_pair in trading_pairs:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            except KeyError:
                continue
            row = rows.get(symbol)
            if row is not None and row.get("last") not in (None, ""):
                try:
                    prices[trading_pair] = float(row["last"])
                except (TypeError, ValueError):
                    pass
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]
