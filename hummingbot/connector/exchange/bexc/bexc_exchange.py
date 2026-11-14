import asyncio
import copy
import json
import time
from collections import deque
from decimal import Decimal, InvalidOperation
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.bexc import (
    bexc_constants as CONSTANTS,
    bexc_utils as utils,
    bexc_web_utils as web_utils,
)
from hummingbot.connector.exchange.bexc.bexc_api_order_book_data_source import BexcAPIOrderBookDataSource
from hummingbot.connector.exchange.bexc.bexc_api_user_stream_data_source import BexcAPIUserStreamDataSource
from hummingbot.connector.exchange.bexc.bexc_auth import BexcAuth
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


class BexcAPIError(IOError):
    """bexc answered with an HTTP error status (its body `code` and the raw answer kept), a request was not sent (the WAF
    breaker, the auth hold), or one of our own verdicts (CODE_PLACEMENT_NOT_FOUND, CODE_EMPTY_ANSWER). IOError keeps
    every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[str], status: Optional[int] = None, body: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.body = body


class BexcPlacementUnknown(IOError):
    """A placement bexc may have accepted (no answer, an ambiguous one, or one without an order id) that the lookup did
    not find: the order is NOT failed, as it may rest on bexc. It stays PENDING_CREATE and the status poll keeps looking
    for it; only the poll's age-gated not-found verdict fails it."""


class _LookupInconclusive(Exception):
    """The placement lookup could not tell: a read failed."""


class BexcExchange(ExchangePyBase):
    """
    bexc spot connector: the documented REST API v1 for everything private, the engine's public socket for books.

    Reference: the docs mirror (VS_code_projects/MDs/bexc-api/), Pavel's placement probe (myserver 2026-11-13), the
    asset_manager's read-only runs (2026-11-13/14) and the wiki page trading/exchanges/bexc-api. What those could not
    settle is audit-logged ([BX-AUDIT]) rather than guessed; money guards log as [BX-ALARM].

    Scope: LIMIT orders only (gtc), which is all arb_l and the position balancer send.

    What bexc does not have, and what stands in for it (BloFin's design, 2026-11-11):
      - No private push for an API key. While any order is open the poll runs every ORDER_POLL_INTERVAL (1 s): ONE
        open-orders read for every market (on the idle poll too: it feeds the orphan guard and finds unconfirmed
        placements), GET /order/{id} only for an order the list doesn't show (it closed, or isn't listed yet),
        /trades/my only when an order's filled quantity grew (one read per market per poll, shared), one balance read.
      - Two order views. The list rows, the place answer and the cancel answer say open / filled / cancelled with
        `filled_quantity`; GET /order/{id} is a CCXT view saying open / closed / canceled with `filled`. Both become one
        row shape here (_order_row). An `open` row with a filled quantity is a partial fill.
      - Fills carry no client id and can't be filtered by order: a market's newest fills are read and joined on
        order_id. They carry the fee (in the quote, live), the quote amount and is_maker.
      - Client ids are real (live): an unconfirmed placement is found by GET /order?client_order_id=, a duplicate
        placement answers 409 naming the existing order, and a cancel may name the client id (the docs' advice when
        the engine id was never seen). Cancels otherwise go by the order id.
      - A WAF in front of the API blocks the IP for hours (403 RATE_002) and 404s may count toward it: documented routes
        only, listed symbols only, no 404 asked for on purpose, and after a RATE_002 no bexc call at all for
        WAF_BLOCK_HOLD_S (process-wide: P2 and P3 share myserver's IP). The requests that answer 404 by design (the
        client-id lookups and cancels) are capped at CLIENT_ID_404_PER_MINUTE per process.
    """

    web_utils = web_utils

    # The WAF blocks the IP, so its breaker is process-wide (a class attribute), as in the asset_manager's client.
    _waf_blocked_until: float = 0.0
    # The 404s of client-id requests (monotonic times, the last 60 s), process-wide for the same reason.
    _client_id_404_times: Deque[float] = deque()
    _SCRUBBED_KEYS = frozenset({"user_id"})   # the account's id rides on every list row: never logged

    def __init__(
        self,
        bexc_api_key: str,
        bexc_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature must match ConnectorSetting.conn_init_parameters in this fork (balance_asset_limit and
        rate_limits_share_pct, no config map): an upstream-style client_config_map crashes `balance` (CoinEx)."""
        self._api_key = bexc_api_key
        self._secret_key = bexc_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: Dict[str, None] = {}
        self._alarm_seen: Dict[str, None] = {}
        self._last_private_read: float = 0.0                 # wall time of the last successful signed read
        self._last_time_sync: float = 0.0
        self._auth_held_until: float = 0.0                   # no signed call before this (wall time)
        self._market_list_cache: Optional[Tuple[float, Any]] = None
        self._market_list_lock = asyncio.Lock()
        self._ticker_cache: Optional[Tuple[float, Dict[str, float]]] = None
        # One poll's reads, shared by its fills pass and its status pass (cleared at each poll's start).
        self._row_cache: Dict[str, Dict[str, Any]] = {}      # order id -> row (the open list, then GET /order/{id})
        self._open_by_client_id: Dict[str, str] = {}         # client id -> order id, from the poll's open list
        self._fills_cache: Dict[str, Tuple[int, List[Dict[str, Any]]]] = {}  # symbol -> (read back to ms, rows)
        # Per-order state, by client id; entries of orders no longer tracked are dropped every poll (_purge_order_state).
        self._placing: Set[str] = set()                      # placements awaiting bexc's answer
        self._size_checked: Set[str] = set()                 # orders whose quantity bexc confirmed
        self._sent_at_ms: Dict[str, int] = {}                # bexc ms just before the placement
        self._cancel_sent: Dict[str, float] = {}             # monotonic time bexc took (or answered 409 to) our cancel
        self._short_since: Dict[str, float] = {}             # monotonic time its terminal state was first held for fills
        self._fills_retry: Dict[str, Tuple[float, float, Tuple[Decimal, bool]]] = {}  # next read, interval, what it saw
        self._unreadable_until: Dict[str, float] = {}        # no GET /order/{id} before this (monotonic)
        self._unreadable_interval: Dict[str, float] = {}     # its current back-off (s)
        self._lookup_after: Dict[str, float] = {}            # no client-id lookup by the poll before this (monotonic)
        self._final_rows: Dict[str, Dict[str, Any]] = {}     # its terminal row: final, never read again
        self._fill_booked_at: Dict[str, int] = {}            # _balance_reads_sent when its last fill was booked
        self._settling: Set[str] = set()
        # The completion gate (_balance_shows_fills): balance reads are numbered as they go out, and the balances are
        # from read number _balance_applied (an older answer arriving later is dropped).
        self._balance_reads_sent = 0
        self._balance_applied = 0
        # Balance answers in a row that left out a coin held (_update_balances).
        self._omitting_since: Optional[float] = None
        self._omitting_reads = 0
        # The orphan guard (_check_orphans): client ids this process sent to bexc; the orders it handled, by order id ->
        # the monotonic time before which it is left alone (inf once handled; a failed cancel re-arms it).
        self._placed_ids: Dict[str, None] = {}
        self._orphan_ids: Dict[str, float] = {}
        # The symbol map this connector last set: the base keeps its own in a C slot a Python subclass can't read, and
        # the next map carries over the markets still holding tracked orders (S5, 2026-11-14).
        self._last_symbol_map: Optional[bidict] = None
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # No balance push exists: the REST poll is authoritative and the base's in-flight snapshot keeps `available`
        # right between polls.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BexcAuth:
        return BexcAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        # The cancel answer does carry the state (live: "cancelled" + filled_quantity), but a terminal state waits for
        # the order's fills and for a balance read showing them: the answer becomes the order's final row and the poll,
        # woken at once, settles it (_place_cancel). Only _execute_order_cancel_and_process_update reads this.
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
        return BexcAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BexcAPIUserStreamDataSource(connector=self)

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [BX-AUDIT] line. Only questions the docs and probes could not answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[BX-AUDIT] {tag} {rendered}")

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
        """A money guard fired: one [BX-ALARM] WARNING, whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[BX-ALARM] {tag} {rendered}")

    def _alarm_once(self, key: str, tag: str, **fields: Any) -> None:
        """An alarm about a state the poll meets again every second (a refused fill row, an order kept open): once."""
        if self._first_time(self._alarm_seen, key):
            self._alarm(tag, **fields)

    @classmethod
    def _scrub(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: cls._scrub(v) for k, v in value.items() if k not in cls._SCRUBBED_KEYS}
        if isinstance(value, list):
            return [cls._scrub(v) for v in value]
        return value

    @classmethod
    def _raw(cls, response: Any) -> str:
        try:
            return json.dumps(cls._scrub(response), ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    # ------------------------------------------------------------------ requests and errors

    def _check_breakers(self, path_url: str, is_auth_required: bool) -> None:
        """Nothing goes to bexc while its WAF blocks this IP (a request then could only prolong the block), and no signed
        call while the key was refused moments ago. Raised locally, before the request: nothing is sent."""
        now = time.time()
        if now < BexcExchange._waf_blocked_until:
            raise BexcAPIError(f"not sent ({path_url}): bexc's WAF blocked this IP (RATE_002); no bexc REST call for "
                               f"{BexcExchange._waf_blocked_until - now:.0f} s more", CONSTANTS.CODE_WAF_BLOCK, 403)
        if is_auth_required and now < self._auth_held_until:
            raise BexcAPIError(f"not sent ({path_url}): bexc refused the key moments ago; no signed call for "
                               f"{self._auth_held_until - now:.0f} s more", CONSTANTS.CODE_AUTH_HELD, 401)

    def _client_id_404_budget_left(self) -> bool:
        """False once CLIENT_ID_404_PER_MINUTE client-id requests answered 404 in the last 60 s, process-wide (404s may
        count toward the WAF's block; review C, 2026-11-14). One [BX-ALARM] client-id-404-budget per 10 minutes."""
        times = BexcExchange._client_id_404_times
        now = time.monotonic()
        while times and now - times[0] > 60.0:
            times.popleft()
        if len(times) < CONSTANTS.CLIENT_ID_404_PER_MINUTE:
            return True
        self._alarm_once(f"client-id-404-budget:{int(time.time() // 600)}", "client-id-404-budget",
                         last_60s=len(times), action="no client-id lookup or cancel until older 404s age out: "
                                                    "unconfirmed placements stay pending (no strike)")
        return False

    @staticmethod
    def _note_client_id_404() -> None:
        BexcExchange._client_id_404_times.append(time.monotonic())

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
    ) -> Any:
        """
        bexc answers an error with an HTTP status and a JSON body {"code", "error"} (docs; live 400, 409, 404). Here:
        every answer is read (return_err), numbers are parsed exactly (Decimal), every request has a timeout
        (REQUEST_TIMEOUT unless given: for its headers, and again for its body), and a 2xx returns the body while
        anything else raises BexcAPIError with the status, the code and the raw answer. A body that is not JSON under a
        2xx raises too: callers can't use it, and a placement then counts as unconfirmed.
          - RATE_002 (the WAF, under any status): no bexc call from this process for WAF_BLOCK_HOLD_S.
          - A signed 401: bexc's codes for a stale timestamp or a replay are undocumented (none was provoked: no
            deliberate refusals), so the clock is re-synced and the request re-signed and sent once more. Safe for a
            placement: a 401 is refused before anything runs. A second 401, or a signed 403, holds signed calls for
            AUTH_REFUSED_HOLD_S with one [BX-ALARM] auth-refused.
        A successful signed read marks private REST as working (BexcAPIUserStreamDataSource).
        """
        self._check_breakers(path_url, is_auth_required)
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)
        request_timeout = timeout if timeout is not None else CONSTANTS.REQUEST_TIMEOUT
        for attempt in range(2):
            response = await rest_assistant.execute_request_and_get_response(
                url=url,
                params=params,
                data=data,
                method=method,
                is_auth_required=is_auth_required,
                return_err=True,
                throttler_limit_id=limit_id if limit_id else path_url,
                headers=headers,
                timeout=request_timeout,
            )
            status = int(response.status)
            # The assistant's timeout ends at the response headers: the body gets its own (review D, 2026-11-14).
            text = await asyncio.wait_for(response.text(), timeout=request_timeout)
            try:
                body = web_utils.loads(text) if text else None
            except ValueError:
                body = None
            shown = body if body is not None else (text or "")[:300]
            if 200 <= status < 300:
                if body is None:
                    raise BexcAPIError(f"bexc answered HTTP {status} to {method.name} {path_url} with no JSON body | "
                                       f"bexc response: {shown!r}", None, status, shown)
                if is_auth_required:
                    self._last_private_read = time.time()
                return body
            code = body.get("code") if isinstance(body, dict) and isinstance(body.get("code"), str) else None
            if code == CONSTANTS.CODE_WAF_BLOCK:
                BexcExchange._waf_blocked_until = time.time() + CONSTANTS.WAF_BLOCK_HOLD_S
                self._alarm("waf-block", path=path_url, status=status, response=self._raw(shown),
                            action=f"no bexc REST call from this process for {CONSTANTS.WAF_BLOCK_HOLD_S:.0f} s")
            elif is_auth_required and status == 401 and attempt == 0:
                self._audit("auth-401-resync", path=path_url, response=self._raw(shown))
                self._time_synchronizer.clear_time_offset_ms_samples()
                await self._update_time_synchronizer(force=True)
                continue
            elif is_auth_required and status in (401, 403):
                self._auth_held_until = time.time() + CONSTANTS.AUTH_REFUSED_HOLD_S
                self._alarm_once(f"auth-refused:{status}:{code}", "auth-refused", path=path_url, status=status,
                                 response=self._raw(shown),
                                 action=f"no signed bexc call for {CONSTANTS.AUTH_REFUSED_HOLD_S:.0f} s at a time: "
                                        f"check the key (connect bexc) and its IP whitelist")
            raise BexcAPIError(f"bexc answered HTTP {status} to {method.name} {path_url}: {code} | bexc response: "
                               f"{self._raw(shown)}", code, status, shown)
        raise BexcAPIError(f"bexc request {method.name} {path_url} was not answered", None)   # never reached

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False, force: bool = False):
        """The base re-reads the server time on every status poll, every second while orders are open; a /time read
        costs ~0.3 s from myserver and bexc accepts ±10 s. Read every TIME_SYNC_INTERVAL, or at once after a signed 401;
        never during a WAF block (nothing goes to bexc then)."""
        now = time.monotonic()
        if not force and self._last_time_sync and now - self._last_time_sync < CONSTANTS.TIME_SYNC_INTERVAL:
            return
        if time.time() < BexcExchange._waf_blocked_until:
            return
        self._last_time_sync = now
        await super()._update_time_synchronizer(pass_on_non_cancelled_error=pass_on_non_cancelled_error)

    async def _make_trading_rules_request(self) -> Any:
        return await self.market_list(max_age=CONSTANTS.MARKET_LIST_SHARE_SECONDS)

    async def _make_trading_pairs_request(self) -> Any:
        return await self.market_list(max_age=CONSTANTS.MARKET_LIST_SHARE_SECONDS)

    @staticmethod
    def _market_rows(market_list: Any) -> List[Dict[str, Any]]:
        """/markets answers {"markets": [...], "count": n} (live); a bare list is read too."""
        rows = market_list.get("markets") if isinstance(market_list, dict) else market_list
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    async def market_list(self, max_age: float = 0.0) -> Any:
        """Every market with its rules and flags (~440 KB, streamed by the origin in 0.5-30 s+), read by the symbol map,
        the trading rules, the book's trading switch and runtime adds. A read younger than max_age (s) is shared instead
        of read again, and callers that ask while a read is under way wait for it. An empty or refused list raises: the
        last symbol map and rules stay (an empty map would fail every order "no rule" until the next read)."""
        async with self._market_list_lock:
            cached = self._market_list_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            response = await self._api_get(path_url=CONSTANTS.MARKETS_PATH, limit_id=CONSTANTS.MARKETS_PATH,
                                           timeout=CONSTANTS.MARKETS_REQUEST_TIMEOUT)
            if not self._market_rows(response):
                raise IOError(f"bexc's market list came back empty | bexc response: {self._raw(response)[:300]}")
            self._market_list_cache = (time.monotonic(), response)
            return response

    async def market_list_by_symbol(self) -> Dict[str, Dict[str, Any]]:
        """symbol -> its /markets row, for the book's trading switch (status / is_active), from the shared read."""
        response = await self.market_list(max_age=CONSTANTS.TRADING_SWITCH_INTERVAL / 2)
        return {str(row["symbol"]): row for row in self._market_rows(response) if row.get("symbol")}

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.SERVER_TIME_PATH, limit_id=CONSTANTS.SERVER_TIME_PATH)
        if not isinstance(response, dict) or response.get("server_time") in (None, ""):
            raise IOError(f"Unexpected bexc answer to the network check: {self._raw(response)}")

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """A refusal is logged as one, with the request and bexc's complete answer (no traceback); an unconfirmed
        placement is NOT failed."""
        if isinstance(exception, BexcPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll looks for it "
                                  f"on bexc and fails it only once it is not there.")
            return
        if isinstance(exception, BexcAPIError):
            self.logger().warning(f"bexc rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False      # a signed 401 re-syncs inside _api_request

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        return isinstance(error, BexcAPIError) and error.code == CONSTANTS.CODE_PLACEMENT_NOT_FOUND

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # 409 ORDER_NOT_OPEN means finished, and a 404 on an id bexc issued can't be believed: the status read decides.
        return False

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (and forgets a FAILED order that may
        still rest on the venue). Only our own verdict counts: an unconfirmed placement not found by its client id, old
        enough (_adopt_placed_order). Anything else is a WARNING and the order stays tracked (Hotcoin's lesson,
        2026-10-06). CODE_EMPTY_ANSWER is the poll's sentinel: nothing to report now."""
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        if isinstance(error, BexcAPIError) and error.code == CONSTANTS.CODE_EMPTY_ANSWER:
            self.logger().debug(f"bexc status of {order.client_order_id}: nothing to report now: {error}")
            return
        self.logger().warning(f"bexc status read for {order.client_order_id} failed, the order stays tracked: "
                              f"{error!r}")

    # ------------------------------------------------------------------ small helpers

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        return format(value, "f")    # never "1E-8" (CoinEx's scientific-notation bug)

    @staticmethod
    def _dec(value: Any, default: str = "0") -> Decimal:
        try:
            return Decimal(str(value)) if value is not None and value != "" and not isinstance(value, bool) \
                else Decimal(default)
        except (InvalidOperation, ValueError, TypeError):
            return Decimal(default)

    @staticmethod
    def _opt_dec(value: Any) -> Optional[Decimal]:
        """A bexc amount (a string, 6 or 18 decimals) -> Decimal; None when absent or unreadable."""
        if value is None or value == "" or isinstance(value, bool):
            return None
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None
        return result if result.is_finite() else None

    @staticmethod
    def _int_ms(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool) or value == "":
            return None
        try:
            ms = int(Decimal(str(value)))
        except (InvalidOperation, ValueError, TypeError):
            return None
        return ms if ms > 10 ** 11 else None

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
        # bexc has no private stream for an API key: nothing ever arrives here.
        while True:
            await asyncio.sleep(3600)

    @classmethod
    def _order_row(cls, raw: Dict[str, Any], view: str) -> Dict[str, Any]:
        """One order, in one shape whatever bexc answered it with (all three live, 2026-11-13):
          "list"    GET /orders rows: order_id (the UUID; `id` is a history row number), status open | filled |
                    cancelled, quantity, filled_quantity, avg_price, execution_value (quote), fee, fee_asset, created_at;
          "ccxt"    GET /order/{id} and ?client_order_id=: id (the UUID), status open | closed | canceled | unknown,
                    amount, filled, average, timestamp (ms), fee null;
          "answer"  the place answer's `order` and the cancel answer: id (the UUID), status open | filled | cancelled,
                    quantity, filled_quantity, avg_fill_price, created_at (ISO, ns)."""
        if view == "list":
            order_id, amount, filled, average = (raw.get("order_id"), raw.get("quantity"), raw.get("filled_quantity"),
                                                 raw.get("avg_price"))
            created_ms = web_utils.iso_to_ms(raw.get("created_at"))
        elif view == "ccxt":
            order_id, amount, filled, average = raw.get("id"), raw.get("amount"), raw.get("filled"), raw.get("average")
            created_ms = cls._int_ms(raw.get("timestamp"))
        else:
            order_id, amount, filled, average = (raw.get("id"), raw.get("quantity"), raw.get("filled_quantity"),
                                                 raw.get("avg_fill_price"))
            created_ms = web_utils.iso_to_ms(raw.get("created_at"))
        return {
            "order_id": str(order_id) if order_id not in (None, "") and not isinstance(order_id, bool) else None,
            "client_order_id": str(raw.get("client_order_id") or ""),
            "state": str(raw.get("status") or "").lower(),
            "amount": cls._opt_dec(amount),
            "filled": cls._opt_dec(filled),
            "average": cls._opt_dec(average),
            "quote_filled": cls._opt_dec(raw.get("execution_value")) if view == "list" else None,
            "fee": cls._opt_dec(raw.get("fee")) if view == "list" else None,
            "fee_asset": str(raw.get("fee_asset") or "").upper() or None if view == "list" else None,
            "created_ms": created_ms,
            "symbol": str(raw.get("symbol") or ""),
            "view": view,
            "raw": raw,
        }

    @staticmethod
    def _is_terminal_row(row: Dict[str, Any]) -> bool:
        return CONSTANTS.ORDER_STATE.get(row["state"]) in CONSTANTS.TERMINAL_STATES

    def _order_state(self, state: str, executed: Decimal, original: Decimal) -> OrderState:
        mapped = CONSTANTS.ORDER_STATE.get(state)
        if mapped is OrderState.OPEN and executed > 0:
            mapped = OrderState.PARTIALLY_FILLED   # bexc has no partial state: an open row with a filled quantity
        if mapped is not None:
            return mapped
        # "unknown" (the CCXT view's untriggered stop) or a word never seen: from the amounts, never guessed.
        self._audit_once(f"unknown-state:{state}", state=state, executed=str(executed), original=str(original))
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    # ------------------------------------------------------------------ the poll

    async def _update_order_status(self) -> None:
        """One poll (the base's fills pass, then its status pass): ONE open-orders read covers every open order on every
        market and is shared by both passes; GET /order/{id} is read only for an order it doesn't list
        (_fetch_order_detail). The same read feeds the orphan guard and finds unconfirmed placements, so it is made on
        the idle poll too (every SHORT_POLL_INTERVAL). First the per-order state of orders no longer tracked is dropped."""
        self._row_cache.clear()
        self._open_by_client_id.clear()
        self._fills_cache.clear()
        self._purge_order_state()
        rows: Optional[List[Dict[str, Any]]] = None
        try:
            rows = await self._open_orders()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._order_tracker.active_orders:
                # Each open order is then read on its own this poll (more requests, same answers).
                self.logger().warning(f"bexc open-orders read failed; reading each order on its own this poll: {e!r}")
            else:
                self.logger().debug(f"bexc open-orders read failed (no order open): {e!r}")
        if rows is not None:
            for row in rows:
                if row["order_id"] is None:
                    continue
                self._row_cache[row["order_id"]] = row
                if row["client_order_id"]:
                    self._open_by_client_id[row["client_order_id"]] = row["order_id"]
                if row["filled"] is not None and row["filled"] > 0:
                    self._audit_once(f"open-row-partial:{row['order_id']}", row=self._raw(row["raw"]))
            self._check_orphans(rows)
        await super()._update_order_status()

    async def _open_orders(self) -> List[Dict[str, Any]]:
        """Every open order on every market (GET /orders?symbol=ALL&status=open), newest first, page by page: a further
        page only when one came back full, up to OPEN_ORDERS_MAX_PAGES."""
        rows: List[Dict[str, Any]] = []
        for page in range(1, CONSTANTS.OPEN_ORDERS_MAX_PAGES + 1):
            response = await self._api_get(path_url=CONSTANTS.ORDERS_PATH,
                                           params={"symbol": "ALL", "status": "open", "page": str(page),
                                                   "limit": str(CONSTANTS.OPEN_ORDERS_PAGE_LIMIT)},
                                           is_auth_required=True, limit_id=CONSTANTS.ORDERS_PATH)
            batch = response.get("orders") if isinstance(response, dict) else None
            if not isinstance(batch, list):
                raise IOError(f"bexc's open-orders answer has no `orders` list | bexc response: {self._raw(response)}")
            if page == 1:
                self._audit_once("open-list-shape", paging=self._raw(response.get("paging")), rows=self._raw(batch[:2]))
            rows.extend(self._order_row(raw, "list") for raw in batch if isinstance(raw, dict))
            if len(batch) < CONSTANTS.OPEN_ORDERS_PAGE_LIMIT:
                return rows
        self._alarm_once("open-pages", "open-pages", rows=len(rows),
                         action="more open orders than the pages read: the rest are read one by one")
        return rows

    def _purge_order_state(self) -> None:
        live = set(self._order_tracker.all_fillable_orders) | set(self._placing)
        for registry in (self._sent_at_ms, self._cancel_sent, self._short_since, self._fills_retry,
                         self._unreadable_until, self._unreadable_interval, self._lookup_after, self._final_rows,
                         self._fill_booked_at):
            for client_order_id in [c for c in registry if c not in live]:
                del registry[client_order_id]
        self._size_checked &= live

    def _check_orphans(self, rows: List[Dict[str, Any]]) -> None:
        """The orphan guard (runbook §1.5), on every poll's open-order list. A row carrying our client-id prefix that no
        active order holds:
          - sent by this process, and Hummingbot gave it up (FAILED; this fork's tracker forgets an order it fails for
            not-found at once) or forgot it -> cancelled by its order id, [BX-ALARM] failed-order-alive. Its fills are
            not booked to Hummingbot (it was failed); the alarm carries bexc's filled quantity, and P3 books them from
            bexc's own fill list;
          - not sent by this process (another process's, or an earlier run's) -> [BX-ALARM] untracked-order-open, NOT
            cancelled (Pavel's call, as for Hotcoin, Poloniex and BloFin);
          - finished in Hummingbot a moment ago (filled or cancelled, still in the tracker's 30 s cache) -> nothing:
            this list was read before that. Should bexc still list it after the cache, it is cancelled as above.
        Each order once; a cancel that fails is sent again after GUARD_CANCEL_RETRY_S while bexc lists the order. The
        account's other keys and the website share the list: only our prefix is looked at."""
        tracker = self._order_tracker
        active = tracker.active_orders
        for row in rows:
            client_order_id = row["client_order_id"]
            exchange_order_id = row["order_id"] or ""
            if (not client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) or not exchange_order_id
                    or client_order_id in active or client_order_id in self._placing
                    or time.monotonic() < self._orphan_ids.get(exchange_order_id, 0.0)):
                continue
            order = tracker.all_fillable_orders.get(client_order_id)
            if order is not None and not order.is_failure:
                continue
            self._orphan_ids[exchange_order_id] = float("inf")     # handled; a failed cancel re-arms it
            fields = dict(client_id=client_order_id, order_id=exchange_order_id, symbol=row["symbol"],
                          state=row["state"], side=row["raw"].get("side"), quantity=str(row["amount"]),
                          price=row["raw"].get("price"), filled=str(row["filled"]))
            if order is None and client_order_id not in self._placed_ids:
                self._alarm("untracked-order-open", **fields, action="not cancelled: not placed by this process")
                continue
            self._alarm("failed-order-alive", **fields,
                        hmb_state=order.current_state.name if order is not None else "no longer tracked",
                        action="cancelling")
            safe_ensure_future(self._orphan_cancel(exchange_order_id))

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """With no trading rule the base stops at a bare KeyError before tracking the order (XT B2-USDT, 2026-09-23):
        here the order is tracked and failed the way the base fails its own pre-send checks."""
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to bexc: the connector has no trading rule for {trading_pair} "
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
        """POST /order with the probe's body (live 2026-11-13). The answers:
          - 2xx {"order": {id, ...}}: placed. A crossing order may be "filled" already: that answer is its final row;
          - 409 DUPLICATE_CLIENT_ORDER_ID naming an order: that order exists under our client id (live): adopted;
          - a status in PLACE_REFUSED_STATUSES, or a 409 other than the duplicate: refused, nothing placed: failed;
          - anything else, or no answer: bexc may have the order: looked up by its client id first."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        body = {
            "symbol": symbol,
            "side": CONSTANTS.SIDE[trade_type],
            "order_type": CONSTANTS.ORDER_TYPE_LIMIT,
            "quantity": self._format_decimal(amount),    # the BASE quantity (min_qty / step_size are base units)
            "price": self._format_decimal(price),
            "time_in_force": CONSTANTS.TIME_IN_FORCE,
            "post_only": False,
            "client_order_id": order_id,
        }
        self._sent_at_ms[order_id] = self._server_ms()
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
            except BexcAPIError as answer:
                self._audit("place-order", client_id=order_id, request=self._raw(body), status=answer.status,
                            response=self._raw(answer.body))
                existing = answer.body.get("order_id") if isinstance(answer.body, dict) else None
                if answer.code == CONSTANTS.CODE_DUPLICATE_CLIENT_ORDER_ID and existing not in (None, ""):
                    named = str(answer.body.get("client_order_id") or order_id)
                    if named == order_id:
                        self._alarm("place-duplicate-adopted", client_id=order_id, exchange_order_id=str(existing),
                                    response=self._raw(answer.body))
                        return str(existing), self._now()
                if answer.status in CONSTANTS.PLACE_REFUSED_STATUSES or (
                        answer.status == 409 and answer.code != CONSTANTS.CODE_DUPLICATE_CLIENT_ORDER_ID):
                    raise BexcAPIError(f"Order {order_id} refused, request {self._raw(body)}: HTTP {answer.status} "
                                       f"{answer.code} | bexc response: {self._raw(answer.body)}",
                                       answer.code, answer.status, answer.body)
                cause = f"HTTP {answer.status} {answer.code}, not a known refusal | bexc response: {self._raw(answer.body)}"
                return await self._locate_unconfirmed_placement(order_id, symbol, cause), self._now()
            except Exception as transport_error:
                # No answer: bexc may still have accepted the order (the base would mark it FAILED and stop tracking a
                # live order). Looked up by its client id.
                return await self._locate_unconfirmed_placement(order_id, symbol, repr(transport_error)), self._now()
            self._audit("place-order", client_id=order_id, request=self._raw(body), response=self._raw(response))
            raw = response.get("order") if isinstance(response, dict) else None
            row = self._order_row(raw, "answer") if isinstance(raw, dict) else None
            if row is None or row["order_id"] is None:
                cause = f"an answer without an order id | bexc response: {self._raw(response)}"
                return await self._locate_unconfirmed_placement(order_id, symbol, cause), self._now()
            if row["client_order_id"] and row["client_order_id"] != order_id:
                self._alarm("place-client-id-mismatch", client_id=order_id, response=self._raw(response))
            if self._is_terminal_row(row):
                # Filled on entry (a crossing order): the answer is its final row; the poll books the fills.
                self._final_rows[order_id] = row
            return row["order_id"], self._now()
        finally:
            self._placing.discard(order_id)

    async def _locate_unconfirmed_placement(self, order_id: str, symbol: str, cause: str) -> str:
        """A placement bexc may have accepted without telling us the order's id. Found on bexc -> its id. Not found, or
        the lookup could not tell -> BexcPlacementUnknown: the order is NOT failed (it may rest on bexc); it stays
        PENDING_CREATE and the status poll keeps looking (_adopt_placed_order), failing it only by its age-gated
        not-found verdict."""
        found, verdict = None, "not on bexc yet"
        try:
            found = await self._find_placed_order(order_id, CONSTANTS.PLACEMENT_LOOKUP_DELAYS)
        except _LookupInconclusive as e:
            verdict = f"lookup inconclusive: {e}"
        self._alarm("place-order-unconfirmed", client_id=order_id, symbol=symbol, cause=cause,
                    found_on_exchange=found, verdict=None if found is not None else verdict)
        if found is not None:
            return found
        raise BexcPlacementUnknown(f"bexc did not confirm the placement ({cause}); {verdict}")

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
        # The first status read checks the quantity bexc booked (the size guard) and settles a fill on entry: now, not
        # at the next tick.
        self._poll_notifier.set()
        return exchange_order_id

    async def _find_placed_order(self, client_order_id: str, delays: Tuple[float, ...]) -> Optional[str]:
        """The placement guard: the order carrying our client id, looked for after each delay (s): in the last poll's
        open list first (free), else GET /order?client_order_id= (the docs: "use it after a timeout or reconnect to
        learn whether an order landed"). Found -> its order id. None -> not (yet): bexc answered 404 ORDER_NOT_FOUND.
        _LookupInconclusive -> a read failed, so it could not tell. An order that merely looks like ours (side, price,
        size) is never adopted: only the id counts."""
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            own = self._order_tracker.fetch_order(client_order_id=client_order_id)
            if own is not None and own.exchange_order_id is not None:
                return str(own.exchange_order_id)         # it got its id meanwhile
            listed = self._open_by_client_id.get(client_order_id)
            if listed is not None:
                return listed
            if not self._client_id_404_budget_left():
                raise _LookupInconclusive("not looked up: the client-id 404 budget of the last 60 s is spent "
                                          "(CLIENT_ID_404_PER_MINUTE)")
            try:
                # Throttled as a read, not in the placements' 500/min pool (GET and POST share the path "/order").
                response = await self._api_get(path_url=CONSTANTS.ORDER_BY_CLIENT_ID_PATH,
                                               params={"client_order_id": client_order_id}, is_auth_required=True,
                                               limit_id=CONSTANTS.ORDER_PATH)
            except asyncio.CancelledError:
                raise
            except BexcAPIError as e:
                if e.status == 404:
                    self._note_client_id_404()
                    if e.code == CONSTANTS.CODE_ORDER_NOT_FOUND:
                        continue
                raise _LookupInconclusive(f"order by client_order_id failed: {e}")
            except Exception as e:
                raise _LookupInconclusive(f"order by client_order_id failed: {e!r}")
            row = self._order_row(response, "ccxt") if isinstance(response, dict) else None
            if row is None or row["order_id"] is None:
                raise _LookupInconclusive(f"order by client_order_id answered no order: {self._raw(response)}")
            if row["client_order_id"] != client_order_id:
                self._alarm_once(f"lookup-client-id-mismatch:{client_order_id}", "lookup-client-id-mismatch",
                                 client_id=client_order_id, row=self._raw(response), action="not adopted")
                return None
            self._audit_once("lookup-by-client-id", client_id=client_order_id, row=self._raw(response))
            return row["order_id"]
        return None

    async def _cancel_by_exchange_id(self, exchange_order_id: str) -> bool:
        """A guard's cancel (the size guard, the orphan guard), by bexc's order id, its answer audit-logged. True when
        the order is no longer open (cancelled, or 409 ORDER_NOT_OPEN); a failure is alarmed and False: the guard sends
        it again after GUARD_CANCEL_RETRY_S (review G, 2026-11-14)."""
        try:
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH,
                                            data={"order_id": exchange_order_id},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-by-exchange-id", exchange_order_id=exchange_order_id, response=self._raw(response))
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if isinstance(e, BexcAPIError) and e.code == CONSTANTS.CODE_ORDER_NOT_OPEN:
                self._audit("cancel-by-exchange-id", exchange_order_id=exchange_order_id, response=self._raw(e.body))
                return True
            self._alarm("cancel-by-exchange-id-failed", exchange_order_id=exchange_order_id, error=repr(e),
                        action=f"sent again after {CONSTANTS.GUARD_CANCEL_RETRY_S:.0f} s while bexc lists it")
            return False

    async def _orphan_cancel(self, exchange_order_id: str) -> None:
        if not await self._cancel_by_exchange_id(exchange_order_id):
            # Re-armed: a poll that still lists the order after the wait cancels it again.
            self._orphan_ids[exchange_order_id] = time.monotonic() + CONSTANTS.GUARD_CANCEL_RETRY_S

    async def _size_guard_cancel(self, order: InFlightOrder) -> None:
        if not await self._cancel_by_exchange_id(str(order.exchange_order_id)):
            await asyncio.sleep(CONSTANTS.GUARD_CANCEL_RETRY_S)
            self._size_checked.discard(order.client_order_id)     # its next read checks it, and cancels, again

    async def _placement_settled(self, order: InFlightOrder) -> Optional[str]:
        """Waits for a placement still awaiting bexc's answer to settle: its id, or given up as unconfirmed (it leaves
        _placing without one). Up to PLACEMENT_SETTLE_MAX_S: the placement's own timeout, its lookups and theirs (review
        E, 2026-11-14: the base's 10 s wait for the id ran out first, and the cancel was dropped). Still placing then:
        asyncio.TimeoutError, the cancel not sent."""
        deadline = time.monotonic() + CONSTANTS.PLACEMENT_SETTLE_MAX_S
        while order.exchange_order_id is None and order.client_order_id in self._placing:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            try:
                await asyncio.wait_for(order.exchange_order_id_update_event.wait(), timeout=min(0.25, remaining))
            except asyncio.TimeoutError:
                pass
        return order.exchange_order_id

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """POST /order/cancel by the order id. An order without one: if its placement is still awaited, the cancel waits
        for it to settle (_placement_settled; still unanswered after PLACEMENT_SETTLE_MAX_S: a timeout reaches
        _execute_order_cancel, not sent); if the placement went unconfirmed, the cancel names the client id, which the
        docs give for exactly this case (live: proven by Pavel's probe), and the answer's order id is adopted (within
        the client-id 404 budget). The answer is synchronous (live: the flat order, "cancelled", with its filled
        quantity): it becomes the order's final row and the poll, woken at once, books any fills and completes it."""
        exchange_order_id = tracked_order.exchange_order_id
        if exchange_order_id is None and order_id in self._placing:
            exchange_order_id = await self._placement_settled(tracked_order)
        if exchange_order_id is not None:
            body = {"order_id": str(exchange_order_id)}
        else:
            if not self._client_id_404_budget_left():
                raise BexcAPIError(f"cancel of {order_id} by its client id not sent: the client-id 404 budget of the "
                                   f"last 60 s is spent (CLIENT_ID_404_PER_MINUTE)", CONSTANTS.CODE_404_BUDGET)
            body = {"client_order_id": order_id}
        try:
            response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, data=body, is_auth_required=True,
                                            limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        except BexcAPIError as e:
            if e.status == 404 and exchange_order_id is None:
                self._note_client_id_404()
            raise
        self._audit("cancel-order", client_id=order_id, request=self._raw(body), response=self._raw(response))
        row = self._order_row(response, "answer") if isinstance(response, dict) else None
        if row is None or row["order_id"] is None:
            raise BexcAPIError(f"bexc's cancel answer for {order_id} carries no order | bexc response: "
                               f"{self._raw(response)}", None, None, response)
        if exchange_order_id is None:
            if tracked_order.exchange_order_id is None:
                tracked_order.update_exchange_order_id(row["order_id"])
            self._alarm("placement-found-by-cancel", client_id=order_id, exchange_order_id=row["order_id"],
                        state=row["state"])
        elif row["order_id"] != str(exchange_order_id):
            self._alarm("cancel-id-mismatch", client_id=order_id, exchange_order_id=str(exchange_order_id),
                        response=self._raw(response))
        self._cancel_sent[order_id] = time.monotonic()
        if self._is_terminal_row(row):
            self._final_rows[order_id] = row
        # A row read just before the cancel is not this poll's answer any more; and one still in flight can't set the
        # order back to OPEN either (_request_order_status keeps PENDING_CANCEL).
        self._row_cache.pop(row["order_id"], None)
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
        """The base's cancel path, except that a refusal is a WARNING with bexc's answer (no traceback) and reads the
        order's status at once (409 ORDER_NOT_OPEN = filled or cancelled: the status says which), an HTTP timeout on the
        cancel reads the status too (the cancel may have landed), and an order still without an exchange id counts no
        not-found strike."""
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            # No not-found strike either way (the base counts one, with no look at bexc). Only the status poll's lookup
            # decides an order without an id (_adopt_placed_order).
            if order.exchange_order_id is None and order.client_order_id in self._placing:
                # Not sent: the placement still had no answer after PLACEMENT_SETTLE_MAX_S.
                self.logger().warning(f"bexc cancel of {order.client_order_id} not sent: its placement is still "
                                      f"unanswered after {CONSTANTS.PLACEMENT_SETTLE_MAX_S:.0f} s. The status poll "
                                      f"looks for it on bexc.")
            else:
                # Sent (by the order id, or by the client id) and not answered in time: it may have landed.
                self.logger().warning(f"bexc did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except BexcAPIError as refusal:
            if refusal.code == CONSTANTS.CODE_ORDER_NOT_OPEN:
                self._cancel_sent.setdefault(order.client_order_id, time.monotonic())
            self.logger().warning(f"bexc did not cancel {order.client_order_id}: {refusal}. Reading its status now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        if order.exchange_order_id is not None:
            self._row_cache.pop(str(order.exchange_order_id), None)
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            self._fills_cache.pop(symbol, None)      # the last poll's fills may predate the cancel
            order_update = await self._request_order_status(tracked_order=order)
            self._order_tracker.process_order_update(order_update)
        except asyncio.CancelledError:
            raise
        except BexcAPIError as e:
            if e.code == CONSTANTS.CODE_EMPTY_ANSWER:
                self.logger().debug(f"bexc status of {order.client_order_id} after its cancel: nothing to report "
                                    f"now: {e}")
                return
            self.logger().warning(f"bexc status read for {order.client_order_id} after its cancel failed; the next "
                                  f"poll reads it again: {e!r}")
        except Exception as e:
            self.logger().warning(f"bexc status read for {order.client_order_id} after its cancel failed; the next "
                                  f"poll reads it again: {e!r}")

    # ------------------------------------------------------------------ order rows (REST) and fills

    def _age(self, order: InFlightOrder) -> float:
        return max(0.0, self._now() - float(order.creation_timestamp or self._now()))

    async def _resolve_missing_order(self, order: InFlightOrder, symbol: str) -> Dict[str, Any]:
        """GET /order/{id} answered 404 for an id bexc itself issued. bexc keeps every order readable, a cancelled one
        too (live), so this "can't happen"; the market's closed orders (filled + cancelled, newest first) stand in.
        Failing that, the order is NOT failed: a backend whose reads come back empty for now would answer exactly this,
        and failing it would forget an order that may rest and fill on bexc (BloFin review, 2026-11-11). It stays
        tracked; it is read again after UNREADABLE_RETRY_S, doubling to UNREADABLE_RETRY_CAP_S (404s may count toward
        the WAF's block), with one [BX-ALARM] order-unreadable once it is older than NOT_FOUND_MIN_AGE. Only a
        placement bexc never confirmed can be failed by not-found (_adopt_placed_order)."""
        response = await self._api_get(path_url=CONSTANTS.ORDERS_PATH,
                                       params={"symbol": symbol, "status": "closed", "page": "1",
                                               "limit": str(CONSTANTS.OPEN_ORDERS_PAGE_LIMIT)},
                                       is_auth_required=True, limit_id=CONSTANTS.ORDERS_PATH)
        batch = response.get("orders") if isinstance(response, dict) else None
        for raw in batch if isinstance(batch, list) else []:
            if isinstance(raw, dict) and str(raw.get("order_id") or "") == str(order.exchange_order_id):
                self._audit_once("detail-404-but-in-history", exchange_order_id=order.exchange_order_id,
                                 state=raw.get("status"))
                return self._order_row(raw, "list")
        client_order_id = order.client_order_id
        interval = min(self._unreadable_interval.get(client_order_id, CONSTANTS.UNREADABLE_RETRY_S / 2) * 2,
                       CONSTANTS.UNREADABLE_RETRY_CAP_S)
        self._unreadable_interval[client_order_id] = interval
        self._unreadable_until[client_order_id] = time.monotonic() + interval
        age = self._age(order)
        if age >= CONSTANTS.NOT_FOUND_MIN_AGE:
            self._alarm_once(f"order-unreadable:{client_order_id}", "order-unreadable",
                             client_id=client_order_id, exchange_order_id=order.exchange_order_id, age_s=round(age, 1),
                             action=f"kept tracked, read again after {interval:.0f} s, doubling to "
                                    f"{CONSTANTS.UNREADABLE_RETRY_CAP_S:.0f} s: bexc keeps every order readable")
        raise BexcAPIError(f"Order {client_order_id} ({order.exchange_order_id}) not readable on bexc (age {age:.1f} "
                           f"s): 404 for its id, not in the market's recent closed orders", CONSTANTS.CODE_EMPTY_ANSWER)

    async def _fetch_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """The order's row, cheapest first: its terminal row once known (final: never read again, and no stale open row
        can follow it; the place and cancel answers count), the poll's open list when it lists the order, else
        GET /order/{id} (read once per poll: the fills and status passes share it; a cancel drops its entry). An order
        without an exchange id is looked up first (_adopt_placed_order); one whose id answered 404 is read again only
        on its back-off (_resolve_missing_order)."""
        client_order_id = order.client_order_id
        final = self._final_rows.get(client_order_id)
        if final is not None:
            return final
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        exchange_order_id = order.exchange_order_id
        if exchange_order_id is None:
            exchange_order_id = await self._adopt_placed_order(order)
        exchange_order_id = str(exchange_order_id)
        row = self._row_cache.get(exchange_order_id)
        if row is None:
            if time.monotonic() < self._unreadable_until.get(client_order_id, 0.0):
                raise BexcAPIError(f"Order {client_order_id} ({exchange_order_id}): its id answered 404; read again "
                                   f"after its back-off", CONSTANTS.CODE_EMPTY_ANSWER)
            try:
                response = await self._api_get(
                    path_url=CONSTANTS.ORDER_PATH.format(order_id=quote(exchange_order_id, safe="")),
                    is_auth_required=True, limit_id=CONSTANTS.ORDER_PATH)
            except BexcAPIError as e:
                if e.status == 404 and e.code == CONSTANTS.CODE_ORDER_NOT_FOUND:
                    response = None
                else:
                    raise
            if response is None:
                row = await self._resolve_missing_order(order, symbol)
            else:
                row = self._order_row(response, "ccxt") if isinstance(response, dict) else None
                if row is None or row["order_id"] != exchange_order_id:
                    raise IOError(f"bexc answered GET /order/{exchange_order_id} with another order or none | bexc "
                                  f"response: {self._raw(response)}")
                self._audit_once("detail-fields", exchange_order_id=exchange_order_id, row=self._raw(response))
            self._row_cache[exchange_order_id] = row
        self._unreadable_until.pop(client_order_id, None)
        self._unreadable_interval.pop(client_order_id, None)
        if self._is_terminal_row(row):
            self._final_rows[client_order_id] = row
        return row

    async def _adopt_placed_order(self, order: InFlightOrder) -> str:
        """The poll's side of an unconfirmed placement: one look for the order, no waits. The poll's open list finds a
        resting one for free; otherwise a client-id lookup, at most every CLIENT_ID_LOOKUP_INTERVAL_S (an unknown id
        answers 404). Found -> the order gets that id. Not readable yet (CODE_EMPTY_ANSWER, no strike): its placement
        still awaits the answer (only the placement looks then), the lookup could not tell or isn't due, or the order is
        younger than NOT_FOUND_MIN_AGE. Otherwise CODE_PLACEMENT_NOT_FOUND: a strike toward FAILED (the tracker's
        lost-order count)."""
        client_order_id = order.client_order_id
        if client_order_id in self._placing:
            raise BexcAPIError(f"Order {client_order_id}: its placement still awaits bexc's answer",
                               CONSTANTS.CODE_EMPTY_ANSWER)
        found = self._open_by_client_id.get(client_order_id)
        if found is None:
            now = time.monotonic()
            if now < self._lookup_after.get(client_order_id, 0.0):
                raise BexcAPIError(f"Order {client_order_id} has no exchange id yet; next client-id lookup in "
                                   f"{self._lookup_after[client_order_id] - now:.1f} s", CONSTANTS.CODE_EMPTY_ANSWER)
            self._lookup_after[client_order_id] = now + CONSTANTS.CLIENT_ID_LOOKUP_INTERVAL_S
            try:
                found = await self._find_placed_order(client_order_id, (0.0,))
            except _LookupInconclusive as e:
                raise BexcAPIError(f"Order {client_order_id} has no exchange id yet: {e}", CONSTANTS.CODE_EMPTY_ANSWER)
            if found is None:
                if self._age(order) < CONSTANTS.NOT_FOUND_MIN_AGE:
                    raise BexcAPIError(f"Order {client_order_id} has no exchange id yet", CONSTANTS.CODE_EMPTY_ANSWER)
                raise BexcAPIError(f"Order {client_order_id} is not on bexc (looked up by client id)",
                                   CONSTANTS.CODE_PLACEMENT_NOT_FOUND)
        self._alarm("placement-found", client_id=client_order_id, exchange_order_id=found)
        order.update_exchange_order_id(found)
        return found

    def _check_size(self, order: InFlightOrder, row: Dict[str, Any]) -> bool:
        """The size guard: bexc's own quantity of the order must be ours (the base quantity, live: 0.000100 BTC sent and
        listed). A mismatch cancels the order at once. True if the order may proceed. The first read also checks that
        bexc kept the client id."""
        if order.client_order_id in self._size_checked:
            return True
        self._check_client_id(order, row)
        quantity = row["amount"]
        if quantity is None:
            return True      # can't tell from this row; the next one may
        if abs(quantity - order.amount) <= order.amount * CONSTANTS.SIZE_MISMATCH_TOLERANCE:
            self._size_checked.add(order.client_order_id)
            return True
        self._alarm("size-semantics", client_id=order.client_order_id, exchange_order_id=order.exchange_order_id,
                    sent_base=str(order.amount), bexc_quantity=str(quantity), row=self._raw(row["raw"]),
                    action="cancelling; the quantity semantics in bexc_exchange._place_order must be checked")
        safe_ensure_future(self._size_guard_cancel(order))
        self._size_checked.add(order.client_order_id)
        return False

    def _check_client_id(self, order: InFlightOrder, row: Dict[str, Any]) -> None:
        """The placement guard finds unanswered orders by the client id, and the asset_manager tells Hummingbot's fills
        by it. An order read back without it means both are blind: one [BX-ALARM] per session."""
        if row["client_order_id"] != order.client_order_id:
            self._alarm_once("client-id-not-kept", "client-id-not-kept", client_id=order.client_order_id,
                             exchange_order_id=order.exchange_order_id, bexc_client_id=row["client_order_id"],
                             view=row["view"], action="unanswered placements can't be found by client id")

    @staticmethod
    def _trade_id(row: Dict[str, Any]) -> str:
        """A fill's id: `trade_id` (an integer, = `id`, live). 0 is an id like any other."""
        for key in ("trade_id", "id"):
            value = row.get(key)
            if value is not None and not isinstance(value, bool) and str(value) != "":
                return str(value)
        return ""

    def _fills_floor_ms(self, order: InFlightOrder, created_ms: Optional[int]) -> int:
        """How far back a market's fills are read for this order (bexc ms): the EARLIEST of what is known of its start
        (the bexc time just before this process sent it, Hummingbot's creation time, the row's time) minus
        FILLS_FLOOR_MARGIN_MS. The earliest, because a row's time is not a creation time: for the same order the CCXT
        view's `timestamp` read 83 ms AFTER its fill on 2026-11-14, 278 ms before it on 11-13. A floor too late only
        stops the paging early (the order is then completed from its totals, alarmed), never books a wrong fill."""
        known = [ms for ms in (self._sent_at_ms.get(order.client_order_id), created_ms) if ms]
        created = order.creation_timestamp
        if created and created == created and created > 0:
            known.append(int(float(created) * 1e3))
        start = min(known) if known else int(self._now() * 1e3)
        return int(start) - CONSTANTS.FILLS_FLOOR_MARGIN_MS

    async def _fill_rows(self, symbol: str, floor_ms: int) -> List[Dict[str, Any]]:
        """The account's fills on one market (GET /trades/my?symbol=S), newest first, paged back past floor_ms (bexc ms):
        a further page only when one came back full and still newer than the floor, up to FILLS_MAX_PAGES. Read once
        per poll and shared by every order on the market and by both passes; a read that went back far enough is
        reused. /trades/my has no time or order filter (docs)."""
        cached = self._fills_cache.get(symbol)
        if cached is not None and cached[0] <= floor_ms:
            return cached[1]
        rows: List[Dict[str, Any]] = []
        complete = False
        for page in range(1, CONSTANTS.FILLS_MAX_PAGES + 1):
            response = await self._api_get(path_url=CONSTANTS.MY_TRADES_PATH,
                                           params={"symbol": symbol, "page": str(page),
                                                   "limit": str(CONSTANTS.FILLS_PAGE_LIMIT)},
                                           is_auth_required=True, limit_id=CONSTANTS.MY_TRADES_PATH)
            batch = response.get("trades") if isinstance(response, dict) else None
            if not isinstance(batch, list):
                raise IOError(f"bexc's fills answer has no `trades` list | bexc response: {self._raw(response)}")
            batch = [r for r in batch if isinstance(r, dict)]
            if page == 1:
                self._audit_once("fills-page-shape", symbol=symbol, paging=self._raw(response.get("paging")),
                                 rows=len(batch))
            rows.extend(batch)
            times = [t for t in (web_utils.iso_to_ms(r.get("executed_at")) for r in batch) if t is not None]
            if len(batch) < CONSTANTS.FILLS_PAGE_LIMIT or (times and min(times) < floor_ms):
                complete = True
                break
        if not complete:
            self._alarm_once(f"fills-pages:{symbol}:{floor_ms // 60000}", "fills-pages", symbol=symbol,
                             rows=len(rows), floor_ms=floor_ms,
                             action="the market's older fills are not read this poll; the rest from the totals")
        self._fills_cache[symbol] = (floor_ms, rows)
        return rows

    async def _fills_from_rows(self, order: InFlightOrder, cumulative: Decimal, created_ms: Optional[int] = None,
                               terminal: bool = False) -> List[TradeUpdate]:
        """bexc's own fill rows for the order, each a TradeUpdate named by bexc's trade_id, booked once, never past the
        order's cumulative filled quantity or its size. Read only when the order's filled quantity is ahead of what is
        booked; after a read that left the order short, again after FILLS_RETRY_MIN_S, doubling to FILLS_RETRY_MAX_S
        while the row shows nothing new (a new filled quantity or a terminal state reads at once): one lagging order
        can't eat the IP budget the placements share."""
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
        all_rows = await self._fill_rows(symbol, self._fills_floor_ms(order, created_ms))
        # Read again after the awaits: a concurrent status read (a cancel's _refresh_order) may have booked meanwhile, a
        # remainder fill included, and the cap below must count it (review H, 2026-11-14).
        booked = order.executed_amount_base
        if cumulative <= booked + tolerance:
            return []
        exchange_order_id = str(order.exchange_order_id)
        rows = [r for r in all_rows if str(r.get("order_id") or "") == exchange_order_id]
        self._audit_once("fill-fields", exchange_order_id=exchange_order_id, rows=self._raw(rows[:3]))
        fills: List[TradeUpdate] = []
        for row in sorted(rows, key=lambda r: (web_utils.iso_to_ms(r.get("executed_at")) or 0, self._trade_id(r))):
            trade_id = self._trade_id(row)
            if not trade_id or trade_id in order.order_fills:
                continue
            quantity, price = self._dec(row.get("quantity")), self._dec(row.get("price"))
            if quantity <= 0 or price <= 0:
                self._alarm_once(f"fill-unreadable:{client_order_id}:{trade_id}", "fill-unreadable",
                                 client_id=client_order_id, row=self._raw(row))
                continue
            if booked + quantity > cumulative + tolerance:
                # Never past the order's own filled quantity: the fill list ran ahead of the order row, or repeats one.
                self._alarm_once(f"fill-capped:{client_order_id}:{trade_id}:{cumulative}", "fill-capped",
                                 client_id=client_order_id, trade_id=trade_id, booked=str(booked),
                                 fill=str(quantity), cumulative=str(cumulative))
                break
            limit = order.price if order.price is not None and not order.price.is_nan() else Decimal("0")
            if limit > 0 and not (limit / 2 <= price <= limit * 2):
                self._alarm_once(f"fill-price-implausible:{client_order_id}:{trade_id}", "fill-price-implausible",
                                 client_id=client_order_id, trade_id=trade_id, price=str(price),
                                 limit=str(limit), action="refused")
                continue
            # The fee is charged in the quote on both sides (live, a buy and a sell); fee_asset names it.
            fee_coin = str(row.get("fee_asset") or "").upper() or order.quote_asset
            raw_fee = self._dec(row.get("fee"))
            fee_amount = abs(raw_fee)
            quote_amount = self._dec(row.get("amount"))
            if quote_amount <= 0:
                quote_amount = quantity * price
            is_maker = row.get("is_maker")
            self._audit_once(f"fee-rate:{order.trade_type.name}", fee=str(raw_fee), fee_coin=fee_coin,
                             quantity=str(quantity), price=str(price), quote=str(quote_amount), is_maker=is_maker,
                             per_quote=str(fee_amount / quote_amount) if quote_amount > 0 else None)
            fill_ms = web_utils.iso_to_ms(row.get("executed_at"))
            fills.append(TradeUpdate(
                trade_id=trade_id,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=order.trading_pair,
                fee=TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_coin)],
                ),
                fill_base_amount=quantity,
                fill_quote_amount=quote_amount,
                fill_price=price,
                fill_timestamp=(fill_ms or int(self._now() * 1e3)) / 1e3,
                is_taker=(not is_maker) if isinstance(is_maker, bool) else True,
            ))
            booked += quantity
        if booked + tolerance < cumulative:
            # /trades/my trails the order row (or a guard refused a row): read again later, not every poll.
            interval = (min(retry[1] * 2, CONSTANTS.FILLS_RETRY_MAX_S) if retry is not None and retry[2] == seen
                        else CONSTANTS.FILLS_RETRY_MIN_S)
            self._fills_retry[client_order_id] = (time.monotonic() + interval, interval, seen)
            self._audit_once(f"fills-behind-state:{client_order_id}", client_id=client_order_id,
                             terminal=terminal, filled=str(cumulative), booked=str(booked), rows=len(rows))
        else:
            self._fills_retry.pop(client_order_id, None)
        return fills

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """The base's fills pass runs over active AND recently done orders (its 30 s cache). A done order is skipped: a
        terminal state is reported only once bexc's fill list showed every fill (_request_order_status), so there is
        nothing left to read."""
        if order.exchange_order_id is None or order.is_done:
            return []
        try:
            row = await self._fetch_order_detail(order)
        except BexcAPIError as e:
            if e.code == CONSTANTS.CODE_EMPTY_ANSWER:
                return []      # not readable now: the status pass reads it again
            raise
        if not self._check_size(order, row):
            return []
        fills = await self._fills_from_rows(order, row["filled"] or Decimal("0"), row["created_ms"],
                                            terminal=self._is_terminal_row(row))
        for fill in fills:
            self._note_fill_booked(order, fill)
        return fills

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """One order's state from its row (_fetch_order_detail). The rules (runbook §1.5):
          - fills first, then the state: a terminal state is never forwarded short of its fills, and nothing is booked
            past bexc's own filled quantity. It is held up to TERMINAL_HOLD_MAX_S for /trades/my to list them; then the
            rest is booked from the order's own totals (_book_remainder). FILLED needs the whole size: a FILLED row
            still short of it then settles CANCELED at bexc's quantity ([BX-ALARM] filled-short);
          - a terminal state then waits for a balance read sent after the order's last fill (_balance_shows_fills);
          - an order we asked bexc to cancel is not set back to OPEN by a row read before the cancel took;
          - whenever the poll leaves the order as it is, it raises the CODE_EMPTY_ANSWER sentinel: never a snapshot of
            the current state (queued as a task, a "no change" update can land after a newer state and roll it back)."""
        row = await self._fetch_order_detail(tracked_order)
        client_order_id = tracked_order.client_order_id
        if not self._check_size(tracked_order, row):
            return self._order_update(tracked_order, OrderState.PENDING_CANCEL)
        cumulative = row["filled"] if row["filled"] is not None else Decimal("0")
        state = row["state"]
        new_state = self._order_state(state, cumulative, tracked_order.amount)
        terminal = new_state in CONSTANTS.TERMINAL_STATES
        # Fills first: a FILLED state with fills still missing would complete the order short.
        for fill in await self._fills_from_rows(tracked_order, cumulative, row["created_ms"], terminal=terminal):
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(tracked_order, fill)
        self._audit_once(f"row-state:{row['view']}:{state}", mapped=new_state.name, filled=str(cumulative),
                         amount=str(row["amount"]))
        if not terminal:
            self._short_since.pop(client_order_id, None)
            if tracked_order.current_state is OrderState.PENDING_CANCEL:
                # A row read before bexc took our cancel. Kept PENDING_CANCEL; a cancel bexc took that still hasn't
                # taken effect after TERMINAL_HOLD_MAX_S is alarmed once.
                sent = self._cancel_sent.get(client_order_id)
                if sent is not None and time.monotonic() - sent >= CONSTANTS.TERMINAL_HOLD_MAX_S:
                    self._alarm_once(f"cancel-not-effective:{client_order_id}", "cancel-not-effective",
                                     client_id=client_order_id, exchange_order_id=tracked_order.exchange_order_id,
                                     bexc_state=state, waited_s=round(time.monotonic() - sent, 1),
                                     action="kept PENDING_CANCEL: check the order on bexc")
                raise BexcAPIError(f"{client_order_id}: cancel pending; bexc still lists it {state}",
                                   CONSTANTS.CODE_EMPTY_ANSWER)
            if new_state is tracked_order.current_state or tracked_order.is_done:
                # Never a no-change snapshot, and nothing non-final onto a finished order: a row read before it
                # finished, which this fork would apply to the order the tracker still holds for 30 s (review I).
                raise BexcAPIError(f"{client_order_id}: still {new_state.name}", CONSTANTS.CODE_EMPTY_ANSWER)
            return self._order_update(tracked_order, new_state)
        # Never past bexc's own filled quantity (review A, 2026-11-14). A FILLED row short of the size is held like a
        # short fill list (bexc's quantity may catch up), then settled CANCELED at bexc's quantity with a [BX-ALARM]:
        # the CCXT view's word for a partly filled order that was cancelled is unseen, and bexc's own `status=closed`
        # filter lists cancelled orders too.
        target = cumulative
        filled_short = new_state is OrderState.FILLED and cumulative + Decimal("1e-12") < tracked_order.amount
        if filled_short or tracked_order.executed_amount_base + Decimal("1e-12") < target:
            # bexc says done but its fill list hasn't shown every fill yet, or a guard refused a fill row: nothing is
            # reported until every fill is booked. Strict on purpose: done with fills missing, the strategy would hedge
            # the wrong size.
            held_s = time.monotonic() - self._short_since.setdefault(client_order_id, time.monotonic())
            if held_s < CONSTANTS.TERMINAL_HOLD_MAX_S:
                self._audit_once(f"terminal-held:{client_order_id}", client_id=client_order_id, state=new_state.name,
                                 filled=str(cumulative), booked=str(tracked_order.executed_amount_base),
                                 amount=str(tracked_order.amount))
                raise BexcAPIError(f"{client_order_id}: {new_state.name} held for its fill rows (booked "
                                   f"{tracked_order.executed_amount_base} of {target}, size {tracked_order.amount})",
                                   CONSTANTS.CODE_EMPTY_ANSWER)
            if (tracked_order.executed_amount_base + Decimal("1e-12") < target
                    and not self._book_remainder(tracked_order, target, cumulative, row, held_s)):
                self._alarm("terminal-forwarded-short", client_id=client_order_id, state=new_state.name,
                            filled=str(cumulative), booked=str(tracked_order.executed_amount_base),
                            held_s=round(held_s, 1))
            if filled_short:
                self._alarm("filled-short", client_id=client_order_id,
                            exchange_order_id=tracked_order.exchange_order_id, bexc_state=state,
                            filled=str(cumulative), amount=str(tracked_order.amount), held_s=round(held_s, 1),
                            action="settled CANCELED at bexc's filled quantity: check the order on bexc")
                new_state = OrderState.CANCELED
        self._short_since.pop(client_order_id, None)
        self._cancel_sent.pop(client_order_id, None)
        update = self._order_update(tracked_order, new_state)
        if self._balance_shows_fills(tracked_order):
            return update
        self._process_terminal_update(tracked_order, update, source="rest")
        raise BexcAPIError(f"{client_order_id}: {new_state.name} held until a balance read shows its fills",
                           CONSTANTS.CODE_EMPTY_ANSWER)

    def _book_remainder(self, order: InFlightOrder, target: Decimal, cumulative: Decimal, row: Dict[str, Any],
                        held_s: float) -> bool:
        """What /trades/my still lacks of a terminal order's fill, booked as ONE fill from the order's own totals
        (CoinW's rule, 2026-10-15): the target (bexc's filled quantity) - booked; the quote execution_value - booked
        quote when the row has it and its filled quantity covers the target (list rows only), else the size x the
        average price, else x the order's price; the fee: the list row's `fee` - booked fee (the CCXT view's fee is
        null: then 0), in the quote; named <order id>:<target>. True if booked. Booked with no await before the
        terminal state that follows, so no fills read can add the same quantity again; later rows find no room (the
        cap at the filled quantity)."""
        target = min(target, order.amount)
        missing = target - order.executed_amount_base
        if missing <= Decimal("1e-12") or order.exchange_order_id is None:
            return False
        fills = list(order.order_fills.values())
        booked_quote = sum((f.fill_quote_amount for f in fills), Decimal("0"))
        quote_total = row["quote_filled"] or Decimal("0")
        average = row["average"] or Decimal("0")
        if cumulative >= target and quote_total > booked_quote:
            quote_amount = quote_total - booked_quote
        elif average > 0:
            quote_amount = missing * average
        else:
            quote_amount = missing * order.price
        token = row["fee_asset"] or order.quote_asset
        booked_fee = sum((fee.amount for f in fills for fee in f.fee.flat_fees if fee.token == token), Decimal("0"))
        fee_known = row["fee"] is not None
        fee = max(abs(row["fee"]) - booked_fee, Decimal("0")) if fee_known else Decimal("0")
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
                    quote=str(quote_amount), fee=str(fee), fee_coin=token, fee_known=fee_known, view=row["view"])
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
        """True once the balances come from a read SENT after the order's last fill was booked. bexc credits a fill to
        the balance at once (the placement probe, 2026-11-13: the first balance read after each taker answer held the
        fill, net of the fee), so such a read holds the fill; an earlier read may not, and the hold-band would read the
        asset without it and buy twice (Hotcoin, 2026-10-06). One rule for every order (BloFin's, 2026-11-11). An order
        without fills booked by this process needs no wait."""
        booked_at = self._fill_booked_at.get(order.client_order_id)
        return booked_at is None or order.executed_amount_base <= 0 or self._balance_applied > booked_at

    async def _await_balance_showing_fills(self, order: InFlightOrder, source: str) -> None:
        """A balance read sent now, after the fills: its answer is what the completion waits for (~0.3 s from myserver).
        Should it fail, the poll's reads (every second while an order is open) count too, up to
        FILL_BALANCE_WAIT_SECONDS; then the completion goes anyway, with a [BX-ALARM]. _update_all_balances, not
        _update_balances: the in-flight snapshot must move with the balance it pairs with, or get_available_balance
        counts fills and locks twice."""
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
        """GET /fees/me: the account's rate per market (`effective_fees[]`, fractions: Bronze 0.001 / 0.001 on every
        market, live 2026-11-13; /markets' maker_fee / taker_fee are defaults, not what is charged). The first row is
        audit-logged and every rate checked against FEE_RATE_SANITY_MAX (a unit slip, LBank's lesson): a rate outside
        it is not used, and the market keeps DEFAULT_FEES. A failed read keeps the last rates. Each fill books bexc's
        own fee anyway: these feed the estimates only."""
        try:
            response = await self._api_get(path_url=CONSTANTS.FEES_PATH, is_auth_required=True,
                                           limit_id=CONSTANTS.FEES_PATH)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"bexc fee rates not read ({e}); the last ones stay (DEFAULT_FEES until a read).")
            return
        rows = response.get("effective_fees") if isinstance(response, dict) else None
        if not isinstance(rows, list):
            self.logger().warning(f"bexc's fee answer has no effective_fees list; the last rates stay | bexc response: "
                                  f"{self._raw(response)[:300]}")
            return
        symbol_map = await self.trading_pair_symbol_map()
        fees: Dict[str, TradeFeeSchema] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            maker, taker = self._opt_dec(row.get("maker_fee")), self._opt_dec(row.get("taker_fee"))
            if (maker is None or taker is None or not Decimal("0") <= maker <= CONSTANTS.FEE_RATE_SANITY_MAX
                    or not Decimal("0") <= taker <= CONSTANTS.FEE_RATE_SANITY_MAX):
                self._alarm_once(f"fee-rate-implausible:{row.get('symbol')}", "fee-rate-implausible",
                                 row=self._raw(row), action="not used: the market keeps DEFAULT_FEES")
                continue
            trading_pair = symbol_map.get(str(row.get("symbol") or ""))
            if trading_pair is None:
                continue
            fees[trading_pair] = TradeFeeSchema(maker_percent_fee_decimal=maker, taker_percent_fee_decimal=taker)
        self._audit_once("fee-rate-row", tier=response.get("tier"), first=self._raw(rows[:1]), markets=len(fees))
        self._trading_fees.update(fees)

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
        """GET /wallet/balances: {"balances": [{asset, total, available, locked, ...}]}, strings (live). Zero balances
        are omitted, a coin held once stays as a 0 row. total = max(total, available + locked): `locked` holds open
        orders (an open buy locks notional x 1.002, live) and pending withdrawals. A malformed row fails the whole read
        (a partial map would drop assets). An answer that leaves out a coin held (an EMPTY list leaves out all of them)
        is believed only after EMPTY_BALANCES_TRUST_S and EMPTY_BALANCES_TRUST_READS such answers in a row (the AM's
        rule; review B): it raises and the last balances stay. Reads are numbered as they go out: an answer arriving
        after a newer read's was applied is dropped (False), never rolling the balances back (the completion gate,
        _balance_shows_fills, counts on it)."""
        self._balance_reads_sent += 1
        sequence = self._balance_reads_sent
        response = await self._api_get(path_url=CONSTANTS.BALANCES_PATH, is_auth_required=True,
                                       limit_id=CONSTANTS.BALANCES_PATH)
        rows = response.get("balances") if isinstance(response, dict) else None
        if not isinstance(rows, list):
            raise IOError(f"bexc's balance answer has no `balances` list | bexc response: {self._raw(response)}")
        parsed: Dict[str, Tuple[Decimal, Decimal]] = {}
        for entry in rows:
            asset = str(entry.get("asset") or entry.get("symbol") or "").strip().upper() \
                if isinstance(entry, dict) else ""
            available = self._opt_dec(entry.get("available")) if isinstance(entry, dict) else None
            if not asset or available is None:
                raise IOError(f"bexc balance row not readable: {self._raw(entry)[:200]}; the last balances stay")
            locked = self._opt_dec(entry.get("locked")) or Decimal("0")
            total = self._opt_dec(entry.get("total")) or Decimal("0")
            parsed[asset] = (available, max(total, available + locked))
            if locked > 0:
                self._audit_once(f"balance-semantics:{asset}", available=str(available), locked=str(locked),
                                 total=str(total), balance_in_trade=entry.get("balance_in_trade"))
        self._audit_once("balance-rest-shape", assets=len(rows), source=response.get("source"),
                         sample=self._raw(rows[:2]))
        held = {asset for asset, total in self._account_balances.items() if total > 0}
        missing = sorted(held.difference(parsed))
        if missing:
            # A coin held once stays as a 0 row: one left out is a partial answer (an empty list leaves out them all),
            # or a sale whose row bexc dropped. Zeroed at once, the hold-band would buy it again (review B).
            now = time.monotonic()
            if self._omitting_since is None:
                self._omitting_since = now
            self._omitting_reads += 1
            if (now - self._omitting_since < CONSTANTS.EMPTY_BALANCES_TRUST_S
                    or self._omitting_reads < CONSTANTS.EMPTY_BALANCES_TRUST_READS):
                raise IOError(f"bexc's balance answer leaves out {len(missing)} of the {len(held)} assets held "
                              f"({', '.join(missing[:5])}): not believed yet ({now - self._omitting_since:.0f} s, "
                              f"{self._omitting_reads} such answers in a row); the last balances stay")
            self._alarm("balance-vanished" if parsed else "balances-emptied", assets=missing[:20],
                        held_before=len(held), omitted_s=round(now - self._omitting_since), reads=self._omitting_reads)
        self._omitting_since = None        # believed, or nothing left out: the next omission counts afresh
        self._omitting_reads = 0
        if sequence < self._balance_applied:
            self._audit_once("balance-answer-dropped", sequence=sequence, applied=self._balance_applied)
            return False
        for asset_name in set(self._account_balances).difference(parsed):
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)
        for asset, (available, total) in parsed.items():
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total
        self._balance_applied = sequence
        return True

    # ------------------------------------------------------------------ symbols / rules / prices

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any) -> None:
        mapping = bidict()
        for info in self._market_rows(exchange_info):
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                symbol = str(info["symbol"])
                trading_pair = combine_to_hb_trading_pair(base=str(info["base_asset"]).upper(),
                                                          quote=str(info["quote_asset"]).upper())
                if trading_pair in mapping.inverse or symbol in mapping:
                    continue      # bidict refuses a second symbol for one pair; the first listed wins
                mapping[symbol] = trading_pair
            except Exception as exception:
                self.logger().error(f"Error parsing bexc market {info.get('symbol')}: {exception}")
        # A market that left the list (a delisting) stays mapped while orders on it are tracked: their status and fill
        # reads need its symbol until they settle (review F, 2026-11-14). It gets no trading rule: no new order.
        pairs_in_use = {order.trading_pair for order in self._order_tracker.all_fillable_orders.values()}
        for symbol, trading_pair in (self._last_symbol_map or {}).items():
            if trading_pair in pairs_in_use and trading_pair not in mapping.inverse and symbol not in mapping:
                mapping[symbol] = trading_pair
                self._alarm_once(f"market-unlisted:{symbol}", "market-unlisted", symbol=symbol,
                                 trading_pair=trading_pair, action="kept in the symbol map while its orders are "
                                                                   "tracked; no new order (no trading rule)")
        self._last_symbol_map = mapping     # the same object: runtime adds (_add_trading_pair_to_symbol_map) land in it
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair the startup list did not have. bexc's form is BASE_QUOTE: the base default
        (f"{base}{quote}") would be wrong here."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote_asset = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.upper()}_{quote_asset.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(f"bexc {trading_pair} was not among the markets listed at startup; mapped to "
                              f"{exchange_symbol}. bexc's answers to its requests will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        """The /markets rows (live 2026-11-13, BTC_USDT): tick_size = the price step (0.01), step_size = the quantity
        step (0.000001), min_qty / min_quantity = the minimum quantity in base units (0.0001: ~$6.35 accepted live),
        max_qty. No minimum notional exists. A limit price must be within ±price_band_ratio (0.10) of the reference:
        refusals (400 PRICE_OUT_OF_BAND) are logged in full. A row without a positive tick or step gets no rule (an
        order on it is then failed "no rule" without being sent): quantizing by zero is impossible."""
        rules: List[TradingRule] = []
        for info in self._market_rows(exchange_info_dict):
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(info["symbol"]))
                tick, step = self._dec(info.get("tick_size")), self._dec(info.get("step_size"))
                if tick <= 0 or step <= 0:
                    self.logger().error(f"bexc market {info.get('symbol')} has no usable tick_size / step_size "
                                        f"({info.get('tick_size')} / {info.get('step_size')}): no trading rule.")
                    continue
                min_qty = info.get("min_qty") if info.get("min_qty") not in (None, "") else info.get("min_quantity")
                rule_kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(min_qty), step),
                    min_price_increment=tick,
                    min_base_amount_increment=step,
                )
                if info.get("max_qty") not in (None, ""):
                    rule_kwargs["max_order_size"] = self._dec(info.get("max_qty"))
                rules.append(TradingRule(**rule_kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the bexc trading rule {info.get('symbol')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """One bulk /ticker read (a bare list of every market, cached 10 s by bexc; shared here for TICKER_CACHE_SECONDS)
        answers every pair; every requested pair gets a value, NaN when bexc has none (or 0): the tracker re-asks a
        pair it got nothing for at once."""
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        cached = self._ticker_cache
        if cached is None or time.monotonic() - cached[0] > CONSTANTS.TICKER_CACHE_SECONDS:
            try:
                response = await self._api_get(path_url=CONSTANTS.TICKER_PATH, limit_id=CONSTANTS.TICKER_PATH)
                rows = response if isinstance(response, list) else []
                lasts: Dict[str, float] = {}
                for row in rows:
                    if not isinstance(row, dict) or not row.get("symbol"):
                        continue
                    last = self._opt_dec(row.get("last_price"))
                    if last is not None and last > 0:
                        lasts[str(row["symbol"])] = float(last)
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
