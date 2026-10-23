import asyncio
import json
import re
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, quote_plus

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinw import (
    coinw_constants as CONSTANTS,
    coinw_utils as utils,
    coinw_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinw.coinw_api_order_book_data_source import CoinwAPIOrderBookDataSource
from hummingbot.connector.exchange.coinw.coinw_api_user_stream_data_source import CoinwAPIUserStreamDataSource
from hummingbot.connector.exchange.coinw.coinw_auth import CoinwAuth
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

_SIGN_RE = re.compile(r"sign=[0-9A-Fa-f]{32}")


class CoinwBusinessError(IOError):
    """A request CoinW refused: HTTP 200 with an envelope whose `code` isn't "200" — a definite answer, never a
    transport failure. IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[str], body: str) -> None:
        super().__init__(message)
        self.code = code
        self.body = body


class CoinwStatusDeferred(Exception):
    """Not a status: the poll leaves this order alone for now (its placement awaits its answer, or its terminal state
    waits for CoinW's fill rows). Raised instead of returning a snapshot of the current state, which the base would queue
    and could apply after a newer state."""


class CoinwPlacementUnknown(IOError):
    """A placement got no answer and the lookup could not find the order. The order is NOT failed (it may rest on
    CoinW): it stays PENDING_CREATE until its RECEIVED push, a later lookup, or the age-gated not-found verdict."""


class CoinwExchange(ExchangePyBase):
    """
    CoinW spot connector.

    Reference: the spot docs (offline mirror VS_code_projects/MDs/coinw-api/), JKorf's CoinW.Net (the maintained SDK),
    and the phase-0 probes on myserver (wiki trading/exchanges/coinw-api, "P2 + P3 onboarding"). Every CoinW-specific
    choice cites its source; open questions are audit-logged ([CW-AUDIT]) rather than guessed, and money-guard firings
    always log as [CW-ALARM].

    Scope: LIMIT orders, which is what arb_l and the position balancer send (CoinW has no post-only flag).
    Templates (runbook §0.4, by API shape): the local book from `poloniex` (a socket snapshot + sequenced deltas, here
    merged from two of its three channels), the request model from `hotcoin` / `bitunix` (refusals as HTTP 200 + an envelope code,
    no server time), the order tracking from `poloniex` + `xt` (client ids on the pushes, fills from REST rows with real
    trade ids, terminal states that wait for their fills) with Hotcoin's §1.5 checklist.

    Fills. CoinW's order push has two events only, RECEIVED and DONE (CoinW.Net OrderEventType): a resting order's
    partial fills are pushed by nothing. Fills are booked from getUserTrades rows (one per fill: tradeId, orderId,
    price, size, fee, time; 18-decimal values), read when something says the order filled: its DONE push, the assets
    push of its coins, or returnOrderTrades' cumulative success_total in the 2 s poll of open orders. One method books
    every fill, under CoinW's own trade ids, capped at the order's size.
    """

    web_utils = web_utils

    def __init__(
        self,
        coinw_api_key: str,
        coinw_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """The signature matches ConnectorSetting.conn_init_parameters in this fork, which always passes
        `balance_asset_limit` (and `rate_limits_share_pct`) and no config map (CoinEx, 2026-09-15)."""
        self._api_key = coinw_api_key
        self._secret_key = coinw_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # shared public reads: returnTicker (pairCodes, last prices, isFrozen) and returnSymbol (rules, state)
        self._ticker_cache: Optional[Tuple[float, Dict[str, Any]]] = None
        self._ticker_lock = asyncio.Lock()
        self._symbols_cache: Optional[Tuple[float, List[Dict[str, Any]]]] = None
        self._symbols_lock = asyncio.Lock()
        # getUserTrades per market: one read in flight at a time, its rows shared for USER_TRADES_MIN_INTERVAL
        self._fills_locks: Dict[str, asyncio.Lock] = {}
        self._fills_cache: Dict[str, Tuple[float, int, List[Dict[str, Any]]]] = {}
        # order ids (exchange and client) a push was seen for: the push-missing alarm
        self._pushed_order_ids: Dict[str, None] = {}
        self._missing_pushes = 0
        self._last_push_alarm = 0.0
        # exchange ids alarmed once: an untracked open order of ours, a failed order still open
        self._orphan_ids: Dict[str, None] = {}
        # client ids whose terminal update waits for its fills (one waiter per order)
        self._settling: set = set()
        # client ids whose placement has not returned yet: the status poll leaves them alone
        self._placing: set = set()
        # client id -> when its terminal state was first held back for missing fill rows (TERMINAL_HOLD_MAX_S)
        self._short_since: Dict[str, float] = {}
        # client ids we sent a cancel for (a vanished order with no fill is then CANCELED, not lost)
        self._cancel_sent: Dict[str, float] = {}
        # client id -> monotonic time an order was first seen gone from returnOpenOrders with no state to read
        self._gone_since: Dict[str, float] = {}
        # per asset: monotonic time of its last balance push; a REST read sent before it never overwrites it
        self._balance_pushed_at: Dict[str, float] = {}
        # per asset: the last assets push's ledger_id (an older one is not applied)
        self._ledger_ids: Dict[str, int] = {}
        # per asset: CoinW's last (available, locked) -- REST onOrders, push hold -- as reported; the available balance
        # HMB reads also loses what HMB's own open orders lock beyond `locked` (_apply_balance)
        self._reported_balances: Dict[str, Tuple[Decimal, Decimal]] = {}
        self._balance_pushes_audited = 0
        # empty balance answers in a row (an empty answer is believed only after a run of them)
        self._empty_balance_reads = 0
        self._empty_balance_since = 0.0
        # assets whose push asked for a fills read (debounced)
        self._fills_check_pending: Set[str] = set()
        self._fills_check_task: Optional[asyncio.Task] = None
        # a REST balance read asked by an order push (CoinW pushes no balance for a hold), debounced
        self._balance_refresh_task: Optional[asyncio.Task] = None
        self._balance_refresh_again = False
        # per asset: answers in a row it was missing from (dropped at BALANCE_MISSING_READS)
        self._missing_balance_reads: Dict[str, int] = {}
        # client ids this process gave up (failed, or struck as not found): a later push for one is ours, untracked
        self._given_up: Dict[str, float] = {}
        # a 29001 answer slows the connector's own reads until this monotonic time
        self._rate_limited_until = 0.0
        self._last_rate_limit_alarm = 0.0
        # Cloudflare's 451 answers since the last [CW-ALARM] cloudflare-legal-block line, and that line's monotonic time
        self._legal_blocks = 0
        self._last_legal_block_alarm = 0.0
        # client id -> (the base asset's total, _booked_base of it) when the order was sent: the baseline its terminal
        # update compares the balance with (_balance_shows_fills)
        self._hold_baseline: Dict[str, Tuple[Decimal, Decimal]] = {}
        # asset -> net base change of every fill this process booked (_note_fill_booked)
        self._booked_base: Dict[str, Decimal] = {}
        # client ids whose terminal update waits for the balance to show its fills (one waiter per order)
        self._balance_waiting: Set[str] = set()
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinwAuth:
        return CoinwAuth(api_key=self._api_key, secret_key=self._secret_key)

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
        return CONSTANTS.CMD_SYMBOLS

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.CMD_SYMBOLS

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.CMD_ORDER_BOOK

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # cancelOrder answers {"clientOrderId": <the order id>} only: the DONE push or the poll settles it.
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    # ------------------------------------------------------------------ factories

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinwAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinwAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [CW-AUDIT] line. Only questions the docs cannot answer; never a URL, a query or a credential (each line
        is scrubbed: CoinW's answers are logged verbatim)."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(self._scrub(f"[CW-AUDIT] {tag} {rendered}"))

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [CW-ALARM] WARNING, logged whatever LIVE_AUDIT_LOGGING says."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(self._scrub(f"[CW-ALARM] {tag} {rendered}"))

    # ------------------------------------------------------------------ requests and errors

    @staticmethod
    def _raw(response: Any) -> str:
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    def _scrub(self, text: Any) -> str:
        """Text safe to log: CoinW's api key and signature travel in the URL query, and a signed CoinW request carries
        no timestamp, so a logged URL could be replayed. The key and the secret in every spelling a URL gives them."""
        s = str(text)
        for secret, tag in ((self._api_key, "<key>"), (self._secret_key, "<secret>")):
            if secret and len(secret) >= 6:
                for form in (secret, quote(secret, safe=""), quote_plus(secret)):
                    s = s.replace(form, tag)
        return _SIGN_RE.sub("sign=<sign>", s)

    async def _request(self, command: str, params: Optional[Dict[str, Any]] = None, *, private: bool,
                       timeout: Optional[float] = None) -> Any:
        """
        One CoinW request, with its error model applied:
          HTTP 200 + {"code":"200",…}  -> the `data`
          HTTP 200 + another code      -> CoinwBusinessError (a refusal: the code and the complete answer)
          HTTP 200 + an empty body     -> CoinwBusinessError "no-envelope" (CoinW's answer to an unauthenticated call)
          HTTP >= 400, an unparseable body, a transport failure -> IOError (an UNKNOWN outcome)
        Private commands are POSTs with an empty body; the auth adds api_key and the MD5 sign to the query. Every request
        has a timeout (REQUEST_TIMEOUT unless given): aiohttp's session default is 300 s. A 29001 answer slows the
        connector's own reads (_on_rate_limited). A 451 (Cloudflare's legal block) also closes the connection that
        answered it (_on_legal_block).
        """
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        path = CONSTANTS.PRIVATE_PATH if private else CONSTANTS.PUBLIC_PATH
        query = {"command": command}
        query.update({k: v for k, v in (params or {}).items() if v is not None})
        try:
            response = await rest_assistant.execute_request_and_get_response(
                url=web_utils.private_rest_url(path) if private else web_utils.public_rest_url(path),
                throttler_limit_id=command, params=query, data=None,
                method=RESTMethod.POST if private else RESTMethod.GET,
                is_auth_required=private, return_err=True,
                timeout=timeout if timeout is not None else CONSTANTS.REQUEST_TIMEOUT)
            status = response.status
            text = await response.text()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise IOError(f"CoinW {command}: no answer ({type(e).__name__}: {self._scrub(e)})") from None
        if status >= 400:
            if status == CONSTANTS.HTTP_LEGAL_BLOCK:
                self._on_legal_block(command, response)
            raise IOError(f"CoinW {command}: HTTP {status} (outcome unknown): {self._scrub((text or '')[:300])}")
        try:
            body = web_utils.loads(text)
        except (TypeError, ValueError):
            raise IOError(f"CoinW {command}: an unparseable answer (outcome unknown): {self._scrub(text[:300])}")
        code = web_utils.error_code(body)
        if code is not None:
            if str(code) == CONSTANTS.CODE_RATE_LIMIT:
                self._on_rate_limited(command)
            raise CoinwBusinessError(f"CoinW refused {command}: code {code} "
                                     f"({body.get('msg') if isinstance(body, dict) else None}) | CoinW response: "
                                     f"{self._scrub(self._raw(body)[:500])}", code, self._raw(body))
        return body.get("data")

    def _on_rate_limited(self, command: str) -> None:
        """CoinW answered 29001: its error policy turns more of these into bans of every call (cancels included). Said
        (at most once a minute), and the connector's own reads slow down for RATE_LIMITED_SLOW_S."""
        now = time.monotonic()
        self._rate_limited_until = now + CONSTANTS.RATE_LIMITED_SLOW_S
        if now - self._last_rate_limit_alarm >= 60.0:
            self._last_rate_limit_alarm = now
            self._alarm("rate-limited", command=command, slow_s=CONSTANTS.RATE_LIMITED_SLOW_S,
                        poll_interval_s=CONSTANTS.RATE_LIMITED_POLL_INTERVAL)

    def _rate_limited(self) -> bool:
        return time.monotonic() < self._rate_limited_until

    def _on_legal_block(self, command: str, response: Any) -> None:
        """Cloudflare answered 451, its legal block (HTTP_LEGAL_BLOCK): the keep-alive connection that answered is
        closed, so no later request reuses it and the next one opens a new connection. The body is read by now, so
        aiohttp has already put the connection back in the shared pool, which drops a closed one. Said as a
        [CW-ALARM] at most once a minute, with the number of 451s since the last line: the network check that fails
        on it logs nothing."""
        protocol = getattr(getattr(response, "_aiohttp_response", None), "_protocol", None)
        if protocol is not None:
            protocol.close()
        self._legal_blocks += 1
        now = time.monotonic()
        if now - self._last_legal_block_alarm >= 60.0:
            self._last_legal_block_alarm = now
            self._alarm("cloudflare-legal-block", command=command,
                        cf_ray=(getattr(response, "headers", None) or {}).get("cf-ray"),
                        count=self._legal_blocks, connection_dropped=protocol is not None)
            self._legal_blocks = 0

    def _give_up(self, client_order_id: str) -> None:
        """Remember an order this process failed or struck as not found: a push for it later is ours."""
        self._given_up[client_order_id] = time.monotonic()
        if len(self._given_up) > 2000:
            for key in list(self._given_up)[:1000]:
                del self._given_up[key]

    def _update_order_after_failure(self, order_id: str, trading_pair: str, exception: Optional[Exception] = None):
        self._give_up(order_id)
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        super()._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
        if tracked is not None:
            self._reapply_balances(self._spend_asset(tracked))     # a failed order holds nothing

    async def _strike_not_found(self, client_order_id: str) -> None:
        """A not-found strike (the tracker fails the order, and purges it, after its limit)."""
        self._give_up(client_order_id)
        tracked = self._order_tracker.fetch_tracked_order(client_order_id)
        await self._order_tracker.process_order_not_found(client_order_id)
        if tracked is not None:
            self._reapply_balances(self._spend_asset(tracked))

    async def _public(self, command: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._request(command, params, private=False)

    async def _private(self, command: str, params: Optional[Dict[str, Any]] = None,
                       timeout: Optional[float] = None) -> Any:
        return await self._request(command, params, private=True, timeout=timeout)

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        """Nothing CoinW accepts carries a timestamp (the signature covers the parameters and the key only), and CoinW
        has no server-time endpoint: there is no clock to agree with."""
        return

    async def _make_network_check_request(self):
        await self._public(CONSTANTS.CMD_ORDER_BOOK, {"symbol": "BTC_USDT", "size": 5})

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        if tracked is not None and (tracked.exchange_order_id is not None
                                    or tracked.current_state is not OrderState.PENDING_CREATE):
            # A push already gave the order its id or moved it: CoinW has it. Never failed on top of that.
            self._alarm("placement-error-but-order-exists", client_id=order_id, order_id=tracked.exchange_order_id,
                        state=str(tracked.current_state), error=repr(exception))
            return
        if isinstance(exception, CoinwPlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. Its RECEIVED push or the "
                                  f"status poll settles it; it fails only once CoinW is shown not to have it.")
            return
        if isinstance(exception, CoinwBusinessError):
            meaning = CONSTANTS.REFUSAL_CODES.get(str(exception.code))
            self.logger().warning(f"CoinW rejected {trade_type.name.lower()} {order_type.name} order {order_id} for "
                                  f"{amount} {trading_pair} at {price}{' (' + meaning + ')' if meaning else ''}: "
                                  f"{exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False   # CoinW's status reads have no not-found code (a made-up id answers -200); see _settle_vanished

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False   # cancelOrder's 200012 means "not open": read the order's state instead (_execute_order_cancel)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """This fork's base counts EVERY failed status read toward failing the order (the 4th strike, never reset) and
        runs no lost-order recovery: a FAILED order is forgotten while it may still rest on CoinW. Here a failed read is
        a WARNING and the order stays tracked; only _settle_vanished (the order provably not on CoinW) settles one
        without a state (Hotcoin's lesson, 2026-10-06)."""
        if isinstance(error, CoinwStatusDeferred):
            return
        self.logger().warning(f"CoinW status read for {order.client_order_id} failed, the order stays tracked: "
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

    # ------------------------------------------------------------------ shared public reads

    async def _ticker(self, max_age: float = CONSTANTS.TICKER_SHARE_SECONDS) -> Dict[str, Any]:
        """returnTicker ({pair: {id, last, isFrozen, ...}}, every market, one call), shared within max_age."""
        async with self._ticker_lock:
            cached = self._ticker_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            data = await self._public(CONSTANTS.CMD_TICKER)
            if not isinstance(data, dict) or not data:
                raise IOError(f"CoinW returnTicker answered no market rows: {self._raw(data)[:200]}")
            self._ticker_cache = (time.monotonic(), data)
            return data

    async def _symbols(self, max_age: float = 0.0) -> List[Dict[str, Any]]:
        """returnSymbol (every market with its rules and `state`), shared within max_age. An empty or refused list
        raises, so the symbol map and the rules keep what they have."""
        async with self._symbols_lock:
            cached = self._symbols_cache
            if cached is not None and time.monotonic() - cached[0] <= max_age:
                return cached[1]
            data = await self._public(CONSTANTS.CMD_SYMBOLS)
            if not isinstance(data, list) or not data:
                raise IOError(f"CoinW returnSymbol answered no market list: {self._raw(data)[:200]}")
            self._symbols_cache = (time.monotonic(), data)
            return data

    async def _make_trading_rules_request(self) -> Any:
        return await self._symbols(max_age=5.0)

    async def _make_trading_pairs_request(self) -> Any:
        return await self._symbols(max_age=5.0)

    async def pair_codes(self, refresh_for: Optional[str] = None) -> Dict[str, str]:
        """{CoinW symbol: its socket pairCode} (returnTicker `id`); re-read when `refresh_for` isn't in it yet."""
        ticker = await self._ticker()
        if refresh_for is not None and refresh_for not in ticker:
            ticker = await self._ticker(max_age=0.0)
        return {symbol: str(row.get("id")) for symbol, row in ticker.items()
                if isinstance(row, dict) and row.get("id") not in (None, "")}

    async def markets_switched_off(self) -> Set[str]:
        """CoinW symbols whose trading is off: returnSymbol state != 1, or returnTicker isFrozen 1 (non-negotiable 3:
        shown as an empty book, never dropped). A market missing from both lists is not here (its subscription and
        orders answer for themselves)."""
        rows = await self._symbols(max_age=CONSTANTS.TRADING_SWITCH_INTERVAL)
        ticker = await self._ticker(max_age=CONSTANTS.TRADING_SWITCH_INTERVAL)
        off = {str(r.get("currencyPair")) for r in rows
               if isinstance(r, dict) and r.get("currencyPair") and r.get("state") not in (None, "")
               and str(r.get("state")) != "1"}
        off |= {symbol for symbol, row in ticker.items() if isinstance(row, dict) and str(row.get("isFrozen")) == "1"}
        return off

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
                       f"was NOT sent to CoinW: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from returnSymbol).")
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
        params = {
            "symbol": symbol,
            "type": CONSTANTS.TRADE_TYPES[trade_type],
            "amount": self._format_decimal(amount),
            "rate": self._format_decimal(price),
            "isMarket": "false",
            "out_trade_no": order_id,
        }
        self._placing.add(order_id)
        sent_at = time.time()
        try:
            try:
                data = await self._private(CONSTANTS.CMD_PLACE, params, timeout=CONSTANTS.PLACE_ORDER_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if isinstance(error, CoinwBusinessError):
                    # AUDIT: every refusal with CoinW's complete answer (the request carries no credential: the key and
                    # the signature are added after this dict, by the auth).
                    self._audit("place-order-refused", request=self._raw(params), code=error.code, response=error.body)
                    if str(error.code) not in CONSTANTS.PLACE_UNKNOWN_CODES:
                        raise
                # No answer, or one that doesn't say no (-200 is also maintenance; an empty body is proven only for an
                # unauthenticated call): CoinW may have accepted the order. Looked up before the failure may stand.
                exchange_order_id, verdict = await self._lookup_placement(order_id, trading_pair, symbol, trade_type,
                                                                          amount, price, sent_at)
                self._alarm("place-order-unanswered", client_id=order_id, error=repr(error),
                            found_on_exchange=exchange_order_id, verdict=verdict)
                if exchange_order_id is not None:
                    return exchange_order_id, self._now()
                raise CoinwPlacementUnknown(f"CoinW did not answer the placement ({error!r}) and the lookup could not "
                                            f"tell ({verdict})") from error
            self._audit("place-order", request=self._raw(params), response=self._raw(data))
            exchange_order_id = data.get("orderNumber") if isinstance(data, dict) else None
            if exchange_order_id in (None, ""):
                found, verdict = await self._lookup_placement(order_id, trading_pair, symbol, trade_type, amount, price,
                                                              sent_at)
                self._alarm("place-order-no-id", client_id=order_id, response=self._raw(data), found_on_exchange=found,
                            verdict=verdict)
                if found is not None:
                    return found, self._now()
                raise CoinwPlacementUnknown(f"CoinW answered the placement without an orderNumber "
                                            f"({self._raw(data)}); lookup: {verdict}")
            return str(exchange_order_id), self._now()
        finally:
            self._placing.discard(order_id)

    async def _lookup_placement(self, client_order_id: str, trading_pair: str, symbol: str, trade_type: TradeType,
                                amount: Decimal, price: Decimal, sent_at: float) -> Tuple[Optional[str], str]:
        """The placement guard: (exchange id, verdict). CoinW reads orders by orderNumber only, so the order is looked
        for (a) among the market's executed orders by its client id (returnUTradeHistory carries `out_trade_no`), and
        (b) among its open orders by side, price, size and creation time (returnOpenOrders carries no client id) —
        matched only when exactly one untracked order fits and no other order of ours like it is unconfirmed."""
        verdict = "absent"
        for delay in CONSTANTS.PLACEMENT_LOOKUP_DELAYS:
            await asyncio.sleep(delay)
            # an order push that came meanwhile gave the order its id
            tracked = self._order_tracker.fetch_tracked_order(client_order_id)
            if tracked is not None and tracked.exchange_order_id is not None:
                return str(tracked.exchange_order_id), "found by its push"
            try:
                found = await self._find_by_client_id(client_order_id, symbol, sent_at)
                if found is not None:
                    return found, "found among executed orders by out_trade_no"
                found, why = await self._find_open_order_like(client_order_id, trading_pair, symbol, trade_type,
                                                              amount, price, sent_at)
                if found is not None:
                    return found, f"found among open orders ({why})"
                verdict = why
            except asyncio.CancelledError:
                raise
            except Exception as e:
                verdict = f"inconclusive: {e!r}"
        return None, verdict

    async def _find_by_client_id(self, client_order_id: str, symbol: str, since: float) -> Optional[str]:
        """The order's id among the market's CLOSED orders by its client id (returnUTradeHistory: tradeID = the order
        id, out_trade_no = the client id; a cancelled order with no fill is listed too, after a lag: see
        NOT_FOUND_MIN_AGE_S), read from CLIENT_ID_LOOKBACK_S before `since` (no startAt would read 30 days)."""
        rows = await self._private(CONSTANTS.CMD_ORDER_HISTORY, {
            "currencyPair": symbol, "startAt": int((since - CONSTANTS.CLIENT_ID_LOOKBACK_S) * 1e3)})
        for row in rows if isinstance(rows, list) else []:
            if isinstance(row, dict) and str(row.get("out_trade_no") or "") == client_order_id:
                return str(row.get("tradeID"))
        return None

    async def _find_open_order_like(self, client_order_id: str, trading_pair: str, symbol: str, trade_type: TradeType,
                                    amount: Decimal, price: Decimal, sent_at: float) -> Tuple[Optional[str], str]:
        """One untracked open order with the same side, price and size, created after the send. Never while another
        order of ours with the same side, price and size has no id yet: either could be the one found (its answer and
        push still in flight), and both would then book its fills. The size matches as `total` (the order's size) or
        `total` + `success_count` (if `total` is what is left of it)."""
        twins = [o.client_order_id for o in self._order_tracker.active_orders.values()
                 if o.client_order_id != client_order_id and o.trading_pair == trading_pair
                 and o.trade_type is trade_type and o.price == price and o.amount == amount
                 and (o.exchange_order_id is None or o.client_order_id in self._placing)
                 and o.current_state not in CONSTANTS.TERMINAL_STATES]
        if twins:
            return None, f"ambiguous: {len(twins)} other identical order(s) of ours unconfirmed ({twins[:3]})"
        rows = await self._private(CONSTANTS.CMD_OPEN_ORDERS, {"currencyPair": symbol})
        if not isinstance(rows, list):
            return None, f"open orders unreadable: {self._raw(rows)[:120]}"
        tracked_ids = {str(o.exchange_order_id) for o in self._order_tracker.all_fillable_orders.values()
                       if o.exchange_order_id is not None}
        side = "buy" if trade_type is TradeType.BUY else "sell"
        candidates = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            oid = str(row.get("orderNumber"))
            created_ms = int(self._dec(row.get("date")))
            total = self._dec(row.get("total"))
            size_matches = total == amount or total + self._dec(row.get("success_count")) == amount
            if (oid not in tracked_ids and str(row.get("type") or "").lower() == side
                    and self._dec(row.get("prize")) == price and size_matches
                    and created_ms >= (sent_at - 2.0) * 1e3):
                candidates.append(oid)
        if len(candidates) == 1:
            return candidates[0], "side, price, size and time"
        return None, ("absent" if not candidates else f"ambiguous: {len(candidates)} identical untracked orders")

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        """
        The base's sequence, except its OPEN update is applied only while the order is still PENDING_CREATE: CoinW's
        pushes carry the client id and are applied at once, so a taker order's DONE push can finish the order before
        this answer arrives; this fork's update_with_order_update sets any state it is given, and an OPEN then would set
        the finished order back to open.
        """
        # The baseline the order's terminal update compares the balance with (_balance_shows_fills).
        asset = order.base_asset
        self._hold_baseline[order.client_order_id] = (self._account_balances.get(asset, Decimal("0")),
                                                      self._booked_base.get(asset, Decimal("0")))
        if len(self._hold_baseline) > 2000:
            for client_order_id in list(self._hold_baseline)[:1000]:
                del self._hold_baseline[client_order_id]
        # The order is tracked already: its hold leaves the available balance now, as a hold push would elsewhere.
        self._reapply_balances(self._spend_asset(order))
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
            self._alarm("order-id-mismatch", client_id=order.client_order_id, push_id=order.exchange_order_id,
                        placement_id=str(exchange_order_id))
        holders = [o.client_order_id for o in self._order_tracker.all_fillable_orders.values()
                   if o.client_order_id != order.client_order_id and str(o.exchange_order_id) == str(exchange_order_id)]
        if holders:
            # Two of our orders on one orderNumber would both book its fills.
            self._alarm("order-id-held-twice", client_id=order.client_order_id, order_id=str(exchange_order_id),
                        also_held_by=holders)
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
        """CoinW pushes RECEIVED for every accepted order. None within ORDER_PUSH_EXPECTED_WITHIN means the private
        stream isn't delivering: poll orders and balances over REST now, and say so (rate-limited [CW-ALARM])."""
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
        """While any order is open the status poll runs every OPEN_ORDER_POLL_INTERVAL: CoinW pushes nothing for a
        resting order's partial fill (RECEIVED and DONE only); RATE_LIMITED_POLL_INTERVAL after a 29001."""
        if self.in_flight_orders:
            if self._rate_limited():
                return CONSTANTS.RATE_LIMITED_POLL_INTERVAL
            return CONSTANTS.OPEN_ORDER_POLL_INTERVAL
        return super()._get_poll_interval(timestamp=timestamp)

    def _mark_cancel_sent(self, client_order_id: str) -> None:
        self._cancel_sent[client_order_id] = time.monotonic()
        if len(self._cancel_sent) > 2000:
            for key in list(self._cancel_sent)[:1000]:
                del self._cancel_sent[key]

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """CoinW cancels by orderNumber only; this waits for the placement answer if needed. The cancel counts as ours
        (a vanished order with no fill is then CANCELED, not struck) once CoinW took it, or when it got no answer (it
        may have landed); never when CoinW refused it (200012: the order was no longer open)."""
        exchange_order_id = await tracked_order.get_exchange_order_id()
        try:
            data = await self._private(CONSTANTS.CMD_CANCEL, {"orderNumber": exchange_order_id})
        except CoinwBusinessError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            self._mark_cancel_sent(order_id)
            raise
        self._mark_cancel_sent(order_id)
        self._audit("cancel-order", client_id=order_id, order_id=exchange_order_id, response=self._raw(data))
        return True

    async def _execute_order_cancel_and_process_update(self, order: InFlightOrder) -> bool:
        """The base's, except PENDING_CANCEL is applied only to an order still live: a DONE push or the poll may have
        finished it while the cancel was in flight, and this fork's update_with_order_update would set it back."""
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
          - a refusal -- 200012 "The order does not exist" included (CoinW's answer for an order no longer open) -- is
            a WARNING with CoinW's answer, and the order's state is read at once: a refused cancel usually means it
            already filled or was cancelled;
          - a timeout counts toward failing the order only when it is the wait for an exchange id that never came; an
            HTTP timeout on the cancel itself reads the order's state instead (the cancel may have landed).
        """
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if order.exchange_order_id is None:
                self.logger().warning(f"Failed to cancel the order {order.client_order_id} because it does not have an "
                                      f"exchange order id yet")
            else:
                self.logger().warning(f"CoinW did not answer the cancel of {order.client_order_id} in time. Reading its "
                                      f"state now.")
                safe_ensure_future(self._refresh_order(order))
        except CoinwBusinessError as refusal:
            self.logger().warning(f"CoinW refused to cancel {order.client_order_id}: {refusal}. Reading its state now.")
            safe_ensure_future(self._refresh_order(order))
        except IOError as e:
            self.logger().warning(f"CoinW did not answer the cancel of {order.client_order_id} ({e}). Reading its "
                                  f"state now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder) -> None:
        try:
            self._forward_update(order, await self._request_order_status(tracked_order=order), source="refresh")
        except asyncio.CancelledError:
            raise
        except CoinwStatusDeferred:
            return
        except Exception as e:
            self.logger().warning(f"CoinW state read for {order.client_order_id} failed; the next poll reads it again: "
                                  f"{e!r}")

    def _order_state(self, raw_status: Any, filled: Decimal, original: Decimal) -> OrderState:
        try:
            state = CONSTANTS.ORDER_STATE.get(int(raw_status))
        except (TypeError, ValueError):
            state = None
        if state is not None:
            return state
        # Not in the documented enum: derive from the amounts rather than guess a terminal state.
        self._audit_once(f"unknown-status:{raw_status}", status=raw_status, filled=str(filled), original=str(original))
        if original > 0 and filled >= original:
            return OrderState.FILLED
        if filled > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    # ------------------------------------------------------------------ order status (REST)

    async def _update_order_status(self):
        """One state read per in-flight order (returnOrderTrades: the state AND the cumulative fill in one answer), and
        a fill read only for an order whose cumulative fill moved — not the base's two passes (a fills read per order,
        then a status read per order), which would spend getUserTrades' 10 / 2 s on orders that did not trade."""
        for order in list(self.in_flight_orders.values()):
            # Not `is_done`: that is also true for an order whose fills are all booked while its state still says
            # PARTIALLY_FILLED, and only this poll (or a DONE push that may be lost) moves it to FILLED.
            if order.current_state in CONSTANTS.TERMINAL_STATES:
                continue
            try:
                update = await self._request_order_status(tracked_order=order)
                self._forward_update(order, update, source="poll")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._handle_update_error_for_active_order(order, error)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id is None:
            if tracked_order.client_order_id in self._placing:
                raise CoinwStatusDeferred(f"{tracked_order.client_order_id}: its placement awaits CoinW's answer")
            return await self._resolve_unconfirmed(tracked_order)
        try:
            detail = await self._private(CONSTANTS.CMD_ORDER_DETAIL, {"orderNumber": tracked_order.exchange_order_id})
        except CoinwBusinessError as e:
            if e.code == CONSTANTS.CODE_BUSINESS:
                # CoinW's answer for an id it can't read (a made-up id, 2026-10-15) AND for maintenance: never a
                # not-found on its own. The market's open orders and the account's fills decide.
                return await self._settle_vanished(tracked_order, reason=f"returnOrderTrades answered {e.code}")
            raise
        if not isinstance(detail, dict):
            raise IOError(f"Unexpected CoinW answer for order {tracked_order.client_order_id}: {self._raw(detail)}")
        self._gone_since.pop(tracked_order.client_order_id, None)
        raw_status = detail.get("status")
        filled = self._dec(detail.get("success_total"))
        state = self._order_state(raw_status, filled, tracked_order.amount)
        self._audit_once(f"order-detail:{raw_status}", status=raw_status, mapped=str(state), keys=sorted(detail.keys()),
                         success_total=detail.get("success_total"), success_amount=detail.get("success_amount"),
                         total=detail.get("total"), fee=detail.get("fee"))
        # FILLED is the whole order whatever the cumulative field says (a missing success_total must not forward FILLED
        # with nothing booked).
        reported = tracked_order.amount if state is OrderState.FILLED else filled
        if reported > tracked_order.executed_amount_base + self._fill_tolerance(tracked_order):
            await self._book_fills(tracked_order, reported=reported, source="poll")
        if state in CONSTANTS.TERMINAL_STATES:
            self._hold_terminal_if_short(tracked_order, reported, state, source="poll",
                                         cumulative=(filled, self._dec(detail.get("success_amount")),
                                                     self._dec(detail.get("fee"), default="-1")))
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    def _fill_tolerance(self, order: InFlightOrder) -> Decimal:
        """REST cumulative quantities are rounded to the market's lot (returnOrderTrades); the fill rows are exact."""
        rule = self._trading_rules.get(order.trading_pair)
        lot = rule.min_base_amount_increment if rule is not None else Decimal("0")
        return max(CONSTANTS.FILL_AMOUNT_TOLERANCE, lot / 2)

    def _hold_terminal_if_short(self, order: InFlightOrder, reported: Decimal, state: OrderState, source: str,
                                cumulative: Optional[Tuple[Decimal, Decimal, Decimal]] = None) -> None:
        """A terminal state whose fills CoinW's trade rows don't show yet is not forwarded short: the order stays open
        to HMB (CoinwStatusDeferred) for at most TERMINAL_HOLD_MAX_S. Then what the rows still lack is booked from
        CoinW's own cumulative totals `cumulative` = (size, quote, fee; fee -1 when unknown) as ONE fill, with a
        [CW-ALARM]; only without those totals does it go short (also a [CW-ALARM])."""
        if order.executed_amount_base + self._fill_tolerance(order) >= reported:
            self._short_since.pop(order.client_order_id, None)
            return
        if len(self._short_since) > 2000:
            self._short_since.clear()
        held_since = self._short_since.setdefault(order.client_order_id, time.time())
        held_s = round(time.time() - held_since, 1)
        if held_s < CONSTANTS.TERMINAL_HOLD_MAX_S:
            self._audit("terminal-held", client_id=order.client_order_id, state=str(state), source=source,
                        reported=str(reported), booked=str(order.executed_amount_base), held_s=held_s)
            raise CoinwStatusDeferred(f"{order.client_order_id}: {state.name} held for its fill rows")
        self._short_since.pop(order.client_order_id, None)
        if cumulative is not None and self._book_remainder(order, reported, cumulative, source=source, held_s=held_s):
            return
        self._alarm("terminal-forwarded-short", client_id=order.client_order_id, state=str(state), source=source,
                    reported=str(reported), booked=str(order.executed_amount_base), held_s=held_s)

    def _book_remainder(self, order: InFlightOrder, reported: Decimal, cumulative: Tuple[Decimal, Decimal, Decimal],
                        source: str, held_s: float) -> bool:
        """Book what CoinW's rows still lack of the order's fill as ONE fill from its cumulative totals; True if done.

        The fill: the size `reported` - booked, the quote the cumulative quote - booked (else the size x the cumulative
        average, else the order's price), the fee the cumulative fee - booked (in the quote, like every CoinW fill; 0
        when unknown), named <orderNumber>:<cumulative size>. Booked in one go with the terminal state that
        follows (no await between), so no row read can add the same quantity again; later rows find no room."""
        size, quote_total, fee_total = cumulative
        missing = min(reported, order.amount) - order.executed_amount_base
        if missing <= self._fill_tolerance(order) or order.exchange_order_id is None:
            return False
        booked_quote = sum((f.fill_quote_amount for f in order.order_fills.values()), Decimal("0"))
        token = self._fee_asset(order)
        booked_fee = sum((fee.amount for f in order.order_fills.values() for fee in f.fee.flat_fees
                          if fee.token == token), Decimal("0"))
        if quote_total > booked_quote and size > 0:
            quote_amount = quote_total - booked_quote
        elif size > 0 and quote_total > 0:
            quote_amount = missing * quote_total / size
        else:
            quote_amount = missing * order.price
        fee = max(fee_total - booked_fee, Decimal("0")) if fee_total >= 0 else Decimal("0")
        update = TradeUpdate(
            trade_id=f"{order.exchange_order_id}:{self._format_decimal(min(reported, order.amount))}",
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
        self._alarm("fill-from-cumulative", client_id=order.client_order_id, order_id=order.exchange_order_id,
                    source=source, held_s=held_s, booked_from_rows=str(order.executed_amount_base),
                    added=str(missing), quote=str(quote_amount), fee=str(fee), fee_known=fee_total >= 0)
        self._note_fill_booked(order, update)
        self._order_tracker.process_trade_update(update)
        return True

    async def _resolve_unconfirmed(self, order: InFlightOrder) -> OrderUpdate:
        """An order whose placement never answered and whose push never came: looked up like the placement guard. Found
        -> it gets its id; not found once it is NOT_FOUND_MIN_AGE_S old -> a not-found strike (the tracker fails it after
        its limit). Never a blind FAILED."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        created = order.creation_timestamp or self._now()
        found = await self._find_by_client_id(order.client_order_id, symbol, created)
        if found is None:
            found, _ = await self._find_open_order_like(order.client_order_id, order.trading_pair, symbol,
                                                        order.trade_type, order.amount, order.price, created)
        if found is not None:
            order.update_exchange_order_id(found)
            self._alarm("unconfirmed-order-found", client_id=order.client_order_id, order_id=found)
            raise CoinwStatusDeferred(f"{order.client_order_id}: found as {found}; read next poll")
        if self._now() - created >= CONSTANTS.NOT_FOUND_MIN_AGE_S:
            await self._strike_not_found(order.client_order_id)
        raise CoinwStatusDeferred(f"{order.client_order_id}: not found on CoinW yet")

    async def _settle_vanished(self, order: InFlightOrder, reason: str) -> OrderUpdate:
        """CoinW can't read the order's state (-200: also its maintenance answer). Open on the market -> it lives (a
        blip: deferred). Not open -> its fills decide: all of its size filled -> FILLED. Otherwise CoinW's order history
        (getBatchHistoryOrders) decides: a row gives the state and the cumulative fill (the rest booked from it, like a
        held terminal state); -110 (no row YET: the history lists a closed order minutes later, see
        NOT_FOUND_MIN_AGE_S), once the order has been gone for NOT_FOUND_MIN_AGE_S on two reads: CANCELED (with the
        fills it has) if we cancelled it, else a not-found strike with a [CW-ALARM] (the tracker fails it after its
        limit) -- never a silent settle of an order nobody cancelled. Any other answer of the history (maintenance)
        decides nothing. The DONE push normally settles every order before this ever runs, and returnOrderTrades still
        reads a cancelled order minutes later (live 2026-10-16): this is for its -200, maintenance."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        rows = await self._private(CONSTANTS.CMD_OPEN_ORDERS, {"currencyPair": symbol})
        if not isinstance(rows, list):
            raise IOError(f"CoinW open orders unreadable for {symbol}: {self._raw(rows)[:200]}")
        if any(isinstance(r, dict) and str(r.get("orderNumber")) == str(order.exchange_order_id) for r in rows):
            self._gone_since.pop(order.client_order_id, None)
            raise CoinwStatusDeferred(f"{order.client_order_id}: {reason}, but it is open on CoinW")
        await self._book_fills(order, reported=order.amount, source="vanished", quiet_cap=True)
        if order.executed_amount_base + self._fill_tolerance(order) >= order.amount:
            self._gone_since.pop(order.client_order_id, None)
            return self._order_update(order, OrderState.FILLED)
        try:
            history = await self._private(CONSTANTS.CMD_BATCH_HISTORY, {"orderIds": str(order.exchange_order_id)})
        except CoinwBusinessError as e:
            if str(e.code) != CONSTANTS.CODE_BATCH_NOT_FOUND:
                raise
            history = None
        row = next((r for r in history if isinstance(r, dict)
                    and str(r.get("orderId")) == str(order.exchange_order_id)), None) if isinstance(history, list) else None
        if history is not None and row is None:
            raise IOError(f"CoinW order history answered without {order.exchange_order_id}: {self._raw(history)[:200]}")
        if row is not None:
            return self._settle_from_history(order, row, reason)
        first_gone = self._gone_since.setdefault(order.client_order_id, time.monotonic())
        if time.monotonic() - first_gone < CONSTANTS.NOT_FOUND_MIN_AGE_S:
            raise CoinwStatusDeferred(f"{order.client_order_id}: {reason}, not open; confirming before settling")
        cancel_sent = order.client_order_id in self._cancel_sent
        self._alarm("order-gone-without-state", client_id=order.client_order_id, order_id=order.exchange_order_id,
                    reason=reason, filled=str(order.executed_amount_base), amount=str(order.amount),
                    cancel_sent=cancel_sent, settled="CANCELED" if cancel_sent else "not-found strike")
        if cancel_sent:
            self._gone_since.pop(order.client_order_id, None)
            return self._order_update(order, OrderState.CANCELED)
        await self._strike_not_found(order.client_order_id)
        raise CoinwStatusDeferred(f"{order.client_order_id}: gone from CoinW without a state or a cancel of ours")

    def _settle_from_history(self, order: InFlightOrder, row: Dict[str, Any], reason: str) -> OrderUpdate:
        """The order's row in CoinW's history (getBatchHistoryOrders: CLOSED orders; dealSize, dealFunds, fee, status).
        Its `status` is not returnOrderTrades' enum (live 2026-10-16: a cancelled order with no fill read 2 there, 4 on
        returnOrderTrades), so the state comes from the amounts: a full fill (or status 3, "completely filled") ->
        FILLED; anything short -> CANCELED (the order is in the history and not open). Status 1 ("unfilled": never
        seen in the history) -> deferred. Settled once the fill rows (or, after the hold, the row's own cumulative
        totals) cover what it says filled."""
        deal = self._dec(row.get("dealSize"))
        raw_status = row.get("status")
        self._audit_once("order-history-row", keys=sorted(row.keys()), sample=self._raw(row))
        if str(raw_status) == "1":
            self._gone_since.pop(order.client_order_id, None)
            raise CoinwStatusDeferred(f"{order.client_order_id}: {reason}; its history says status 1 (unfilled)")
        filled = str(raw_status) == "3" or deal + self._fill_tolerance(order) >= order.amount
        state = OrderState.FILLED if filled else OrderState.CANCELED
        reported = order.amount if state is OrderState.FILLED else deal
        self._hold_terminal_if_short(order, reported, state, source="history",
                                     cumulative=(deal, self._dec(row.get("dealFunds")),
                                                 self._dec(row.get("fee"), default="-1")))
        self._gone_since.pop(order.client_order_id, None)
        return self._order_update(order, state)

    def _order_update(self, order: InFlightOrder, state: OrderState) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id) if order.exchange_order_id is not None else None,
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    # ------------------------------------------------------------------ terminal updates wait for the balance

    def _forward_update(self, order: InFlightOrder, update: OrderUpdate, source: str) -> None:
        """An order update on its way to the tracker. A terminal one is held until the base asset's balance shows the
        order's own fills, by ONE waiter per order: the DONE push and the status poll can both bring it."""
        if update.new_state not in CONSTANTS.TERMINAL_STATES or self._balance_shows_fills(order):
            if update.new_state in CONSTANTS.TERMINAL_STATES:
                self._hold_baseline.pop(order.client_order_id, None)
            self._order_tracker.process_order_update(update)
            return
        if order.client_order_id in self._balance_waiting:
            return  # its waiter reports it
        self._balance_waiting.add(order.client_order_id)
        safe_ensure_future(self._report_when_balance_shows_fills(order, update, source))

    async def _report_when_balance_shows_fills(self, order: InFlightOrder, update: OrderUpdate, source: str) -> None:
        """Waits FILL_BALANCE_PUSH_WAIT_S for the balance push, then reads balances over REST (up to
        BALANCE_MISSING_READS times: a sold-out coin leaves the answer and is dropped on its second miss), then reports
        the update anyway: shown -> audit fill-balance-wait; still short -> [CW-ALARM] fill-balance-late."""
        started = time.monotonic()
        via = "push"
        try:
            while (not self._balance_shows_fills(order)
                   and time.monotonic() - started < CONSTANTS.FILL_BALANCE_PUSH_WAIT_S):
                await asyncio.sleep(0.05)
            for _ in range(CONSTANTS.BALANCE_MISSING_READS):
                if self._balance_shows_fills(order):
                    break
                via = "rest"
                try:
                    await self._update_balances()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().warning(f"CoinW balance read for {order.client_order_id} failed: {self._scrub(e)}")
                    break
            waited_s = round(time.monotonic() - started, 3)
            if self._balance_shows_fills(order):
                self._audit("fill-balance-wait", client_id=order.client_order_id, source=source, via=via,
                            waited_s=waited_s)
            else:
                self._alarm("fill-balance-late", client_id=order.client_order_id, source=source, waited_s=waited_s,
                            asset=order.base_asset, total=str(self._account_balances.get(order.base_asset)))
            if order.client_order_id in self._order_tracker.all_updatable_orders:
                self._order_tracker.process_order_update(update)
        finally:
            self._balance_waiting.discard(order.client_order_id)
            self._hold_baseline.pop(order.client_order_id, None)

    def _base_delta(self, order: InFlightOrder, fills: List[TradeUpdate]) -> Decimal:
        """The fills' net change of the base asset's total: + on a buy, - on a sell (CoinW charges every fee in the quote,
        so no fee comes off the base; a base fee, were one booked, is taken off)."""
        delta = Decimal("0")
        for fill in fills:
            base_fee = sum((fee.amount for fee in fill.fee.flat_fees if fee.token == order.base_asset), Decimal("0"))
            delta += (fill.fill_base_amount if order.trade_type is TradeType.BUY else -fill.fill_base_amount) - base_fee
        return delta

    def _note_fill_booked(self, order: InFlightOrder, fill: TradeUpdate) -> None:
        """Every fill this process books, summed per asset, so a held terminal update can tell the order's own balance
        change from other orders' (_balance_shows_fills)."""
        asset = order.base_asset
        self._booked_base[asset] = self._booked_base.get(asset, Decimal("0")) + self._base_delta(order, [fill])

    def _balance_shows_fills(self, order: InFlightOrder) -> bool:
        """True once the base asset's total balance carries the order's own fills (Hotcoin's rule, 2026-10-06). Since the
        order was sent the balance moved by its own fills and by other orders' fills booked meanwhile: the others' are
        taken off, and what is left must reach 99% of the order's own (fee rounding). So another order's fill can't
        stand in for this one's, and a fill that landed long ago passes at once however much the asset traded since. An
        order without fills, or with no baseline (sent before this process), needs no wait."""
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

    # ------------------------------------------------------------------ fills (REST rows)

    async def _fill_rows(self, symbol: str, since_ms: int) -> List[Dict[str, Any]]:
        """The account's fills on one market since `since_ms` (getUserTrades: one row per fill). One read per market in
        flight at a time, shared for USER_TRADES_MIN_INTERVAL by every order on that market; paged by time while a page
        comes back full."""
        lock = self._fills_locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            cached = self._fills_cache.get(symbol)
            if (cached is not None and time.monotonic() - cached[0] <= CONSTANTS.USER_TRADES_MIN_INTERVAL
                    and cached[1] <= since_ms):
                return cached[2]
            params: Dict[str, Any] = {"symbol": symbol, "startAt": since_ms, "limit": CONSTANTS.USER_TRADES_PAGE}
            rows: Dict[str, Dict[str, Any]] = {}
            for _ in range(CONSTANTS.USER_TRADES_MAX_PAGES):
                data = await self._private(CONSTANTS.CMD_USER_TRADES, params)
                page = data.get("list") if isinstance(data, dict) else None
                if not isinstance(page, list):
                    raise IOError(f"Unexpected CoinW getUserTrades answer for {symbol}: {self._raw(data)[:200]}")
                page = [row for row in page if isinstance(row, dict) and row.get("tradeId") not in (None, "")]
                times = [int(self._dec(row.get("time"))) for row in page]
                if page:
                    self._audit_once("user-trades-row", keys=sorted(page[0].keys()), first=self._raw(page[0]),
                                     page_keys=sorted(data.keys()), rows=len(page),
                                     order="ascending" if times[0] <= times[-1] else "descending")
                fresh = [row for row in page if str(row["tradeId"]) not in rows]
                for row in page:
                    rows[str(row["tradeId"])] = row
                if len(page) < CONSTANTS.USER_TRADES_PAGE or not fresh:
                    break
                # A full page: more rows lie beyond it. The next page is asked by time, past the page's far end.
                if times[0] >= times[-1]:
                    params = {**params, "endAt": min(times)}         # newest first: the older rows next
                else:
                    params = {**params, "startAt": max(times)}       # oldest first: the newer rows next
            else:
                self._alarm("user-trades-paging-stopped", symbol=symbol, since_ms=since_ms, rows=len(rows),
                            pages=CONSTANTS.USER_TRADES_MAX_PAGES)
            result = list(rows.values())
            self._fills_cache[symbol] = (time.monotonic(), since_ms, result)
            return result

    def _trade_update(self, order: InFlightOrder, row: Dict[str, Any]) -> TradeUpdate:
        quantity = self._dec(row.get("size"))
        price = self._dec(row.get("price"))
        fee = self._dec(row.get("fee"))
        # The row names no fee asset. CoinW charges the QUOTE on both sides (live 2026-10-16: a BTC_USDT maker buy's
        # 0.0494 fee came off USDT, the BTC credited in full; the sells' fees are USDT too) -- not the received asset.
        token = self._fee_asset(order)
        self._audit_once(f"fee-asset:{order.trade_type.name}", fee=str(fee), quantity=str(quantity), price=str(price),
                         per_quantity=str(fee / quantity) if quantity > 0 else None,
                         per_value=str(fee / (quantity * price)) if quantity > 0 and price > 0 else None, token=token)
        return TradeUpdate(
            trade_id=str(row.get("tradeId")),
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=TradeFeeBase.new_spot_fee(fee_schema=self.trade_fee_schema(), trade_type=order.trade_type,
                                          flat_fees=[TokenAmount(amount=fee, token=token)]),
            fill_base_amount=quantity,
            fill_quote_amount=quantity * price,
            fill_price=price,
            fill_timestamp=int(self._dec(row.get("time"), default=str(int(self._now() * 1e3)))) * 1e-3,
            # CoinW's row doesn't say; resting legs do fill as makers. Nothing downstream reads it; the fee is CoinW's.
            is_taker=True,
        )

    async def _fill_updates(self, order: InFlightOrder) -> List[TradeUpdate]:
        """The order's fills not booked yet, capped at its size (Hummingbot de-duplicates by trade id only and never
        caps an order's executed amount)."""
        if order.exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        # From the order's creation, or its newest booked fill (a long-resting order on a busy market would otherwise
        # re-read every fill of the market since it was placed), minus the clock margin.
        start = max([order.creation_timestamp or self._now()]
                    + [f.fill_timestamp for f in order.order_fills.values() if f.fill_timestamp])
        since_ms = int((start - CONSTANTS.FILL_ROWS_LOOKBACK_S) * 1e3)
        rows = await self._fill_rows(symbol, since_ms)
        known = order.order_fills
        room = order.amount - order.executed_amount_base
        updates: List[TradeUpdate] = []
        for row in sorted((r for r in rows if str(r.get("orderId")) == str(order.exchange_order_id)),
                          key=lambda r: int(self._dec(r.get("time")))):
            if str(row.get("tradeId")) in known:
                continue
            update = self._trade_update(order, row)
            if update.fill_base_amount <= 0:
                continue
            if update.fill_base_amount > room + CONSTANTS.FILL_AMOUNT_TOLERANCE:
                self._alarm("fill-over-amount", client_id=order.client_order_id, trade_id=update.trade_id,
                            fill=str(update.fill_base_amount), room=str(room), amount=str(order.amount))
                continue
            updates.append(update)
            room -= update.fill_base_amount
        return updates

    async def _book_fills(self, order: InFlightOrder, reported: Decimal, source: str, quiet_cap: bool = False) -> None:
        """Book the order's new fill rows now. `reported` = what CoinW says the order filled (a push or a REST read):
        rows that stay short of it after the read are audit-logged (CoinW's rows can trail its order state)."""
        for update in await self._fill_updates(order):
            self._note_fill_booked(order, update)
            self._order_tracker.process_trade_update(update)
        if not quiet_cap and order.executed_amount_base + self._fill_tolerance(order) < reported:
            self._audit("fills-behind-state", client_id=order.client_order_id, source=source, reported=str(reported),
                        booked=str(order.executed_amount_base))

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        return await self._fill_updates(order)

    @staticmethod
    def _fee_asset(order: InFlightOrder) -> str:
        """CoinW charges every spot fee in the quote, a buy's too (the partial-fill probe, 2026-10-16)."""
        return order.quote_asset

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
        """CoinW's spot API has no fee-rate route: the connector's DEFAULT_FEES stand (see coinw_utils)."""
        return

    # ------------------------------------------------------------------ balances

    def _hb_asset(self, coin: str) -> Optional[str]:
        """CoinW's coin name -> the name Hummingbot uses (TICKER_ALIASES); None for a coin shadowed by an alias (the
        dead plain TRUMP next to TRUMP(OFFICIAL))."""
        coin = str(coin or "").strip()
        if coin in CONSTANTS.TICKER_ALIASES:
            return CONSTANTS.TICKER_ALIASES[coin]
        if coin in CONSTANTS.TICKER_ALIASES.values():
            return None
        return coin.upper() or None

    def _own_locked(self, asset: str) -> Decimal:
        """What HMB's own open CoinW orders lock of `asset`: a buy its unfilled size x price in the quote, a sell its
        unfilled size in the base (a buy's fee is CoinW's to hold too, ~0.1% more: left out, it only shows as CoinW's
        `locked` exceeding ours, which takes nothing off). An order this
        process gave up (failed, or not found) locks nothing from that moment -- the tracker applies its FAILED state a
        moment later."""
        locked = Decimal("0")
        for order in self.in_flight_orders.values():
            if order.is_done or order.is_failure or order.is_cancelled or order.client_order_id in self._given_up:
                continue
            rest = max(order.amount - order.executed_amount_base, Decimal("0"))
            if order.trade_type is TradeType.BUY:
                if order.quote_asset == asset:
                    locked += rest * order.price
            elif order.base_asset == asset:
                locked += rest
        return locked

    def _apply_balance(self, asset: str, available: Decimal, locked: Decimal) -> None:
        """CoinW's figures for `asset` -> the balances HMB reads. Total = available + locked. Available also loses what
        HMB's own open orders lock beyond `locked`: CoinW pushes nothing for an order's hold, so a REST answer or push
        from before CoinW took an order's hold still counts its coins as free, and a second order sized on them would
        be refused (the order/balance review's gap 2, 2026-10-16). Other processes' orders only raise `locked`."""
        self._reported_balances[asset] = (available, locked)
        self._account_balances[asset] = available + locked
        shortfall = self._own_locked(asset) - locked
        self._account_available_balances[asset] = (max(available - shortfall, Decimal("0"))
                                                   if shortfall > CONSTANTS.OWN_LOCK_TOLERANCE else available)

    def _reapply_balances(self, *assets: str) -> None:
        """HMB's own orders changed (one sent, one failed): CoinW's last figures re-applied, so a new order's hold shows
        at once and a refused one's coins come back -- no push or read is needed for either."""
        for asset in assets:
            reported = self._reported_balances.get(asset)
            if reported is not None:
                self._apply_balance(asset, *reported)

    @staticmethod
    def _spend_asset(order: InFlightOrder) -> str:
        return order.quote_asset if order.trade_type is TradeType.BUY else order.base_asset

    async def _update_balances(self) -> None:
        """returnCompleteBalances ({coin: {available, onOrders}}, held coins only). An asset whose push arrived after
        this request was sent keeps the push (runbook §1.5; Hotcoin's rule). An EMPTY answer is believed only after
        EMPTY_BALANCE_READS of them over EMPTY_BALANCE_SECONDS: one stray {} would zero every balance, and the hold-band
        would buy the "missing" positions back."""
        requested_at = time.monotonic()
        data = await self._private(CONSTANTS.CMD_BALANCES)
        if not isinstance(data, dict):
            raise IOError(f"Unexpected CoinW balances answer: {self._raw(data)[:200]}")
        if not data and self._account_balances:
            if self._empty_balance_reads == 0:
                self._empty_balance_since = time.monotonic()
            self._empty_balance_reads += 1
            if (self._empty_balance_reads < CONSTANTS.EMPTY_BALANCE_READS
                    or time.monotonic() - self._empty_balance_since < CONSTANTS.EMPTY_BALANCE_SECONDS):
                self._alarm("balances-empty-answer", reads=self._empty_balance_reads,
                            held_assets=len(self._account_balances), action="kept the last balances")
                return
        else:
            self._empty_balance_reads = 0
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for coin, row in data.items():
            asset = self._hb_asset(coin)
            if asset is None or not isinstance(row, dict):
                continue
            remote_asset_names.add(asset)
            if self._balance_pushed_at.get(asset, 0.0) > requested_at:
                continue
            self._apply_balance(asset, self._dec(row.get("available")), self._dec(row.get("onOrders")))
        for asset_name in remote_asset_names:
            self._missing_balance_reads.pop(asset_name, None)
        for asset_name in local_asset_names.difference(remote_asset_names):
            if self._balance_pushed_at.get(asset_name, 0.0) > requested_at:
                continue
            # Held coins only: a missing coin holds nothing — once it has been missing BALANCE_MISSING_READS times in a
            # row (one partial answer must not zero a held position: the hold-band would buy it back).
            missing = self._missing_balance_reads[asset_name] = self._missing_balance_reads.get(asset_name, 0) + 1
            if missing < CONSTANTS.BALANCE_MISSING_READS:
                continue
            self._missing_balance_reads.pop(asset_name, None)
            self._account_available_balances.pop(asset_name, None)
            self._account_balances.pop(asset_name, None)
            self._reported_balances.pop(asset_name, None)

    # ------------------------------------------------------------------ symbols / rules

    def _hb_pair(self, info: Dict[str, Any]) -> Optional[str]:
        base = str(info.get("currencyBase") or "").strip()
        quote = str(info.get("currencyQuote") or "").strip().upper()
        hb_base = CONSTANTS.TICKER_ALIASES.get(base)
        if hb_base is None:
            if base in CONSTANTS.TICKER_ALIASES.values() or "-" in base or not base or not quote:
                return None
            hb_base = base.upper()
        return combine_to_hb_trading_pair(base=hb_base, quote=quote)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any) -> None:
        if not isinstance(exchange_info, list) or not exchange_info:
            self.logger().warning(f"CoinW returnSymbol answered no market list; the symbol map is kept: "
                                  f"{self._raw(exchange_info)[:200]}")
            return
        mapping = bidict()
        skipped = []
        for info in exchange_info:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            pair = self._hb_pair(info)
            if pair is None or pair in mapping.inverse:
                skipped.append(str(info.get("currencyPair")))
                continue
            mapping[str(info["currencyPair"])] = pair
        if skipped:
            # Names a Hummingbot pair can't carry (a hyphen in the base: CHUTES-SN64 …) or shadowed by an alias (the
            # dead plain TRUMP_USDT next to TRUMP(OFFICIAL)_USDT): left out of the map, as P1 leaves them out.
            self._audit_once("symbols-not-mapped", count=len(skipped), symbols=sorted(skipped)[:20])
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """Runtime add of a pair not in the startup map. The base builds f"{base}{quote}", which CoinW does not
        recognise; its form is BASE_QUOTE, with CoinW's own name for an aliased base."""
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        coinw_base = next((k for k, v in CONSTANTS.TICKER_ALIASES.items() if v == base.upper()), base.upper())
        exchange_symbol = f"{coinw_base}_{quote.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(f"CoinW {trading_pair} was not among the markets returnSymbol listed at startup; mapped "
                              f"to {exchange_symbol}. CoinW's answers will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        """returnSymbol: pricePrecision / countPrecision (decimals), minBuyCount (may read 0E-20), minBuyAmount (the
        minimum notional: 1 or 5 USDT on 2026-10-15), maxBuyCount. A 2xx that isn't the market list raises, so the base
        keeps the rules it has (it clears them only after this returns)."""
        if not isinstance(exchange_info_dict, list) or not exchange_info_dict:
            raise IOError(f"CoinW returnSymbol answered no market list: {self._raw(exchange_info_dict)[:200]}")
        rules: List[TradingRule] = []
        for info in exchange_info_dict:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=str(info["currencyPair"]))
            except KeyError:
                continue   # not in the map (see _initialize_trading_pair_symbols_from_exchange_info)
            try:
                price_increment = Decimal(1).scaleb(-int(info["pricePrecision"]))
                amount_increment = Decimal(1).scaleb(-int(info["countPrecision"]))
                kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=max(self._dec(info.get("minBuyCount")), amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_notional_size=self._dec(info.get("minBuyAmount")),
                )
                if self._dec(info.get("maxBuyCount")) > 0:
                    kwargs["max_order_size"] = self._dec(info.get("maxBuyCount"))
                rules.append(TradingRule(**kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the CoinW trading rule {info.get('currencyPair')}. Skipping.")
        return rules

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        One returnTicker for every market, not one call per pair: the order book tracker re-reads the REST price of
        every quiet book every 5 s (XT's 1,842 throttler warnings in 28 h). Every requested pair gets a value, NaN when
        CoinW has none (the tracker re-asks a pair it got nothing for at once).
        """
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        ticker = await self._ticker()
        for trading_pair in trading_pairs:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            except KeyError:
                continue
            row = ticker.get(symbol)
            price = self._dec(row.get("last")) if isinstance(row, dict) else Decimal("0")
            if price > 0:
                prices[trading_pair] = float(price)
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]

    # ------------------------------------------------------------------ user stream

    def on_private_stream_connected(self) -> None:
        """The pushes only report changes: what moved while no socket was up is read over REST at once."""
        self._poll_notifier.set()

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                kind = event_message.get("type")
                data = event_message.get("data")
                if isinstance(data, str):
                    data = json.loads(data, parse_float=Decimal)
                for item in data if isinstance(data, list) else [data]:
                    if not isinstance(item, dict):
                        continue
                    if kind == CONSTANTS.WS_ORDER_CHANNEL:
                        self._process_order_push(item)
                    elif kind == CONSTANTS.WS_ASSETS_CHANNEL:
                        self._process_assets_push(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the CoinW user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        order = tracker.all_fillable_orders.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id:
            order = tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        return order

    def _push_age(self, data: Dict[str, Any]) -> Optional[float]:
        try:
            return round(time.time() - int(data["time"]) * 1e-3, 3)
        except (KeyError, TypeError, ValueError):
            return None

    def _process_order_push(self, data: Dict[str, Any]) -> None:
        """
        The `order` push: order_id, client_id, product_id, side, order_type, size, remaining_size, price, dealFunds,
        dealAvgPrice, fee, time, `type` RECEIVED | DONE and on DONE a `reason` FILLED | CANCELLED | REJECTED. The client id
        rides on it, so a push that beats the placement answer is matched by it.
        """
        self._audit_once(f"order-push-key-set:{data.get('type')}", keys=sorted(data.keys()), sample=self._raw(data))
        client_order_id = str(data.get("client_id") or "")
        exchange_order_id = str(data["order_id"]) if data.get("order_id") not in (None, "") else None
        for seen in (exchange_order_id, client_order_id):
            if seen:
                self._pushed_order_ids[seen] = None
        if len(self._pushed_order_ids) > 4000:
            for seen in list(self._pushed_order_ids)[:2000]:
                del self._pushed_order_ids[seen]
        event = str(data.get("type") or "").upper()
        if event in (CONSTANTS.PUSH_RECEIVED, CONSTANTS.PUSH_DONE):
            self._schedule_balance_refresh()
        reason = str(data.get("reason") or "").upper()
        size, remaining = self._dec(data.get("size")), self._dec(data.get("remaining_size"))
        has_amounts = data.get("size") not in (None, "") and data.get("remaining_size") not in (None, "")
        pushed_fill = size - remaining if has_amounts and size > 0 else Decimal("0")
        order = self._locate_order(client_order_id, exchange_order_id)
        if order is None:
            self._on_untracked_push(client_order_id, exchange_order_id, event, reason, pushed_fill, data)
            return
        self._audit("order-push", client_id=order.client_order_id, order_id=exchange_order_id, event=event,
                    reason=reason, size=data.get("size"), remaining=data.get("remaining_size"),
                    deal_funds=data.get("dealFunds"), fee=data.get("fee"), age_s=self._push_age(data))
        if exchange_order_id is not None and order.client_order_id == client_order_id:
            if order.exchange_order_id is None:
                # The push beat the placement answer: give the order its id before a fill is named after it.
                order.update_exchange_order_id(exchange_order_id)
            elif str(order.exchange_order_id) != exchange_order_id:
                # CoinW's own push ties this client id to this orderNumber: the id the order holds (a lookup's match by
                # side, price and size) was another order. The push's is taken.
                self._alarm("push-order-id-mismatch", client_id=order.client_order_id,
                            held_id=str(order.exchange_order_id), push_id=exchange_order_id, action="took the push's")
                order.update_exchange_order_id(exchange_order_id)
        if order.is_failure:
            if event == CONSTANTS.PUSH_RECEIVED and exchange_order_id is not None:
                self._cancel_failed_but_open(order, exchange_order_id)
            elif event == CONSTANTS.PUSH_DONE and (pushed_fill > 0 or reason == "FILLED"):
                # HMB failed it, yet CoinW filled it: a position nobody books. Said, always.
                self._alarm("failed-order-filled", client_id=order.client_order_id, order_id=exchange_order_id,
                            reason=reason, filled=str(pushed_fill), deal_funds=data.get("dealFunds"),
                            fee=data.get("fee"))
            return
        if order.client_order_id not in self._order_tracker.all_updatable_orders:
            return  # already final: a late push never sets a state
        if event == CONSTANTS.PUSH_RECEIVED:
            if order.current_state is OrderState.PENDING_CREATE:
                self._order_tracker.process_order_update(self._order_update(order, OrderState.OPEN))
            return
        if event != CONSTANTS.PUSH_DONE:
            self._audit_once(f"order-push-event:{event}", sample=self._raw(data))
            return
        state = CONSTANTS.PUSH_REASON_STATE.get(reason)
        if state is None:
            self._audit_once(f"order-push-reason:{reason}", sample=self._raw(data))
            state = OrderState.FILLED if has_amounts and size > 0 and remaining <= 0 else OrderState.CANCELED
        if state is OrderState.FILLED:
            filled = order.amount                     # the whole order, whatever the push's amounts say
        elif has_amounts:
            filled = pushed_fill
        else:
            # A cancel that doesn't say how much filled: the status poll (state AND cumulative fill) settles it.
            self._audit_once("order-push-done-no-amounts", sample=self._raw(data))
            self._poll_notifier.set()
            return
        if order.client_order_id not in self._settling:
            self._settling.add(order.client_order_id)
            safe_ensure_future(self._settle_done(order, state, filled))

    def _on_untracked_push(self, client_order_id: str, exchange_order_id: Optional[str], event: str, reason: str,
                           pushed_fill: Decimal, data: Dict[str, Any]) -> None:
        """A push for an order nothing tracks. One this process gave up (failed, or struck as not found and purged)
        is ours for sure: open -> cancelled; filled -> said. Any other HBOT order: open -> said (it may be another
        process's), filled -> said. Never silent when a fill nobody books is involved."""
        self._audit("order-push-unmatched", client_id=client_order_id or None, order_id=exchange_order_id,
                    event=event, reason=reason, age_s=self._push_age(data))
        ours = client_order_id in self._given_up
        if not ours and not client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX):
            return
        if event == CONSTANTS.PUSH_RECEIVED and exchange_order_id is not None and exchange_order_id not in self._orphan_ids:
            self._orphan_ids[exchange_order_id] = None
            if ours:
                self._alarm("given-up-order-alive", client_id=client_order_id, order_id=exchange_order_id,
                            action="cancelling")
                safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id))
            else:
                # One of ours, open on CoinW, that nothing tracks: its fills would never be booked. Said, not
                # cancelled -- it may be another process's.
                self._alarm("untracked-order-open", client_id=client_order_id, order_id=exchange_order_id)
        elif event == CONSTANTS.PUSH_DONE and (pushed_fill > 0 or reason == "FILLED"):
            self._alarm("untracked-order-filled", client_id=client_order_id, order_id=exchange_order_id,
                        given_up_here=ours, reason=reason, filled=str(pushed_fill), deal_funds=data.get("dealFunds"),
                        fee=data.get("fee"))

    async def _settle_done(self, order: InFlightOrder, state: OrderState, filled: Decimal) -> None:
        """A DONE push: the fills first (getUserTrades), then the terminal state; still short after the read, the
        status poll (which books fills before it reports a state) settles it, never a short terminal update here."""
        try:
            if filled > order.executed_amount_base + self._fill_tolerance(order):
                for attempt in range(3):
                    try:
                        await self._book_fills(order, reported=filled, source="done-push", quiet_cap=attempt < 2)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.logger().warning(f"CoinW fills read for {order.client_order_id} failed: {e!r}")
                    if order.executed_amount_base + self._fill_tolerance(order) >= filled:
                        break
                    await asyncio.sleep(0.3 * (attempt + 1))
            if order.executed_amount_base + self._fill_tolerance(order) < filled:
                self._alarm("fill-backfill-short", client_id=order.client_order_id, reported=str(filled),
                            booked=str(order.executed_amount_base), action="the status poll settles it")
                self._poll_notifier.set()
                return
            if order.client_order_id not in self._order_tracker.all_updatable_orders:
                return
            self._forward_update(order, self._order_update(order, state), source="done-push")
        finally:
            self._settling.discard(order.client_order_id)

    def _cancel_failed_but_open(self, order: InFlightOrder, exchange_order_id: str) -> None:
        """HMB gave the order up (FAILED), yet CoinW reports it accepted: nothing would track it, so it is cancelled
        now, with a [CW-ALARM]."""
        if exchange_order_id in self._orphan_ids:
            return
        self._orphan_ids[exchange_order_id] = None
        if len(self._orphan_ids) > 2000:
            for seen in list(self._orphan_ids)[:1000]:
                del self._orphan_ids[seen]
        self._alarm("failed-order-alive", client_id=order.client_order_id, order_id=exchange_order_id,
                    action="cancelling")
        safe_ensure_future(self._cancel_by_exchange_id(exchange_order_id))

    async def _cancel_by_exchange_id(self, exchange_order_id: str) -> None:
        try:
            data = await self._private(CONSTANTS.CMD_CANCEL, {"orderNumber": exchange_order_id})
            self._audit("cancel-untracked", order_id=exchange_order_id, response=self._raw(data))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinW cancel of the untracked order {exchange_order_id} failed: {e!r}")

    def _process_assets_push(self, data: Dict[str, Any]) -> None:
        """The `assets` push: `available` + `hold` of one coin after a change, with a `ledger_id` that orders them (an
        older one is dropped). A change on a coin an open order trades asks for a fills read: a resting order's partial
        fill gets no order push."""
        self._audit_once("assets-push-key-set", keys=sorted(data.keys()), sample=self._raw(data))
        if str(data.get("account") or "spot").lower() != "spot":
            return
        asset = self._hb_asset(data.get("currency"))
        if asset is None:
            return
        try:
            ledger = int(data.get("ledger_id") or 0)
        except (TypeError, ValueError):
            ledger = 0
        if ledger and ledger < self._ledger_ids.get(asset, 0):
            self._audit("assets-push-out-of-order", asset=asset, ledger=ledger, last=self._ledger_ids[asset])
            return
        if ledger:
            self._ledger_ids[asset] = ledger
        self._apply_balance(asset, self._dec(data.get("available")), self._dec(data.get("hold")))
        self._balance_pushed_at[asset] = time.monotonic()
        if CONSTANTS.LIVE_AUDIT_LOGGING and self._balance_pushes_audited < CONSTANTS.BALANCE_AUDIT_PUSHES:
            self._balance_pushes_audited += 1
            safe_ensure_future(self._audit_balance_push(asset, data))
        # The BASE coin only: every fill moves it (a buy's arrives, a sell's hold leaves), while a quote push (USDT)
        # can come from any market's fill. (No push comes for an order's hold: placement and cancel pushed nothing in
        # the placement probe, 2026-10-16; the docs: trading activity only.) Not while rate-limited: the poll books
        # them then.
        if not self._rate_limited() and any(asset == o.base_asset for o in self.in_flight_orders.values()):
            self._fills_check_pending.add(asset)
            if self._fills_check_task is None or self._fills_check_task.done():
                self._fills_check_task = safe_ensure_future(self._fills_check_after_push())

    def _schedule_balance_refresh(self) -> None:
        """An order push (a hold placed or released) asks for one REST balance read: CoinW pushes no balance for a hold
        (BALANCE_REFRESH_DEBOUNCE_S). Not while rate-limited: the slowed poll reads balances then."""
        if self._rate_limited():
            return
        if self._balance_refresh_task is not None and not self._balance_refresh_task.done():
            self._balance_refresh_again = True
            return
        self._balance_refresh_task = safe_ensure_future(self._refresh_balances_after_push())

    async def _refresh_balances_after_push(self) -> None:
        try:
            while True:
                await asyncio.sleep(CONSTANTS.BALANCE_REFRESH_DEBOUNCE_S)
                self._balance_refresh_again = False      # pushes during the wait are covered by this read
                await self._update_balances()
                if not self._balance_refresh_again:      # one landed while the read was in flight: read again
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"CoinW balance read after an order push failed; the poll reads them: {self._scrub(e)}")

    async def _fills_check_after_push(self) -> None:
        """Debounced: the open orders on coins whose balance moved get their state and fills read now; a push that
        lands while this runs is read by the next check."""
        await asyncio.sleep(0.2)
        while self._fills_check_pending:
            assets, self._fills_check_pending = self._fills_check_pending, set()
            for order in list(self.in_flight_orders.values()):
                if order.current_state in CONSTANTS.TERMINAL_STATES or order.base_asset not in assets:
                    continue
                await self._refresh_order(order)

    async def _audit_balance_push(self, asset: str, push: Dict[str, Any]) -> None:
        """The first few balance pushes next to CoinW's REST balance (balance-push-vs-rest)."""
        try:
            data = await self._private(CONSTANTS.CMD_BALANCES)
            coin = next((c for c in (data or {}) if self._hb_asset(c) == asset), None)
            rest = (data or {}).get(coin, "absent") if coin else "absent"
        except Exception as e:
            rest = f"REST error {e!r}"
        self._audit("balance-push-vs-rest", asset=asset, push_available=push.get("available"),
                    push_hold=push.get("hold"), ledger=push.get("ledger_id"), rest=rest)
