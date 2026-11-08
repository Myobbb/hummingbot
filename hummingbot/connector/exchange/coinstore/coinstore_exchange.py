import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinstore import (
    coinstore_constants as CONSTANTS,
    coinstore_utils as utils,
    coinstore_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinstore.coinstore_api_order_book_data_source import (
    CoinstoreAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.coinstore.coinstore_api_user_stream_data_source import (
    CoinstoreAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.coinstore.coinstore_auth import CoinstoreAuth
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


class CoinstoreBusinessError(IOError):
    """A request Coinstore answered with a code other than 0: a definite refusal (the code and message say why), never a
    transport failure. IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, code: Optional[str], msg: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code
        self.msg = msg


class CoinstoreRateLimited(IOError):
    """Refused for rate before the request reached Coinstore's order logic: Cloudflare's HTTP 429 page, or the API's
    own HTTP 200 + code 429. Nothing was executed."""


class CoinstoreStatusDeferred(Exception):
    """The status poll must leave this order alone (its placement awaits its answer, or its terminal state waits for
    its fill rows). Raised instead of returning the order's current state: the base queues process_order_update as a
    task, and a "no change" update can land after a newer one and roll it back (Poloniex, 2026-10-09)."""


class CoinstorePlacementUnknown(IOError):
    """A placement Coinstore may have accepted (no answer, or an ambiguous one) that the lookup could not settle: one of
    its reads failed, so "not there" was never established. The order is NOT failed — it may rest on Coinstore. It stays
    PENDING_CREATE and the status poll keeps looking for it; only the poll's not-found verdict fails it (Bitunix's rule,
    2026-10-07)."""


class _LookupInconclusive(Exception):
    """A placement lookup could not tell: its last read failed."""


class CoinstoreExchange(ExchangePyBase):
    """
    Coinstore spot connector (REST under https://api.coinstore.com/api + the public / private socket).

    Reference: the docs (offline mirror VS_code_projects/MDs/coinstore-api/, English + the Chinese page), Coinstore's own
    Python SDK, live probes of 2026-11-08 from myserver (hmb_local_tools/coinstore/), and the wiki page
    trading/exchanges/coinstore-api. Every Coinstore-specific choice cites one of them; what they could not settle is
    audit-logged ([CS-AUDIT]) rather than guessed.

    Scope: LIMIT GTC orders only, which is all arb_l and the position balancer send.

    Rate budget (constants §Rate limits): the order-status route is free, so the status poll reads every open order by
    its id; balances, fill rows and open-order lists come out of Cloudflare's ~20-per-10-s bucket, which the asset
    manager shares from the same host, so the fill rows of an order are read only once its filled quantity has grown
    past what is booked.
    """

    web_utils = web_utils

    def __init__(
        self,
        coinstore_api_key: str,
        coinstore_secret_key: str,
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
        self._api_key = coinstore_api_key
        self._secret_key = coinstore_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # The symbol list, shared by the symbol map, the rules, the fees and the data source's trading switch.
        self._symbol_list: Tuple[float, Dict[str, Dict[str, Any]]] = (0.0, {})
        self._symbol_list_lock = asyncio.Lock()
        # client order id -> the newest cumulative (filled qty, filled value) the venue reported (status read or push)
        self._venue_cumulative: Dict[str, Tuple[Decimal, Decimal]] = {}
        # client order ids whose placement request is still awaiting its answer: only the placement looks them up
        self._placing: set = set()
        # asset -> monotonic time of its last balance push: an older REST snapshot doesn't overwrite it
        self._balance_pushed_at: Dict[str, float] = {}
        # exchange order ids an orphan alarm (and cancel) was raised for, once each
        self._orphan_ids: Dict[str, None] = {}
        # client order id -> monotonic time its fill rows were last read / first found short of the venue's total
        self._rows_read_at: Dict[str, float] = {}
        self._rows_short_since: Dict[str, float] = {}
        # asset -> net base change of every fill this process booked (_note_fill_booked, _balance_shows_fills)
        self._booked_base: Dict[str, Decimal] = {}
        # client order id -> (base asset total balance, _booked_base of that asset) when the order was sent
        self._hold_baseline: Dict[str, Tuple[Decimal, Decimal]] = {}
        # client order ids whose terminal update is waiting for the balance (one waiter each)
        self._settling: set = set()
        # client order id -> the `timestamp` (ms) its placement sent, unique per order: Coinstore keeps it as the
        # order's own, so it identifies the order once the order is done and its clOrdId reads "0"
        self._sent_at_ms: Dict[str, int] = {}
        self._last_sent_ms = 0
        # Coinstore currency id -> asset, learned from the account pushes and the fill rows (a push's fee names an id)
        self._currency_by_id: Dict[str, str] = {}
        # trade ids booked from a push (bounded): a REST row repeating one proves the ids match (fill-ids-match)
        self._push_trade_ids: Dict[str, None] = {}
        # exchange AND client order ids that got at least one order push (bounded), for _expect_order_push
        self._pushed_order_ids: Dict[str, None] = {}
        self._missing_pushes = 0
        self._last_push_alarm = 0.0
        self._last_stream_reconnect = 0.0
        self._balance_audits_left = CONSTANTS.BALANCE_PUSH_AUDITS
        # monotonic time Cloudflare's Error 1015 ban ends (the pool routes refuse at once until then)
        self._cf_banned_until = 0.0
        # asset -> monotonic time its balance was last set (a push: on arrival; a REST read: when it was sent), and
        # client order id -> monotonic time its last fill was booked: the balance gate counts only a balance set after
        # the order's last fill (review F5: another order's booked-but-unshown fill must not pass the gate)
        self._balance_set_at: Dict[str, float] = {}
        self._fill_booked_at: Dict[str, float] = {}
        # client order ids with a fill-row read in flight (a cumulative rest is never booked meanwhile: review F4), and
        # those whose terminal state waits for their rows (settled when the read ends: review F6)
        self._rows_in_flight: set = set()
        self._terminal_waiting: set = set()
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinstoreAuth:
        return CoinstoreAuth(api_key=self._api_key, secret_key=self._secret_key,
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
        return CONSTANTS.SYMBOLS_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.SYMBOLS_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.NETWORK_CHECK_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # The cancel answer names a state, but a CANCELED there says nothing about a fill that raced it: the order's own
        # status read (free) settles the order with its fills (_place_cancel).
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
        return CoinstoreAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinstoreAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit + alarms

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [CS-AUDIT] line. Only questions the docs and probes could not answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[CS-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    def _alarm(self, tag: str, **fields: Any) -> None:
        """A money guard fired: one [CS-ALARM] WARNING, logged whatever LIVE_AUDIT_LOGGING says. Rare by design."""
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().warning(f"[CS-ALARM] {tag} {rendered}")

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
        Every Coinstore answer is HTTP 200, business errors included, except Cloudflare's own rate refusal (HTTP 429, an
        HTML page). Numbers are parsed exactly (Decimal). Both rate refusals raise CoinstoreRateLimited: nothing behind
        them was executed. A POST with no body still sends "{}" (docs: "If there are no parameters, {} needs to be
        sent"), which is what the balance route signs and reads. Every request has a timeout (REQUEST_TIMEOUT unless the
        caller sets one). After Cloudflare's Error 1015 every pool route refuses at once, sending nothing, for the ban's
        retry_after.
        """
        limit = limit_id if limit_id else path_url
        if limit in CONSTANTS.CF_POOL_PATHS and time.monotonic() < self._cf_banned_until:
            raise CoinstoreRateLimited(f"{path_url} not sent: Cloudflare's Error 1015 ban has "
                                       f"{self._cf_banned_until - time.monotonic():.0f} s to run")
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)
        if method is RESTMethod.POST and data is None:
            data = {}
        response = await rest_assistant.execute_request_and_get_response(
            url=url,
            params=params,
            data=data,
            method=method,
            is_auth_required=is_auth_required,
            return_err=True,
            throttler_limit_id=limit,
            headers=headers,
            timeout=timeout if timeout is not None else CONSTANTS.REQUEST_TIMEOUT,
        )
        text = await response.text()
        if response.status == 429:
            ban = self._cloudflare_ban_seconds(text)
            if ban:
                self._cf_banned_until = max(self._cf_banned_until, time.monotonic() + ban)
                self._alarm("cloudflare-1015", path=path_url, ban_s=ban)
            raise CoinstoreRateLimited(f"Cloudflare refused {path_url} for rate (HTTP 429{', Error 1015' if ban else ''})")
        if response.status >= 400:
            raise IOError(f"Error executing request {method.name} {path_url}. HTTP status is {response.status}. "
                          f"Error: {text[:300]}")
        try:
            result = web_utils.loads(text)
        except ValueError:
            raise IOError(f"Coinstore answered {path_url} with something that is not JSON: {text[:300]}")
        if web_utils.code_of(result) == CONSTANTS.CODE_TOO_MANY:
            raise CoinstoreRateLimited(f"Coinstore refused {path_url} for rate (code 429): {self._raw(result)}")
        return result

    @staticmethod
    def _cloudflare_ban_seconds(text: str) -> float:
        """Cloudflare's Error 1015 (a JSON 429, live 2026-11-08: error_code 1015, retry_after 30): the ban's length. 0
        for its plain HTML 429 (Retry-After: 0), which bans nothing."""
        try:
            body = json.loads(text)
        except (TypeError, ValueError):
            return 0.0
        if not isinstance(body, dict) or str(body.get("error_code")) != "1015":
            return 0.0
        try:
            return max(float(body.get("retry_after") or 0), 0.0) or CONSTANTS.CF_BAN_DEFAULT_SECONDS
        except (TypeError, ValueError):
            return CONSTANTS.CF_BAN_DEFAULT_SECONDS

    async def _make_trading_rules_request(self) -> Any:
        return await self._fetch_symbol_list_response()

    async def _make_trading_pairs_request(self) -> Any:
        return await self._fetch_symbol_list_response()

    async def _fetch_symbol_list_response(self) -> Dict[str, Any]:
        """POST /v2/public/config/spot/symbols {}, or an exception. An empty or refused list would otherwise become an
        empty symbol map and no trading rules until the next poll, 30 min later, with every order failing as "no
        trading rule". Raising keeps the last map and rules; the base retries in 0.5 s."""
        response = await self._api_post(path_url=CONSTANTS.SYMBOLS_PATH, data={}, limit_id=CONSTANTS.SYMBOLS_PATH)
        self._raise_on_error(response, f"Error reading Coinstore's symbol list ({CONSTANTS.SYMBOLS_PATH})")
        rows = response.get("data")
        if not isinstance(rows, list) or not rows:
            raise IOError(f"Coinstore's symbol list came back empty | Coinstore response: {self._raw(response)}")
        self._symbol_list = (time.monotonic(), {str(r.get("symbolCode") or "").upper(): r
                                                for r in rows if isinstance(r, dict) and r.get("symbolCode")})
        return response

    async def symbol_list_by_symbol(self, max_age: float = 30.0) -> Dict[str, Dict[str, Any]]:
        """{SYMBOL: symbol-list row}, reused for max_age s (the data source's trading switch reads it every 60 s)."""
        async with self._symbol_list_lock:
            fetched_at, rows = self._symbol_list
            if not rows or time.monotonic() - fetched_at > max_age:
                await self._fetch_symbol_list_response()
            return self._symbol_list[1]

    async def _make_network_check_request(self):
        response = await self._api_get(path_url=CONSTANTS.NETWORK_CHECK_PATH, params={"depth": 5},
                                       limit_id=CONSTANTS.NETWORK_CHECK_PATH)
        if not web_utils.is_ok(response):
            raise IOError(f"Unexpected Coinstore answer to the network check: {response}")

    @staticmethod
    def _raw(response: Any) -> str:
        """Coinstore's response body as JSON, complete, for logs and error messages."""
        try:
            return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return repr(response)

    def _raise_on_error(self, response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if not web_utils.is_ok(response):
            code = web_utils.code_of(response)
            msg = (response.get("message") or response.get("msg")) if isinstance(response, dict) else None
            self._audit_once(f"error-code:{code}", context=context, response=self._raw(response))
            raise CoinstoreBusinessError(f"{context}: code {code} ({msg}) | Coinstore response: {self._raw(response)}",
                                         code, msg)
        return response

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """
        The base treats only HTTP 4xx as an exchange rejection and logs anything else as a network error with a
        traceback. Coinstore refuses orders with HTTP 200 and a code, so a refusal would look like an outage. It is
        logged as a refusal, with the request and Coinstore's complete response; transport failures take the base path.
        A placement the lookup could not settle is not failed at all: the status poll keeps looking for it. Nor is an
        order a push has named meanwhile, whatever the placement said: it exists on Coinstore (review F1).
        """
        self._hold_baseline.pop(order_id, None)
        order = self._order_tracker.fetch_order(client_order_id=order_id)
        if order is not None and order.exchange_order_id is not None:
            self._alarm("placement-failed-but-pushed", client_id=order_id, order_id=order.exchange_order_id,
                        error=repr(exception))
            return
        if isinstance(exception, CoinstorePlacementUnknown):
            self.logger().warning(f"{trade_type.name.lower()} {order_type.name} order {order_id} for {amount} "
                                  f"{trading_pair} at {price} stays pending: {exception}. The status poll looks for it "
                                  f"on Coinstore and fails it only once it is not there.")
            return
        if isinstance(exception, (CoinstoreBusinessError, CoinstoreRateLimited)):
            self.logger().warning(f"Coinstore rejected {trade_type.name.lower()} {order_type.name} order {order_id} "
                                  f"for {amount} {trading_pair} at {price}: {exception}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        # Coinstore has no clock error: X-CS-EXPIRES 300 s off was accepted (live); a bad signature is 1401 / 401.
        return False

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        """Coinstore's own "this order does not exist" (4003 live; 3103 in the docs)."""
        return isinstance(error, CoinstoreBusinessError) and error.code in CONSTANTS.ORDER_NOT_FOUND_CODES

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found(cancelation_exception)

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        """
        This fork's base counts EVERY failed status read toward failing the order (the 4th strike, never reset) and
        runs no lost-order recovery: a FAILED order is forgotten while it may still rest on Coinstore, and its later
        fills are never booked. So only Coinstore's own "order not found" counts; a deferral is silent; anything else (a
        timeout, a rate refusal, an error page) is a WARNING and the order stays tracked.
        """
        if isinstance(error, CoinstoreStatusDeferred):
            return
        if self._is_not_found(error):
            await self._order_tracker.process_order_not_found(order.client_order_id)
            return
        self.logger().warning(f"Coinstore status read for {order.client_order_id} failed, the order stays tracked: "
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
        """The clock's time, or the wall clock before the first tick (current_timestamp is NaN until then: a connector
        built at runtime can get pushes before the clock reaches it)."""
        timestamp = self.current_timestamp
        return time.time() if timestamp is None or timestamp != timestamp else timestamp

    # ------------------------------------------------------------------ symbols / rules / fees / prices

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        """Exchange symbol = the symbol code upper-cased (the list mixes cases: btcUSDT, ETHUSDT, uniusdt); every
        socket channel takes it lower-cased, the pushes name it upper-cased."""
        mapping = bidict()
        for info in exchange_info.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                symbol = str(info["symbolCode"]).upper()
                trading_pair = combine_to_hb_trading_pair(base=str(info["tradeCurrencyCode"]).upper(),
                                                          quote=str(info["quoteCurrencyCode"]).upper())
                if symbol in mapping or trading_pair in mapping.inverse:
                    self.logger().warning(f"Coinstore lists {symbol} / {trading_pair} twice; the first is kept.")
                    continue
                mapping[symbol] = trading_pair
            except Exception as exception:
                self.logger().error(f"Error parsing Coinstore symbol {info.get('symbolCode')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """
        Runtime add of a pair that is not in the startup map, i.e. not among the markets the symbol list gave at startup
        (every listed market is mapped). Coinstore's form is BASEQUOTE, which the base builds too; mapped here so the
        warning says so.
        """
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.upper()}{quote.upper()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(
            f"Coinstore {trading_pair} was not in the symbol list at startup; mapped to {exchange_symbol}. Coinstore's "
            f"answers to its requests will show whether it exists.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Live 2026-11-08 (376 markets): tickSz = price decimals (BTC 2, SHPING 8: = minLmtPr), lotSz = quantity decimals
        (BTC 6, DMTR 0), minLmtSz = the minimum quantity (BTC 0.000001, ETH 0.0004: can exceed one lot). No minimum
        value is published for a limit order; what Coinstore enforces is audit-logged from its refusals.
        """
        rules: List[TradingRule] = []
        for info in exchange_info_dict.get("data") or []:
            if not isinstance(info, dict) or not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(
                    symbol=str(info["symbolCode"]).upper())
                price_increment = Decimal(1).scaleb(-int(info["tickSz"]))
                amount_increment = Decimal(1).scaleb(-int(info["lotSz"]))
                min_quantity = self._dec(info.get("minLmtSz"))
                rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=max(min_quantity, amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_notional_size=Decimal("0"),
                ))
            except KeyError:
                continue   # not in the symbol map (a duplicate)
            except Exception:
                self.logger().exception(f"Error parsing the Coinstore trading rule {info.get('symbolCode')}. Skipping.")
        return rules

    async def _update_trading_fees(self) -> None:
        """Per-market rates from the symbol list's makerFee / takerFee (fractions: 0.002 = 0.2%). They feed the fee
        estimates only; the fee actually charged arrives with every fill row."""
        rows = await self.symbol_list_by_symbol(max_age=0.0)
        for symbol, info in rows.items():
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=symbol)
            except KeyError:
                continue
            maker, taker = self._dec(info.get("makerFee"), "0.002"), self._dec(info.get("takerFee"), "0.002")
            self._trading_fees[trading_pair] = TradeFeeSchema(maker_percent_fee_decimal=maker,
                                                              taker_percent_fee_decimal=taker)

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

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        One GET /v1/market/tickers (every market) instead of one call per pair: the order book tracker re-reads the REST
        last price of every quiet book every 5 s (XT: 1,842 throttler warnings in 28 h with per-pair calls). Every
        requested pair gets a value, NaN when Coinstore has none: the tracker re-asks a pair it got nothing for at once.
        `close` is the last price; the per-symbol price route answers 404 since 2026-10-20 (P1's D49).
        """
        prices: Dict[str, float] = {trading_pair: float("nan") for trading_pair in trading_pairs}
        symbol_to_pair: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                symbol_to_pair[await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)] = trading_pair
            except KeyError:
                continue  # not a market Coinstore lists: stays NaN
        if not symbol_to_pair:
            return prices
        response = self._raise_on_error(await self._api_get(path_url=CONSTANTS.TICKERS_PATH,
                                                            limit_id=CONSTANTS.TICKERS_PATH),
                                        "Error fetching the last Coinstore prices")
        for entry in response.get("data") or []:
            if not isinstance(entry, dict):
                continue
            trading_pair = symbol_to_pair.get(str(entry.get("symbol") or "").upper())
            if trading_pair is not None and entry.get("close") not in (None, ""):
                prices[trading_pair] = float(entry["close"])
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return (await self.get_last_traded_prices([trading_pair]))[trading_pair]

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        """POST /spot/accountList {}: one row per currency and type, {currency, balance, type 1 AVAILABLE | 4 FROZEN}
        (live: USDT AVAILABLE 100 + FROZEN 0; currencies never held are not listed). Total = available + frozen. An
        asset whose balance push arrived after this request was sent keeps the push: the snapshot is older. An asset no
        longer listed is gone (sold out)."""
        requested_at = time.monotonic()
        response = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.BALANCE_PATH, data={}, is_auth_required=True,
                                 limit_id=CONSTANTS.BALANCE_PATH),
            "Error fetching Coinstore balances",
        )
        rows = response.get("data")
        if not isinstance(rows, list):
            raise IOError(f"Coinstore balance answer carried no rows | Coinstore response: {self._raw(response)}")
        self._audit_once("balance-rest-shape", rows=len(rows), keys=sorted(rows[0].keys()) if rows else [])
        available: Dict[str, Decimal] = {}
        frozen: Dict[str, Decimal] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = str(row.get("currency") or "").upper()
            if not asset:
                continue
            kind = str(row.get("typeName") or "").upper() or {"1": "AVAILABLE", "4": "FROZEN"}.get(str(row.get("type")))
            if kind == "AVAILABLE":
                available[asset] = available.get(asset, Decimal("0")) + self._dec(row.get("balance"))
            elif kind == "FROZEN":
                frozen[asset] = frozen.get(asset, Decimal("0")) + self._dec(row.get("balance"))
            else:
                self._audit_once(f"balance-row-type:{row.get('type')}:{row.get('typeName')}", row=self._raw(row))
        remote = set(available) | set(frozen)
        for asset in remote:
            if self._balance_pushed_at.get(asset, 0.0) > requested_at:
                continue
            self._account_available_balances[asset] = available.get(asset, Decimal("0"))
            self._account_balances[asset] = available.get(asset, Decimal("0")) + frozen.get(asset, Decimal("0"))
            self._balance_set_at[asset] = max(self._balance_set_at.get(asset, 0.0), requested_at)
        for asset in set(self._account_balances) - remote:
            if self._balance_pushed_at.get(asset, 0.0) > requested_at:
                continue
            self._account_available_balances.pop(asset, None)
            self._account_balances.pop(asset, None)
            self._balance_set_at[asset] = max(self._balance_set_at.get(asset, 0.0), requested_at)

    # ------------------------------------------------------------------ orders

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        """
        The base reads self._trading_rules[trading_pair] before it tracks the order. With no rule it stops there with a
        bare KeyError in a background task (XT B2-USDT, 2026-09-23): nothing is sent, the order is never tracked, no
        failure event fires, and the strategy keeps cancelling an order that does not exist. Here the order is tracked
        and failed the way the base fails its own pre-send checks, with a message saying it was not sent.
        """
        if trading_pair not in self._trading_rules:
            message = (f"{trade_type.name} {order_type.name} order {order_id} for {amount} {trading_pair} at {price} "
                       f"was NOT sent to Coinstore: the connector has no trading rule for {trading_pair} "
                       f"({len(self._trading_rules)} rules built from {CONSTANTS.SYMBOLS_PATH}).")
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
        """
        POST /trade/order/place {clOrdId, symbol, side, ordType LIMIT, timeInForce GTC, ordPrice, ordQty, timestamp}
        -> data.ordId. `timestamp` is unique per order: Coinstore keeps it as the order's own (its second id).
          - A refusal for rate (Cloudflare's 429, the API's code 429) executed nothing: the order fails.
          - No answer, or an answer the venue has shown to be ambiguous (PLACE_UNKNOWN_CODES, or code 0 without an id;
            CoinW created an order 0.45 s after answering -3): the order is looked up (_settle_unconfirmed_placement).
            Found -> it lives on; not there -> the failure stands; a lookup that could not tell -> the order stays
            pending (CoinstorePlacementUnknown). Otherwise the order could live on Coinstore with nobody tracking it.
          - A definite refusal while a push already names the order (the venue created it anyway) adopts the order,
            with an [CS-ALARM].
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        sent_ms = max(int(time.time() * 1e3), self._last_sent_ms + 1)
        self._last_sent_ms = sent_ms
        self._sent_at_ms[order_id] = sent_ms
        if len(self._sent_at_ms) > 2000:
            for client_order_id in list(self._sent_at_ms)[:1000]:
                if client_order_id not in self._order_tracker.all_fillable_orders:
                    del self._sent_at_ms[client_order_id]
        body = {
            "clOrdId": order_id,
            "symbol": symbol,
            "side": CONSTANTS.TRADE_TYPES[trade_type],
            "ordType": CONSTANTS.ORDER_TYPE_LIMIT,
            "timeInForce": CONSTANTS.TIME_IN_FORCE_GTC,
            "ordPrice": self._format_decimal(price),
            "ordQty": self._format_decimal(amount),
            "timestamp": sent_ms,
        }
        self._placing.add(order_id)
        try:
            try:
                response = await self._api_post(path_url=CONSTANTS.PLACE_ORDER_PATH, data=body, is_auth_required=True,
                                                limit_id=CONSTANTS.PLACE_ORDER_PATH,
                                                timeout=CONSTANTS.PLACE_ORDER_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except CoinstoreRateLimited:
                raise
            except Exception as transport_error:
                return await self._settle_unconfirmed_placement(order_id, CONSTANTS.PLACEMENT_LOOKUP_DELAYS,
                                                                "place-order-unanswered", transport_error,
                                                                repr(transport_error))
            self._audit("place-order", request=self._raw(body), response=self._raw(response))
            code = web_utils.code_of(response)
            data = response.get("data") if isinstance(response.get("data"), dict) else {}
            exchange_order_id = data.get("ordId")
            if code == CONSTANTS.CODE_OK and exchange_order_id is not None:
                return str(exchange_order_id), self._now()
            if code is None or code == CONSTANTS.CODE_OK or code in CONSTANTS.PLACE_UNKNOWN_CODES:
                error = IOError(f"Coinstore answered order {order_id} without an order id, and the order is not on "
                                f"Coinstore | request {self._raw(body)} | Coinstore response: {self._raw(response)}")
                if code != CONSTANTS.CODE_OK:
                    message = response.get("message") or response.get("msg")
                    error = CoinstoreBusinessError(f"Order {order_id} refused, request {self._raw(body)}: code {code} "
                                                   f"({message}) | Coinstore response: {self._raw(response)}",
                                                   code, message)
                return await self._settle_unconfirmed_placement(order_id, CONSTANTS.PLACEMENT_AMBIGUOUS_LOOKUP_DELAYS,
                                                                "place-order-ambiguous", error, self._raw(response))
            pushed = self._pushed_exchange_id(order_id)
            if pushed is not None:
                self._alarm("place-order-refused-but-created", client_id=order_id, order_id=pushed,
                            response=self._raw(response))
                return pushed, self._now()
            self._raise_on_error(response, f"Order {order_id} refused, request {self._raw(body)}")
            raise IOError(f"Unexpected Coinstore answer to order {order_id} | {self._raw(response)}")
        finally:
            self._placing.discard(order_id)

    async def _settle_unconfirmed_placement(self, order_id: str, delays: Tuple[float, ...], tag: str,
                                            error: Exception, detail: str) -> Tuple[str, float]:
        """A placement without a definite answer: the order's id if the lookup finds it. Otherwise the order is NOT
        failed here, found or not (CoinstorePlacementUnknown): it stays pending and the status poll decides, by the
        same lookup, once it is past the young-order gate and after the tracker's not-found strikes (the runbook's
        Bitunix rule; review F1: an order created late, or whose push lands during the last look, would otherwise be
        failed while it rests on Coinstore)."""
        try:
            found = await self._resolve_unconfirmed_placement(order_id, delays)
            verdict = "not found yet"
        except _LookupInconclusive as unclear:
            found, verdict = None, f"the lookup could not tell: {unclear}"
        self._alarm(tag, client_id=order_id, error=detail, found_on_exchange=found, lookup=verdict)
        if found is not None:
            return found, self._now()
        raise CoinstorePlacementUnknown(f"Coinstore did not confirm order {order_id} ({detail}); {verdict}") from error

    def _pushed_exchange_id(self, client_order_id: str) -> Optional[str]:
        """The exchange id a push gave the order meanwhile (pushes carry the clOrdId and can beat the REST answer)."""
        order = self._order_tracker.fetch_order(client_order_id=client_order_id)
        if order is None or order.exchange_order_id is None:
            return None
        return str(order.exchange_order_id)

    async def _resolve_unconfirmed_placement(self, client_order_id: str, delays: Tuple[float, ...]) -> Optional[str]:
        """The placement guard: after each delay (s), the id a push gave the order, else _locate_unconfirmed (the done-
        order search only at the last look: by then the fill rows show an instant fill). None = the last look completed
        and found nothing; _LookupInconclusive = the last look failed."""
        last_error: Optional[Exception] = None
        for index, delay in enumerate(delays):
            await asyncio.sleep(delay)
            pushed = self._pushed_exchange_id(client_order_id)
            if pushed is not None:
                return pushed
            try:
                found = await self._locate_unconfirmed(client_order_id, search_done=index == len(delays) - 1)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().debug(f"Coinstore lookup of {client_order_id} failed: {e!r}")
                last_error = e
                continue
            if found is not None:
                return found
            last_error = None
        if last_error is not None:
            raise _LookupInconclusive(repr(last_error))
        return None

    async def _locate_unconfirmed(self, client_order_id: str, search_done: bool) -> Optional[str]:
        """An order whose placement went unconfirmed: by clOrdId (v2 orderInfo, free), which finds it while it is open;
        then, with search_done, as a done order by its send time. None = not there; a failed read raises."""
        row = await self._order_info(client_order_id=client_order_id)
        if row is not None and row.get("ordId") is not None:
            return str(row["ordId"])
        if not search_done:
            return None
        return await self._find_done_order_by_send_time(client_order_id)

    async def _find_done_order_by_send_time(self, client_order_id: str) -> Optional[str]:
        """
        An order of ours already done when looked for (an instant fill): its clOrdId reads "0" by then and the lookup by
        clOrdId answers 4003 (live). It has fills, so it is among the market's newest fill rows (one read from the
        Cloudflare bucket): each order there that nothing tracks, on our side and matched since the send, is read by id
        (free), and the one with the body's exact `timestamp` (ms, unique per order: our second id), side, price and
        quantity is ours. An order done without a fill never needed finding: nothing moved.
        """
        order = self._order_tracker.fetch_order(client_order_id=client_order_id)
        sent_ms = self._sent_at_ms.get(client_order_id)
        if order is None or sent_ms is None:
            return None
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.FILLS_PATH,
                                params={"symbol": symbol, "pageNum": 1, "pageSize": CONSTANTS.FILL_SEARCH_PAGE_SIZE},
                                is_auth_required=True, limit_id=CONSTANTS.FILLS_PATH),
            f"Error reading the newest {symbol} fill rows")
        side = "1" if order.trade_type is TradeType.BUY else "-1"
        since_s = sent_ms // 1000 - CONSTANTS.FILL_SEARCH_SLACK_S
        tracked = self._order_tracker.all_fillable_orders_by_exchange_order_id
        candidates: List[str] = []
        for row in response.get("data") or []:
            exchange_order_id = str(row.get("orderId") or "") if isinstance(row, dict) else ""
            if (exchange_order_id and exchange_order_id not in candidates and exchange_order_id not in tracked
                    and str(row.get("side")) == side and self._dec(row.get("matchTime")) >= since_s):
                candidates.append(exchange_order_id)
        for exchange_order_id in candidates[:CONSTANTS.FILL_SEARCH_MAX_CANDIDATES]:
            info = await self._order_info(ord_id=exchange_order_id)
            if (info is not None and str(info.get("timestamp")) == str(sent_ms)
                    and str(info.get("symbol") or "").upper() == symbol.upper()
                    and str(info.get("side") or "").upper() == CONSTANTS.TRADE_TYPES[order.trade_type]
                    and self._dec(info.get("ordPrice")) == order.price
                    and self._dec(info.get("ordQty")) == order.amount):
                self._alarm("placement-found-by-send-time", client_id=client_order_id, order_id=exchange_order_id,
                            status=self._row_status(info), cumQty=info.get("cumQty"))
                return exchange_order_id
        return None

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> str:
        """The base sets OPEN after the placement answer. A push can beat that answer and take the order further
        (filled, cancelled), and this fork's update_with_order_update sets any state it is given, so OPEN is applied
        only while the order is still PENDING_CREATE (Poloniex, 2026-10-09)."""
        self._note_hold_baseline(order)
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
        if order.current_state is OrderState.PENDING_CREATE:
            await self._order_tracker._process_order_update(OrderUpdate(
                client_order_id=order.client_order_id,
                exchange_order_id=str(exchange_order_id),
                trading_pair=order.trading_pair,
                update_timestamp=update_timestamp,
                new_state=OrderState.OPEN,
            ))
        elif order.exchange_order_id is None:
            order.update_exchange_order_id(str(exchange_order_id))
        safe_ensure_future(self._expect_order_push(order.client_order_id, str(exchange_order_id)))
        return exchange_order_id

    async def _order_info(self, ord_id: Optional[str] = None, client_order_id: Optional[str] = None
                          ) -> Optional[Dict[str, Any]]:
        """GET /v2/trade/order/orderInfo by ordId (or by clOrdId): the order's row, or None for Coinstore's own "Order
        Not Found" (4003). Free of the Cloudflare bucket (10/s clean live). data is a LIST in the docs' v2 example; a
        dict is read too."""
        params = {"ordId": ord_id} if ord_id is not None else {"clOrdId": client_order_id}
        response = await self._api_get(path_url=CONSTANTS.ORDER_INFO_PATH, params=params, is_auth_required=True,
                                       limit_id=CONSTANTS.ORDER_INFO_PATH)
        if web_utils.code_of(response) in CONSTANTS.ORDER_NOT_FOUND_CODES:
            return None
        self._raise_on_error(response, f"Error reading Coinstore order {params}")
        data = response.get("data")
        row = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) and data else None)
        if row is None:
            raise CoinstoreBusinessError(f"Coinstore answered order {params} with code 0 and no row | "
                                         f"{self._raw(response)}", CONSTANTS.CODE_EMPTY_ANSWER)
        self._audit_once("order-info-key-set", keys=sorted(row.keys()))
        return row

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """POST /trade/order/cancel {symbol, ordId}: by exchange id only (a cancel by clOrdId of an unknown id answers
        "success", so its answer proves nothing). The answer's state is audit-logged; the order's own status read, which
        is free, follows at once, so a fill that raced the cancel is booked before the cancel is."""
        exchange_order_id = await tracked_order.get_exchange_order_id()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        body = {"symbol": symbol, "ordId": int(exchange_order_id)}
        response = await self._api_post(path_url=CONSTANTS.CANCEL_ORDER_PATH, data=body, is_auth_required=True,
                                        limit_id=CONSTANTS.CANCEL_ORDER_PATH)
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    response=self._raw(response))
        self._raise_on_error(response, f"Coinstore refused to cancel order {order_id} ({exchange_order_id})")
        safe_ensure_future(self._refresh_order(tracked_order, delay=CONSTANTS.CANCEL_SETTLE_READ_DELAY))
        return True

    async def _execute_order_cancel(self, order: InFlightOrder) -> Optional[str]:
        """
        The base's cancel path, except:
          - a Coinstore refusal (or a rate refusal) is a WARNING with Coinstore's answer, and the order's status is
            read at once: a refused cancel usually means the order already filled or was cancelled;
          - a timeout never counts toward failing the order: the wait for an exchange id that never came leaves the order
            to the status poll's lookup (its placement may have gone unconfirmed and the order may rest on Coinstore),
            and an HTTP timeout on the cancel itself reads the order's status (the cancel may have landed).
        """
        try:
            cancelled = await self._execute_order_cancel_and_process_update(order=order)
            if cancelled:
                return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if order.exchange_order_id is None:
                self.logger().warning(f"Coinstore cancel of {order.client_order_id} not sent: the order has no exchange "
                                      f"id yet. The status poll is looking for it on Coinstore.")
                self._poll_notifier.set()
            else:
                self.logger().warning(f"Coinstore did not answer the cancel of {order.client_order_id} in time. "
                                      f"Reading its status now.")
                safe_ensure_future(self._refresh_order(order))
        except (CoinstoreBusinessError, CoinstoreRateLimited) as refusal:
            self.logger().warning(f"Coinstore refused to cancel {order.client_order_id}: {refusal}. Reading its status "
                                  f"now.")
            safe_ensure_future(self._refresh_order(order))
        except Exception:
            self.logger().error(f"Failed to cancel order {order.client_order_id}", exc_info=True)
        return None

    async def _refresh_order(self, order: InFlightOrder, delay: float = 0.0) -> None:
        if delay:
            await asyncio.sleep(delay)
        if order.client_order_id not in self._order_tracker.all_updatable_orders:
            return
        try:
            order_update = await self._request_order_status(tracked_order=order)
            self._order_tracker.process_order_update(order_update)
        except asyncio.CancelledError:
            raise
        except CoinstoreStatusDeferred:
            return
        except Exception as e:
            await self._handle_update_error_for_active_order(order, e)

    def _order_state(self, status: Any, executed: Decimal, original: Decimal) -> OrderState:
        """A REST state name (ORDER_STATE: the status reads) or number (ORDER_STATE_CODES: the pushes and fill rows).
        Anything else is derived from the amounts rather than guessed and audit-logged; an unknown push number also asks
        the status poll, whose read names the state (an unknown name doesn't: the poll would only read it again)."""
        text = str(status if status is not None else "").strip().upper()
        numeric = text.lstrip("-").isdigit()
        if numeric:
            state = CONSTANTS.ORDER_STATE_CODES.get(int(text))
        else:
            state = CONSTANTS.ORDER_STATE.get(text)
            if state is not None and text in ("EXPIRED", "STOPPED"):
                self._audit_once(f"status:{status}", executed=str(executed), original=str(original))
        if state is not None:
            return state
        self._audit_once(f"unknown-status:{status}", executed=str(executed), original=str(original))
        if numeric:
            self._poll_notifier.set()
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    # ------------------------------------------------------------------ order status (REST) and fills

    @staticmethod
    def _row_status(row: Dict[str, Any]) -> Any:
        """The state of a v2 status row (ordStatus), a v1 row or a push (ordState), a fill row (orderState)."""
        return row.get("ordStatus", row.get("ordState", row.get("orderState")))

    def _is_young(self, order: InFlightOrder) -> bool:
        """Too young for a not-found to mean anything (ORDER_NOT_FOUND_MIN_AGE: an instant fill reads 4003 at first).
        Aged by the placement's own send time when this process sent it: the tracker's creation_timestamp is the
        clock's, NaN for an order made before the clock's first tick."""
        sent_ms = self._sent_at_ms.get(order.client_order_id)
        if sent_ms is not None:
            return time.time() * 1e3 - sent_ms < CONSTANTS.ORDER_NOT_FOUND_MIN_AGE * 1e3
        created = order.creation_timestamp
        return created == created and self._now() - created < CONSTANTS.ORDER_NOT_FOUND_MIN_AGE

    def _note_cumulative(self, order: InFlightOrder, filled: Decimal, value: Optional[Decimal]) -> None:
        """The newest cumulative (filled, value) the venue reported. A value of None (a FILLED push whose own execAmt
        may not be the total) keeps the last known value; the next status read brings Coinstore's cumAmt."""
        held = self._venue_cumulative.get(order.client_order_id)
        if (held is None or filled > held[0]
                or (filled == held[0] and value is not None and value > held[1])):
            self._venue_cumulative[order.client_order_id] = (
                filled, value if value is not None else (held[1] if held is not None else Decimal("0")))
            if len(self._venue_cumulative) > 2000:
                for client_order_id in list(self._venue_cumulative)[:1000]:
                    if client_order_id not in self._order_tracker.all_fillable_orders:
                        del self._venue_cumulative[client_order_id]

    def _fills_behind(self, order: InFlightOrder) -> bool:
        venue = self._venue_cumulative.get(order.client_order_id)
        return venue is not None and venue[0] > order.executed_amount_base + CONSTANTS.FILL_AMOUNT_TOLERANCE

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        The order's state from its v2 orderInfo row (free). Fills first: when the row's cumulative quantity is past what
        the order has booked, its fill rows are read and booked before the state goes out, so a FILLED never completes an
        order short. A terminal state whose rows still fall short is deferred (CoinstoreStatusDeferred) until they show,
        at most FILL_ROWS_WAIT_SECONDS; then the rest is booked from the venue's cumulative totals as one fill, with a
        [CS-ALARM]. An order with no exchange id (its placement went unconfirmed) is looked for as the placement did. A
        not-found counts as a strike only once the order is older than ORDER_NOT_FOUND_MIN_AGE.
        """
        if tracked_order.client_order_id in self._placing:
            raise CoinstoreStatusDeferred()   # the placement looks the order up itself; the poll never does meanwhile
        young = self._is_young(tracked_order)
        if tracked_order.exchange_order_id is None:
            found = await self._locate_unconfirmed(tracked_order.client_order_id, search_done=not young)
            if found is None:
                if young:
                    raise CoinstoreStatusDeferred()
                raise CoinstoreBusinessError(f"Order {tracked_order.client_order_id} is not on Coinstore (looked up by "
                                             f"clOrdId and by its send time)", CONSTANTS.CODE_ORDER_NOT_FOUND)
            self._alarm("unconfirmed-order-found", client_id=tracked_order.client_order_id, order_id=found)
            tracked_order.update_exchange_order_id(found)
        row = await self._order_info(ord_id=tracked_order.exchange_order_id)
        if row is None:
            if young:
                raise CoinstoreStatusDeferred()
            raise CoinstoreBusinessError(f"Order {tracked_order.client_order_id} ({tracked_order.exchange_order_id}) "
                                         f"is not on Coinstore", CONSTANTS.CODE_ORDER_NOT_FOUND)
        filled, value = self._dec(row.get("cumQty")), self._dec(row.get("cumAmt"))
        self._note_cumulative(tracked_order, filled, value)
        status = self._row_status(row)
        new_state = self._order_state(status, filled, self._dec(row.get("ordQty"), str(tracked_order.amount)))
        self._audit_once(f"order-status:{status}", mapped=str(new_state), cumQty=row.get("cumQty"),
                         cumAmt=row.get("cumAmt"), leavesQty=row.get("leavesQty"))
        if self._fills_behind(tracked_order):
            await self._book_fill_rows(tracked_order)
        if new_state in CONSTANTS.TERMINAL_STATES and self._fills_behind(tracked_order):
            if tracked_order.client_order_id in self._rows_in_flight or not self._fill_rows_overdue(tracked_order):
                raise CoinstoreStatusDeferred()   # a read still in flight may bring the rows (review F4)
            self._book_cumulative_rest(tracked_order, source="status")
            if self._fills_behind(tracked_order):
                # The rest couldn't be booked from the totals (no value, or past the order's size): never forwarded
                # short (review F2); the next read tries again, and the alarm says so.
                self._alarm("fill-rest-unbookable", client_id=tracked_order.client_order_id,
                            venue=str(self._venue_cumulative.get(tracked_order.client_order_id)),
                            booked=str(tracked_order.executed_amount_base))
                raise CoinstoreStatusDeferred()
        if new_state in CONSTANTS.OPEN_STATES and tracked_order.current_state is OrderState.PENDING_CANCEL:
            raise CoinstoreStatusDeferred()   # a read older than the cancel must not roll it back (review F10)
        update = self._order_update(tracked_order, new_state, exchange_order_id=row.get("ordId"))
        if new_state in CONSTANTS.TERMINAL_STATES and not self._balance_shows_fills(tracked_order):
            # The caller reports what this returns at once: a terminal update the balance doesn't show yet goes to the
            # shared waiter instead (one per order), and the poll leaves the order alone meanwhile.
            self._process_terminal_update(tracked_order, update, source="rest")
            raise CoinstoreStatusDeferred()
        if new_state in CONSTANTS.TERMINAL_STATES:
            self._forget_settled(tracked_order)
        return update

    def _order_update(self, order: InFlightOrder, state: OrderState, exchange_order_id: Any = None) -> OrderUpdate:
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id or order.exchange_order_id),
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=state,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """The base calls this for every fillable order on every poll. Fill rows come out of the Cloudflare bucket, so
        they are read only when the venue has reported more filled than the order has booked (a push, or the status
        read). Booked here directly; nothing is handed back to the base."""
        if order.exchange_order_id is not None and self._fills_behind(order):
            await self._book_fill_rows(order)
        return []

    def _fill_rows_overdue(self, order: InFlightOrder) -> bool:
        first_short = self._rows_short_since.setdefault(order.client_order_id, time.monotonic())
        return time.monotonic() - first_short >= CONSTANTS.FILL_ROWS_WAIT_SECONDS

    async def _fetch_fill_rows(self, order: InFlightOrder) -> List[Dict[str, Any]]:
        """GET /trade/match/accountMatches?symbol=&ordId=, page by page (pageSize FILLS_PAGE_SIZE) until a short page.
        Every row is the order's own (orderId checked)."""
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        rows: List[Dict[str, Any]] = []
        for page in range(1, CONSTANTS.FILLS_MAX_PAGES + 1):
            response = self._raise_on_error(
                await self._api_get(path_url=CONSTANTS.FILLS_PATH,
                                    params={"symbol": symbol, "ordId": order.exchange_order_id, "pageNum": page,
                                            "pageSize": CONSTANTS.FILLS_PAGE_SIZE},
                                    is_auth_required=True, limit_id=CONSTANTS.FILLS_PATH),
                f"Error fetching the fills of order {order.client_order_id} ({order.exchange_order_id})")
            batch = [r for r in response.get("data") or [] if isinstance(r, dict)]
            if batch:
                self._audit_once("fill-row-key-set", keys=sorted(batch[0].keys()), row=self._raw(batch[0]))
            rows.extend(r for r in batch if str(r.get("orderId")) == str(order.exchange_order_id))
            if len(batch) < CONSTANTS.FILLS_PAGE_SIZE:
                return rows
        self.logger().warning(f"Coinstore fills of {order.client_order_id}: {CONSTANTS.FILLS_MAX_PAGES} full pages read, "
                              f"the rest is left for the next read.")
        return rows

    async def _book_fill_rows(self, order: InFlightOrder) -> None:
        """Read the order's fill rows (at most once per FILL_ROWS_READ_INTERVAL s) and book the new ones. While the read
        is in flight no cumulative rest is booked for the order (review F4). A terminal push that waited for these
        rows is settled right after (its own status read: review F6)."""
        cid = order.client_order_id
        now = time.monotonic()
        if now - self._rows_read_at.get(cid, 0.0) < CONSTANTS.FILL_ROWS_READ_INTERVAL:
            return
        self._rows_read_at[cid] = now
        self._rows_in_flight.add(cid)
        try:
            rows = await self._fetch_fill_rows(order)
        except CoinstoreRateLimited as e:
            self.logger().debug(f"Coinstore fills of {cid} refused for rate; next read: {e}")
            return
        finally:
            self._rows_in_flight.discard(cid)
        for row in sorted(rows, key=lambda r: int(r.get("tradeId") or 0)):
            fill = self._fill_from_row(order, row)
            if fill is not None:
                self._order_tracker.process_trade_update(fill)
                self._note_fill_booked(order, fill)
        if not self._fills_behind(order):
            self._rows_short_since.pop(cid, None)
            if cid in self._terminal_waiting:
                self._terminal_waiting.discard(cid)
                safe_ensure_future(self._refresh_order(order))

    def _learn_currency(self, currency_id: Any, asset: str) -> None:
        if currency_id is not None and asset:
            self._currency_by_id[str(currency_id)] = asset.upper()

    def _fee_token(self, order: InFlightOrder, fee_currency_id: Any) -> str:
        """
        The asset a fill's fee was taken in, from Coinstore's currency id (feeCurrencyId), learned from the account
        pushes and the fill rows. Live 2026-11-08 it is the asset received: a DMTR buy and a DOGE buy paid in the base, a
        DOGE sell in USDT. An id not learned yet books the received asset; an id naming a third asset (a platform token)
        books that asset, with an [CS-ALARM].
        """
        received = order.base_asset if order.trade_type is TradeType.BUY else order.quote_asset
        asset = self._currency_by_id.get(str(fee_currency_id))
        if asset is None:
            self._audit_once(f"fee-currency-unlearned:{fee_currency_id}", client_id=order.client_order_id,
                             booked_as=received)
            return received
        if asset not in (order.base_asset, order.quote_asset):
            self._alarm("fee-currency-unknown", client_id=order.client_order_id, fee_currency_id=fee_currency_id,
                        asset=asset)
        self._audit_once(f"fee-asset:{order.trade_type.name}:{'received' if asset == received else asset}",
                         client_id=order.client_order_id, fee_currency_id=fee_currency_id)
        return asset

    def _fill_from_row(self, order: InFlightOrder, row: Dict[str, Any]) -> Optional[TradeUpdate]:
        """One fill row -> one TradeUpdate named by Coinstore's own tradeId, so a row read twice is booked once (the
        tracker dedupes by trade id; a push names the fill with the same tradeId). Guards, each an [CS-ALARM]: never
        past the order's size; never a quantity without value."""
        self._learn_currency(row.get("baseCurrencyId"), order.base_asset)
        self._learn_currency(row.get("quoteCurrencyId"), order.quote_asset)
        trade_id = row.get("tradeId")
        quantity, value = self._dec(row.get("execQty")), self._dec(row.get("execAmt"))
        if trade_id is None or quantity <= 0:
            return None
        if str(trade_id) in order.order_fills:
            if str(trade_id) in self._push_trade_ids:
                self._audit_once("fill-ids-match", client_id=order.client_order_id, trade_id=trade_id)
            return None
        if order.executed_amount_base + quantity > order.amount + CONSTANTS.FILL_AMOUNT_TOLERANCE:
            self._alarm("fill-over-amount", client_id=order.client_order_id, trade_id=trade_id, quantity=str(quantity),
                        held=str(order.executed_amount_base), amount=str(order.amount))
            return None
        venue = self._venue_cumulative.get(order.client_order_id)
        if venue is not None and order.executed_amount_base + quantity > venue[0] + CONSTANTS.FILL_AMOUNT_TOLERANCE:
            # Never past the venue's own total (review F4). A row beyond a total read earlier waits for the next read,
            # after the next status read; beyond a cumulative rest already booked it is a double count.
            if any("-cum-" in str(t) for t in order.order_fills):
                self._alarm("fill-after-cumulative-rest", client_id=order.client_order_id, trade_id=trade_id,
                            quantity=str(quantity), venue_total=str(venue[0]))
            else:
                self._audit("fill-row-beyond-venue-total", client_id=order.client_order_id, trade_id=trade_id,
                            quantity=str(quantity), venue_total=str(venue[0]))
            return None
        if value <= 0:
            self._alarm("fill-without-value", client_id=order.client_order_id, trade_id=trade_id, row=self._raw(row))
            return None
        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            flat_fees=[TokenAmount(amount=self._dec(row.get("fee")), token=self._fee_token(order,
                                                                                         row.get("feeCurrencyId")))],
        )
        role = str(row.get("role") or "").upper()
        is_taker = role == "TAKER" if role in ("TAKER", "MAKER") else str(row.get("matchRole")) == "1"
        match_time = self._dec(row.get("matchTime"))
        return TradeUpdate(
            trade_id=str(trade_id),
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=quantity,
            fill_quote_amount=value,
            fill_price=value / quantity,
            # matchTime is in SECONDS (docs' example 1637825389)
            fill_timestamp=float(match_time) if match_time > 0 else self._now(),
            is_taker=is_taker,
        )

    def _book_cumulative_rest(self, order: InFlightOrder, source: str) -> None:
        """A terminal order whose fill rows stayed short past FILL_ROWS_WAIT_SECONDS: the rest, from the venue's own
        cumulative totals, as one fill with an estimated fee (the market's taker rate, in the received asset). Never
        forwarded short, never booked twice (named by the cumulative quantity)."""
        venue = self._venue_cumulative.get(order.client_order_id)
        if venue is None:
            return
        filled, value = venue
        base = filled - order.executed_amount_base
        quote = value - order.executed_amount_quote
        if base <= CONSTANTS.FILL_AMOUNT_TOLERANCE or quote <= 0:
            return
        if filled > order.amount + CONSTANTS.FILL_AMOUNT_TOLERANCE:
            self._alarm("fill-over-amount", client_id=order.client_order_id, source=source, cumulative=str(filled),
                        amount=str(order.amount))
            return
        received = order.base_asset if order.trade_type is TradeType.BUY else order.quote_asset
        rate = self._get_fee(order.base_asset, order.quote_asset, order.order_type, order.trade_type, base,
                             quote / base).percent
        fee_amount = (base if received == order.base_asset else quote) * rate
        self._alarm("fill-from-cumulative", client_id=order.client_order_id, source=source, base=str(base),
                    quote=str(quote), estimated_fee=str(fee_amount), fee_asset=received)
        fill = TradeUpdate(
            trade_id=f"{order.exchange_order_id}-cum-{self._format_decimal(filled.normalize())}",
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=TradeFeeBase.new_spot_fee(fee_schema=self.trade_fee_schema(), trade_type=order.trade_type,
                                          flat_fees=[TokenAmount(amount=fee_amount, token=received)]),
            fill_base_amount=base,
            fill_quote_amount=quote,
            fill_price=quote / base,
            fill_timestamp=self._now(),
            is_taker=True,
        )
        self._order_tracker.process_trade_update(fill)
        self._note_fill_booked(order, fill)
        self._rows_short_since.pop(order.client_order_id, None)

    # ------------------------------------------------------------------ terminal updates wait for the balance

    def _note_hold_baseline(self, order: InFlightOrder) -> None:
        """The base asset's total balance and this process's booked base when the order is sent (_balance_shows_fills)."""
        asset = order.base_asset
        self._hold_baseline[order.client_order_id] = (self._account_balances.get(asset, Decimal("0")),
                                                      self._booked_base.get(asset, Decimal("0")))
        if len(self._hold_baseline) > 2000:
            # only orders nothing can fill any more: a long-resting order keeps its gate (review F9)
            for client_order_id in list(self._hold_baseline)[:1000]:
                if client_order_id not in self._order_tracker.all_fillable_orders:
                    del self._hold_baseline[client_order_id]

    @staticmethod
    def _base_delta(order: InFlightOrder, fills: List[TradeUpdate]) -> Decimal:
        """What the fills change the base asset's balance by: + quantity on a buy, - on a sell, less any fee charged in
        the base asset."""
        delta = Decimal("0")
        for fill in fills:
            base_fee = sum((fee.amount for fee in fill.fee.flat_fees if fee.token == order.base_asset), Decimal("0"))
            delta += (fill.fill_base_amount if order.trade_type is TradeType.BUY else -fill.fill_base_amount) - base_fee
        return delta

    def _note_fill_booked(self, order: InFlightOrder, fill: TradeUpdate) -> None:
        asset = order.base_asset
        self._booked_base[asset] = self._booked_base.get(asset, Decimal("0")) + self._base_delta(order, [fill])
        self._fill_booked_at[order.client_order_id] = time.monotonic()

    def _balance_shows_fills(self, order: InFlightOrder) -> bool:
        """
        True once the base asset's total balance carries the order's own fills (Hotcoin's rule, 2026-10-06: a
        completion reported before the balance that holds it made the hold-band read the asset as 0 and buy it twice).
        Since the order was sent the balance has moved by its own fills and by other orders' fills booked meanwhile: the
        others' are taken off, and what is left must reach 99% of the order's own. Only a balance set after the order's
        last fill was booked counts (a push on arrival, a REST read from when it was sent): an older one may show
        another order's fill in its place (review F5). An order without fills, or with no baseline (sent before this
        process), needs no wait.
        """
        baseline = self._hold_baseline.get(order.client_order_id)
        if baseline is None or order.executed_amount_base <= 0:
            return True
        own = self._base_delta(order, list(order.order_fills.values()))
        if own == 0:
            return True
        balance_then, booked_then = baseline
        asset = order.base_asset
        if self._balance_set_at.get(asset, 0.0) <= self._fill_booked_at.get(order.client_order_id, 0.0):
            return False
        others = self._booked_base.get(asset, Decimal("0")) - booked_then - own
        moved_own = self._account_balances.get(asset, Decimal("0")) - balance_then - others
        return moved_own / own >= Decimal("0.99")

    def _forget_settled(self, order: InFlightOrder) -> None:
        self._hold_baseline.pop(order.client_order_id, None)
        self._rows_short_since.pop(order.client_order_id, None)
        self._rows_read_at.pop(order.client_order_id, None)
        self._fill_booked_at.pop(order.client_order_id, None)
        self._terminal_waiting.discard(order.client_order_id)

    async def _await_balance_showing_fills(self, order: InFlightOrder, source: str) -> None:
        """Hold a terminal update until the balance shows the order's fills, at most FILL_BALANCE_WAIT_SECONDS; then read
        REST balances (bounded by the same time) and let it go anyway, with an [CS-ALARM]."""
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
            self.logger().warning(f"Coinstore balance read for {order.client_order_id} failed: {e!r}")
        self._alarm("fill-balance-late", client_id=order.client_order_id, source=source,
                    waited_s=CONSTANTS.FILL_BALANCE_WAIT_SECONDS,
                    shows_fills_after_rest=self._balance_shows_fills(order))

    def _process_terminal_update(self, order: InFlightOrder, update: OrderUpdate, source: str = "push") -> None:
        """An order update on its way to the tracker. A terminal one is held until the balance shows the order's fills,
        by ONE waiter per order: the push and the status poll can both bring the same update."""
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

    def _get_poll_interval(self, timestamp: float) -> float:
        """While any order is open the status poll runs every SHORT_POLL_INTERVAL (10 s; the status route is free of the
        Cloudflare bucket): a resting order's fill or cancel is seen within 10 s even if its push is lost."""
        if self.in_flight_orders:
            return self.SHORT_POLL_INTERVAL
        return super()._get_poll_interval(timestamp=timestamp)

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        """The private stream's frames: `T` order -> _process_order_push, `T` account -> _process_account_push. A push's
        fields sit at the frame's top level (live 2026-11-08); a `data` list is read too."""
        async for event_message in self._iter_user_event_queue():
            try:
                kind = event_message.get("T")
                data = event_message.get("data", event_message)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if kind == "order":
                        self._process_order_push(item)
                    elif kind == "account":
                        self._process_account_push(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the Coinstore user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str]) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        order = tracker.all_fillable_orders.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id is not None:
            order = tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        return order

    def _is_open(self, data: Dict[str, Any]) -> bool:
        """Does this push (or status row) say the order rests on Coinstore?"""
        return self._order_state(self._row_status(data), self._dec(data.get("execQty", data.get("cumQty"))),
                                 self._dec(data.get("ordQty"))) in CONSTANTS.OPEN_STATES

    def _process_order_push(self, data: Dict[str, Any]) -> None:
        """
        A `!@order` push (live format in constants §Private WebSocket): one per placement (ordState 11), fill (the fill
        itself: tradeId, matchQty, matchAmt; ordState 50 once filled) and cancel (31). Every push of ours carries our
        clOrdId (13 of 13 live), so it finds its order even when it beats the REST placement answer home, and a fill
        never waits for its ids to be linked. Pushes for orders HMB did not place, or no longer tracks, are only
        audit-logged; one of ours open on Coinstore that nothing tracks raises an [CS-ALARM].
        """
        self._audit_once("order-push-key-set", keys=sorted(data.keys()), push=self._raw(data))
        client_order_id = str(data.get("clOrdId") or "")
        if client_order_id == "0":
            client_order_id = ""   # what a done order's clOrdId reads over REST
        raw_id = data.get("ordId")
        exchange_order_id = str(raw_id) if raw_id is not None else None
        for seen in (exchange_order_id, client_order_id):
            if seen:
                self._pushed_order_ids[seen] = None
        if len(self._pushed_order_ids) > 4000:
            for seen in list(self._pushed_order_ids)[:2000]:
                del self._pushed_order_ids[seen]
        order = self._locate_order(client_order_id, exchange_order_id)
        if order is None:
            self._audit("order-push-unmatched", client_id=client_order_id or None, order_id=exchange_order_id,
                        ordState=data.get("ordState"))
            if client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) and data.get("tradeId") is not None:
                # A fill of ours that no tracked order takes: it is not booked anywhere in HMB (review F1)
                self._alarm("fill-unattributed", client_id=client_order_id, order_id=exchange_order_id,
                            trade_id=data.get("tradeId"), matchQty=data.get("matchQty"), matchAmt=data.get("matchAmt"))
            if (client_order_id.startswith(CONSTANTS.HBOT_ORDER_ID_PREFIX) and self._is_open(data)
                    and exchange_order_id is not None and exchange_order_id not in self._orphan_ids):
                # One of ours, open on Coinstore, that nothing tracks: said, not cancelled (Pavel's call).
                self._orphan_ids[exchange_order_id] = None
                self._alarm("untracked-order-open", client_id=client_order_id, order_id=exchange_order_id,
                            ordState=data.get("ordState"))
            return
        self._apply_order_push(order, data)

    def _apply_order_push(self, order: InFlightOrder, data: Dict[str, Any]) -> None:
        """
        The fill the push carries is booked first, then the state applied: a fill push is also the order's last push
        when it fills the order (ordState 50 with the fill), so the terminal state never goes out before its fill.
        execQty / execAmt are noted as the venue's cumulative totals: past what is booked (a fill push lost), the fill
        rows are read and the terminal state waits for them.
        """
        raw_id = data.get("ordId")
        exchange_order_id = str(raw_id) if raw_id is not None else order.exchange_order_id
        status = data.get("ordState")
        executed = self._dec(data.get("execQty"))
        self._audit("order-push", client_id=order.client_order_id, order_id=exchange_order_id, ordState=status,
                    matchQty=data.get("matchQty"), matchAmt=data.get("matchAmt"), execQty=data.get("execQty"),
                    execAmt=data.get("execAmt"), remainingQty=data.get("remainingQty"), fee=data.get("fee"),
                    feeCurrencyId=data.get("feeCurrencyId"), tradeId=data.get("tradeId"), role=data.get("role"),
                    seq=data.get("S"))
        if order.exchange_order_id is None and exchange_order_id is not None:
            # The push beat the placement answer: give the order its id before a fill is named after it.
            order.update_exchange_order_id(exchange_order_id)
        fill = self._fill_from_push(order, data)
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            self._note_fill_booked(order, fill)
        if executed > 0:
            self._note_cumulative(order, executed, self._dec(data.get("execAmt")))
        if self._fills_behind(order):
            safe_ensure_future(self._book_fill_rows(order))
        if order.is_failure and self._is_open(data) and exchange_order_id is not None:
            self._cancel_failed_but_open(order, exchange_order_id, status)
            return
        if order.client_order_id not in self._order_tracker.all_updatable_orders or status is None:
            return  # already final (a late push may still carry a fill, booked above), or no state to apply
        original = self._dec(data.get("ordQty"), str(order.amount))
        new_state = self._order_state(status, executed, original)
        if new_state is OrderState.OPEN and order.current_state is not OrderState.PENDING_CREATE:
            return  # an OPEN after the order has moved on would roll it back
        if new_state in CONSTANTS.OPEN_STATES and order.current_state is OrderState.PENDING_CANCEL:
            return  # the cancel is on its way; its own push (or the status read) settles the order
        if new_state in CONSTANTS.TERMINAL_STATES and new_state is not OrderState.FILLED:
            # A cancel (or another end) whose push may not carry the order's whole total: Coinstore's own status read
            # (free) books any fill still missing and then settles the order (review F2).
            safe_ensure_future(self._refresh_order(order))
            return
        if new_state is OrderState.FILLED:
            # FILLED is the whole order, whatever this push's execQty says (review F2): a fill whose push was lost
            # leaves the order behind, and its rows are read before the state goes out.
            self._note_cumulative(order, max(executed, original),
                                  self._dec(data.get("execAmt")) if executed >= original else None)
        if new_state in CONSTANTS.TERMINAL_STATES and self._fills_behind(order):
            # The rows are read; their reader settles the order once they are booked (review F6), the poll otherwise.
            self._terminal_waiting.add(order.client_order_id)
            safe_ensure_future(self._book_fill_rows(order))
            self._poll_notifier.set()
            return
        self._process_terminal_update(order, OrderUpdate(
            trading_pair=order.trading_pair,
            update_timestamp=self._now(),
            new_state=new_state,
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
        ))

    def _fill_from_push(self, order: InFlightOrder, data: Dict[str, Any]) -> Optional[TradeUpdate]:
        """
        The fill an order push carries (tradeId; matchQty / matchAmt = this fill), named by Coinstore's tradeId, which
        the REST fill row repeats (2 of 2 live), so whichever source comes first books it, once. The fee is this fill's:
        the push's actualFeeRate x the amount in the fee asset (the base on a buy, the quote on a sell), which is what
        Coinstore charged on both live fills, because the push's `fee` may be the order's running total on a later
        fill; that is open until a multi-fill order (audit push-fee-check). A fee in a third asset takes `fee` as is.
        Guards, each an [CS-ALARM]: never past the order's size; never a quantity without value.
        """
        trade_id = data.get("tradeId")
        quantity, value = self._dec(data.get("matchQty")), self._dec(data.get("matchAmt"))
        if trade_id is None or quantity <= 0:
            return None
        if str(trade_id) in order.order_fills:
            return None
        if order.executed_amount_base + quantity > order.amount + CONSTANTS.FILL_AMOUNT_TOLERANCE:
            self._alarm("fill-over-amount", client_id=order.client_order_id, trade_id=trade_id, quantity=str(quantity),
                        held=str(order.executed_amount_base), amount=str(order.amount), source="push")
            return None
        if value <= 0:
            self._alarm("fill-without-value", client_id=order.client_order_id, trade_id=trade_id, push=self._raw(data))
            return None
        token = self._fee_token(order, data.get("feeCurrencyId"))
        rate, reported = self._dec(data.get("actualFeeRate")), self._dec(data.get("fee"))
        if token not in (order.base_asset, order.quote_asset):
            fee_amount = reported
        elif rate == 0 and reported > 0:
            # No rate but a fee (review F8): the push's own fee, which is this fill's when the fill is the order's
            # whole execution so far; on a later fill it may be a running total, so the alarm says so.
            fee_amount = reported
            if self._dec(data.get("execQty")) != quantity:
                self._alarm("fee-rate-missing", client_id=order.client_order_id, trade_id=trade_id, fee=data.get("fee"),
                            matchQty=data.get("matchQty"), execQty=data.get("execQty"))
        elif token == order.base_asset:
            fee_amount = rate * quantity
        else:
            fee_amount = rate * value
        self._audit_once(f"push-fee-check:{'equal' if reported == fee_amount else 'differs'}",
                         client_id=order.client_order_id, trade_id=trade_id, fee=data.get("fee"),
                         booked=str(fee_amount), rate=data.get("actualFeeRate"), matchQty=data.get("matchQty"),
                         execQty=data.get("execQty"))
        self._push_trade_ids[str(trade_id)] = None
        if len(self._push_trade_ids) > 4000:
            for seen in list(self._push_trade_ids)[:2000]:
                del self._push_trade_ids[seen]
        return TradeUpdate(
            trade_id=str(trade_id),
            client_order_id=order.client_order_id,
            exchange_order_id=str(order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=TradeFeeBase.new_spot_fee(fee_schema=self.trade_fee_schema(), trade_type=order.trade_type,
                                          flat_fees=[TokenAmount(amount=fee_amount, token=token)]),
            fill_base_amount=quantity,
            fill_quote_amount=value,
            fill_price=value / quantity,
            fill_timestamp=self._now(),   # the push carries no fill time (`timestamp` is the order's send time)
            is_taker=str(data.get("role") or "").upper() != "MAKER",
        )

    def _cancel_failed_but_open(self, order: InFlightOrder, exchange_order_id: str, status: Any) -> None:
        """HMB gave the order up (FAILED) yet Coinstore reports it open: nothing would track it once its 30 s in the
        cache are over, so it is cancelled now, with an [CS-ALARM]."""
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
                                            data={"symbol": symbol, "ordId": int(exchange_order_id)},
                                            is_auth_required=True, limit_id=CONSTANTS.CANCEL_ORDER_PATH)
            self._audit("cancel-untracked", order_id=exchange_order_id, response=self._raw(response))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Coinstore cancel of the untracked order {exchange_order_id} failed: {e!r}")

    async def _expect_order_push(self, client_order_id: str, exchange_order_id: str) -> None:
        """No `!@order` push within ORDER_PUSH_EXPECTED_WITHIN of the placement answer means the private stream isn't
        delivering (a SUB ack proves nothing; live 2026-11-08 one socket pushed nothing for 154 orders): poll orders
        and balances over REST now, reconnect the private stream, and say so (rate-limited)."""
        await asyncio.sleep(CONSTANTS.ORDER_PUSH_EXPECTED_WITHIN)
        if (exchange_order_id in self._pushed_order_ids or client_order_id in self._pushed_order_ids
                or client_order_id not in self._order_tracker.all_fillable_orders):
            return
        self._poll_notifier.set()
        self._reconnect_private_stream("no push for order " + client_order_id)
        self._missing_pushes += 1
        now = time.time()
        if now - self._last_push_alarm >= CONSTANTS.ORDER_PUSH_ALARM_INTERVAL:
            self._last_push_alarm = now
            self._alarm("order-push-missing", client_id=client_order_id, order_id=exchange_order_id,
                        within_s=CONSTANTS.ORDER_PUSH_EXPECTED_WITHIN, missing_since_last_alarm=self._missing_pushes)
            self._missing_pushes = 0

    def _reconnect_private_stream(self, reason: str) -> None:
        """Drop the private socket, at most every PRIVATE_STREAM_RECONNECT_MIN_INTERVAL s: the listen loop logs in
        again at once, on whatever server node Coinstore picks, and asks for a REST poll."""
        now = time.monotonic()
        if now - self._last_stream_reconnect < CONSTANTS.PRIVATE_STREAM_RECONNECT_MIN_INTERVAL:
            return
        self._last_stream_reconnect = now
        data_source = self._user_stream_tracker.data_source
        if isinstance(data_source, CoinstoreAPIUserStreamDataSource):
            safe_ensure_future(data_source.reconnect(reason))

    def _process_account_push(self, data: Dict[str, Any]) -> None:
        """
        A `!@account` push: one currency's absolute available + frozen, the hold included, after every placement
        (order.place), fill (match.taker; a maker's type unseen) and cancel (match.cancel), the same instant as the
        order push (live 2026-11-08). Its currency id is learned for the fees. A shape this can't read triggers a REST
        balance read instead of guessing.
        """
        self._audit_once("account-push-key-set", keys=sorted(data.keys()), push=self._raw(data))
        self._audit_once(f"account-push-type:{data.get('type')}", push=self._raw(data))
        asset = str(data.get("currency") or "").upper()
        if not asset or data.get("available") is None or data.get("frozen") is None:
            self._audit_once("account-push-unreadable", push=self._raw(data))
            self._poll_notifier.set()
            return
        self._learn_currency(data.get("currencyId"), asset)
        available, frozen = self._dec(data.get("available")), self._dec(data.get("frozen"))
        if self._balance_audits_left > 0:
            self._balance_audits_left -= 1
            self._audit("balance-push-vs-rest", asset=asset, type=data.get("type"), available=str(available),
                        frozen=str(frozen), held_available=str(self._account_available_balances.get(asset)),
                        held_total=str(self._account_balances.get(asset)))
        self._account_available_balances[asset] = available
        self._account_balances[asset] = available + frozen
        self._balance_pushed_at[asset] = self._balance_set_at[asset] = time.monotonic()
