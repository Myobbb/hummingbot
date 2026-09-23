import asyncio
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS, xt_utils as utils, xt_web_utils as web_utils
from hummingbot.connector.exchange.xt.xt_api_order_book_data_source import XtAPIOrderBookDataSource
from hummingbot.connector.exchange.xt.xt_api_user_stream_data_source import XtAPIUserStreamDataSource
from hummingbot.connector.exchange.xt.xt_auth import XtAuth
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


class XtBusinessError(IOError):
    """A request XT answered with rc=1: a definite refusal (the `mc` code says why), never a transport
    failure. IOError keeps every existing `except IOError` path working."""

    def __init__(self, message: str, mc: Optional[str]) -> None:
        super().__init__(message)
        self.mc = mc


class XtExchange(ExchangePyBase):
    """
    XT.com v4 spot connector.

    Reference: the SPOT docs (offline mirror at VS_code_projects/MDs/xt-api-v4/, wiki page
    trading/exchanges/xt-api) and the pyxt SDK. Every XT-specific choice below cites the doc page, the
    SDK, or a live observation; open questions are audit-logged ([XT-AUDIT]) rather than guessed.

    Scope: LIMIT (GTC) orders only, which is all arb_l and the position balancer send. XT's
    `timeInForces` on API-tradable markets list GTC/IOC and no GTX (post-only), and MARKET orders
    have never been exercised here, so neither is offered.
    """

    web_utils = web_utils

    def __init__(
        self,
        xt_api_key: str,
        xt_secret_key: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        """
        The signature must match ConnectorSetting.conn_init_parameters in this fork, which always
        passes `balance_asset_limit` (and `rate_limits_share_pct`) and no config map. An upstream-style
        `client_config_map` argument imports fine and then crashes `balance` (CoinEx, 2026-09-15).
        """
        self._api_key = xt_api_key
        self._secret_key = xt_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()
        # exchange order id -> [(received_at, trade push)] for fills that beat the order id home
        self._pending_fills: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}
        # trade ids first recorded from REST, so a later WS push of the same fill under another id is
        # recognised (see _apply_push_fill)
        self._rest_fill_ids: Dict[str, float] = {}
        self._balance_pushes_audited = 0
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # WS-authoritative until the fill test proves otherwise (runbook §6.3); one switch.
        self.real_time_balance_update = CONSTANTS.REAL_TIME_BALANCE_UPDATE

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> XtAuth:
        return XtAuth(api_key=self._api_key, secret_key=self._secret_key, time_provider=self._time_synchronizer)

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
        return CONSTANTS.SYMBOL_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.SYMBOL_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # DELETE /v4/order/{id} answers only {"cancelId"}; the CANCELED state arrives on the order push.
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
        return XtAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return XtAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit

    def _audit(self, tag: str, **fields: Any) -> None:
        """One [XT-AUDIT] line. Only questions the docs cannot answer; never headers or credentials."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[XT-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

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
        **kwargs,
    ) -> Dict[str, Any]:
        """
        XT reports clock drift as HTTP 200 + mc=AUTH_105, so the base class's resync-and-retry (which
        only runs on an IOError from the transport) never sees it. It is handled here: re-sync server
        time and repeat once. The first attempt was refused at authentication, so nothing was executed
        and the repeat is safe even for an order placement.
        """
        request = dict(path_url=path_url, overwrite_url=overwrite_url, method=method, params=params, data=data,
                       is_auth_required=is_auth_required, return_err=return_err, limit_id=limit_id,
                       headers=headers, **kwargs)
        response = await super()._api_request(**request)
        if is_auth_required and web_utils.error_code(response) in CONSTANTS.TIME_SYNC_ERROR_CODES:
            await self._update_time_synchronizer()
            response = await super()._api_request(**request)
        return response

    @staticmethod
    def _raise_on_error(response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if web_utils.is_error_response(response):
            mc = web_utils.error_code(response)
            raise XtBusinessError(f"{context}: {mc} ({response})", mc)
        return response

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """
        The base treats only HTTP 4xx as an exchange rejection and logs anything else as a network
        error with a traceback. XT refuses orders with HTTP 200 + rc=1, so a routine refusal
        (ORDER_002 insufficient funds, a filter) would look like an outage. It is logged as the
        rejection it is; transport failures still take the base path.
        """
        if isinstance(exception, XtBusinessError):
            self.logger().warning(f"XT rejected {trade_type.name.lower()} {order_type.name} order for {amount} "
                                  f"{trading_pair} at {price}: {exception.mc}")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return any(code in str(request_exception) for code in CONSTANTS.TIME_SYNC_ERROR_CODES)

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # ORDER_005 "Order not exist" — verified live for orderId, clientOrderId and the path form.
        return CONSTANTS.MC_ORDER_NOT_FOUND in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.MC_ORDER_NOT_FOUND in str(cancelation_exception)

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

    # ------------------------------------------------------------------ orders

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
        # Prices and quantities go as decimal strings, as in the docs' own signing sample; JSON floats
        # would render small values in exponent form (1e-08).
        data = {
            "symbol": symbol,
            "clientOrderId": order_id,
            "side": CONSTANTS.TRADE_TYPES[trade_type],
            "type": CONSTANTS.ORDER_TYPE_LIMIT,
            "timeInForce": CONSTANTS.TIME_IN_FORCE_GTC,
            "bizType": CONSTANTS.BIZ_TYPE_SPOT,
            "price": self._format_decimal(price),
            "quantity": self._format_decimal(amount),
        }
        try:
            response = await self._api_post(path_url=CONSTANTS.ORDER_PATH, data=data, is_auth_required=True,
                                            limit_id=CONSTANTS.PLACE_ORDER_LIMIT_ID)
        except asyncio.CancelledError:
            raise
        except Exception as transport_error:
            # No answer (timeout, dropped connection): XT may still have accepted the order. The base
            # would mark it FAILED and stop tracking it, leaving a live order nobody watches. Ask XT by
            # client id first; only a confirmed absence lets the failure stand.
            exchange_order_id = await self._find_order_by_client_id(order_id)
            self._audit("place-order-unanswered", client_id=order_id, error=str(transport_error)[:160],
                        found_on_exchange=exchange_order_id)
            if exchange_order_id is not None:
                return exchange_order_id, self.current_timestamp
            raise
        # AUDIT: whether string price/quantity are accepted, on a market with orderTypes [] as well
        # as one that lists LIMIT, and what the response echoes.
        self._audit("place-order", request=data, rc=response.get("rc"), mc=response.get("mc"),
                    result=response.get("result"))
        self._raise_on_error(response, f"Error submitting order {order_id}")
        result = response.get("result") or {}
        if result.get("orderId") is None:
            raise IOError(f"Error submitting order {order_id}: XT returned no orderId ({response})")
        return str(result["orderId"]), self.current_timestamp

    async def _find_order_by_client_id(self, client_order_id: str) -> Optional[str]:
        """Exchange order id for a client id, or None once XT says ORDER_005 twice (the second look
        allows for an order that is not yet queryable). Any other answer, or no answer, is None too:
        the caller then keeps the original failure, which is what happened before this check."""
        for delay in CONSTANTS.PLACEMENT_LOOKUP_DELAYS:
            await asyncio.sleep(delay)
            try:
                response = await self._api_get(path_url=CONSTANTS.ORDER_PATH, params={"clientOrderId": client_order_id},
                                               is_auth_required=True, limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID)
            except asyncio.CancelledError:
                raise
            except Exception:
                return None
            if not web_utils.is_error_response(response):
                order_id = (response.get("result") or {}).get("orderId")
                return str(order_id) if order_id is not None else None
            if web_utils.error_code(response) != CONSTANTS.MC_ORDER_NOT_FOUND:
                return None
        return None

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        # XT cancels by exchange order id only; this waits for the placement response if needed.
        exchange_order_id = await tracked_order.get_exchange_order_id()
        response = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH.format(order_id=exchange_order_id),
            is_auth_required=True,
            limit_id=CONSTANTS.CANCEL_ORDER_LIMIT_ID,
        )
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    rc=response.get("rc"), mc=response.get("mc"), result=response.get("result"))
        self._raise_on_error(response, f"Error cancelling order {order_id}")
        return True

    def _order_state(self, raw_state: Any, executed: Decimal, original: Decimal) -> OrderState:
        state = CONSTANTS.ORDER_STATE.get(str(raw_state).upper()) if raw_state is not None else None
        if state is not None:
            return state
        # Not in the documented enum: derive from amounts rather than guess a terminal state.
        if original > 0 and executed >= original:
            return OrderState.FILLED
        if executed > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id is not None:
            params = {"orderId": tracked_order.exchange_order_id}
        else:
            # The placement response never arrived; XT can also look an order up by client id.
            params = {"clientOrderId": tracked_order.client_order_id}
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ORDER_PATH, params=params, is_auth_required=True,
                                limit_id=CONSTANTS.QUERY_ORDER_LIMIT_ID),
            f"Error fetching status of order {tracked_order.client_order_id}",
        )
        order = response.get("result") or {}
        if not order:
            raise IOError(f"Error fetching status of order {tracked_order.client_order_id}: "
                          f"{CONSTANTS.MC_ORDER_NOT_FOUND} (empty result)")
        raw_state = order.get("state")
        new_state = self._order_state(raw_state, self._dec(order.get("executedQty")), self._dec(order.get("origQty")))
        self._audit_once(f"order-status:{raw_state}", state=raw_state, mapped=str(new_state),
                         in_documented_map=str(raw_state).upper() in CONSTANTS.ORDER_STATE, keys=sorted(order.keys()))
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order.get("orderId") or tracked_order.exchange_order_id),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(order.get("updatedTime") or self.current_timestamp * 1e3) * 1e-3,
            new_state=new_state,
        )

    def _trade_update_from_rest(self, item: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        """One row of GET /v4/trade: price, quantity, quoteQty, fee, feeCurrency, takerMaker."""
        price = self._dec(item.get("price"))
        quantity = self._dec(item.get("quantity"))
        quote = self._dec(item.get("quoteQty"), default=str(price * quantity))
        fee_token = str(item.get("feeCurrency") or self._received_asset(order)).upper()
        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            flat_fees=[TokenAmount(amount=self._dec(item.get("fee")), token=fee_token)],
        )
        return TradeUpdate(
            trade_id=str(item["tradeId"]),
            client_order_id=order.client_order_id,
            exchange_order_id=str(item.get("orderId") or order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=quantity,
            fill_quote_amount=quote,
            fill_price=price,
            fill_timestamp=int(item.get("time") or self.current_timestamp * 1e3) * 1e-3,
            is_taker=str(item.get("takerMaker", "taker")).lower() == "taker",
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        REST backstop for fills (GET /v4/trade?orderId=). Paginated by hasNext + fromId/direction; the
        direction semantics are not documented, so rows are de-duplicated by tradeId and the page
        shape is audit-logged once. An arbitrage leg normally fits one page of 100.
        """
        if order.exchange_order_id is None:
            return []
        params: Dict[str, Any] = {"orderId": order.exchange_order_id, "bizType": CONSTANTS.BIZ_TYPE_SPOT,
                                  "limit": CONSTANTS.TRADE_PAGE_SIZE}
        updates: Dict[str, TradeUpdate] = {}
        for page in range(CONSTANTS.TRADE_MAX_PAGES):
            response = self._raise_on_error(
                await self._api_get(path_url=CONSTANTS.TRADE_PATH, params=dict(params), is_auth_required=True,
                                    limit_id=CONSTANTS.TRADE_PATH),
                f"Error fetching fills for order {order.client_order_id}",
            )
            result = response.get("result") or {}
            items = result.get("items") or []
            if items:
                self._audit_once(CONSTANTS.AUDIT_ONCE_REST_TRADE_KEYS, keys=sorted(items[0].keys()),
                                 fee=items[0].get("fee"), fee_ccy=items[0].get("feeCurrency"),
                                 taker_maker=items[0].get("takerMaker"), deduct=items[0].get("deductType"))
            self._audit_once(CONSTANTS.AUDIT_ONCE_PAGINATION, has_next=result.get("hasNext"),
                             has_prev=result.get("hasPrev"), rows=len(items), page=page)
            for item in items:
                if str(item.get("orderId", order.exchange_order_id)) != str(order.exchange_order_id):
                    continue
                updates[str(item["tradeId"])] = self._trade_update_from_rest(item, order)
            if not result.get("hasNext") or not items:
                break
            params["fromId"] = items[-1]["tradeId"]
            params["direction"] = "NEXT"
        else:
            self.logger().warning(f"Stopped paging XT fills for {order.client_order_id} at "
                                  f"{CONSTANTS.TRADE_MAX_PAGES} pages; some fills may be missing.")
        return self._fills_not_yet_counted(order, list(updates.values()))

    def _fills_not_yet_counted(self, order: InFlightOrder, rest_fills: List[TradeUpdate]) -> List[TradeUpdate]:
        """
        The REST rows for one order are XT's complete record of its fills, so their total is the true
        executed quantity. Hummingbot de-duplicates fills by trade id only and never caps an order's
        executed amount. If the WS push and the REST row ever carried different ids for the same fill,
        every status poll would count that fill again. So a REST row with an unseen id is applied only
        while the order's executed amount is still below XT's total: exact when the ids agree, and a
        cap when they do not. Whether they agree is audit-logged the first time both are seen.
        """
        known = order.order_fills
        if known and rest_fills:
            self._audit_once("fill-ids-match", ws_ids=sorted(known)[:3], rest_ids=sorted(f.trade_id for f in rest_fills)[:3],
                             match=any(f.trade_id in known for f in rest_fills))
        room = sum((f.fill_base_amount for f in rest_fills), Decimal("0")) - order.executed_amount_base
        counted: List[TradeUpdate] = []
        for fill in sorted(rest_fills, key=lambda f: f.fill_timestamp):
            if fill.trade_id in known:
                continue
            if fill.fill_base_amount > room + CONSTANTS.FILL_AMOUNT_TOLERANCE:
                self._audit("fill-capped", client_id=order.client_order_id, trade_id=fill.trade_id,
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
        return counted

    # ------------------------------------------------------------------ fees

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
        """Per-market makerFeeRate / takerFeeRate from /v4/public/symbol (0.2%/0.2% on 1179 of 1185)."""
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        if web_utils.is_error_response(exchange_info):
            self.logger().warning(f"Could not refresh XT trading fees: {exchange_info}")
            return
        for info in (exchange_info.get("result") or {}).get("symbols") or []:
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=info["symbol"])
                self._trading_fees[trading_pair] = TradeFeeSchema(
                    maker_percent_fee_decimal=Decimal(str(info["makerFeeRate"])),
                    taker_percent_fee_decimal=Decimal(str(info["takerFeeRate"])),
                )
            except Exception:
                continue

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.BALANCES_PATH, is_auth_required=True,
                                limit_id=CONSTANTS.BALANCES_PATH),
            "Error fetching XT balances",
        )
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for entry in (response.get("result") or {}).get("assets") or []:
            asset = str(entry["currency"]).upper()
            available = self._dec(entry.get("availableAmount"))
            total = (self._dec(entry.get("totalAmount")) if entry.get("totalAmount") is not None
                     else available + self._dec(entry.get("frozenAmount")))
            self._account_available_balances[asset] = available
            self._account_balances[asset] = total
            remote_asset_names.add(asset)
        for asset_name in local_asset_names.difference(remote_asset_names):
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for info in (exchange_info.get("result") or {}).get("symbols") or []:
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                mapping[info["symbol"]] = combine_to_hb_trading_pair(
                    base=str(info["baseCurrency"]).upper(), quote=str(info["quoteCurrency"]).upper())
            except Exception as exception:
                self.logger().error(f"Error parsing XT symbol {info.get('symbol')}: {exception}")
        self._set_trading_pair_symbol_map(mapping)

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        """
        Runtime add of a pair missing from the startup map. The base builds `f"{base}{quote}"`
        ("BTCUSDT"), which XT does not recognise, so the pair would subscribe and trade under a dead
        symbol with no error (the OKX 'hyphen poison' class). XT's form is lowercase with an
        underscore. A pair that is missing from the startup map was filtered out there as not
        API-tradable, so its orders will be refused (SYMBOL_005); that is said loudly.
        """
        symbol_map = await self.trading_pair_symbol_map()
        if trading_pair in symbol_map.inverse:
            return
        base, quote = split_hb_trading_pair(trading_pair)
        exchange_symbol = f"{base.lower()}_{quote.lower()}"
        symbol_map[exchange_symbol] = trading_pair
        self.logger().warning(
            f"XT {trading_pair} was not in the startup symbol map: it is not API-tradable (offline, trading "
            f"disabled or openapiEnabled=false). Added as {exchange_symbol} for market data; orders on it will "
            f"be rejected with {CONSTANTS.MC_SYMBOL_NOT_API_TRADABLE}.")

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        pricePrecision / quantityPrecision give the default increments. Filters override them when
        present: QUANTITY (min, max, tickSize) exists on a handful of tiny-price markets with lot sizes
        of 10/100/1000 units, QUOTE_QTY.min is the minimum notional ($1 or $5 on nearly all), and PRICE
        has not been seen on any USDT market (live 2026-09-23).
        """
        rules: List[TradingRule] = []
        for info in (exchange_info_dict.get("result") or {}).get("symbols") or []:
            if not utils.is_exchange_information_valid(info):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=info["symbol"])
                filters = {f.get("filter"): f for f in info.get("filters") or []}
                price_increment = Decimal(1).scaleb(-int(info["pricePrecision"]))
                if (filters.get("PRICE") or {}).get("tickSize"):
                    price_increment = Decimal(str(filters["PRICE"]["tickSize"]))
                amount_increment = Decimal(1).scaleb(-int(info["quantityPrecision"]))
                quantity_filter = filters.get("QUANTITY") or {}
                if quantity_filter.get("tickSize"):
                    amount_increment = Decimal(str(quantity_filter["tickSize"]))
                kwargs: Dict[str, Any] = dict(
                    trading_pair=trading_pair,
                    min_order_size=(Decimal(str(quantity_filter["min"])) if quantity_filter.get("min")
                                    else amount_increment),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_quote_amount_increment=Decimal(1).scaleb(-int(info.get("quoteCurrencyPrecision") or 8)),
                    min_notional_size=self._dec((filters.get("QUOTE_QTY") or {}).get("min")),
                )
                if quantity_filter.get("max"):
                    kwargs["max_order_size"] = Decimal(str(quantity_filter["max"]))
                rules.append(TradingRule(**kwargs))
            except Exception:
                self.logger().exception(f"Error parsing the XT trading rule {info.get('symbol')}. Skipping.")
        return rules

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.TICKER_PRICE_PATH, params={"symbol": symbol}),
            f"Error fetching the last XT price for {trading_pair}",
        )
        entries = response.get("result") or []
        if not entries:
            raise IOError(f"No XT ticker for {trading_pair}")
        return float(entries[0]["p"])

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                topic = event_message.get("topic")
                data = event_message.get("data") or {}
                if topic == CONSTANTS.WS_PRIVATE_TOPIC_ORDER:
                    self._process_order_push(data)
                elif topic == CONSTANTS.WS_PRIVATE_TOPIC_TRADE:
                    self._process_trade_push(data)
                elif topic == CONSTANTS.WS_PRIVATE_TOPIC_BALANCE:
                    self._process_balance_push(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the XT user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Optional[str], fillable: bool) -> Optional[InFlightOrder]:
        tracker = self._order_tracker
        by_client = tracker.all_fillable_orders if fillable else tracker.all_updatable_orders
        order = by_client.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id is not None:
            by_exchange = (tracker.all_fillable_orders_by_exchange_order_id if fillable
                           else tracker.all_updatable_orders_by_exchange_order_id)
            order = by_exchange.get(str(exchange_order_id))
        return order

    def _process_order_push(self, data: Dict[str, Any]) -> None:
        """
        Order push (WebSocket Private/OrderChange): i = order id, ci = client order id, st = state,
        eq = executed qty, oq = original qty, lq = remaining qty. It is also what ties a client order id
        to its exchange id when a fill arrived first (see _process_trade_push).
        """
        self._audit_once(CONSTANTS.AUDIT_ONCE_ORDER_KEYS, keys=sorted(data.keys()))
        client_order_id = str(data.get("ci") or "")
        exchange_order_id = str(data["i"]) if data.get("i") is not None else None
        tracked_order = self._locate_order(client_order_id, exchange_order_id, fillable=False)
        if tracked_order is None:
            return
        executed = self._dec(data.get("eq"))
        new_state = self._order_state(data.get("st"), executed, self._dec(data.get("oq")))
        self._audit("order-push", client_id=client_order_id, order_id=exchange_order_id, st=data.get("st"),
                    state=str(new_state), eq=data.get("eq"), lq=data.get("lq"))
        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(data.get("t") or self.current_timestamp * 1e3) * 1e-3,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id or tracked_order.exchange_order_id,
        ))
        if exchange_order_id is not None:
            # The tracker applies the exchange id on a later loop pass, so the parked fills are
            # replayed onto the order found here by client id, not looked up by exchange id.
            self._replay_pending_fills(exchange_order_id, tracked_order)
        # Safety net: the push says more was executed than the fills we hold, e.g. a trade push that
        # never came. Fetch this order's fills over REST (de-duplicated by trade id downstream).
        # CANCELED is included: a partial fill whose push was lost must still be counted before the
        # order leaves the tracker.
        if (new_state in (OrderState.PARTIALLY_FILLED, OrderState.FILLED, OrderState.CANCELED)
                and executed > tracked_order.executed_amount_base):
            safe_ensure_future(self._fetch_fills_for(tracked_order, executed))

    async def _fetch_fills_for(self, order: InFlightOrder, reported_executed: Decimal) -> None:
        await asyncio.sleep(0.5)   # give the trade push its chance first
        if order.executed_amount_base >= reported_executed:
            return
        self._audit("fill-backfill", client_id=order.client_order_id, reported=str(reported_executed),
                    held=str(order.executed_amount_base))
        try:
            for trade_update in await self._all_trade_updates_for_order(order):
                self._order_tracker.process_trade_update(trade_update)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"XT fill backfill for {order.client_order_id} failed: {e}")

    def _process_trade_push(self, data: Dict[str, Any]) -> None:
        """
        Fill push (WebSocket Private/OrderFilled). Documented keys: i = trade id, oi = order id, p, q,
        v = quote qty, b = buyer is maker, tm = 1 taker / 2 maker. It carries NO client order id and NO
        fee (per the docs; the live key set is audit-logged). A fill that arrives before the placement
        response has given the order its exchange id cannot be matched yet, so it is parked for the order
        push to link.
        """
        self._audit_once(CONSTANTS.AUDIT_ONCE_TRADE_KEYS, keys=sorted(data.keys()), sample=data)
        exchange_order_id = str(data["oi"]) if data.get("oi") is not None else None
        tracked_order = self._locate_order("", exchange_order_id, fillable=True)
        if tracked_order is None:
            if exchange_order_id is None:
                self._audit("fill-unattributed", data=data)
                return
            self._pending_fills.setdefault(exchange_order_id, []).append((time.time(), data))
            self._audit("fill-parked", order_id=exchange_order_id, trade_id=data.get("i"))
            self._prune_pending_fills()
            # Covers the order push having come first but not yet been applied by the tracker.
            safe_ensure_future(self._retry_parked_fills(exchange_order_id))
            return
        self._apply_push_fill(data, tracked_order)

    async def _retry_parked_fills(self, exchange_order_id: str) -> None:
        for _ in range(5):
            await asyncio.sleep(0.2)
            if exchange_order_id not in self._pending_fills:
                return
            self._replay_pending_fills(exchange_order_id)

    def _replay_pending_fills(self, exchange_order_id: str, tracked_order: Optional[InFlightOrder] = None) -> None:
        parked = self._pending_fills.pop(exchange_order_id, None)
        if not parked:
            return
        tracked_order = tracked_order or self._locate_order("", exchange_order_id, fillable=True)
        if tracked_order is None:
            self._pending_fills[exchange_order_id] = parked
            return
        for _, data in parked:
            self._apply_push_fill(data, tracked_order)
        self._audit("fill-replayed", order_id=exchange_order_id, count=len(parked))

    def _prune_pending_fills(self) -> None:
        cutoff = time.time() - CONSTANTS.PENDING_FILL_TTL_SECONDS
        for exchange_order_id in list(self._pending_fills):
            kept = [(ts, d) for ts, d in self._pending_fills[exchange_order_id] if ts >= cutoff]
            if len(kept) != len(self._pending_fills[exchange_order_id]):
                self._audit("fill-expired", order_id=exchange_order_id,
                            dropped=len(self._pending_fills[exchange_order_id]) - len(kept))
            if kept:
                self._pending_fills[exchange_order_id] = kept
            else:
                del self._pending_fills[exchange_order_id]

    def _apply_push_fill(self, data: Dict[str, Any], order: InFlightOrder) -> None:
        """
        One WS fill onto its order, with the two checks Hummingbot does not make:
          - an order is never filled past its size: a push that would do that is a duplicate
          - a fill already recorded from REST (the backfill ran first) is not counted again when its
            WS push arrives, even if XT gave the two different ids: same price, same quantity, within
            FILL_MATCH_SECONDS
        """
        trade_update = self._trade_update_from_push(data, order)
        if trade_update.trade_id in order.order_fills:
            return
        if order.executed_amount_base + trade_update.fill_base_amount > order.amount + CONSTANTS.FILL_AMOUNT_TOLERANCE:
            self._audit("fill-over-amount", client_id=order.client_order_id, trade_id=trade_update.trade_id,
                        executed=str(order.executed_amount_base), fill=str(trade_update.fill_base_amount),
                        amount=str(order.amount))
            return
        for known_id, known in order.order_fills.items():
            if (known_id in self._rest_fill_ids
                    and known.fill_base_amount == trade_update.fill_base_amount
                    and known.fill_price == trade_update.fill_price
                    and abs(known.fill_timestamp - trade_update.fill_timestamp) <= CONSTANTS.FILL_MATCH_SECONDS):
                self._audit("fill-duplicate-of-rest", client_id=order.client_order_id, ws_id=trade_update.trade_id,
                            rest_id=known_id)
                return
        self._order_tracker.process_trade_update(trade_update)

    def _trade_update_from_push(self, data: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        price = self._dec(data.get("p"))
        quantity = self._dec(data.get("q"))
        quote = self._dec(data.get("v"), default=str(price * quantity))
        tm = data.get("tm")
        if tm in (1, 2, "1", "2"):
            is_taker = str(tm) == "1"
        else:
            # b = buyer is maker. Our BUY is then the maker; our SELL is then the taker.
            buyer_is_maker = bool(data.get("b"))
            is_taker = buyer_is_maker if order.trade_type is TradeType.SELL else not buyer_is_maker
        fee_amount = data.get("fee", data.get("f"))
        fee_token = data.get("feeCurrency") or data.get("fc")
        if fee_amount is not None and fee_token:
            flat_fee = TokenAmount(amount=self._dec(fee_amount), token=str(fee_token).upper())
        else:
            # No fee in the push: charge the market's own rate on the received asset (XT's SDK example
            # shows the fee in the received asset). The REST row carries the exact fee; this one is the
            # fill of record because trade ids are de-duplicated. Audit-logged so the gap is visible.
            schema = self._trading_fees.get(order.trading_pair) or utils.DEFAULT_FEES
            rate = schema.taker_percent_fee_decimal if is_taker else schema.maker_percent_fee_decimal
            received = quantity if order.trade_type is TradeType.BUY else quote
            flat_fee = TokenAmount(amount=received * rate, token=self._received_asset(order))
            self._audit_once("fee-estimated-from-rate", rate=str(rate), token=flat_fee.token)
        fee = TradeFeeBase.new_spot_fee(fee_schema=self.trade_fee_schema(), trade_type=order.trade_type,
                                        flat_fees=[flat_fee])
        return TradeUpdate(
            trade_id=str(data["i"]),
            client_order_id=order.client_order_id,
            exchange_order_id=str(data.get("oi") or order.exchange_order_id),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=quantity,
            fill_quote_amount=quote,
            fill_price=price,
            fill_timestamp=int(data.get("t") or self.current_timestamp * 1e3) * 1e-3,
            is_taker=is_taker,
        )

    def _process_balance_push(self, data: Dict[str, Any]) -> None:
        """Balance push (WebSocket Private/BalanceChange): c = currency, b = total, f = frozen, z = SPOT|LEVER."""
        self._audit_once("balance-push-key-set", keys=sorted(data.keys()))
        if str(data.get("z") or CONSTANTS.BIZ_TYPE_SPOT).upper() != CONSTANTS.BIZ_TYPE_SPOT:
            return
        asset = str(data.get("c") or "").upper()
        if not asset:
            return
        total = self._dec(data.get("b"))
        frozen = self._dec(data.get("f"))
        self._account_balances[asset] = total
        self._account_available_balances[asset] = total - frozen
        if CONSTANTS.LIVE_AUDIT_LOGGING and self._balance_pushes_audited < CONSTANTS.BALANCE_AUDIT_PUSHES:
            self._balance_pushes_audited += 1
            safe_ensure_future(self._audit_balance_push(asset, data))

    async def _audit_balance_push(self, asset: str, push: Dict[str, Any]) -> None:
        """AUDIT: the docs call `b` the total and `f` the frozen amount. The first few pushes are logged
        next to XT's REST balance for the same asset, which settles what they really are."""
        try:
            response = await self._api_get(path_url=CONSTANTS.BALANCE_PATH, params={"currency": asset.lower()},
                                           is_auth_required=True, limit_id=CONSTANTS.BALANCE_PATH)
            rest = response.get("result") if not web_utils.is_error_response(response) else response
        except Exception as e:
            rest = f"REST error {e}"
        self._audit("balance-push-vs-rest", asset=asset, push_b=push.get("b"), push_f=push.get("f"),
                    push_z=push.get("z"), rest=rest)
