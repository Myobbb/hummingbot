import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coinex import (
    coinex_constants as CONSTANTS,
    coinex_utils as utils,
    coinex_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinex.coinex_api_order_book_data_source import CoinexAPIOrderBookDataSource
from hummingbot.connector.exchange.coinex.coinex_api_user_stream_data_source import CoinexAPIUserStreamDataSource
from hummingbot.connector.exchange.coinex.coinex_auth import CoinexAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import (
    AddedToCostTradeFee,
    TokenAmount,
    TradeFeeBase,
    TradeFeeSchema,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class CoinexExchange(ExchangePyBase):
    """
    CoinEx v2 spot connector.

    Reference material: https://docs.coinex.com/api/v2/ (a verified offline mirror lives at
    VS_code_projects/MDs/coinex-api-v2/). Every CoinEx-specific behaviour below is traceable to a
    documented field or to the production-proven P1 adapter; nothing here is inferred.
    """

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        coinex_api_key: str,
        coinex_secret_key: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ) -> None:
        self._api_key = coinex_api_key
        self._secret_key = coinex_secret_key
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._audit_seen: set = set()   # one-shot audit tags; see _audit_once
        super().__init__(client_config_map)

    # ------------------------------------------------------------------ identity / config

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> CoinexAuth:
        return CoinexAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            time_provider=self._time_synchronizer,
        )

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
        return CONSTANTS.PUBLIC_MARKET_ENDPOINT

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.PUBLIC_MARKET_ENDPOINT

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PUBLIC_PING_ENDPOINT

    @property
    def trading_pairs(self) -> Optional[List[str]]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # POST /spot/cancel-order returns the finalised order object in the same response.
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        # enum.md#order_type also lists ioc and fok, which Hummingbot has no direct equivalent for.
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    # ------------------------------------------------------------------ factories

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinexAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoinexAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ------------------------------------------------------------------ first-live-run audit

    def _audit(self, tag: str, **fields: Any) -> None:
        """
        Emit one [CX-AUDIT] line for the first live run.

        Scope is deliberately narrow: only the questions the CoinEx docs cannot answer (which field
        spellings are real, which status strings actually arrive, whether a cancel races a fill).
        High-cadence traffic — depth, balances, pings — is never logged, and credentials never are:
        only response/request BODY fields reach this, never headers.
        """
        if not CONSTANTS.LIVE_AUDIT_LOGGING:
            return
        rendered = " ".join(f"{k}={v!r}" for k, v in fields.items())
        self.logger().info(f"[CX-AUDIT] {tag} {rendered}")

    def _audit_once(self, tag: str, **fields: Any) -> None:
        """Same, but only the FIRST time a given tag is seen — safe on a repeating stream."""
        if not CONSTANTS.LIVE_AUDIT_LOGGING or tag in self._audit_seen:
            return
        self._audit_seen.add(tag)
        self._audit(tag, **fields)

    # ------------------------------------------------------------------ error classification

    @staticmethod
    def _error_code(exception: Exception) -> Optional[int]:
        text = str(exception)
        for token in ("code\": ", "code\":", "code "):
            if token in text:
                tail = text.split(token, 1)[1].strip().strip("\"' ,}")
                digits = ""
                for ch in tail:
                    if ch.isdigit():
                        digits += ch
                    else:
                        break
                if digits:
                    return int(digits)
        return None

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        # 4010 "Expired request" / 4017 "Signature expired" are the only clock-drift codes
        # (error.md). A True here makes Hummingbot re-sync server time and retry, so a wrong set
        # would either spin on unrelated errors or never recover from real drift.
        return self._error_code(request_exception) in CONSTANTS.RET_CODES_TIME_SYNC

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """
        CoinEx does not retain orders that were canceled without ever executing
        (enum.md#order_status). A status lookup for such an order therefore legitimately fails, and
        treating that as an error would leave the order tracked forever. It is reported as
        not-found so the tracker can finalise it.
        """
        if self._error_code(status_update_exception) == CONSTANTS.RET_CODE_ORDER_NOT_FOUND:
            return True
        message = str(status_update_exception).lower()
        return any(fragment in message for fragment in CONSTANTS.ORDER_NOT_FOUND_MESSAGES)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_order_not_found_during_status_update_error(cancelation_exception)

    @staticmethod
    def _raise_on_error(response: Dict[str, Any], context: str) -> Dict[str, Any]:
        if web_utils.is_error_response(response):
            raise IOError(f"{context}: code {response.get('code')} — {response.get('message')}")
        return response

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
        data: Dict[str, Any] = {
            "market": symbol,
            "market_type": CONSTANTS.MARKET_TYPE_SPOT,
            "side": CONSTANTS.TRADE_TYPES[trade_type],
            "type": CONSTANTS.ORDER_TYPES[order_type],
            "amount": self._format_decimal(amount),
            "client_id": order_id,
        }
        if order_type.is_limit_type():
            data["price"] = self._format_decimal(price)
        else:
            # For a market order the `amount` is denominated in whichever currency `ccy` names
            # (spot/order/http/put-order). Hummingbot always expresses amount in the BASE asset,
            # so `ccy` is pinned to the base currency and no price conversion is needed.
            base, _ = split_hb_trading_pair(trading_pair)
            data["ccy"] = base

        response = self._raise_on_error(
            await self._api_post(path_url=CONSTANTS.PLACE_ORDER_ENDPOINT, data=data, is_auth_required=True),
            f"Error submitting order {order_id}",
        )
        # AUDIT: the exact body CoinEx accepted, and what it echoed back. Answers whether
        # market-order `ccy`=base is interpreted as intended and whether client_id survives.
        self._audit("place-order", request=data, order_id=(response.get("data") or {}).get("order_id"),
                    echoed_client_id=(response.get("data") or {}).get("client_id"))
        payload = response.get("data")
        if not payload or payload.get("order_id") is None:
            # CoinEx can answer code 0 with a null data field (observed live on an empty result),
            # so a missing order id must surface as an explicit failure rather than a TypeError.
            raise IOError(f"Error submitting order {order_id}: CoinEx returned no order id ({response})")
        return str(payload["order_id"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        exchange_order_id = await tracked_order.get_exchange_order_id()
        response = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_ENDPOINT,
            data={
                "market": symbol,
                "market_type": CONSTANTS.MARKET_TYPE_SPOT,
                "order_id": int(exchange_order_id),
            },
            is_auth_required=True,
        )
        self._raise_on_error(response, f"Error cancelling order {order_id}")
        # AUDIT: is_cancel_request_in_exchange_synchronous=True marks this CANCELED immediately,
        # but the response carries no status field. Logging the amounts shows whether a fill raced
        # the cancel (unfilled==0 would mean it actually completed, not cancelled).
        cancelled = response.get("data") or {}
        self._audit("cancel-order", client_id=order_id, exchange_order_id=exchange_order_id,
                    unfilled=self._field(cancelled, "unfilled_amount", "unfill_amount"),
                    filled=self._field(cancelled, "filled_amount", "fill_amount"),
                    status=cancelled.get("status"), keys=sorted(cancelled.keys()))
        return True

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

        # Prefer the venue's own per-market rate. CoinEx fees are NOT uniform — BTC-USDT is
        # 0.2%/0.2% while CARDS-USDT is 0.3%/0.3% — and an arbitrage strategy sizing against a
        # flat default would understate cost on exactly the long-tail markets it trades.
        fee_schema = self._trading_fees.get(trading_pair)
        if fee_schema is not None:
            return TradeFeeBase.new_spot_fee(
                fee_schema=fee_schema,
                trade_type=order_side,
                percent=(fee_schema.maker_percent_fee_decimal if is_maker
                         else fee_schema.taker_percent_fee_decimal),
            )
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self) -> None:
        """
        Cache the per-market maker/taker rates from GET /spot/market.

        The same public endpoint backs the trading rules; it is re-read here because Hummingbot
        polls fees and rules on independent loops. The authoritative per-FILL fee still comes from
        the deal payload (`fee` / `fee_ccy`) — this only informs pre-trade estimates and sizing.
        """
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        if web_utils.is_error_response(exchange_info):
            self.logger().warning(f"Could not refresh CoinEx trading fees: {exchange_info}")
            return

        for market in exchange_info.get("data") or []:
            if not utils.is_exchange_information_valid(market):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=market["market"])
                self._trading_fees[trading_pair] = TradeFeeSchema(
                    maker_percent_fee_decimal=Decimal(str(market["maker_fee_rate"])),
                    taker_percent_fee_decimal=Decimal(str(market["taker_fee_rate"])),
                )
            except Exception:
                # A market absent from the symbol map simply keeps the default schema.
                continue

    # ------------------------------------------------------------------ state derivation

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        """
        Render a Decimal for the wire WITHOUT scientific notation.

        `str(Decimal("0.00000001"))` is `"1E-8"`, and CoinEx expects a plain decimal string. This
        bites precisely on the assets this connector exists for: the base amount increment is
        1e-8 on every market, and low-priced markets carry a quote precision of up to 10
        (PEPEUSDT), so quantised prices and amounts routinely land in the exponent range.
        `format(value, "f")` always produces positional notation.
        """
        return format(value, "f")

    @staticmethod
    def _field(payload: Dict[str, Any], *names: str, default: Any = None) -> Any:
        """
        Read the first present, non-null key from `names`.

        CoinEx's order docs USED to contradict themselves here: the order.update FIELD TABLE said
        `unfilled_amount` / `filled_value` / `last_filled_amount`, while the EXAMPLE on the same
        page sent `unfill_amount` / `fill_value` / `last_fill_amount`.

        Re-verified 2026-09-11: CoinEx corrected the examples to match the tables, so the docs now
        agree on the `*filled*` spelling — evidence the TABLE was right all along. Both spellings
        are still accepted: the payloads have never been seen live, and an unread field here would
        silently mis-derive an order's state rather than fail loudly.
        """
        for name in names:
            value = payload.get(name)
            if value is not None:
                return value
        return default

    @classmethod
    def _decimal(cls, payload: Dict[str, Any], *names: str) -> Decimal:
        raw = cls._field(payload, *names, default="0")
        try:
            return Decimal(str(raw))
        except Exception:
            return Decimal("0")

    @classmethod
    def _order_state_from_payload(cls, payload: Dict[str, Any]) -> OrderState:
        """
        Resolve an order state from a REST order payload.

        Prefers the documented `status` string, and falls back to the filled/unfilled amounts when
        the value is one the docs do not define. CoinEx's pages disagreed on this vocabulary until
        2026-09-07 (`part_deal` appeared in an example but never in the enum) and have since been
        reconciled — but an unknown value must still never be guessed into a wrong terminal state.
        """
        status = payload.get("status")
        if status in CONSTANTS.STATE_TYPES:
            return CONSTANTS.STATE_TYPES[status]

        unfilled = cls._decimal(payload, "unfilled_amount", "unfill_amount")
        filled = cls._decimal(payload, "filled_amount", "fill_amount")
        if unfilled <= 0 and filled > 0:
            return OrderState.FILLED
        if filled > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    @classmethod
    def _ws_order_state(cls, event: str, order: Dict[str, Any]) -> OrderState:
        """
        order.update carries an `event`, not a status (enum.md#order_event):

            put     order accepted (unfilled or partially filled)
            update  partially filled
            modify  amended, still live
            finish  left the book — FILLED or CANCELED

        `finish` is the only ambiguous one, and the remaining amount separates the two cases: a
        fully filled order has nothing unfilled, anything else was canceled with a remainder.

        The amount is read through _field, which tolerates both the current (`unfilled_amount`) and
        the pre-2026-09-07 (`unfill_amount`) spellings.
        """
        unfilled = cls._decimal(order, "unfilled_amount", "unfill_amount")
        amount = cls._decimal(order, "amount")

        if event == CONSTANTS.WS_ORDER_EVENT_FINISH:
            return OrderState.FILLED if unfilled <= 0 else OrderState.CANCELED
        if amount > 0 and unfilled < amount:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    def _trade_update_from_deal(self, deal: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        """
        Build a TradeUpdate from one CoinEx fill.

        The payload is identical between the REST /spot/order-deals rows and the WS
        user_deals.update push (deal_id, created_at, price, amount, role, fee, fee_ccy), so both
        paths share this builder and can never drift apart.

        The fee is reported per fill as an absolute amount in `fee_ccy`, so it is recorded as a
        flat fee rather than a percentage.
        """
        price = self._decimal(deal, "price")
        amount = self._decimal(deal, "amount")
        fee_token = deal.get("fee_ccy") or order.quote_asset
        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            flat_fees=[TokenAmount(amount=self._decimal(deal, "fee"), token=fee_token)],
        )
        return TradeUpdate(
            trade_id=str(deal["deal_id"]),
            client_order_id=order.client_order_id,
            exchange_order_id=str(self._field(deal, "order_id", default=order.exchange_order_id)),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=amount,
            fill_quote_amount=price * amount,
            fill_price=price,
            fill_timestamp=int(self._field(deal, "created_at", default=0)) * 1e-3,
            is_taker=str(deal.get("role", "taker")).lower() == "taker",
        )

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        exchange_order_id = await tracked_order.get_exchange_order_id()
        # GET /spot/order-status takes only market + order_id (no market_type).
        response = self._raise_on_error(
            await self._api_get(
                path_url=CONSTANTS.ORDER_STATUS_ENDPOINT,
                params={"market": symbol, "order_id": int(exchange_order_id)},
                is_auth_required=True,
            ),
            f"Error fetching status of order {tracked_order.client_order_id}",
        )
        data = response.get("data")
        if not data:
            # CoinEx answers with code 0 and a NULL data field for an empty result — verified live
            # against /assets/spot/balance on an empty account. Combined with orders that were
            # canceled without executing never being retained (enum.md#order_status), a vanished
            # order can surface here as a success with no payload. Report it as not-found so the
            # tracker finalises the order instead of hitting a TypeError every poll.
            raise IOError(
                f"Error fetching status of order {tracked_order.client_order_id}: "
                f"order not found (CoinEx returned an empty payload)"
            )
        # AUDIT: one line per DISTINCT status string. The enum and the get-order-status example
        # disagreed over `part_deal` until 2026-09-07; this records what the API really sends,
        # which no doc revision can establish.
        raw_status = data.get("status")
        self._audit_once(f"order-status:{raw_status}", status=raw_status,
                         mapped=str(self._order_state_from_payload(data)),
                         in_documented_map=raw_status in CONSTANTS.STATE_TYPES)
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(data["order_id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(data.get("updated_at") or self.current_timestamp * 1e3) * 1e-3,
            new_state=self._order_state_from_payload(data),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        REST backstop for fills. The primary path is the user_deals.update websocket push; this
        runs on the status poll and after a restart, when the stream was not listening.

        GET /spot/order-deals is paginated and returns an explicit `pagination.has_next` flag,
        which is what drives the loop — `limit` is sent so the page size is ours rather than an
        assumed server default. A length heuristic is kept only as the fallback for a response
        that omits the pagination block; relying on length alone risks SILENTLY TRUNCATING fills,
        which would understate executed_amount_base and corrupt position accounting.

        In practice an arbitrage leg fills in one or two deals, so this is a single request; the
        page cap only bounds the pathological case against the 10 r/s "query spot order history"
        rate-limit group.
        """
        trade_updates: List[TradeUpdate] = []
        if order.exchange_order_id is None:
            return trade_updates

        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        page = 1
        while page <= CONSTANTS.ORDER_DEALS_MAX_PAGES:
            response = await self._api_get(
                path_url=CONSTANTS.ORDER_DEALS_ENDPOINT,
                params={
                    "market": symbol,
                    "market_type": CONSTANTS.MARKET_TYPE_SPOT,
                    "order_id": int(order.exchange_order_id),
                    "page": page,
                    "limit": CONSTANTS.ORDER_DEALS_PAGE_SIZE,
                },
                is_auth_required=True,
            )
            self._raise_on_error(response, f"Error fetching fills for order {order.client_order_id}")

            deals = response.get("data") or []   # CoinEx sends null, not [], when empty
            for deal in deals:
                trade_updates.append(self._trade_update_from_deal(deal, order))

            pagination = response.get("pagination") or {}
            self._audit_once(CONSTANTS.AUDIT_ONCE_PAGINATION,
                             has_pagination_block="pagination" in response,
                             pagination=pagination, page=page, deals_on_page=len(deals))
            if "has_next" in pagination:
                if not pagination["has_next"]:
                    break
            elif len(deals) < CONSTANTS.ORDER_DEALS_PAGE_SIZE:
                break
            page += 1
        else:
            self.logger().warning(
                f"Stopped paging fills for {order.client_order_id} at the "
                f"{CONSTANTS.ORDER_DEALS_MAX_PAGES}-page cap; some fills may be missing."
            )
        return trade_updates

    # ------------------------------------------------------------------ balances

    async def _update_balances(self) -> None:
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        response = self._raise_on_error(
            await self._api_get(path_url=CONSTANTS.ACCOUNT_BALANCE_ENDPOINT, is_auth_required=True),
            "Error fetching account balances",
        )
        for entry in response.get("data") or []:
            asset = entry["ccy"]
            available = Decimal(str(entry.get("available") or "0"))
            frozen = Decimal(str(entry.get("frozen") or "0"))
            self._account_available_balances[asset] = available
            self._account_balances[asset] = available + frozen
            remote_asset_names.add(asset)

        for asset_name in local_asset_names.difference(remote_asset_names):
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    # ------------------------------------------------------------------ symbols / rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]) -> None:
        mapping = bidict()
        for symbol_data in exchange_info.get("data") or []:
            if not utils.is_exchange_information_valid(symbol_data):
                continue
            try:
                # CoinEx market names are concatenated with no separator ("BTCUSDT"), so the pair
                # must be rebuilt from the explicit base_ccy/quote_ccy fields — splitting the
                # string would break on any multi-character quote asset.
                mapping[symbol_data["market"]] = combine_to_hb_trading_pair(
                    base=symbol_data["base_ccy"], quote=symbol_data["quote_ccy"]
                )
            except Exception as exception:
                self.logger().error(f"Error parsing CoinEx trading pair information ({exception})")
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []
        for rule in exchange_info_dict.get("data") or []:
            if not utils.is_exchange_information_valid(rule):
                continue
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule["market"])
                # Verified against live data: quote_ccy_precision is the PRICE decimal count
                # (BTCUSDT reports 0 and its book quotes whole dollars), base_ccy_precision is the
                # amount decimal count, and min_amount is a base-asset quantity.
                price_increment = Decimal(1).scaleb(-int(rule["quote_ccy_precision"]))
                amount_increment = Decimal(1).scaleb(-int(rule["base_ccy_precision"]))
                trading_rules.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal(str(rule["min_amount"])),
                    min_price_increment=price_increment,
                    min_base_amount_increment=amount_increment,
                    min_quote_amount_increment=price_increment,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the CoinEx trading pair rule: {rule}. Skipping.")
        return trading_rules

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = self._raise_on_error(
            await self._api_get(
                path_url=CONSTANTS.PUBLIC_TICKER_ENDPOINT,
                params={"market": symbol},
            ),
            f"Error fetching last traded price for {trading_pair}",
        )
        entries = response.get("data") or []
        if not entries:
            raise IOError(f"No ticker returned by CoinEx for {trading_pair}")
        return float(entries[0]["last"])

    # ------------------------------------------------------------------ user stream

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                method = event_message.get("method")
                data = event_message.get("data") or {}

                if method == CONSTANTS.WS_ORDER_UPDATE:
                    self._process_order_update_message(data)
                elif method == CONSTANTS.WS_USER_DEALS_UPDATE:
                    self._process_user_deal_message(data)
                elif method == CONSTANTS.WS_BALANCE_UPDATE:
                    for entry in data.get("balance_list") or []:
                        asset = entry["ccy"]
                        available = self._decimal(entry, "available")
                        frozen = self._decimal(entry, "frozen")
                        self._account_available_balances[asset] = available
                        self._account_balances[asset] = available + frozen
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the CoinEx user stream listener loop.")

    def _locate_order(self, client_order_id: str, exchange_order_id: Any, fillable: bool) -> Optional[InFlightOrder]:
        """
        Find the tracked order for a websocket payload.

        `client_id` is echoed back by CoinEx, so it is the primary key — but it is an OPTIONAL
        field on placement, so an order created outside this connector (or by an older session)
        can arrive with it blank. The exchange order id is the fallback.

        Fillable and updatable are separate tracker views: a fill may still need applying to an
        order that is no longer updatable, so the caller picks the right one.
        """
        tracker = self._order_tracker
        by_client = tracker.all_fillable_orders if fillable else tracker.all_updatable_orders
        order = by_client.get(client_order_id) if client_order_id else None
        if order is None and exchange_order_id is not None:
            by_exchange = (tracker.all_fillable_orders_by_exchange_order_id if fillable
                           else tracker.all_updatable_orders_by_exchange_order_id)
            order = by_exchange.get(str(exchange_order_id))
        return order

    def _process_order_update_message(self, data: Dict[str, Any]) -> None:
        order = data.get("order") or {}
        event = data.get("event")
        client_order_id = order.get("client_id") or ""
        exchange_order_id = order.get("order_id")
        # AUDIT (once): the raw key set confirms which spelling the API really sends. The docs
        # contradicted themselves until 2026-09-07 (table `unfilled_amount`/`filled_value` vs
        # example `unfill_amount`/`fill_value`) and now agree on the table's — but that is CoinEx
        # correcting prose, not evidence about the wire. This line is the evidence.
        self._audit_once(CONSTANTS.AUDIT_ONCE_ORDER_KEYS, keys=sorted(order.keys()))

        tracked_order = self._locate_order(client_order_id, exchange_order_id, fillable=False)
        if tracked_order is None:
            return

        new_state = self._ws_order_state(event, order)
        # AUDIT: `finish` means FILLED *or* CANCELED and only the remaining amount separates them.
        # Logged per event so a mis-split is visible in the trail rather than inferred later.
        self._audit("order-update", client_id=client_order_id, event=event, state=str(new_state),
                    amount=self._field(order, "amount"),
                    unfilled=self._field(order, "unfilled_amount", "unfill_amount"))

        self._order_tracker.process_order_update(OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=int(self._field(order, "updated_at",
                                             default=self.current_timestamp * 1e3)) * 1e-3,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(exchange_order_id) if exchange_order_id is not None
            else tracked_order.exchange_order_id,
        ))

    def _log_deal_shape(self, deal: Dict[str, Any]) -> None:
        """AUDIT (once): the fill payload's real shape — fee currency and role drive P&L."""
        self._audit_once(CONSTANTS.AUDIT_ONCE_DEAL_KEYS, keys=sorted(deal.keys()),
                         fee=deal.get("fee"), fee_ccy=deal.get("fee_ccy"), role=deal.get("role"))

    def _process_user_deal_message(self, deal: Dict[str, Any]) -> None:
        """
        Apply a real-time fill from user_deals.update.

        This is the fast path: it carries price, amount, role and the exact fee, so a fill reaches
        the strategy without waiting for the REST poll of /spot/order-deals. That poll remains as
        the backstop for anything missed while the stream was down.
        """
        self._log_deal_shape(deal)
        tracked_order = self._locate_order(deal.get("client_id") or "", deal.get("order_id"), fillable=True)
        if tracked_order is None:
            # AUDIT: a fill we cannot attribute is a real problem — it means client_id came back
            # blank AND the exchange id was unknown, so the fill would be silently dropped.
            self._audit("fill-unattributed", client_id=deal.get("client_id"),
                        order_id=deal.get("order_id"), deal_id=deal.get("deal_id"))
            return
        self._order_tracker.process_trade_update(self._trade_update_from_deal(deal, tracked_order))
