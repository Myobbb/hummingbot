from decimal import Decimal
from typing import Any, Dict, List, Tuple, Union
import logging

from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.event.events import LimitOrderStatus
from hummingbot.core.event.events import OrderType
from hummingbot.core.clock import Clock

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange_base import ExchangeBase

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.utils import order_age

from hummingbot.logger import HummingbotLogger
hws_logger = None

class OrderBookAlignment(StrategyPyBase):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global hws_logger
        if hws_logger is None:
            hws_logger = logging.getLogger(__name__)
        return hws_logger

    def __init__(self,
             market_info: MarketTradingPairTuple,
             target_asset_amount: Decimal,
             price_limit: Decimal,
             spread: Decimal,
             is_buy: bool,
             price_limit_retry_duration: float = 300,
             order_refresh_time: float = 10.0):

        super().__init__()
        self._market_info = market_info
        self._prev_timestamp = 0
        self._is_price_limit_timeout = False
        self._is_buy = is_buy
        self._spread = spread
        self._price_limit = price_limit
        self._price_limit_retry_duration = price_limit_retry_duration
        self._target_asset_amount = target_asset_amount
        self._order_refresh_interval = order_refresh_time
        self._active_orders = {} # {order_id, LimitOrderStatus}

        self.add_markets([market_info.market])

    def format_status(self) -> str:
        lines = ["", "  Configuration:"]
        return "\n".join(lines)

    def start(self, clock: Clock, timestamp: float):
        self.logger().info("Start OBA strategy")
        self._prev_timestamp = timestamp

    def tick(self, timestamp: float):
        # Check if update interval has passed and process strategy
        if timestamp - self._prev_timestamp < self._order_refresh_interval:
            return

        # Check if the strategy is in timeout because the price limit has been reached
        if self._is_price_limit_timeout:
            if timestamp - self._prev_timestamp < self._price_limit_retry_duration:
                return
            self._is_price_limit_timeout = False

        # Check that all markets ready
        _market_ready = all([market.ready for market in self.active_markets])
        if not _market_ready:
            for market in self.active_markets:
                if not market.ready:
                    self.logger().warning(f"Market {market.name} is not ready.")
            self.logger().warning(f"Markets are not ready. No trades are permitted.")
            return

        # Check that all markets are connected
        if not all([market.network_status is NetworkStatus.CONNECTED for market in self.active_markets]):
            self.logger().warning(
                "WARNING: Market are not connected or are down at the moment."
            )
            return

        # Cancel active orders if exists
        if self.check_and_cancel_active_orders():
            self.logger().info(f"Cancel active order")
            return

        # Place new order
        self.place_order()
        self._prev_timestamp = self.current_timestamp

    def place_order(self):
        market = self._market_info.market
        quantized_amount = market.quantize_order_amount(self._market_info.trading_pair, Decimal(self._target_asset_amount))
        base_price = market.get_price(self._market_info.trading_pair, not self._is_buy)

        if base_price == Decimal("nan"):
            self.logger().warning(f"{'Ask' if not self._is_buy else 'Bid'} orderbook for {self._market_info.trading_pair} is empty, can't calculate price.")
            return

        price_with_spread = base_price * (Decimal("1.0") + (Decimal("1.0") if self._is_buy else Decimal("-1.0")) * Decimal(self._spread) / Decimal("100.0"))
        quantized_price = market.quantize_order_price(self._market_info.trading_pair, price_with_spread)

        if (self._is_buy and quantized_price > self._price_limit) or (not self._is_buy and quantized_price < self._price_limit):
            self.logger().info(f"The price ({quantized_price}) has reached the price limit ({self._price_limit}). "
                               f"Retrying after {self._price_limit_retry_duration}.")
            self._is_price_limit_timeout = True
            return

        if quantized_amount != 0:
            if self.has_enough_balance(self._market_info, quantized_amount):
                if self._is_buy:
                    order_id = self.buy_with_specific_market(
                        self._market_info,
                        amount=quantized_amount,
                        order_type=OrderType.LIMIT,
                        price=quantized_price
                    )
                else:
                    order_id = self.sell_with_specific_market(
                        self._market_info,
                        amount=quantized_amount,
                        order_type=OrderType.LIMIT,
                        price=quantized_price
                    )
                self._active_orders[order_id] = LimitOrderStatus.OPEN
                self.logger().info(f"Create limit order: {order_id}   Base price: {base_price}   Order price: {quantized_price}")
            else:
                self.logger().info("Not enough balance to place the order. Please check balance.")
        else:
            self.logger().warning("Not valid asset amount. Please change target_asset_amount in config.")

    def check_and_cancel_active_orders(self) -> bool:
        """
        Check if there are any active orders and cancel them
        :return: True if there are active orders, False otherwise.
        """
        if len(self._active_orders.values()) == 0:
            return False

        any_canceling = False
        for key, value in self._active_orders.items():
            if value == LimitOrderStatus.CANCELING:
                continue

            any_canceling = True
            self.logger().info(f"Cancel order: {key}")
            self._active_orders[key] = LimitOrderStatus.CANCELING
            self._market_info.market.cancel(self._market_info.trading_pair, key)
        return any_canceling

    def remove_order_from_dict(self, order_id: str):
        self._active_orders.pop(order_id, None)

    # Buy order fully completes
    def did_complete_buy_order(self, order_completed_event):
        self.logger().info(f"Your order {order_completed_event.order_id} has been completed")
        self.remove_order_from_dict(order_completed_event.order_id)

    # Sell order fully completes
    def did_complete_sell_order(self, order_completed_event):
        self.logger().info(f"Your order {order_completed_event.order_id} has been completed")
        self.remove_order_from_dict(order_completed_event.order_id)

    # Order can't be executed (diff reasons)
    def did_fail_order(self, order_failed_event):
        self.logger().info(f"Your order {order_failed_event.order_id} has been failed")
        self.remove_order_from_dict(order_failed_event.order_id)

    # Order was canceled
    def did_cancel_order(self, cancelled_event):
        self.logger().info(f"Your order {cancelled_event.order_id} has been canceled")
        self.remove_order_from_dict(cancelled_event.order_id)

    # Lifetime of order is over
    def did_expire_order(self, expired_event):
        self.logger().info(f"Your order {expired_event.order_id} has been expired")
        self.remove_order_from_dict(expired_event.order_id)

    def has_enough_balance(self, market_info, amount: Decimal):
        """
        Checks to make sure the user has the sufficient balance in order to place the specified order

        :param market_info: a market trading pair
        :param amount: order amount
        :return: True if user has enough balance, False if not
        """
        market: ExchangeBase = market_info.market
        base_asset_balance = market.get_balance(market_info.base_asset)
        quote_asset_balance = market.get_balance(market_info.quote_asset)
        order_book: OrderBook = market_info.order_book
        price = order_book.get_price_for_volume(True, float(amount)).result_price

        return quote_asset_balance >= (amount * Decimal(price)) \
            if self._is_buy \
            else base_asset_balance >= amount