import logging
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.model.trade_fill import TradeFill

s_decimal_0 = Decimal("0")
s_decimal_nan = Decimal("NaN")

@dataclass
class PerformanceMetrics:
    _logger = None

    num_buys: int = 0
    num_sells: int = 0
    num_trades: int = 0

    b_vol_base: Decimal = s_decimal_0
    s_vol_base: Decimal = s_decimal_0
    tot_vol_base: Decimal = s_decimal_0

    b_vol_quote: Decimal = s_decimal_0
    s_vol_quote: Decimal = s_decimal_0
    tot_vol_quote: Decimal = s_decimal_0

    avg_b_price: Decimal = s_decimal_0
    avg_s_price: Decimal = s_decimal_0
    avg_tot_price: Decimal = s_decimal_0

    start_base_bal: Decimal = s_decimal_0
    start_quote_bal: Decimal = s_decimal_0
    cur_base_bal: Decimal = s_decimal_0
    cur_quote_bal: Decimal = s_decimal_0
    start_price: Decimal = s_decimal_0
    cur_price: Decimal = s_decimal_0
    start_base_ratio_pct: Decimal = s_decimal_0
    cur_base_ratio_pct: Decimal = s_decimal_0

    hold_value: Decimal = s_decimal_0
    cur_value: Decimal = s_decimal_0
    trade_pnl: Decimal = s_decimal_0

    fee_in_quote: Decimal = s_decimal_0
    total_pnl: Decimal = s_decimal_0
    return_pct: Decimal = s_decimal_0

    def __init__(self):
        self.fees: Dict[str, Decimal] = defaultdict(lambda: s_decimal_0)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @classmethod
    async def create(cls,
                     trading_pair: str,
                     trades: List[Any],
                     current_balances: Dict[str, Decimal]) -> 'PerformanceMetrics':
        # Simply return an empty instance without processing
        return PerformanceMetrics()

    @staticmethod
    def position_order(open: list, close: list) -> Tuple[Any, Any]:
        return None

    @staticmethod
    def aggregate_orders(orders: list) -> list:
        return []

    @staticmethod
    def aggregate_position_order(buys: list, sells: list) -> Tuple[list, list]:
        return [], []

    @staticmethod
    def derivative_pnl(long: list, short: list) -> List[Decimal]:
        return [s_decimal_0]

    @staticmethod
    def smart_round(value: Decimal, precision: Optional[int] = None) -> Decimal:
        return s_decimal_0

    @staticmethod
    def divide(value, divisor):
        return s_decimal_0

    def _is_trade_fill(self, trade):
        return False

    def _are_derivatives(self, trades: List[Any]) -> bool:
        return False

    def _preprocess_trades_and_group_by_type(self, trades: List[Any]) -> Tuple[List[Any], List[Any]]:
        return [], []

    def _process_deducted_fees_impact_in_quote_vol(self, trade):
        return s_decimal_0

    async def _calculate_fees(self, quote: str, trades: List[Any]):
        pass

    def _calculate_trade_pnl(self, buys: list, sells: list):
        pass

    async def _initialize_metrics(self,
                                  trading_pair: str,
                                  trades: List[Any],
                                  current_balances: Dict[str, Decimal]):
        pass
