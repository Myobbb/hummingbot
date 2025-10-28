#!/usr/bin/env python

from typing import NamedTuple

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


class ArbitrageMMarketPair(NamedTuple):
    """
    Specifies a pair of markets for arbitrage (multi-market variant)
    """
    first: MarketTradingPairTuple
    second: MarketTradingPairTuple
