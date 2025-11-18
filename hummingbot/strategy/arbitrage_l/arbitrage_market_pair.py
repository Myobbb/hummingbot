#!/usr/bin/env python

from typing import NamedTuple

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


class ArbitrageLMarketPair(NamedTuple):
    """
    Specifies a pair of markets for arbitrage (limit order variant)
    """
    first: MarketTradingPairTuple
    second: MarketTradingPairTuple
