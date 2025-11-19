__all__ = [
    "ArbitrageLMarketPair",
    "ArbitrageLStrategy",
]


def __getattr__(name):
    if name == "ArbitrageLStrategy":
        from .arbitrage import ArbitrageLStrategy as _ArbitrageLStrategy
        return _ArbitrageLStrategy
    if name == "ArbitrageLMarketPair":
        from .arbitrage_market_pair import ArbitrageLMarketPair as _ArbitrageLMarketPair
        return _ArbitrageLMarketPair
    raise AttributeError(f"module {__name__} has no attribute {name}")
