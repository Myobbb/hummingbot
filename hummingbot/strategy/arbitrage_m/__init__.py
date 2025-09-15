__all__ = [
    "ArbitrageMMarketPair",
    "ArbitrageMStrategy",
]


def __getattr__(name):
    if name == "ArbitrageMStrategy":
        from .arbitrage import ArbitrageMStrategy as _ArbitrageMStrategy
        return _ArbitrageMStrategy
    if name == "ArbitrageMMarketPair":
        from .arbitrage_market_pair import ArbitrageMMarketPair as _ArbitrageMMarketPair
        return _ArbitrageMMarketPair
    raise AttributeError(f"module {__name__} has no attribute {name}")
