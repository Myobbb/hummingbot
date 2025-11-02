"""
Arbitrage M V2 Controller

High-performance arbitrage controller migrated from V1 Cython strategy.
"""

from controllers.arbitrage_m.arbitrage_m_controller import (
    ArbitrageMConfig,
    ArbitrageMController,
)

__all__ = [
    "ArbitrageMConfig",
    "ArbitrageMController",
]
