"""Strategy scaffolding for user-defined trading logic."""

from .base import (  # noqa: F401
    BaseStrategy,
    OrderIntent,
    StrategyConfig,
    StrategyContext,
    StrategyResult,
    TargetPosition,
)
from .templates import MovingAverageCrossoverStrategy  # noqa: F401

__all__ = [
    "BaseStrategy",
    "StrategyConfig",
    "StrategyContext",
    "TargetPosition",
    "OrderIntent",
    "StrategyResult",
    "MovingAverageCrossoverStrategy",
]

