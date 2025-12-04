from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from quasar.lib.strategies.base import (
    BaseStrategy,
    StrategyConfig,
    StrategyContext,
    StrategyResult,
)


@dataclass(slots=True)
class MovingAverageConfig(StrategyConfig):
    """Config helpers for the moving average scaffold."""

    fast_window: int = 20
    slow_window: int = 50
    max_position: float = 0.0


class MovingAverageCrossoverStrategy(BaseStrategy):
    """
    Scaffold for a long-only moving average crossover.

    The implementation is intentionally incomplete so users can focus on
    plugging in their custom signals without modifying the runtime contract.
    """

    name = "moving_average_crossover"

    def __init__(self, config: MovingAverageConfig):
        super().__init__(config=config)
        self._state: Dict[str, Any] = {}

    async def on_bar(self, context: StrategyContext) -> StrategyResult:
        """
        Evaluate the latest bar and decide whether to hold a long position.

        This scaffold only returns an empty result, but the docstring documents
        the intended workflow:
        1. Pull the merged price series from ``context.prices``.
        2. Compute fast/slow moving averages.
        3. Determine the desired net position per broker account.
        4. Return StrategyResult with populated TargetPosition entries.
        """

        return StrategyResult.empty(
            reason="MovingAverageCrossoverStrategy is a scaffold; implement signal logic."
        )

