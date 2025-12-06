from __future__ import annotations

from typing import Any, Dict
import numpy as np

from quasar.lib.strategies.base import (
    BaseStrategy,
    StrategyConfig,
    StrategyContext,
    StrategyResult,
)


class MovingAverageConfig(StrategyConfig):
    """Configuration for the moving average crossover strategy."""

    fast_window: int = StrategyConfig.int_field(
        default=20,
        min_value=1,
        max_value=200,
        title="Fast Moving Average Window",
        description="Number of periods for the fast moving average"
    )
    slow_window: int = StrategyConfig.int_field(
        default=50,
        min_value=1,
        max_value=500,
        title="Slow Moving Average Window",
        description="Number of periods for the slow moving average"
    )
    max_position: float = StrategyConfig.float_field(
        default=0.0,
        min_value=0.0,
        title="Maximum Position Size",
        description="Maximum position size in base currency (0.0 = unlimited)"
    )
    strategy_direction: str = StrategyConfig.option_field(
        options=["long", "short", "both"],
        default="long",
        title="Trading Direction",
        description="Whether to trade long positions, short positions, or both"
    )


class MovingAverageCrossoverStrategy(BaseStrategy):
    """
    Scaffold for a long-only moving average crossover.

    The implementation is intentionally incomplete so users can focus on
    plugging in their custom signals without modifying the runtime contract.
    """

    ConfigModel = MovingAverageConfig

    def __init__(self):
        super().__init__()
        self._state: Dict[str, Any] = {}

    async def _on_bar(self, context: StrategyContext) -> StrategyResult:
        """
        Evaluate the latest bar and decide whether to hold a long position.

        This scaffold only returns an empty result, but the docstring documents
        the intended workflow:
        1. Pull the merged price series from ``context.prices``.
        2. Compute fast/slow moving averages.
        3. Determine the desired net position per broker account.
        4. Return StrategyResult with populated TargetPosition entries.
        """

        # Configuration Parameters
        fast_window = self.config.fast_window
        slow_window = self.config.slow_window
        direction = self.config.strategy_direction

        closes = context.prices.get_close()
        if closes.empty:
            return StrategyResult.empty(reason="No price data available.")

        fast_ma = closes.rolling(fast_window).mean()
        slow_ma = closes.rolling(slow_window).mean()
        signal = np.sign(fast_ma - slow_ma).iloc[-1]

        if direction == "long":
            signal = max(signal, 0)
        elif direction == "short":
            signal = min(signal, 0)

        return StrategyResult.empty(
            reason="MovingAverageCrossoverStrategy is a scaffold; implement signal logic."
        )

