from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Mapping, Sequence
from abc import ABC, abstractmethod

from quasar.lib.providers.core import Bar


@dataclass(slots=True)
class StrategyConfig:
    """Minimal configuration payload shared by all strategies."""

    name: str
    interval: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TargetPosition:
    """Desired net position for a symbol on a given broker account."""

    common_symbol: str
    broker_account: str
    quantity: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderIntent:
    """Explicit order instructions emitted by a strategy."""

    broker_account: str
    common_symbol: str
    side: str  # BUY or SELL
    quantity: float
    order_type: str = "market"
    limit_price: float | None = None
    time_in_force: str = "day"
    tags: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyResult:
    """Container for strategy outputs."""

    targets: List[TargetPosition] = field(default_factory=list)
    orders: List[OrderIntent] = field(default_factory=list)
    notes: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def empty(cls, reason: str | None = None) -> "StrategyResult":
        notes: Dict[str, Any] = {}
        if reason:
            notes["reason"] = reason
        return cls(notes=notes)


@dataclass(slots=True)
class StrategyContext:
    """
    Snapshot of runtime state that strategies can act on.

    This intentionally mirrors the minimal data we expect to wire up when the
    StrategyEngine is fully implemented.
    """

    clock: datetime
    interval: str
    prices: Mapping[str, Sequence[Bar]]
    positions: Mapping[str, Any]  # placeholder for broker-native positions
    cash: Mapping[str, float]
    policies: Mapping[str, Any] = field(default_factory=dict)


class BaseStrategy(ABC):
    """Base class all user strategies must inherit from."""

    name: str = "BaseStrategy"

    def __init__(self, config: StrategyConfig):
        self.config = config

    @abstractmethod
    async def on_bar(self, context: StrategyContext) -> StrategyResult:
        """
        Execute strategy logic for the current bar.

        Implementations should return a StrategyResult that either specifies
        target positions or explicit orders. Returning StrategyResult.empty()
        indicates "no action".
        """

    async def on_start(self, context: StrategyContext) -> None:
        """Optional hook invoked when a strategy instance is bootstrapped."""

    async def on_stop(self, context: StrategyContext) -> None:
        """Optional hook invoked when a strategy instance is shut down."""

