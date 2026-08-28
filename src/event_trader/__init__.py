"""Event-driven, paper-only trading research system."""

from .domain import (
    Direction,
    EventSnapshot,
    FilingEvent,
    MarketSnapshot,
    NewsInsight,
    RiskDecision,
    Signal,
)

__all__ = [
    "Direction",
    "EventSnapshot",
    "FilingEvent",
    "MarketSnapshot",
    "NewsInsight",
    "RiskDecision",
    "Signal",
]

__version__ = "0.1.0"
