"""
Deterministic Replay Engine — anti-lookahead walk-forward streamer.

Replays historical data (OHLCV, orderbooks, trades, sentiment)
exactly as if it were a live stream, blocking any attempt to
access future data. This guarantees that Skills and Workflows
tested against this engine cannot introduce lookahead bias.

Design Goals:
    - Causality enforcement: State(t) can ONLY read data up to time t
    - Identical interface: produces the exact same MarketStateSummary
      as the live gateway, so Skills are portable between live/replay
    - Speed control: replay at 1x, 10x, or max speed
    - Stateless persistence: can serialize replay position and resume
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncIterator

from omni_data.schemas.models import (
    OHLCV,
    MarketStateSummary,
    OrderBookDepth,
    SentimentMetric,
    Ticker,
    Trade,
)

logger = logging.getLogger(__name__)


class LookaheadViolation(RuntimeError):
    """Raised when a replay consumer attempts to access future data."""


class ReplayEngine:
    """
    Walk-forward historical data streamer.

    Usage:
        engine = ReplayEngine(
            symbol="BTC/USDT",
            ohlcv_history=historical_candles,
        )

        async for state in engine.stream(speed=10.0):
            # state is a MarketStateSummary at time t
            # future candles are inaccessible
            process(state)
    """

    def __init__(
        self,
        symbol: str,
        ohlcv_history: list[OHLCV],
        *,
        exchange: str = "replay",
        trades_history: list[Trade] | None = None,
        sentiment_history: list[SentimentMetric] | None = None,
    ) -> None:
        """
        Initialize the replay engine with historical data.

        Args:
            symbol: Market pair symbol.
            ohlcv_history: Complete historical OHLCV data (oldest first).
            exchange: Exchange label for the replay.
            trades_history: Optional historical trades (oldest first).
            sentiment_history: Optional historical sentiment (oldest first).

        Raises:
            ValueError: If OHLCV history is empty.
        """
        if not ohlcv_history:
            raise ValueError("OHLCV history cannot be empty.")

        # Sort by timestamp to guarantee ordering
        self._ohlcv = sorted(ohlcv_history, key=lambda c: c.timestamp)
        self._trades = sorted(trades_history or [], key=lambda t: t.timestamp)
        self._sentiment = sorted(sentiment_history or [], key=lambda s: s.timestamp)
        self._symbol = symbol
        self._exchange = exchange
        self._cursor = 0

    @property
    def total_bars(self) -> int:
        """Total number of bars in the replay dataset."""
        return len(self._ohlcv)

    @property
    def current_bar(self) -> int:
        """Current replay position (0-indexed)."""
        return self._cursor

    @property
    def current_time(self) -> datetime:
        """Timestamp of the current bar."""
        return self._ohlcv[min(self._cursor, len(self._ohlcv) - 1)].timestamp

    def reset(self) -> None:
        """Reset replay to the beginning."""
        self._cursor = 0

    def get_state_at(self, bar_index: int) -> MarketStateSummary:
        """
        Get the MarketStateSummary at a specific bar index.
        Only data up to and including bar_index is visible.

        Args:
            bar_index: The bar index (0-based).

        Returns:
            MarketStateSummary containing only data up to bar_index.

        Raises:
            LookaheadViolation: If bar_index is ahead of the cursor.
        """
        if bar_index > self._cursor:
            raise LookaheadViolation(
                f"Attempted to access bar {bar_index} but cursor is at {self._cursor}. "
                f"This would introduce lookahead bias."
            )

        if bar_index < 0 or bar_index >= len(self._ohlcv):
            raise IndexError(f"Bar index {bar_index} out of range [0, {len(self._ohlcv) - 1}].")

        current_candle = self._ohlcv[bar_index]
        visible_candles = self._ohlcv[:bar_index + 1]

        # Filter trades up to current time
        cutoff = current_candle.timestamp
        visible_trades = [t for t in self._trades if t.timestamp <= cutoff]
        visible_sentiment = [s for s in self._sentiment if s.timestamp <= cutoff]

        # Build a synthetic ticker from the current candle
        ticker = Ticker(
            symbol=self._symbol,
            bid=current_candle.close,
            ask=current_candle.close,
            last=current_candle.close,
            volume_24h=current_candle.volume,
            change_pct_24h=0.0,
            timestamp=current_candle.timestamp,
        )

        return MarketStateSummary(
            symbol=self._symbol,
            exchange=self._exchange,
            ticker=ticker,
            ohlcv=visible_candles,
            recent_trades=visible_trades[-50:] if visible_trades else None,
            sentiment=visible_sentiment[-10:] if visible_sentiment else None,
            broadcast_timestamp=current_candle.timestamp,
        )

    async def stream(
        self,
        *,
        speed: float = 1.0,
        start_bar: int = 0,
    ) -> AsyncIterator[MarketStateSummary]:
        """
        Async generator that streams MarketStateSummary bar by bar.

        Args:
            speed: Replay speed multiplier (1.0 = real-time, 10.0 = 10x, 0 = max speed).
            start_bar: Bar index to start from.

        Yields:
            MarketStateSummary at each bar, with strictly causal data.
        """
        self._cursor = max(0, start_bar)

        while self._cursor < len(self._ohlcv):
            state = self.get_state_at(self._cursor)
            yield state

            self._cursor += 1

            # Delay for speed control
            if speed > 0 and self._cursor < len(self._ohlcv):
                next_ts = self._ohlcv[self._cursor].timestamp
                prev_ts = self._ohlcv[self._cursor - 1].timestamp
                delta = (next_ts - prev_ts).total_seconds()
                await asyncio.sleep(max(0, delta / speed))

    def serialize_position(self) -> dict:
        """Serialize replay position for resumption."""
        return {
            "symbol": self._symbol,
            "exchange": self._exchange,
            "cursor": self._cursor,
            "total_bars": self.total_bars,
        }

    def restore_position(self, position: dict) -> None:
        """Restore replay position from serialized state."""
        self._cursor = position.get("cursor", 0)
