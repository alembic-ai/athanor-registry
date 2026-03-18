"""
Tests for the Omni-Data Market Engine.

Covers schema validation, quant calculations, replay engine
causality enforcement, broadcaster truncation, and execution blocking.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta

import pytest

from omni_data.schemas.models import (
    OHLCV,
    MarketStateSummary,
    OrderBookDepth,
    OrderBookLevel,
    OrderflowMetrics,
    PortfolioState,
    Position,
    Side,
    Ticker,
    Trade,
    IndicatorSet,
    SkillInput,
    SkillOutput,
    Timeframe,
)
from omni_data.quant.indicators import (
    compute_atr,
    compute_rsi,
    compute_macd,
    compute_realized_volatility,
    build_indicator_set,
)
from omni_data.integrations.orderflow import compute_orderflow_metrics
from omni_data.replay.engine import ReplayEngine, LookaheadViolation
from omni_data.broadcaster.ipc import truncate_state_for_token_budget


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_candles(n: int = 50, base_price: float = 50000.0) -> list[OHLCV]:
    """Generate synthetic OHLCV candles for testing."""
    candles: list[OHLCV] = []
    price = base_price
    for i in range(n):
        import random
        random.seed(i)
        change = random.uniform(-500, 500)
        o = price
        h = price + abs(change) * 1.5
        l = price - abs(change) * 1.2
        c = price + change
        v = random.uniform(100, 10000)
        candles.append(OHLCV(
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i),
            open=o, high=h, low=l, close=c, volume=v,
        ))
        price = c
    return candles


def _make_orderbook() -> OrderBookDepth:
    """Generate a synthetic order book."""
    return OrderBookDepth(
        symbol="BTC/USDT",
        exchange="test",
        bids=[OrderBookLevel(price=50000 - i * 10, quantity=1.0 + i * 0.5) for i in range(20)],
        asks=[OrderBookLevel(price=50000 + i * 10, quantity=0.8 + i * 0.3) for i in range(20)],
        timestamp=datetime.now(tz=timezone.utc),
    )


def _make_trades(n: int = 20) -> list[Trade]:
    """Generate synthetic trades."""
    return [
        Trade(
            symbol="BTC/USDT",
            exchange="test",
            price=50000 + i,
            amount=0.1 * (i + 1),
            side=Side.BID if i % 2 == 0 else Side.ASK,
            timestamp=datetime.now(tz=timezone.utc),
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Schema Tests
# ---------------------------------------------------------------------------

class TestSchemas:
    """Validate that all schemas serialize/deserialize correctly."""

    def test_ohlcv_roundtrip(self) -> None:
        candle = OHLCV(
            timestamp=datetime.now(tz=timezone.utc),
            open=50000, high=51000, low=49000, close=50500, volume=1000,
        )
        json_str = candle.model_dump_json()
        restored = OHLCV.model_validate_json(json_str)
        assert restored.close == 50500

    def test_market_state_summary_minimal(self) -> None:
        state = MarketStateSummary(symbol="BTC/USDT")
        assert state.symbol == "BTC/USDT"
        assert state.ticker is None
        assert state.engine_version == "0.1.0"

    def test_market_state_summary_full(self) -> None:
        state = MarketStateSummary(
            symbol="BTC/USDT",
            exchange="binance",
            ticker=Ticker(
                symbol="BTC/USDT", bid=50000, ask=50010,
                last=50005, volume_24h=10000, change_pct_24h=2.5,
                timestamp=datetime.now(tz=timezone.utc),
            ),
            ohlcv=_make_candles(5),
        )
        json_str = state.model_dump_json()
        restored = MarketStateSummary.model_validate_json(json_str)
        assert len(restored.ohlcv) == 5

    def test_skill_input_output_contract(self) -> None:
        state = MarketStateSummary(symbol="ETH/USDT")
        skill_in = SkillInput(state=state, requested_symbols=["ETH/USDT"])
        assert skill_in.requested_timeframe == Timeframe.H1

        skill_out = SkillOutput(
            bias="bullish", confidence=0.85,
            summary="Strong momentum detected",
            key_levels={"support_1": 3000, "resistance_1": 3500},
        )
        assert skill_out.confidence == 0.85

    def test_portfolio_state_serialization(self) -> None:
        state = PortfolioState(
            total_equity=100000,
            free_margin=80000,
            used_margin=20000,
            positions=[
                Position(
                    symbol="BTC/USDT", exchange="binance", side=Side.BID,
                    size=1.0, entry_price=50000, current_price=51000,
                    unrealized_pnl=1000, leverage=10,
                ),
            ],
            balances={"binance:USDT": 80000},
            exchanges=["binance"],
            timestamp=datetime.now(tz=timezone.utc),
        )
        json_str = state.model_dump_json()
        restored = PortfolioState.model_validate_json(json_str)
        assert len(restored.positions) == 1
        assert restored.positions[0].leverage == 10


# ---------------------------------------------------------------------------
# Quant Indicator Tests
# ---------------------------------------------------------------------------

class TestQuant:
    """Validate procedural quant calculations."""

    def test_atr_returns_float(self) -> None:
        candles = _make_candles(30)
        atr = compute_atr(candles, 14)
        assert atr is not None
        assert isinstance(atr, float)
        assert atr > 0

    def test_atr_insufficient_data(self) -> None:
        candles = _make_candles(5)
        assert compute_atr(candles, 14) is None

    def test_rsi_range(self) -> None:
        candles = _make_candles(30)
        rsi = compute_rsi(candles, 14)
        assert rsi is not None
        assert 0 <= rsi <= 100

    def test_macd_returns_tuple(self) -> None:
        candles = _make_candles(50)
        signal, histogram = compute_macd(candles)
        assert signal is not None
        assert histogram is not None

    def test_realized_volatility(self) -> None:
        candles = _make_candles(50)
        vol = compute_realized_volatility(candles, 30)
        assert vol is not None
        assert vol >= 0

    def test_build_indicator_set(self) -> None:
        candles = _make_candles(50)
        indicators = build_indicator_set("BTC/USDT", candles)
        assert indicators.symbol == "BTC/USDT"
        assert indicators.atr_14 is not None
        assert indicators.rsi_14 is not None


# ---------------------------------------------------------------------------
# Orderflow Tests
# ---------------------------------------------------------------------------

class TestOrderflow:
    """Validate orderflow computations."""

    def test_compute_metrics(self) -> None:
        ob = _make_orderbook()
        trades = _make_trades()
        metrics = compute_orderflow_metrics(ob, trades)
        assert isinstance(metrics, OrderflowMetrics)
        assert 0 <= metrics.imbalance_ratio <= 1

    def test_volume_delta_direction(self) -> None:
        ob = _make_orderbook()
        # All buy trades
        buy_trades = [
            Trade(
                symbol="BTC/USDT", exchange="test",
                price=50000, amount=1.0, side=Side.BID,
                timestamp=datetime.now(tz=timezone.utc),
            )
            for _ in range(10)
        ]
        metrics = compute_orderflow_metrics(ob, buy_trades)
        assert metrics.volume_delta > 0


# ---------------------------------------------------------------------------
# Replay Engine Tests
# ---------------------------------------------------------------------------

class TestReplayEngine:
    """Validate causality enforcement in the replay engine."""

    def test_basic_replay(self) -> None:
        candles = _make_candles(10)
        engine = ReplayEngine(symbol="BTC/USDT", ohlcv_history=candles)
        assert engine.total_bars == 10
        assert engine.current_bar == 0

    def test_lookahead_violation(self) -> None:
        candles = _make_candles(10)
        engine = ReplayEngine(symbol="BTC/USDT", ohlcv_history=candles)

        # Cursor is at 0, trying to access bar 5 should fail
        with pytest.raises(LookaheadViolation):
            engine.get_state_at(5)

    def test_get_state_at_cursor(self) -> None:
        candles = _make_candles(10)
        engine = ReplayEngine(symbol="BTC/USDT", ohlcv_history=candles)

        # Accessing bar 0 should work (cursor is at 0)
        state = engine.get_state_at(0)
        assert state.symbol == "BTC/USDT"
        assert len(state.ohlcv) == 1  # Only bar 0

    @pytest.mark.asyncio
    async def test_stream_produces_all_bars(self) -> None:
        candles = _make_candles(5)
        engine = ReplayEngine(symbol="BTC/USDT", ohlcv_history=candles)

        states: list[MarketStateSummary] = []
        async for state in engine.stream(speed=0):
            states.append(state)

        assert len(states) == 5
        # Verify each state has strictly increasing OHLCV length
        for i, state in enumerate(states):
            assert len(state.ohlcv) == i + 1

    @pytest.mark.asyncio
    async def test_stream_no_future_data(self) -> None:
        candles = _make_candles(10)
        engine = ReplayEngine(symbol="BTC/USDT", ohlcv_history=candles)

        async for state in engine.stream(speed=0):
            # Verify no candle in state.ohlcv is from the future
            for candle in state.ohlcv:
                assert candle.timestamp <= state.broadcast_timestamp

    def test_serialize_restore_position(self) -> None:
        candles = _make_candles(20)
        engine = ReplayEngine(symbol="BTC/USDT", ohlcv_history=candles)
        engine._cursor = 10

        position = engine.serialize_position()
        assert position["cursor"] == 10

        engine.reset()
        assert engine.current_bar == 0

        engine.restore_position(position)
        assert engine.current_bar == 10

    def test_empty_history_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            ReplayEngine(symbol="BTC/USDT", ohlcv_history=[])


# ---------------------------------------------------------------------------
# Broadcaster Tests
# ---------------------------------------------------------------------------

class TestBroadcaster:
    """Validate token-aware truncation."""

    def test_truncation_within_budget(self) -> None:
        state = MarketStateSummary(
            symbol="BTC/USDT",
            ohlcv=_make_candles(100),
        )
        result = truncate_state_for_token_budget(state, max_chars=2000)
        assert len(result) <= 2000

    def test_truncation_preserves_small_state(self) -> None:
        state = MarketStateSummary(symbol="BTC/USDT")
        full = state.model_dump_json()
        result = truncate_state_for_token_budget(state, max_chars=10000)
        assert result == full  # No truncation needed
