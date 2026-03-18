"""
Pydantic data contracts for all Omni-Data layers.

These schemas define the canonical data structures that flow
through the entire engine. Every subpackage produces or consumes
these types. External systems (Skills, Workflows, Inference)
depend ONLY on these contracts — never on internal implementation.
"""

from __future__ import annotations

import datetime as _dt
import enum
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Timeframe(str, enum.Enum):
    """Supported OHLCV timeframes."""
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"


class Side(str, enum.Enum):
    """Trade side."""
    BID = "bid"
    ASK = "ask"


class EventSeverity(str, enum.Enum):
    """Severity level for regulatory / corporate events."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# ---------------------------------------------------------------------------
# Market Data Schemas
# ---------------------------------------------------------------------------

class OHLCV(BaseModel):
    """Single candlestick bar."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class Ticker(BaseModel):
    """Real-time ticker snapshot."""
    symbol: str
    bid: float
    ask: float
    last: float
    volume_24h: float
    change_pct_24h: float
    timestamp: datetime


class OrderBookLevel(BaseModel):
    """Single price level in an order book."""
    price: float
    quantity: float


class OrderBookDepth(BaseModel):
    """L2/L3 order book snapshot."""
    symbol: str
    exchange: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    timestamp: datetime


class Trade(BaseModel):
    """Individual trade event."""
    symbol: str
    exchange: str
    price: float
    amount: float
    side: Side
    timestamp: datetime


# ---------------------------------------------------------------------------
# Orderflow Physics
# ---------------------------------------------------------------------------

class OrderflowMetrics(BaseModel):
    """Derived orderflow analytics — computed procedurally, not by LLM."""
    symbol: str
    volume_delta: float = Field(description="Buy volume minus sell volume")
    voi: float = Field(description="Volume Order Imbalance")
    liquidity_above: float = Field(description="Total ask liquidity within 2% of mid")
    liquidity_below: float = Field(description="Total bid liquidity within 2% of mid")
    imbalance_ratio: float = Field(description="bid_liq / (bid_liq + ask_liq)")
    timestamp: datetime


# ---------------------------------------------------------------------------
# Alternative / Macro / Sentiment
# ---------------------------------------------------------------------------

class MacroIndicator(BaseModel):
    """Single macro-economic data point (e.g., from FRED, CoinGecko)."""
    name: str
    value: float
    unit: str = ""
    source: str
    timestamp: datetime


class SentimentMetric(BaseModel):
    """Pre-computed sentiment score from an external source."""
    source: str = Field(description="e.g. 'crypto_twitter', 'newsapi', 'fear_greed'")
    symbol: str | None = None
    score: float = Field(ge=-1.0, le=1.0, description="Normalized -1 (extreme fear) to +1 (extreme greed)")
    raw_value: Any = None
    timestamp: datetime


class OnChainMetric(BaseModel):
    """Blockchain network health indicators."""
    network: str
    metric_name: str = Field(description="e.g. 'active_addresses', 'tvl', 'hash_rate'")
    value: float
    source: str
    timestamp: datetime


class EventWarning(BaseModel):
    """Discrete event object for regulatory filings, token unlocks, earnings."""
    title: str
    description: str
    severity: EventSeverity
    event_type: str = Field(description="e.g. 'sec_filing', 'token_unlock', 'earnings'")
    symbol: str | None = None
    scheduled_at: datetime | None = None
    source: str
    timestamp: datetime


# ---------------------------------------------------------------------------
# Derivatives & Risk
# ---------------------------------------------------------------------------

class OptionContract(BaseModel):
    """Single option contract snapshot."""
    symbol: str
    underlying: str
    strike: float
    expiry: datetime
    option_type: str = Field(description="'call' or 'put'")
    iv: float = Field(description="Implied volatility")
    delta: float
    gamma: float
    theta: float
    vega: float
    open_interest: float
    volume: float
    last_price: float
    timestamp: datetime


class FundingRate(BaseModel):
    """Perpetual swap funding rate snapshot."""
    symbol: str
    exchange: str
    rate: float
    next_funding_time: datetime | None = None
    timestamp: datetime


class LiquidationEvent(BaseModel):
    """Observed liquidation event."""
    symbol: str
    exchange: str
    side: Side
    quantity: float
    price: float
    timestamp: datetime


class IndicatorSet(BaseModel):
    """Computed risk/technical indicators — pure math, no LLM."""
    symbol: str
    atr_14: float | None = Field(default=None, description="Average True Range (14-period)")
    realized_vol_30: float | None = Field(default=None, description="30-day realized volatility")
    rsi_14: float | None = Field(default=None, description="Relative Strength Index (14-period)")
    macd_signal: float | None = None
    macd_histogram: float | None = None
    correlation_btc: float | None = Field(default=None, description="30-day correlation to BTC")
    timestamp: datetime


# ---------------------------------------------------------------------------
# Portfolio State
# ---------------------------------------------------------------------------

class Position(BaseModel):
    """Single open position."""
    symbol: str
    exchange: str
    side: Side
    size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float
    realized_pnl: float = 0.0
    leverage: float = 1.0
    liquidation_price: float | None = None
    margin_used: float | None = None


class PortfolioState(BaseModel):
    """Complete portfolio snapshot across all connected exchanges."""
    total_equity: float
    free_margin: float
    used_margin: float
    positions: list[Position]
    balances: dict[str, float] = Field(description="Asset -> Available balance")
    exchanges: list[str] = Field(description="List of connected exchange IDs")
    max_drawdown_pct: float | None = None
    daily_pnl: float | None = None
    timestamp: datetime


# ---------------------------------------------------------------------------
# Unified Broadcast State
# ---------------------------------------------------------------------------

class MarketStateSummary(BaseModel):
    """
    The master broadcast payload. This is the single object that the
    Omni-Broadcaster serializes and pushes over IPC to any number of
    downstream consumers (agents, skills, workflows).

    Every field is optional — the broadcaster populates whichever
    layers are active for the current configuration.
    """
    # Core market data
    symbol: str
    exchange: str | None = None
    ticker: Ticker | None = None
    ohlcv: list[OHLCV] | None = None
    orderbook: OrderBookDepth | None = None
    recent_trades: list[Trade] | None = None
    orderflow: OrderflowMetrics | None = None

    # Derivatives
    funding_rates: list[FundingRate] | None = None
    options_chain: list[OptionContract] | None = None
    liquidations: list[LiquidationEvent] | None = None

    # Indicators & risk
    indicators: IndicatorSet | None = None

    # Alternative context
    macro: list[MacroIndicator] | None = None
    sentiment: list[SentimentMetric] | None = None
    on_chain: list[OnChainMetric] | None = None
    events: list[EventWarning] | None = None

    # Portfolio
    portfolio: PortfolioState | None = None

    # Metadata
    broadcast_timestamp: datetime = Field(default_factory=lambda: _dt.datetime.now(_dt.timezone.utc))
    engine_version: str = "0.1.0"


# ---------------------------------------------------------------------------
# Skill Interface Contract
# ---------------------------------------------------------------------------

class SkillInput(BaseModel):
    """
    The data contract that an external Analysis Skill receives.
    This is a thin wrapper around MarketStateSummary with metadata
    about the request context.
    """
    state: MarketStateSummary
    requested_symbols: list[str] = Field(default_factory=list)
    requested_timeframe: Timeframe = Timeframe.H1
    max_output_tokens: int | None = None
    request_id: str = ""


class SkillOutput(BaseModel):
    """
    The data contract that an Analysis Skill must return.
    Strictly typed — no freeform text blobs in programmatic pipelines.
    """
    request_id: str = ""
    bias: str | None = Field(default=None, description="'bullish', 'bearish', 'neutral'")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    summary: str = ""
    key_levels: dict[str, float] = Field(default_factory=dict, description="e.g. {'support_1': 50000, 'resistance_1': 52000}")
    signals: list[str] = Field(default_factory=list, description="Discrete signal tags")
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: _dt.datetime.now(_dt.timezone.utc))
