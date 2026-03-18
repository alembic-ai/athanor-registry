"""
Quant calculators — procedural technical indicators and risk metrics.

All functions are stateless, operate on numpy arrays or lists of
OHLCV data, and return strict schema outputs. Zero LLM involvement.

Memory: ~O(n) where n = number of candles | CPU: O(n) per indicator
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from omni_data.schemas.models import IndicatorSet, OHLCV


def compute_atr(candles: list[OHLCV], period: int = 14) -> float | None:
    """
    Average True Range (ATR) over the given period.

    Args:
        candles: OHLCV data, oldest first. Must have at least `period + 1` bars.
        period: Lookback period (default 14).

    Returns:
        ATR value, or None if insufficient data.
    """
    if len(candles) < period + 1:
        return None

    highs = np.array([c.high for c in candles])
    lows = np.array([c.low for c in candles])
    closes = np.array([c.close for c in candles])

    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1]),
        ),
    )

    # Wilder's smoothing
    atr = np.mean(tr[:period])
    for i in range(period, len(tr)):
        atr = (atr * (period - 1) + tr[i]) / period

    return float(atr)


def compute_rsi(candles: list[OHLCV], period: int = 14) -> float | None:
    """
    Relative Strength Index (RSI).

    Args:
        candles: OHLCV data, oldest first.
        period: Lookback period (default 14).

    Returns:
        RSI value (0-100), or None if insufficient data.
    """
    if len(candles) < period + 1:
        return None

    closes = np.array([c.close for c in candles])
    deltas = np.diff(closes)

    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def compute_macd(
    candles: list[OHLCV],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[float | None, float | None]:
    """
    MACD signal line and histogram values.

    Args:
        candles: OHLCV data, oldest first.
        fast: Fast EMA period.
        slow: Slow EMA period.
        signal: Signal EMA period.

    Returns:
        Tuple of (signal_value, histogram_value), or (None, None).
    """
    if len(candles) < slow + signal:
        return None, None

    closes = np.array([c.close for c in candles])

    def ema(data: np.ndarray, span: int) -> np.ndarray:
        """Exponential moving average."""
        alpha = 2.0 / (span + 1)
        result = np.empty_like(data)
        result[0] = data[0]
        for i in range(1, len(data)):
            result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]
        return result

    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)
    macd_line = fast_ema - slow_ema

    signal_line = ema(macd_line[slow - 1:], signal)
    histogram = macd_line[slow - 1:] - signal_line

    return float(signal_line[-1]), float(histogram[-1])


def compute_realized_volatility(candles: list[OHLCV], period: int = 30) -> float | None:
    """
    Annualized realized volatility from log returns.

    Args:
        candles: OHLCV data, oldest first.
        period: Number of periods to compute over.

    Returns:
        Annualized volatility as a decimal, or None.
    """
    if len(candles) < period + 1:
        return None

    closes = np.array([c.close for c in candles[-(period + 1):]])
    log_returns = np.log(closes[1:] / closes[:-1])
    return float(np.std(log_returns) * np.sqrt(365))


def compute_correlation(
    candles_a: list[OHLCV],
    candles_b: list[OHLCV],
    period: int = 30,
) -> float | None:
    """
    Pearson correlation between two assets' returns.

    Args:
        candles_a: OHLCV data for asset A.
        candles_b: OHLCV data for asset B (must cover same time range).
        period: Number of periods.

    Returns:
        Correlation coefficient (-1 to +1), or None.
    """
    min_len = min(len(candles_a), len(candles_b))
    if min_len < period + 1:
        return None

    closes_a = np.array([c.close for c in candles_a[-min_len:]])
    closes_b = np.array([c.close for c in candles_b[-min_len:]])

    returns_a = np.diff(np.log(closes_a))[-period:]
    returns_b = np.diff(np.log(closes_b))[-period:]

    if len(returns_a) < 2:
        return None

    corr_matrix = np.corrcoef(returns_a, returns_b)
    return float(corr_matrix[0, 1])


def build_indicator_set(
    symbol: str,
    candles: list[OHLCV],
    btc_candles: list[OHLCV] | None = None,
) -> IndicatorSet:
    """
    Build a complete IndicatorSet from OHLCV data.

    Args:
        symbol: The asset symbol.
        candles: OHLCV data for the asset.
        btc_candles: Optional BTC candles for correlation calculation.

    Returns:
        IndicatorSet with all computed indicators.
    """
    macd_signal, macd_hist = compute_macd(candles)

    return IndicatorSet(
        symbol=symbol,
        atr_14=compute_atr(candles, 14),
        realized_vol_30=compute_realized_volatility(candles, 30),
        rsi_14=compute_rsi(candles, 14),
        macd_signal=macd_signal,
        macd_histogram=macd_hist,
        correlation_btc=compute_correlation(candles, btc_candles) if btc_candles else None,
        timestamp=datetime.now(tz=timezone.utc),
    )
