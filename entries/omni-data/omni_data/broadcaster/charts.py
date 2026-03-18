"""
Chart Renderer — generates visual chart images from market data.

Produces candlestick charts, orderbook heatmaps, and indicator
overlays using mplfinance and plotly. Returns raw bytes (PNG/WebP)
for broadcasting to vision-capable models or UI display.

Stateless: data in, image bytes out. No persistent state.
"""

from __future__ import annotations

import io
import logging
from typing import Any

import pandas as pd

from omni_data.schemas.models import OHLCV, MarketStateSummary

logger = logging.getLogger(__name__)


def render_candlestick_chart(
    candles: list[OHLCV],
    *,
    title: str = "",
    volume: bool = True,
    style: str = "nightclouds",
    figsize: tuple[int, int] = (12, 6),
    dpi: int = 100,
) -> bytes:
    """
    Render a candlestick chart from OHLCV data as PNG bytes.

    Args:
        candles: OHLCV data (oldest first).
        title: Chart title.
        volume: Whether to include volume subplot.
        style: mplfinance style name.
        figsize: Figure size in inches.
        dpi: Dots per inch.

    Returns:
        PNG image bytes.
    """
    import mplfinance as mpf

    df = pd.DataFrame([
        {
            "Date": c.timestamp,
            "Open": c.open,
            "High": c.high,
            "Low": c.low,
            "Close": c.close,
            "Volume": c.volume,
        }
        for c in candles
    ])
    df.set_index("Date", inplace=True)
    df.index = pd.DatetimeIndex(df.index)

    buf = io.BytesIO()
    mpf.plot(
        df,
        type="candle",
        volume=volume,
        style=style,
        title=title,
        figsize=figsize,
        savefig=dict(fname=buf, dpi=dpi, bbox_inches="tight"),
    )
    buf.seek(0)
    return buf.read()


def render_orderbook_heatmap(
    state: MarketStateSummary,
    *,
    levels: int = 20,
) -> bytes | None:
    """
    Render an orderbook depth heatmap as PNG bytes using plotly.

    Args:
        state: MarketStateSummary with orderbook data.
        levels: Number of price levels to display per side.

    Returns:
        PNG image bytes, or None if no orderbook data.
    """
    if not state.orderbook:
        return None

    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("plotly not available for heatmap rendering")
        return None

    ob = state.orderbook
    bid_prices = [b.price for b in ob.bids[:levels]]
    bid_sizes = [b.quantity for b in ob.bids[:levels]]
    ask_prices = [a.price for a in ob.asks[:levels]]
    ask_sizes = [a.quantity for a in ob.asks[:levels]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=bid_prices, y=bid_sizes,
        name="Bids", marker_color="rgba(0, 200, 100, 0.7)",
    ))
    fig.add_trace(go.Bar(
        x=ask_prices, y=ask_sizes,
        name="Asks", marker_color="rgba(255, 80, 80, 0.7)",
    ))
    fig.update_layout(
        title=f"Order Book Depth — {state.symbol}",
        xaxis_title="Price",
        yaxis_title="Quantity",
        template="plotly_dark",
        width=800, height=400,
    )

    return fig.to_image(format="png")
