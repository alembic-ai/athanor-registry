"""
Orderflow Physics — procedural calculations from raw order book
and trade data. Zero LLM involvement, pure numpy math.

All functions are stateless: input data in, metrics out.
Designed to run on a fraction of a CPU core.
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from omni_data.schemas.models import (
    OrderBookDepth,
    OrderflowMetrics,
    Side,
    Trade,
)


def compute_orderflow_metrics(
    orderbook: OrderBookDepth,
    recent_trades: list[Trade],
    *,
    depth_pct: float = 0.02,
) -> OrderflowMetrics:
    """
    Compute orderflow analytics from an order book snapshot and recent trades.

    Args:
        orderbook: L2/L3 order book snapshot.
        recent_trades: Recent trade list for volume delta calculation.
        depth_pct: Percentage depth around mid-price for liquidity measurement (default 2%).

    Returns:
        OrderflowMetrics with volume_delta, VOI, liquidity measurements, and imbalance ratio.
    """
    # Volume Delta — buy volume minus sell volume
    buy_volume = sum(t.amount for t in recent_trades if t.side == Side.BID)
    sell_volume = sum(t.amount for t in recent_trades if t.side == Side.ASK)
    volume_delta = buy_volume - sell_volume

    # Mid price
    best_bid = orderbook.bids[0].price if orderbook.bids else 0.0
    best_ask = orderbook.asks[0].price if orderbook.asks else 0.0
    mid_price = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else 0.0

    # Liquidity within depth_pct of mid
    upper_bound = mid_price * (1 + depth_pct)
    lower_bound = mid_price * (1 - depth_pct)

    liquidity_above = sum(
        a.quantity for a in orderbook.asks if a.price <= upper_bound
    )
    liquidity_below = sum(
        b.quantity for b in orderbook.bids if b.price >= lower_bound
    )

    # Imbalance ratio
    total_liq = liquidity_below + liquidity_above
    imbalance_ratio = liquidity_below / total_liq if total_liq > 0 else 0.5

    # Volume Order Imbalance (VOI) — simplified
    # VOI = bid_volume_change - ask_volume_change (approximated from current snapshot)
    bid_vol = sum(b.quantity for b in orderbook.bids[:10])
    ask_vol = sum(a.quantity for a in orderbook.asks[:10])
    voi = bid_vol - ask_vol

    return OrderflowMetrics(
        symbol=orderbook.symbol,
        volume_delta=volume_delta,
        voi=voi,
        liquidity_above=liquidity_above,
        liquidity_below=liquidity_below,
        imbalance_ratio=imbalance_ratio,
        timestamp=datetime.now(tz=timezone.utc),
    )
