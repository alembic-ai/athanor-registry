"""
Portfolio State Injector — exchange-agnostic position tracking.

Aggregates balances, positions, and PnL from any number of
exchanges through the UniversalDataGateway (read-only).

Stateless: takes gateway instances in, returns PortfolioState out.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from omni_data.integrations.gateway import UniversalDataGateway
from omni_data.schemas.models import PortfolioState, Position, Side

logger = logging.getLogger(__name__)


async def build_portfolio_state(
    gateways: list[UniversalDataGateway],
) -> PortfolioState:
    """
    Aggregate portfolio state across multiple exchange gateways.

    This function queries each connected gateway for balances and
    open positions, then merges them into a single PortfolioState.

    Args:
        gateways: List of initialized UniversalDataGateway instances
                  (must already have markets loaded via __aenter__).

    Returns:
        PortfolioState with aggregated balances, positions, and equity metrics.
    """
    all_positions: list[Position] = []
    all_balances: dict[str, float] = {}
    exchange_ids: list[str] = []
    total_equity = 0.0
    total_free = 0.0
    total_used = 0.0

    for gw in gateways:
        exchange_ids.append(gw.exchange_id)

        # Fetch balances
        try:
            balances = await gw.fetch_balances()
            for asset, bal in balances.items():
                all_balances[f"{gw.exchange_id}:{asset}"] = bal
                total_free += bal
        except Exception as exc:
            logger.warning("Failed to fetch balances from %s: %s", gw.exchange_id, exc)

        # Fetch positions (derivatives exchanges)
        try:
            raw_positions = await gw.fetch_positions()
            for raw in raw_positions:
                pos = _format_position(raw, gw.exchange_id)
                if pos is not None:
                    all_positions.append(pos)
                    total_used += abs(pos.margin_used or 0)
        except Exception as exc:
            logger.warning("Failed to fetch positions from %s: %s", gw.exchange_id, exc)

    # Calculate total equity
    unrealized = sum(p.unrealized_pnl for p in all_positions)
    total_equity = total_free + total_used + unrealized

    return PortfolioState(
        total_equity=total_equity,
        free_margin=total_free,
        used_margin=total_used,
        positions=all_positions,
        balances=all_balances,
        exchanges=exchange_ids,
        timestamp=datetime.now(tz=timezone.utc),
    )


def _format_position(raw: dict[str, Any], exchange_id: str) -> Position | None:
    """
    Convert a raw CCXT position dict into a Position schema.

    Args:
        raw: Raw position dict from exchange.fetch_positions().
        exchange_id: Exchange identifier.

    Returns:
        Position schema, or None if position is empty/closed.
    """
    contracts = float(raw.get("contracts") or raw.get("contractSize") or 0)
    if contracts == 0:
        return None

    side_str = (raw.get("side") or "").lower()
    side = Side.BID if side_str in ("long", "buy") else Side.ASK

    entry_price = float(raw.get("entryPrice") or 0)
    mark_price = float(raw.get("markPrice") or raw.get("currentPrice") or 0)
    unrealized = float(raw.get("unrealizedPnl") or 0)
    leverage = float(raw.get("leverage") or 1)
    liq_price = raw.get("liquidationPrice")
    margin = raw.get("initialMargin") or raw.get("collateral")

    return Position(
        symbol=raw.get("symbol", "UNKNOWN"),
        exchange=exchange_id,
        side=side,
        size=contracts,
        entry_price=entry_price,
        current_price=mark_price,
        unrealized_pnl=unrealized,
        realized_pnl=float(raw.get("realizedPnl") or 0),
        leverage=leverage,
        liquidation_price=float(liq_price) if liq_price else None,
        margin_used=float(margin) if margin else None,
    )
