"""
Derivatives data formatters — funding rates, liquidations, options.

Transforms raw exchange responses into strict Pydantic schemas.
Stateless script functions, zero execution code.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from omni_data.schemas.models import FundingRate, LiquidationEvent, Side


def format_funding_rate(
    raw: dict[str, Any],
    exchange_id: str,
) -> FundingRate | None:
    """
    Convert a raw CCXT funding rate response into a FundingRate schema.

    Args:
        raw: Raw dict from exchange.fetch_funding_rate().
        exchange_id: Exchange identifier.

    Returns:
        FundingRate schema, or None if data is malformed.
    """
    if not raw or "fundingRate" not in raw:
        return None

    next_ts = raw.get("fundingTimestamp") or raw.get("nextFundingTimestamp")

    return FundingRate(
        symbol=raw.get("symbol", "UNKNOWN"),
        exchange=exchange_id,
        rate=float(raw["fundingRate"]),
        next_funding_time=(
            datetime.fromtimestamp(next_ts / 1000, tz=timezone.utc)
            if next_ts else None
        ),
        timestamp=datetime.fromtimestamp(
            (raw.get("timestamp") or 0) / 1000, tz=timezone.utc
        ) if raw.get("timestamp") else datetime.now(tz=timezone.utc),
    )


def format_liquidation(
    raw: dict[str, Any],
    exchange_id: str,
) -> LiquidationEvent:
    """
    Convert a raw liquidation event dict into a LiquidationEvent schema.

    Args:
        raw: Raw liquidation dict (from exchange WebSocket or REST).
        exchange_id: Exchange identifier.

    Returns:
        LiquidationEvent schema.
    """
    return LiquidationEvent(
        symbol=raw.get("symbol", "UNKNOWN"),
        exchange=exchange_id,
        side=Side.BID if raw.get("side", "").lower() in ("buy", "long") else Side.ASK,
        quantity=float(raw.get("amount", 0) or raw.get("quantity", 0)),
        price=float(raw.get("price", 0)),
        timestamp=datetime.fromtimestamp(
            (raw.get("timestamp") or 0) / 1000, tz=timezone.utc
        ) if raw.get("timestamp") else datetime.now(tz=timezone.utc),
    )
