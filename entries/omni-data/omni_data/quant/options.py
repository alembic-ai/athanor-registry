"""
Options Chain Adapter — fetches and formats options data.

Supports Deribit (crypto options) and generic CCXT exchanges
that expose options endpoints. Formats raw data into strict
OptionContract schemas with computed Greeks.

Stateless: data in, formatted schemas out.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp

from omni_data.integrations.retry import RateLimitedSession
from omni_data.schemas.models import OptionContract

logger = logging.getLogger(__name__)


async def fetch_deribit_options(
    underlying: str = "BTC",
    currency: str = "BTC",
    *,
    expired: bool = False,
) -> list[OptionContract]:
    """
    Fetch all active options instruments and their greeks from Deribit.

    Uses the public (no auth) Deribit API v2.

    Args:
        underlying: Underlying asset (e.g. 'BTC', 'ETH').
        currency: Settlement currency.
        expired: Whether to include expired instruments.

    Returns:
        List of OptionContract schemas with IV and Greeks.
    """
    # Step 1: Get all option instruments
    instruments_url = "https://www.deribit.com/api/v2/public/get_instruments"
    instruments_params = {
        "currency": currency,
        "kind": "option",
        "expired": str(expired).lower(),
    }

    async with RateLimitedSession(timeout=20.0) as session:
        instruments_data = await session.get_json(
            instruments_url, params=instruments_params
        )

    instruments = instruments_data.get("result", [])
    if not instruments:
        return []

    # Step 2: Fetch ticker data for each instrument (batched)
    contracts: list[OptionContract] = []
    now = datetime.now(tz=timezone.utc)

    async with RateLimitedSession(timeout=20.0) as session:
        for inst in instruments:
            instrument_name = inst.get("instrument_name", "")
            try:
                ticker_url = "https://www.deribit.com/api/v2/public/ticker"
                ticker_data = await session.get_json(
                    ticker_url, params={"instrument_name": instrument_name}
                )
                ticker = ticker_data.get("result", {})

                greeks = ticker.get("greeks", {})

                contract = OptionContract(
                    symbol=instrument_name,
                    underlying=underlying,
                    strike=float(inst.get("strike", 0)),
                    expiry=datetime.fromtimestamp(
                        inst.get("expiration_timestamp", 0) / 1000,
                        tz=timezone.utc,
                    ),
                    option_type=inst.get("option_type", "call"),
                    iv=float(ticker.get("mark_iv", 0)) / 100.0,
                    delta=float(greeks.get("delta", 0)),
                    gamma=float(greeks.get("gamma", 0)),
                    theta=float(greeks.get("theta", 0)),
                    vega=float(greeks.get("vega", 0)),
                    open_interest=float(ticker.get("open_interest", 0)),
                    volume=float(ticker.get("stats", {}).get("volume", 0) or 0),
                    last_price=float(ticker.get("last_price", 0) or 0),
                    timestamp=now,
                )
                contracts.append(contract)

            except Exception as exc:
                logger.warning(
                    "Failed to fetch option %s: %s", instrument_name, exc
                )
                continue

    logger.info(
        "Fetched %d %s option contracts from Deribit", len(contracts), underlying
    )
    return contracts


async def fetch_deribit_iv_surface(
    underlying: str = "BTC",
) -> list[dict[str, Any]]:
    """
    Build a simplified IV surface from Deribit option contracts.

    Returns a list of dicts with strike, expiry, iv, delta, and
    option_type — ready for heatmap visualization.

    Args:
        underlying: Underlying asset (e.g. 'BTC', 'ETH').

    Returns:
        List of IV surface data points.
    """
    contracts = await fetch_deribit_options(underlying)

    surface: list[dict[str, Any]] = []
    for c in contracts:
        surface.append({
            "strike": c.strike,
            "expiry": c.expiry.isoformat(),
            "days_to_expiry": max(0, (c.expiry - datetime.now(tz=timezone.utc)).days),
            "iv": c.iv,
            "delta": c.delta,
            "gamma": c.gamma,
            "option_type": c.option_type,
            "open_interest": c.open_interest,
        })

    return sorted(surface, key=lambda x: (x["days_to_expiry"], x["strike"]))


async def fetch_deribit_funding_history(
    instrument: str = "BTC-PERPETUAL",
    *,
    count: int = 24,
) -> list[dict[str, Any]]:
    """
    Fetch historical funding rates from Deribit.

    Args:
        instrument: Perpetual instrument name.
        count: Number of historical funding rate entries.

    Returns:
        List of raw funding rate dicts.
    """
    url = "https://www.deribit.com/api/v2/public/get_funding_rate_history"
    end_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ts = end_ts - (count * 8 * 3600 * 1000)  # 8h funding intervals

    params = {
        "instrument_name": instrument,
        "start_timestamp": str(start_ts),
        "end_timestamp": str(end_ts),
    }

    async with RateLimitedSession(timeout=15.0) as session:
        data = await session.get_json(url, params=params)

    return data.get("result", [])
