"""
Alternative Data Pipelines — Macro, On-Chain, Sentiment, Regulatory.

Stateless async fetchers that ingest data from external APIs
and return strict Pydantic schemas. No RAG, no embedding,
no vector search — just structured data normalization.

Each function is a standalone script designed for micro-compute.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp

from omni_data.schemas.models import (
    EventSeverity,
    EventWarning,
    MacroIndicator,
    OnChainMetric,
    SentimentMetric,
)

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=15)


# ---------------------------------------------------------------------------
# CoinGecko — Market overview, global metrics, coin data
# ---------------------------------------------------------------------------

async def fetch_coingecko_global() -> list[MacroIndicator]:
    """
    Fetch global crypto market data from CoinGecko (free, no API key).

    Returns:
        List of MacroIndicator schemas for market cap, volume, dominance, etc.
    """
    url = "https://api.coingecko.com/api/v3/global"
    async with aiohttp.ClientSession(timeout=_DEFAULT_TIMEOUT) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("CoinGecko global returned %d", resp.status)
                return []
            data = (await resp.json()).get("data", {})

    now = datetime.now(tz=timezone.utc)
    indicators: list[MacroIndicator] = []

    mapping: dict[str, tuple[str, str]] = {
        "total_market_cap": ("Total Market Cap (USD)", "USD"),
        "total_volume": ("Total 24h Volume (USD)", "USD"),
        "market_cap_change_percentage_24h_usd": ("Market Cap Change 24h", "%"),
    }

    for key, (name, unit) in mapping.items():
        val = data.get(key)
        if isinstance(val, dict):
            val = val.get("usd", 0)
        if val is not None:
            indicators.append(MacroIndicator(
                name=name, value=float(val), unit=unit,
                source="coingecko", timestamp=now,
            ))

    # BTC dominance
    btc_dom = data.get("market_cap_percentage", {}).get("btc")
    if btc_dom is not None:
        indicators.append(MacroIndicator(
            name="BTC Dominance", value=float(btc_dom), unit="%",
            source="coingecko", timestamp=now,
        ))

    return indicators


async def fetch_coingecko_coin(coin_id: str = "bitcoin") -> list[MacroIndicator]:
    """
    Fetch detailed market data for a specific coin from CoinGecko.

    Args:
        coin_id: CoinGecko coin identifier (e.g. 'bitcoin', 'ethereum').

    Returns:
        List of MacroIndicator schemas.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "community_data": "false",
        "developer_data": "false",
    }

    async with aiohttp.ClientSession(timeout=_DEFAULT_TIMEOUT) as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.warning("CoinGecko coin %s returned %d", coin_id, resp.status)
                return []
            data = await resp.json()

    now = datetime.now(tz=timezone.utc)
    market = data.get("market_data", {})
    indicators: list[MacroIndicator] = []

    fields: list[tuple[str, str, str]] = [
        ("current_price", f"{coin_id.upper()} Price", "USD"),
        ("market_cap", f"{coin_id.upper()} Market Cap", "USD"),
        ("total_volume", f"{coin_id.upper()} 24h Volume", "USD"),
        ("price_change_percentage_24h", f"{coin_id.upper()} 24h Change", "%"),
        ("price_change_percentage_7d", f"{coin_id.upper()} 7d Change", "%"),
        ("ath_change_percentage", f"{coin_id.upper()} ATH Change", "%"),
    ]

    for key, name, unit in fields:
        val = market.get(key)
        if isinstance(val, dict):
            val = val.get("usd", 0)
        if val is not None:
            indicators.append(MacroIndicator(
                name=name, value=float(val), unit=unit,
                source="coingecko", timestamp=now,
            ))

    return indicators


# ---------------------------------------------------------------------------
# Fear & Greed Index — alternative.me
# ---------------------------------------------------------------------------

async def fetch_fear_greed_index() -> SentimentMetric | None:
    """
    Fetch the Crypto Fear & Greed Index from alternative.me.

    Returns:
        SentimentMetric schema, or None on failure.
    """
    url = "https://api.alternative.me/fng/?limit=1&format=json"
    async with aiohttp.ClientSession(timeout=_DEFAULT_TIMEOUT) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("Fear & Greed API returned %d", resp.status)
                return None
            data = await resp.json()

    entries = data.get("data", [])
    if not entries:
        return None

    entry = entries[0]
    raw_value = int(entry.get("value", 50))
    # Normalize 0-100 to -1 to +1 range
    normalized = (raw_value - 50) / 50.0

    return SentimentMetric(
        source="fear_greed_index",
        symbol=None,
        score=normalized,
        raw_value=raw_value,
        timestamp=datetime.fromtimestamp(
            int(entry.get("timestamp", 0)), tz=timezone.utc
        ),
    )


# ---------------------------------------------------------------------------
# FRED — Federal Reserve Economic Data (requires API key)
# ---------------------------------------------------------------------------

async def fetch_fred_series(
    series_id: str,
    api_key: str,
    *,
    limit: int = 1,
) -> list[MacroIndicator]:
    """
    Fetch a specific data series from FRED.

    Args:
        series_id: FRED series ID (e.g. 'DFF' for Fed Funds Rate,
                   'CPIAUCSL' for CPI, 'T10Y2Y' for yield curve).
        api_key: FRED API key (from environment variable).
        limit: Number of observations to return.

    Returns:
        List of MacroIndicator schemas.
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": str(limit),
    }

    async with aiohttp.ClientSession(timeout=_DEFAULT_TIMEOUT) as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.warning("FRED %s returned %d", series_id, resp.status)
                return []
            data = await resp.json()

    observations = data.get("observations", [])
    indicators: list[MacroIndicator] = []

    for obs in observations:
        val = obs.get("value", ".")
        if val == ".":
            continue
        indicators.append(MacroIndicator(
            name=series_id,
            value=float(val),
            unit="",
            source="fred",
            timestamp=datetime.strptime(obs["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc),
        ))

    return indicators


# ---------------------------------------------------------------------------
# On-Chain Metrics — placeholder adapters for Glassnode/DeFiLlama
# ---------------------------------------------------------------------------

async def fetch_defillama_tvl(protocol: str = "ethereum") -> OnChainMetric | None:
    """
    Fetch Total Value Locked (TVL) from DeFi Llama.

    Args:
        protocol: Protocol name (e.g. 'ethereum', 'aave', 'lido').

    Returns:
        OnChainMetric schema, or None on failure.
    """
    url = f"https://api.llama.fi/tvl/{protocol}"
    async with aiohttp.ClientSession(timeout=_DEFAULT_TIMEOUT) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("DeFi Llama %s returned %d", protocol, resp.status)
                return None
            tvl = await resp.json()

    if not isinstance(tvl, (int, float)):
        return None

    return OnChainMetric(
        network=protocol,
        metric_name="tvl",
        value=float(tvl),
        source="defillama",
        timestamp=datetime.now(tz=timezone.utc),
    )


async def fetch_defillama_chains() -> list[OnChainMetric]:
    """
    Fetch TVL for all major chains from DeFi Llama.

    Returns:
        List of OnChainMetric schemas.
    """
    url = "https://api.llama.fi/v2/chains"
    async with aiohttp.ClientSession(timeout=_DEFAULT_TIMEOUT) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("DeFi Llama chains returned %d", resp.status)
                return []
            chains = await resp.json()

    now = datetime.now(tz=timezone.utc)
    return [
        OnChainMetric(
            network=chain.get("name", "unknown"),
            metric_name="tvl",
            value=float(chain.get("tvl", 0)),
            source="defillama",
            timestamp=now,
        )
        for chain in chains[:50]  # Top 50 chains
        if chain.get("tvl")
    ]
