"""
On-Chain Data Adapters — Glassnode, TokenTerminal, and Blockchain.info.

Stateless async fetch functions that return strict Pydantic
OnChainMetric schemas. All require API keys except Blockchain.info.

Uses RateLimitedSession for all HTTP calls.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from omni_data.integrations.retry import RateLimitedSession
from omni_data.schemas.models import OnChainMetric

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Glassnode — requires API key (env: GLASSNODE_API_KEY)
# ---------------------------------------------------------------------------

async def fetch_glassnode_metric(
    asset: str = "BTC",
    metric: str = "market/price_usd_close",
    *,
    api_key: str | None = None,
    interval: str = "24h",
) -> list[OnChainMetric]:
    """
    Fetch a specific metric from Glassnode.

    Args:
        asset: Asset symbol (e.g. 'BTC', 'ETH').
        metric: Glassnode metric path (e.g. 'market/price_usd_close',
                'addresses/active_count', 'mining/hash_rate_mean').
        api_key: Glassnode API key. Falls back to GLASSNODE_API_KEY env var.
        interval: Resolution ('1h', '24h', '1w', '1month').

    Returns:
        List of OnChainMetric schemas.
    """
    key = api_key or os.environ.get("GLASSNODE_API_KEY", "")
    if not key:
        logger.warning("GLASSNODE_API_KEY not set. Skipping Glassnode fetch.")
        return []

    url = f"https://api.glassnode.com/v1/metrics/{metric}"
    params = {
        "a": asset,
        "api_key": key,
        "i": interval,
        "f": "JSON",
        "s": "",  # from start (will return latest by default)
    }

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("Glassnode fetch failed for %s/%s: %s", asset, metric, exc)
            return []

    if not isinstance(data, list):
        return []

    metrics: list[OnChainMetric] = []
    for entry in data[-10:]:  # Last 10 data points
        metrics.append(OnChainMetric(
            network=asset,
            metric_name=metric.split("/")[-1],
            value=float(entry.get("v", 0)),
            source="glassnode",
            timestamp=datetime.fromtimestamp(
                entry.get("t", 0), tz=timezone.utc
            ),
        ))

    return metrics


async def fetch_glassnode_suite(
    asset: str = "BTC",
    *,
    api_key: str | None = None,
) -> list[OnChainMetric]:
    """
    Fetch a comprehensive suite of on-chain metrics from Glassnode.

    Args:
        asset: Asset symbol.
        api_key: Glassnode API key.

    Returns:
        Combined list of OnChainMetric schemas.
    """
    metric_paths = [
        "addresses/active_count",
        "addresses/new_non_zero_count",
        "transactions/count",
        "mining/hash_rate_mean",
        "supply/current",
        "market/marketcap_usd",
        "indicators/sopr",
        "indicators/nvt",
    ]

    all_metrics: list[OnChainMetric] = []
    for path in metric_paths:
        result = await fetch_glassnode_metric(
            asset, path, api_key=api_key
        )
        all_metrics.extend(result)

    return all_metrics


# ---------------------------------------------------------------------------
# TokenTerminal — requires API key (env: TOKEN_TERMINAL_API_KEY)
# ---------------------------------------------------------------------------

async def fetch_token_terminal_metrics(
    project_id: str = "ethereum",
    *,
    api_key: str | None = None,
) -> list[OnChainMetric]:
    """
    Fetch protocol metrics from Token Terminal.

    Args:
        project_id: Token Terminal project identifier.
        api_key: Token Terminal API key. Falls back to TOKEN_TERMINAL_API_KEY env var.

    Returns:
        List of OnChainMetric schemas.
    """
    key = api_key or os.environ.get("TOKEN_TERMINAL_API_KEY", "")
    if not key:
        logger.warning("TOKEN_TERMINAL_API_KEY not set. Skipping TokenTerminal fetch.")
        return []

    url = f"https://api.tokenterminal.com/v2/projects/{project_id}/metrics"
    headers = {"Authorization": f"Bearer {key}"}

    async with RateLimitedSession(
        timeout=15.0,
        headers=headers,
    ) as session:
        try:
            data = await session.get_json(url)
        except Exception as exc:
            logger.warning("TokenTerminal fetch failed for %s: %s", project_id, exc)
            return []

    if not isinstance(data, dict):
        return []

    now = datetime.now(tz=timezone.utc)
    metrics: list[OnChainMetric] = []

    # Map common TokenTerminal fields
    field_map = {
        "revenue_30d": "revenue_30d",
        "fees_30d": "fees_30d",
        "tvl": "tvl",
        "price": "price",
        "market_cap": "market_cap",
        "fully_diluted_valuation": "fdv",
        "pe_ratio": "pe_ratio",
    }

    for field, metric_name in field_map.items():
        val = data.get(field)
        if val is not None:
            metrics.append(OnChainMetric(
                network=project_id,
                metric_name=metric_name,
                value=float(val),
                source="token_terminal",
                timestamp=now,
            ))

    return metrics


# ---------------------------------------------------------------------------
# Blockchain.info — free, no API key required
# ---------------------------------------------------------------------------

async def fetch_blockchain_info_stats() -> list[OnChainMetric]:
    """
    Fetch Bitcoin network statistics from Blockchain.info (free, no key).

    Returns:
        List of OnChainMetric schemas for hash rate, difficulty,
        transaction count, market price, etc.
    """
    url = "https://api.blockchain.info/stats"

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url)
        except Exception as exc:
            logger.warning("Blockchain.info fetch failed: %s", exc)
            return []

    now = datetime.now(tz=timezone.utc)
    metrics: list[OnChainMetric] = []

    field_map = {
        "hash_rate": ("hash_rate", "TH/s"),
        "difficulty": ("difficulty", ""),
        "n_tx": ("daily_transactions", ""),
        "market_price_usd": ("price_usd", "USD"),
        "n_blocks_total": ("total_blocks", ""),
        "totalbc": ("total_btc_mined", "satoshi"),
        "minutes_between_blocks": ("block_time", "minutes"),
        "total_fees_btc": ("total_fees", "BTC"),
    }

    for field, (metric_name, _unit) in field_map.items():
        val = data.get(field)
        if val is not None:
            metrics.append(OnChainMetric(
                network="BTC",
                metric_name=metric_name,
                value=float(val),
                source="blockchain_info",
                timestamp=now,
            ))

    return metrics
