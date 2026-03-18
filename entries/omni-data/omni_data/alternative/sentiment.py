"""
Sentiment & Social Data Pipelines — NewsAPI, LunarCrush, and aggregators.

Ingests raw text streams and pre-computed sentiment scores from
external APIs. Formats into strict SentimentMetric schemas.

NO RAG. NO EMBEDDINGS. NO VECTOR SEARCH.
Pure structured metric normalization only.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from omni_data.integrations.retry import RateLimitedSession
from omni_data.schemas.models import SentimentMetric

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NewsAPI — requires API key (env: NEWSAPI_KEY)
# ---------------------------------------------------------------------------

async def fetch_newsapi_headlines(
    query: str = "bitcoin OR crypto",
    *,
    api_key: str | None = None,
    language: str = "en",
    page_size: int = 20,
) -> list[SentimentMetric]:
    """
    Fetch recent news headlines from NewsAPI and compute a basic
    headline sentiment proxy (headline count as activity indicator).

    Args:
        query: Search query (e.g. 'bitcoin', 'ethereum').
        api_key: NewsAPI key. Falls back to NEWSAPI_KEY env var.
        language: Language filter.
        page_size: Number of articles.

    Returns:
        List of SentimentMetric schemas (one per source category).
    """
    key = api_key or os.environ.get("NEWSAPI_KEY", "")
    if not key:
        logger.warning("NEWSAPI_KEY not set. Skipping NewsAPI fetch.")
        return []

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": language,
        "pageSize": str(page_size),
        "sortBy": "publishedAt",
        "apiKey": key,
    }

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("NewsAPI fetch failed: %s", exc)
            return []

    articles = data.get("articles", [])
    if not articles:
        return []

    now = datetime.now(tz=timezone.utc)

    # Compute activity-based sentiment proxy
    # Higher article count = more market attention = higher score
    total_results = data.get("totalResults", 0)

    # Normalize: 0-20 articles = low, 20-100 = medium, 100+ = high
    if total_results > 100:
        score = 0.8
    elif total_results > 50:
        score = 0.5
    elif total_results > 20:
        score = 0.2
    else:
        score = -0.2  # Low activity can mean complacency

    metrics: list[SentimentMetric] = [
        SentimentMetric(
            source="newsapi",
            symbol=query.split()[0].lower() if query else None,
            score=score,
            raw_value={
                "total_results": total_results,
                "article_count": len(articles),
                "top_headlines": [
                    {
                        "title": a.get("title", ""),
                        "source": a.get("source", {}).get("name", ""),
                        "published": a.get("publishedAt", ""),
                    }
                    for a in articles[:5]
                ],
            },
            timestamp=now,
        ),
    ]

    return metrics


# ---------------------------------------------------------------------------
# LunarCrush — requires API key (env: LUNARCRUSH_API_KEY)
# ---------------------------------------------------------------------------

async def fetch_lunarcrush_social(
    symbol: str = "BTC",
    *,
    api_key: str | None = None,
) -> list[SentimentMetric]:
    """
    Fetch social metrics from LunarCrush (Galaxy Score, AltRank, etc.).

    Args:
        symbol: Asset symbol.
        api_key: LunarCrush API key. Falls back to LUNARCRUSH_API_KEY env var.

    Returns:
        List of SentimentMetric schemas.
    """
    key = api_key or os.environ.get("LUNARCRUSH_API_KEY", "")
    if not key:
        logger.warning("LUNARCRUSH_API_KEY not set. Skipping LunarCrush fetch.")
        return []

    url = "https://lunarcrush.com/api4/public/coins"
    params = {
        "symbol": symbol,
        "key": key,
    }

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("LunarCrush fetch failed for %s: %s", symbol, exc)
            return []

    coins = data.get("data", [])
    if not coins:
        return []

    coin = coins[0]
    now = datetime.now(tz=timezone.utc)
    metrics: list[SentimentMetric] = []

    # Galaxy Score: 0-100, normalize to -1 to +1
    galaxy = coin.get("galaxy_score")
    if galaxy is not None:
        metrics.append(SentimentMetric(
            source="lunarcrush_galaxy",
            symbol=symbol,
            score=(float(galaxy) - 50) / 50.0,
            raw_value=galaxy,
            timestamp=now,
        ))

    # Social Volume
    social_volume = coin.get("social_volume")
    if social_volume is not None:
        metrics.append(SentimentMetric(
            source="lunarcrush_social_volume",
            symbol=symbol,
            score=min(1.0, float(social_volume) / 10000.0),  # Rough normalization
            raw_value=social_volume,
            timestamp=now,
        ))

    # Sentiment (bullish vs bearish mentions)
    sentiment_score = coin.get("average_sentiment")
    if sentiment_score is not None:
        # LunarCrush sentiment is 1-5, normalize to -1 to +1
        normalized = (float(sentiment_score) - 3) / 2.0
        metrics.append(SentimentMetric(
            source="lunarcrush_sentiment",
            symbol=symbol,
            score=max(-1.0, min(1.0, normalized)),
            raw_value=sentiment_score,
            timestamp=now,
        ))

    return metrics


# ---------------------------------------------------------------------------
# Aggregator — combine all sentiment sources
# ---------------------------------------------------------------------------

async def fetch_all_sentiment(
    symbol: str = "BTC",
    query: str = "bitcoin",
) -> list[SentimentMetric]:
    """
    Aggregate sentiment from all available sources.

    This function attempts all sentiment pipelines and returns
    whatever data is available (gracefully skips unavailable sources).

    Args:
        symbol: Asset symbol for LunarCrush.
        query: Search query for NewsAPI.

    Returns:
        Combined list of SentimentMetric schemas from all sources.
    """
    # Import here to avoid circular deps
    from omni_data.alternative.pipelines import fetch_fear_greed_index

    all_metrics: list[SentimentMetric] = []

    # Fear & Greed (always available, no key)
    fng = await fetch_fear_greed_index()
    if fng:
        all_metrics.append(fng)

    # NewsAPI
    news = await fetch_newsapi_headlines(query)
    all_metrics.extend(news)

    # LunarCrush
    lunar = await fetch_lunarcrush_social(symbol)
    all_metrics.extend(lunar)

    return all_metrics
