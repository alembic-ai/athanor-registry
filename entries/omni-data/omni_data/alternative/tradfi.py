"""
TradFi Data Adapters — equities, forex, and traditional markets.

Fetches data from free and freemium APIs:
  - Polygon.io (equities, forex, options)
  - Alpha Vantage (equities, forex, crypto)
  - Yahoo Finance (equities via yfinance-style REST)
  - Treasury yields and economic calendar

All return strict Pydantic schemas compatible with the Omni-Data
broadcast pipeline. Uses RateLimitedSession for resilience.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

from omni_data.integrations.retry import RateLimitedSession
from omni_data.schemas.models import (
    OHLCV,
    EventSeverity,
    EventWarning,
    MacroIndicator,
    Ticker,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Polygon.io — requires API key (env: POLYGON_API_KEY)
# ---------------------------------------------------------------------------

async def fetch_polygon_ticker(
    symbol: str,
    *,
    api_key: str | None = None,
) -> Ticker | None:
    """
    Fetch the latest ticker snapshot from Polygon.io.

    Args:
        symbol: Stock ticker (e.g. 'AAPL', 'MSFT').
        api_key: Polygon.io API key. Falls back to POLYGON_API_KEY env var.

    Returns:
        Ticker schema, or None on failure.
    """
    key = api_key or os.environ.get("POLYGON_API_KEY", "")
    if not key:
        logger.warning("POLYGON_API_KEY not set. Skipping Polygon fetch.")
        return None

    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
    params = {"apiKey": key}

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("Polygon ticker failed for %s: %s", symbol, exc)
            return None

    ticker_data = data.get("ticker", {})
    day = ticker_data.get("day", {})
    prev_day = ticker_data.get("prevDay", {})

    last_price = day.get("c", 0)
    prev_close = prev_day.get("c", 0)
    change_pct = ((last_price - prev_close) / prev_close * 100) if prev_close else 0

    return Ticker(
        symbol=symbol,
        bid=float(ticker_data.get("lastQuote", {}).get("p", 0) or 0),
        ask=float(ticker_data.get("lastQuote", {}).get("P", 0) or 0),
        last=float(last_price),
        volume_24h=float(day.get("v", 0)),
        change_pct_24h=change_pct,
        timestamp=datetime.now(tz=timezone.utc),
    )


async def fetch_polygon_ohlcv(
    symbol: str,
    *,
    timespan: str = "day",
    multiplier: int = 1,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 100,
    api_key: str | None = None,
) -> list[OHLCV]:
    """
    Fetch historical OHLCV data from Polygon.io.

    Args:
        symbol: Stock ticker.
        timespan: 'minute', 'hour', 'day', 'week', 'month'.
        multiplier: Timespan multiplier.
        from_date: Start date (YYYY-MM-DD). Defaults to 100 days ago.
        to_date: End date (YYYY-MM-DD). Defaults to today.
        limit: Max results.
        api_key: Polygon.io API key.

    Returns:
        List of OHLCV schemas.
    """
    key = api_key or os.environ.get("POLYGON_API_KEY", "")
    if not key:
        logger.warning("POLYGON_API_KEY not set.")
        return []

    if not to_date:
        to_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    if not from_date:
        from_dt = datetime.now(tz=timezone.utc) - timedelta(days=limit)
        from_date = from_dt.strftime("%Y-%m-%d")

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}"
        f"/range/{multiplier}/{timespan}/{from_date}/{to_date}"
    )
    params = {"adjusted": "true", "sort": "asc", "limit": str(limit), "apiKey": key}

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("Polygon OHLCV failed for %s: %s", symbol, exc)
            return []

    results = data.get("results", [])
    return [
        OHLCV(
            timestamp=datetime.fromtimestamp(
                r.get("t", 0) / 1000, tz=timezone.utc
            ),
            open=float(r.get("o", 0)),
            high=float(r.get("h", 0)),
            low=float(r.get("l", 0)),
            close=float(r.get("c", 0)),
            volume=float(r.get("v", 0)),
        )
        for r in results
    ]


# ---------------------------------------------------------------------------
# Alpha Vantage — requires API key (env: ALPHAVANTAGE_API_KEY)
# ---------------------------------------------------------------------------

async def fetch_alphavantage_quote(
    symbol: str,
    *,
    api_key: str | None = None,
) -> Ticker | None:
    """
    Fetch a real-time quote from Alpha Vantage.

    Args:
        symbol: Stock or forex ticker (e.g. 'AAPL', 'EURUSD').
        api_key: Alpha Vantage API key.

    Returns:
        Ticker schema, or None on failure.
    """
    key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not key:
        logger.warning("ALPHAVANTAGE_API_KEY not set.")
        return None

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": key,
    }

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("Alpha Vantage quote failed for %s: %s", symbol, exc)
            return None

    quote = data.get("Global Quote", {})
    if not quote:
        return None

    return Ticker(
        symbol=symbol,
        bid=float(quote.get("05. price", 0)),
        ask=float(quote.get("05. price", 0)),
        last=float(quote.get("05. price", 0)),
        volume_24h=float(quote.get("06. volume", 0)),
        change_pct_24h=float(quote.get("10. change percent", "0").rstrip("%") or 0),
        timestamp=datetime.now(tz=timezone.utc),
    )


async def fetch_alphavantage_forex(
    from_currency: str = "EUR",
    to_currency: str = "USD",
    *,
    api_key: str | None = None,
) -> MacroIndicator | None:
    """
    Fetch a forex exchange rate from Alpha Vantage.

    Args:
        from_currency: Source currency code.
        to_currency: Target currency code.
        api_key: Alpha Vantage API key.

    Returns:
        MacroIndicator with the exchange rate.
    """
    key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not key:
        return None

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": from_currency,
        "to_currency": to_currency,
        "apikey": key,
    }

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.warning("Alpha Vantage forex failed: %s", exc)
            return None

    rate_data = data.get("Realtime Currency Exchange Rate", {})
    rate = rate_data.get("5. Exchange Rate")
    if not rate:
        return None

    return MacroIndicator(
        name=f"{from_currency}/{to_currency} Exchange Rate",
        value=float(rate),
        unit=to_currency,
        source="alphavantage",
        timestamp=datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Economic Calendar / Earnings
# ---------------------------------------------------------------------------

async def fetch_earnings_calendar(
    *,
    api_key: str | None = None,
    horizon: str = "3month",
) -> list[EventWarning]:
    """
    Fetch upcoming earnings announcements from Alpha Vantage.

    Args:
        api_key: Alpha Vantage API key.
        horizon: '3month', '6month', or '12month'.

    Returns:
        List of EventWarning schemas.
    """
    key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not key:
        logger.warning("ALPHAVANTAGE_API_KEY not set.")
        return []

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "EARNINGS_CALENDAR",
        "horizon": horizon,
        "apikey": key,
    }

    import aiohttp
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
        except Exception as exc:
            logger.warning("Earnings calendar failed: %s", exc)
            return []

    # CSV format
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return []

    headers = lines[0].split(",")
    events: list[EventWarning] = []

    for line in lines[1:51]:  # Max 50 events
        fields = line.split(",")
        if len(fields) < len(headers):
            continue

        row = dict(zip(headers, fields))
        symbol_val = row.get("symbol", "")
        report_date = row.get("reportDate", "")
        estimate = row.get("estimate", "N/A")

        events.append(EventWarning(
            title=f"Earnings Report: {symbol_val}",
            description=f"Estimated EPS: {estimate}. Fiscal end: {row.get('fiscalDateEnding', 'N/A')}.",
            severity=EventSeverity.MEDIUM,
            event_type="earnings",
            symbol=symbol_val,
            scheduled_at=datetime.strptime(report_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            ) if report_date else None,
            source="alphavantage",
            timestamp=datetime.now(tz=timezone.utc),
        ))

    return events


# ---------------------------------------------------------------------------
# Treasury Yields
# ---------------------------------------------------------------------------

async def fetch_treasury_yields(
    *,
    api_key: str | None = None,
) -> list[MacroIndicator]:
    """
    Fetch US Treasury yield data from Alpha Vantage.

    Returns yield for 2Y, 5Y, 10Y, and 30Y maturities.
    """
    key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not key:
        return []

    maturities = {
        "2year": "TREASURY_YIELD",
        "5year": "TREASURY_YIELD",
        "10year": "TREASURY_YIELD",
        "30year": "TREASURY_YIELD",
    }

    indicators: list[MacroIndicator] = []

    for maturity in ["2year", "5year", "10year", "30year"]:
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TREASURY_YIELD",
            "interval": "daily",
            "maturity": maturity,
            "apikey": key,
        }

        async with RateLimitedSession(timeout=15.0) as session:
            try:
                data = await session.get_json(url, params=params)
            except Exception as exc:
                logger.warning("Treasury yield failed for %s: %s", maturity, exc)
                continue

        data_points = data.get("data", [])
        if data_points:
            latest = data_points[0]
            val = latest.get("value", ".")
            if val != ".":
                indicators.append(MacroIndicator(
                    name=f"US Treasury Yield {maturity.upper()}",
                    value=float(val),
                    unit="%",
                    source="alphavantage",
                    timestamp=datetime.strptime(
                        latest.get("date", "2024-01-01"), "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc),
                ))

    return indicators
