"""
SEC EDGAR & Regulatory Data Pipeline.

Fetches recent SEC filings (10-K, 10-Q, 8-K, etc.) from the
EDGAR EFTS full-text search API and token unlock schedules.
Formats results into EventWarning schemas.

No API key required — EDGAR is a free public service.
SEC requires a User-Agent header with contact info.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from omni_data.integrations.retry import RateLimitedSession
from omni_data.schemas.models import EventSeverity, EventWarning

logger = logging.getLogger(__name__)

# SEC requires a descriptive User-Agent
_SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "OmniDataMarketEngine/0.1.0 (contact@alembic.ai)"
)

# Filing type to severity mapping
_FILING_SEVERITY: dict[str, EventSeverity] = {
    "8-K": EventSeverity.HIGH,       # Material events
    "10-K": EventSeverity.MEDIUM,    # Annual report
    "10-Q": EventSeverity.MEDIUM,    # Quarterly report
    "S-1": EventSeverity.HIGH,       # IPO registration
    "4": EventSeverity.LOW,          # Insider trading
    "SC 13D": EventSeverity.HIGH,    # Activist investor
    "SC 13G": EventSeverity.MEDIUM,  # Passive investor
    "DEF 14A": EventSeverity.LOW,    # Proxy statement
}


async def fetch_sec_filings(
    company_name: str | None = None,
    ticker: str | None = None,
    *,
    filing_type: str = "",
    limit: int = 20,
) -> list[EventWarning]:
    """
    Fetch recent SEC EDGAR filings using the EFTS full-text search API (v2).

    Falls back to the submissions API if EFTS search fails.

    Args:
        company_name: Company name to search for.
        ticker: Ticker symbol to search for.
        filing_type: Optional filing type filter (e.g. '10-K', '8-K').
        limit: Maximum number of filings to return.

    Returns:
        List of EventWarning schemas.
    """
    query = ticker or company_name or ""
    if not query:
        logger.warning("No company name or ticker provided for SEC search.")
        return []

    headers = {"User-Agent": _SEC_USER_AGENT}

    # --- Primary: EFTS full-text search v2 ---
    efts_url = "https://efts.sec.gov/LATEST/search-index"
    params: dict[str, str | int] = {
        "q": query,
        "dateRange": "custom",
        "startdt": "2024-01-01",
        "enddt": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
    }
    if filing_type:
        params["forms"] = filing_type

    events = await _parse_efts_response(efts_url, params, headers, query, ticker, limit)
    if events:
        return events

    # --- Fallback: EDGAR company search API ---
    # This endpoint returns structured JSON without Elasticsearch wrapping
    fallback_url = "https://efts.sec.gov/LATEST/search-index"
    fallback_params: dict[str, str | int] = {
        "q": query,
        "from": 0,
        "size": limit,
    }
    if filing_type:
        fallback_params["forms"] = filing_type

    events = await _parse_efts_response(
        fallback_url, fallback_params, headers, query, ticker, limit
    )
    if events:
        return events

    # --- Final fallback: CIK-based submissions API ---
    cik = await _resolve_ticker_to_cik(query, headers)
    if cik:
        return await _fetch_from_submissions(cik, headers, filing_type, ticker, limit)

    logger.warning("All SEC EDGAR fetch paths failed for '%s'", query)
    return []


async def _parse_efts_response(
    url: str,
    params: dict[str, str | int],
    headers: dict[str, str],
    query: str,
    ticker: str | None,
    limit: int,
) -> list[EventWarning]:
    """Parse EFTS search results from either v1 or v2 response format."""
    async with RateLimitedSession(timeout=20.0, headers=headers) as session:
        try:
            data = await session.get_json(url, params=params)
        except Exception as exc:
            logger.debug("EFTS fetch from %s failed: %s", url, exc)
            return []

    if not isinstance(data, dict):
        return []

    # v2 format: {"hits": {"hits": [...]}} (Elasticsearch-style)
    hits = data.get("hits", {})
    if isinstance(hits, dict):
        hit_list = hits.get("hits", [])
    elif isinstance(hits, list):
        hit_list = hits
    else:
        hit_list = []

    # Alternative v2 format: {"filings": [...]} (REST-style)
    if not hit_list:
        hit_list = data.get("filings", [])

    events: list[EventWarning] = []
    for hit in hit_list[:limit]:
        source = hit.get("_source", hit) if isinstance(hit, dict) else {}
        form_type = (
            source.get("form_type", "")
            or source.get("formType", "")
            or source.get("type", "")
        )
        entity_name = (
            source.get("entity_name", "")
            or source.get("entityName", "")
            or source.get("companyName", "Unknown")
        )
        entity_id = (
            source.get("entity_id", "")
            or source.get("entityId", "")
            or source.get("CIK", "N/A")
        )
        file_date = (
            source.get("file_date")
            or source.get("fileDate")
            or source.get("filedAt")
        )

        severity = _FILING_SEVERITY.get(form_type, EventSeverity.LOW)

        ts = datetime.now(tz=timezone.utc)
        if file_date:
            try:
                ts = datetime.fromisoformat(
                    str(file_date).replace("Z", "+00:00")
                )
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass

        events.append(EventWarning(
            title=f"SEC {form_type} Filing",
            description=f"{entity_name} filed {form_type}. CIK: {entity_id}.",
            severity=severity,
            event_type="sec_filing",
            symbol=ticker,
            scheduled_at=None,
            source="sec_edgar",
            timestamp=ts,
        ))

    if events:
        logger.info("Fetched %d SEC filings for '%s'", len(events), query)
    return events


async def _resolve_ticker_to_cik(
    ticker: str,
    headers: dict[str, str],
) -> str | None:
    """Resolve a ticker symbol to a CIK using SEC's company tickers JSON."""
    url = "https://www.sec.gov/files/company_tickers.json"
    async with RateLimitedSession(timeout=15.0, headers=headers) as session:
        try:
            data = await session.get_json(url)
        except Exception as exc:
            logger.debug("CIK ticker resolution failed: %s", exc)
            return None

    if not isinstance(data, dict):
        return None

    ticker_upper = ticker.upper().replace("/", "")
    for entry in data.values():
        if isinstance(entry, dict) and entry.get("ticker", "").upper() == ticker_upper:
            cik = str(entry.get("cik_str", ""))
            return cik.zfill(10) if cik else None

    return None


async def _fetch_from_submissions(
    cik: str,
    headers: dict[str, str],
    filing_type: str,
    ticker: str | None,
    limit: int,
) -> list[EventWarning]:
    """Fetch filings from SEC's submissions REST API (CIK-based)."""
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    async with RateLimitedSession(timeout=20.0, headers=headers) as session:
        try:
            data = await session.get_json(url)
        except Exception as exc:
            logger.debug("SEC submissions API failed for CIK %s: %s", cik, exc)
            return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    acc_numbers = recent.get("accessionNumber", [])
    company_name = data.get("name", "Unknown")

    events: list[EventWarning] = []
    for i, form in enumerate(forms[:limit * 3]):
        if filing_type and form != filing_type:
            continue
        if len(events) >= limit:
            break

        severity = _FILING_SEVERITY.get(form, EventSeverity.LOW)
        ts = datetime.now(tz=timezone.utc)
        if i < len(dates) and dates[i]:
            try:
                ts = datetime.fromisoformat(dates[i]).replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass

        events.append(EventWarning(
            title=f"SEC {form} Filing",
            description=f"{company_name} filed {form}. CIK: {cik_padded}.",
            severity=severity,
            event_type="sec_filing",
            symbol=ticker,
            scheduled_at=None,
            source="sec_edgar_submissions",
            timestamp=ts,
        ))

    logger.info("Fetched %d SEC filings from submissions API for CIK %s", len(events), cik)
    return events


async def fetch_sec_company_facts(cik: str) -> dict[str, Any]:
    """
    Fetch company facts (financial data) from SEC EDGAR XBRL API.

    Args:
        cik: Central Index Key (zero-padded 10 digits).

    Returns:
        Raw company facts dict with financial data.
    """
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    headers = {"User-Agent": _SEC_USER_AGENT}

    async with RateLimitedSession(timeout=15.0, headers=headers) as session:
        try:
            return await session.get_json(url)
        except Exception as exc:
            logger.warning("SEC company facts failed for CIK %s: %s", cik, exc)
            return {}


# ---------------------------------------------------------------------------
# Token Unlock Schedules — uses public APIs
# ---------------------------------------------------------------------------

async def fetch_token_unlocks() -> list[EventWarning]:
    """
    Fetch upcoming token unlock events from public tracker APIs.

    Returns:
        List of EventWarning schemas for upcoming unlocks.
    """
    # TokenUnlocks.app public calendar endpoint
    url = "https://token.unlocks.app/api/v1/unlocks/upcoming"

    async with RateLimitedSession(timeout=15.0) as session:
        try:
            data = await session.get_json(url)
        except Exception as exc:
            logger.warning("Token unlocks fetch failed: %s", exc)
            return []

    if not isinstance(data, list):
        return []

    events: list[EventWarning] = []
    now = datetime.now(tz=timezone.utc)

    for item in data[:30]:
        unlock_value = item.get("unlock_value_usd", 0)
        severity = (
            EventSeverity.CRITICAL if unlock_value > 100_000_000
            else EventSeverity.HIGH if unlock_value > 10_000_000
            else EventSeverity.MEDIUM if unlock_value > 1_000_000
            else EventSeverity.LOW
        )

        events.append(EventWarning(
            title=f"Token Unlock: {item.get('project_name', 'Unknown')}",
            description=(
                f"${unlock_value:,.0f} unlock scheduled. "
                f"{item.get('unlock_percent', 0):.1f}% of circulating supply."
            ),
            severity=severity,
            event_type="token_unlock",
            symbol=item.get("token_symbol"),
            scheduled_at=datetime.fromisoformat(
                item.get("unlock_date", now.isoformat())
            ).replace(tzinfo=timezone.utc) if item.get("unlock_date") else None,
            source="token_unlocks",
            timestamp=now,
        ))

    return events
