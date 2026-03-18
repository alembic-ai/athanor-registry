"""
Historical Data Archiver — Parquet/CSV persistence for replay.

Provides utilities to archive OHLCV, trades, and market state
to local Parquet or CSV files for the Replay Engine to consume.
Also handles loading archived data back into schemas.

Uses pandas for efficient serialization. Stateless operations.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from omni_data.schemas.models import OHLCV, Trade, Side

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Archive operations
# ---------------------------------------------------------------------------

def archive_ohlcv(
    candles: list[OHLCV],
    filepath: str | Path,
    *,
    format: str = "parquet",
    append: bool = True,
) -> Path:
    """
    Archive OHLCV data to a Parquet or CSV file.

    Args:
        candles: OHLCV data to archive.
        filepath: Output file path (without extension).
        format: 'parquet' or 'csv'.
        append: Whether to append to existing file or overwrite.

    Returns:
        Path to the written file.
    """
    filepath = Path(filepath)
    ext = ".parquet" if format == "parquet" else ".csv"
    output_path = filepath.with_suffix(ext)

    df = pd.DataFrame([
        {
            "timestamp": c.timestamp.isoformat(),
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume,
        }
        for c in candles
    ])

    if append and output_path.exists():
        if format == "parquet":
            existing = pd.read_parquet(output_path)
        else:
            existing = pd.read_csv(output_path)
        df = pd.concat([existing, df], ignore_index=True)
        df.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "parquet":
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)

    logger.info("Archived %d OHLCV records to %s", len(df), output_path)
    return output_path


def archive_trades(
    trades: list[Trade],
    filepath: str | Path,
    *,
    format: str = "parquet",
    append: bool = True,
) -> Path:
    """
    Archive trade data to Parquet or CSV.

    Args:
        trades: Trade data to archive.
        filepath: Output file path (without extension).
        format: 'parquet' or 'csv'.
        append: Whether to append or overwrite.

    Returns:
        Path to the written file.
    """
    filepath = Path(filepath)
    ext = ".parquet" if format == "parquet" else ".csv"
    output_path = filepath.with_suffix(ext)

    df = pd.DataFrame([
        {
            "timestamp": t.timestamp.isoformat(),
            "symbol": t.symbol,
            "exchange": t.exchange,
            "price": t.price,
            "amount": t.amount,
            "side": t.side.value,
        }
        for t in trades
    ])

    if append and output_path.exists():
        if format == "parquet":
            existing = pd.read_parquet(output_path)
        else:
            existing = pd.read_csv(output_path)
        df = pd.concat([existing, df], ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "parquet":
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)

    logger.info("Archived %d trades to %s", len(df), output_path)
    return output_path


# ---------------------------------------------------------------------------
# Load operations
# ---------------------------------------------------------------------------

def load_ohlcv(
    filepath: str | Path,
) -> list[OHLCV]:
    """
    Load OHLCV data from a Parquet or CSV archive.

    Args:
        filepath: Path to the archive file.

    Returns:
        List of OHLCV schemas, sorted oldest first.
    """
    filepath = Path(filepath)

    if filepath.suffix == ".parquet":
        df = pd.read_parquet(filepath)
    elif filepath.suffix == ".csv":
        df = pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported format: {filepath.suffix}")

    candles: list[OHLCV] = []
    for _, row in df.iterrows():
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        if not hasattr(ts, 'tzinfo') or ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc) if hasattr(ts, 'replace') else ts

        candles.append(OHLCV(
            timestamp=ts,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        ))

    return sorted(candles, key=lambda c: c.timestamp)


def load_trades(
    filepath: str | Path,
) -> list[Trade]:
    """
    Load trade data from a Parquet or CSV archive.

    Args:
        filepath: Path to the archive file.

    Returns:
        List of Trade schemas, sorted oldest first.
    """
    filepath = Path(filepath)

    if filepath.suffix == ".parquet":
        df = pd.read_parquet(filepath)
    elif filepath.suffix == ".csv":
        df = pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported format: {filepath.suffix}")

    trades: list[Trade] = []
    for _, row in df.iterrows():
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        if not hasattr(ts, 'tzinfo') or ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc) if hasattr(ts, 'replace') else ts

        trades.append(Trade(
            symbol=row["symbol"],
            exchange=row["exchange"],
            price=float(row["price"]),
            amount=float(row["amount"]),
            side=Side(row["side"]),
            timestamp=ts,
        ))

    return sorted(trades, key=lambda t: t.timestamp)


# ---------------------------------------------------------------------------
# Bulk download utility
# ---------------------------------------------------------------------------

async def download_and_archive_ohlcv(
    gateway: Any,
    symbol: str,
    timeframe: str = "1h",
    *,
    since: int | None = None,
    limit: int = 1000,
    filepath: str | Path = "data/ohlcv",
    format: str = "parquet",
) -> Path:
    """
    Download OHLCV from an exchange gateway and archive to disk.

    Args:
        gateway: UniversalDataGateway instance (must be connected).
        symbol: Market pair (e.g. 'BTC/USDT').
        timeframe: Candle timeframe string.
        since: Start timestamp in ms.
        limit: Max candles to fetch.
        filepath: Output file path.
        format: 'parquet' or 'csv'.

    Returns:
        Path to the archive file.
    """
    from omni_data.schemas.models import Timeframe

    tf = Timeframe(timeframe)
    candles = await gateway.fetch_ohlcv(symbol, tf, since=since, limit=limit)

    safe_symbol = symbol.replace("/", "_")
    full_path = Path(filepath) / f"{safe_symbol}_{timeframe}"

    return archive_ohlcv(candles, full_path, format=format)
