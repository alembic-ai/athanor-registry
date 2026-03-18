"""
WebSocket Stream Manager — real-time data via CCXT Pro.

Provides async generators for continuous orderbook, trade, and
ticker streams from any CCXT Pro-supported exchange. Includes
automatic reconnection with exponential backoff + jitter.

Design Goals:
    - Stateless per-tick: each yielded item is independent
    - Auto-reconnect: exponential backoff + jitter on disconnect
    - Read-only: inherits execution blocking from UniversalDataGateway
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from omni_data.schemas.models import (
    OrderBookDepth,
    OrderBookLevel,
    Side,
    Ticker,
    Trade,
)

logger = logging.getLogger(__name__)

# Reconnection constants
_MAX_RETRIES = 50
_BASE_DELAY = 1.0
_MAX_DELAY = 60.0
_JITTER_RANGE = 0.5


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter."""
    delay = min(_BASE_DELAY * (2 ** attempt), _MAX_DELAY)
    jitter = random.uniform(-_JITTER_RANGE, _JITTER_RANGE)
    return max(0.1, delay + jitter)


class WebSocketStreamManager:
    """
    Real-time WebSocket stream manager using CCXT Pro.

    Usage:
        manager = WebSocketStreamManager("binance")
        await manager.connect()

        async for ticker in manager.stream_ticker("BTC/USDT"):
            print(ticker)

        await manager.close()
    """

    def __init__(
        self,
        exchange_id: str,
        *,
        sandbox: bool = False,
        config: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the WebSocket stream manager.

        Args:
            exchange_id: CCXT exchange identifier.
            sandbox: Whether to use sandbox/testnet.
            config: Optional CCXT config overrides.
        """
        self._exchange_id = exchange_id
        self._sandbox = sandbox
        self._config = config or {}
        self._exchange: Any = None
        self._connected = False

    async def connect(self) -> None:
        """Initialize the CCXT Pro exchange instance and load markets."""
        try:
            import ccxt.pro as ccxt_pro
        except ImportError as exc:
            raise ImportError(
                "CCXT Pro is required for WebSocket streaming. "
                "Install with: pip install ccxt[pro]"
            ) from exc

        exchange_class = getattr(ccxt_pro, self._exchange_id, None)
        if exchange_class is None:
            raise ValueError(
                f"Exchange '{self._exchange_id}' not supported by CCXT Pro."
            )

        self._exchange = exchange_class({
            "enableRateLimit": True,
            **self._config,
        })

        if self._sandbox:
            self._exchange.set_sandbox_mode(True)

        await self._exchange.load_markets()
        self._connected = True
        logger.info("WebSocket manager connected to %s", self._exchange_id)

    async def close(self) -> None:
        """Close the exchange connection."""
        if self._exchange:
            await self._exchange.close()
            self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    # -- Ticker Stream --------------------------------------------------------

    async def stream_ticker(
        self,
        symbol: str,
        *,
        max_retries: int = _MAX_RETRIES,
    ) -> AsyncIterator[Ticker]:
        """
        Stream real-time ticker updates for a symbol.

        Args:
            symbol: Market pair (e.g. 'BTC/USDT').
            max_retries: Max reconnection attempts.

        Yields:
            Ticker schemas.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                while True:
                    raw: dict[str, Any] = await self._exchange.watch_ticker(symbol)
                    attempt = 0  # Reset on success
                    yield Ticker(
                        symbol=raw["symbol"],
                        bid=float(raw.get("bid") or 0),
                        ask=float(raw.get("ask") or 0),
                        last=float(raw.get("last") or 0),
                        volume_24h=float(raw.get("baseVolume") or 0),
                        change_pct_24h=float(raw.get("percentage") or 0),
                        timestamp=datetime.fromtimestamp(
                            (raw.get("timestamp") or 0) / 1000, tz=timezone.utc
                        ),
                    )
            except Exception as exc:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Ticker stream %s disconnected (attempt %d/%d): %s. "
                    "Reconnecting in %.1fs...",
                    symbol, attempt, max_retries, exc, delay,
                )
                await asyncio.sleep(delay)

        logger.error("Ticker stream %s: max retries exhausted.", symbol)

    # -- Order Book Stream ----------------------------------------------------

    async def stream_orderbook(
        self,
        symbol: str,
        *,
        limit: int = 25,
        max_retries: int = _MAX_RETRIES,
    ) -> AsyncIterator[OrderBookDepth]:
        """
        Stream real-time order book snapshots.

        Args:
            symbol: Market pair.
            limit: Number of levels per side.
            max_retries: Max reconnection attempts.

        Yields:
            OrderBookDepth schemas.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                while True:
                    raw: dict[str, Any] = await self._exchange.watch_order_book(
                        symbol, limit=limit
                    )
                    attempt = 0
                    yield OrderBookDepth(
                        symbol=symbol,
                        exchange=self._exchange_id,
                        bids=[
                            OrderBookLevel(price=b[0], quantity=b[1])
                            for b in raw.get("bids", [])[:limit]
                        ],
                        asks=[
                            OrderBookLevel(price=a[0], quantity=a[1])
                            for a in raw.get("asks", [])[:limit]
                        ],
                        timestamp=datetime.fromtimestamp(
                            (raw.get("timestamp") or 0) / 1000, tz=timezone.utc
                        ) if raw.get("timestamp") else datetime.now(tz=timezone.utc),
                    )
            except Exception as exc:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Orderbook stream %s disconnected (attempt %d/%d): %s. "
                    "Reconnecting in %.1fs...",
                    symbol, attempt, max_retries, exc, delay,
                )
                await asyncio.sleep(delay)

        logger.error("Orderbook stream %s: max retries exhausted.", symbol)

    # -- Trades Stream --------------------------------------------------------

    async def stream_trades(
        self,
        symbol: str,
        *,
        max_retries: int = _MAX_RETRIES,
    ) -> AsyncIterator[Trade]:
        """
        Stream real-time trade events.

        Args:
            symbol: Market pair.
            max_retries: Max reconnection attempts.

        Yields:
            Trade schemas.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                while True:
                    raw_trades: list[dict] = await self._exchange.watch_trades(symbol)
                    attempt = 0
                    for t in raw_trades:
                        yield Trade(
                            symbol=t["symbol"],
                            exchange=self._exchange_id,
                            price=float(t["price"]),
                            amount=float(t["amount"]),
                            side=Side.BID if t.get("side") == "buy" else Side.ASK,
                            timestamp=datetime.fromtimestamp(
                                (t.get("timestamp") or 0) / 1000, tz=timezone.utc
                            ),
                        )
            except Exception as exc:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Trades stream %s disconnected (attempt %d/%d): %s. "
                    "Reconnecting in %.1fs...",
                    symbol, attempt, max_retries, exc, delay,
                )
                await asyncio.sleep(delay)

        logger.error("Trades stream %s: max retries exhausted.", symbol)

    # -- OHLCV Stream ---------------------------------------------------------

    async def stream_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        *,
        max_retries: int = _MAX_RETRIES,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Stream real-time OHLCV candle updates.

        Args:
            symbol: Market pair.
            timeframe: Candle timeframe string (e.g. '1m', '5m').
            max_retries: Max reconnection attempts.

        Yields:
            Raw OHLCV dicts (formatted by the consumer).
        """
        attempt = 0
        while attempt < max_retries:
            try:
                while True:
                    raw: list[list] = await self._exchange.watch_ohlcv(
                        symbol, timeframe
                    )
                    attempt = 0
                    for candle in raw:
                        yield {
                            "timestamp": candle[0],
                            "open": candle[1],
                            "high": candle[2],
                            "low": candle[3],
                            "close": candle[4],
                            "volume": candle[5],
                        }
            except Exception as exc:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.warning(
                    "OHLCV stream %s disconnected (attempt %d/%d): %s. "
                    "Reconnecting in %.1fs...",
                    symbol, attempt, max_retries, exc, delay,
                )
                await asyncio.sleep(delay)

        logger.error("OHLCV stream %s: max retries exhausted.", symbol)

    # -- Funding Rate Stream --------------------------------------------------

    async def stream_funding_rate(
        self,
        symbol: str,
        *,
        poll_interval: float = 30.0,
        max_retries: int = _MAX_RETRIES,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Poll funding rates at intervals (most exchanges don't WS-stream these).

        Args:
            symbol: Perpetual swap pair.
            poll_interval: Seconds between polls.
            max_retries: Max retry attempts on failure.

        Yields:
            Raw funding rate dicts.
        """
        attempt = 0
        while attempt < max_retries:
            try:
                if not self._exchange.has.get("fetchFundingRate"):
                    logger.warning(
                        "%s does not support funding rates.", self._exchange_id
                    )
                    return

                while True:
                    raw = await self._exchange.fetch_funding_rate(symbol)
                    attempt = 0
                    if raw:
                        yield raw
                    await asyncio.sleep(poll_interval)
            except Exception as exc:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Funding rate poll %s failed (attempt %d/%d): %s. "
                    "Retrying in %.1fs...",
                    symbol, attempt, max_retries, exc, delay,
                )
                await asyncio.sleep(delay)

        logger.error("Funding rate stream %s: max retries exhausted.", symbol)
