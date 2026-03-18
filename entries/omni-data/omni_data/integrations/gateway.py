"""
UniversalDataGateway — CCXT-powered, strictly read-only.

Provides async methods for fetching OHLCV, tickers, order books,
and trades from any CCXT-supported exchange. Execution methods
(create_order, cancel_order, etc.) are fundamentally absent from
this wrapper.

Design Goals:
    - Stateless: no persistent connections held between calls
    - Micro-compute: each function is a standalone async script
    - Model-agnostic: returns Pydantic schemas, not raw dicts
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import ccxt.async_support as ccxt_async

from omni_data.schemas.models import (
    OHLCV,
    OrderBookDepth,
    OrderBookLevel,
    Side,
    Ticker,
    Timeframe,
    Trade,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Blocked methods — these must NEVER be callable on any gateway instance.
# ---------------------------------------------------------------------------
_BLOCKED_METHODS: frozenset[str] = frozenset({
    "create_order",
    "cancel_order",
    "cancel_all_orders",
    "edit_order",
    "transfer",
    "withdraw",
    "create_deposit_address",
    "set_leverage",
    "set_margin_mode",
    "add_margin",
    "reduce_margin",
})


class ExecutionBlockedError(RuntimeError):
    """Raised when any execution-related method is invoked."""


class UniversalDataGateway:
    """
    Read-only async data gateway for any CCXT-supported exchange.

    Usage:
        async with UniversalDataGateway("binance") as gw:
            ticker = await gw.fetch_ticker("BTC/USDT")
            ohlcv  = await gw.fetch_ohlcv("BTC/USDT", Timeframe.H1, limit=100)
    """

    def __init__(
        self,
        exchange_id: str,
        *,
        sandbox: bool = False,
        rate_limit: bool = True,
        config: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize a read-only gateway for the given exchange.

        Args:
            exchange_id: CCXT exchange identifier (e.g. 'binance', 'kraken').
            sandbox: Whether to use the exchange's sandbox/testnet.
            rate_limit: Whether to enable CCXT built-in rate limiting.
            config: Optional extra CCXT config overrides.
        """
        if exchange_id not in ccxt_async.exchanges:
            raise ValueError(
                f"Exchange '{exchange_id}' not supported. "
                f"Available: {len(ccxt_async.exchanges)} exchanges."
            )

        merged_config: dict[str, Any] = {
            "enableRateLimit": rate_limit,
            **(config or {}),
        }

        exchange_class = getattr(ccxt_async, exchange_id)
        self._exchange: ccxt_async.Exchange = exchange_class(merged_config)

        if sandbox:
            self._exchange.set_sandbox_mode(True)

        self._exchange_id = exchange_id
        self._apply_execution_block()

    def _apply_execution_block(self) -> None:
        """Patch all execution methods to raise on invocation."""
        for method_name in _BLOCKED_METHODS:
            if hasattr(self._exchange, method_name):
                setattr(self._exchange, method_name, self._blocked_factory(method_name))

    @staticmethod
    def _blocked_factory(method_name: str):
        """Create a closure that raises ExecutionBlockedError."""
        async def _blocked(*args: Any, **kwargs: Any) -> None:
            raise ExecutionBlockedError(
                f"Execution method '{method_name}' is permanently blocked. "
                f"The Omni-Data Market Engine is strictly read-only."
            )
        return _blocked

    # -- Context manager ------------------------------------------------------

    async def __aenter__(self) -> UniversalDataGateway:
        await self._exchange.load_markets()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Release the underlying CCXT exchange connection."""
        await self._exchange.close()

    # -- Discovery ------------------------------------------------------------

    @staticmethod
    def list_exchanges() -> list[str]:
        """Return all supported exchange identifiers."""
        return list(ccxt_async.exchanges)

    @property
    def exchange_id(self) -> str:
        return self._exchange_id

    @property
    def symbols(self) -> list[str]:
        """Return loaded market symbols (call after __aenter__)."""
        return list(self._exchange.symbols) if self._exchange.symbols else []

    # -- Ticker ---------------------------------------------------------------

    async def fetch_ticker(self, symbol: str) -> Ticker:
        """
        Fetch the latest ticker for a symbol.

        Args:
            symbol: Market pair (e.g. 'BTC/USDT').

        Returns:
            Ticker schema.
        """
        raw: dict[str, Any] = await self._exchange.fetch_ticker(symbol)
        return Ticker(
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

    async def fetch_tickers(self, symbols: list[str] | None = None) -> list[Ticker]:
        """
        Fetch tickers for multiple symbols (or all if None).

        Args:
            symbols: Optional list of market pairs.

        Returns:
            List of Ticker schemas.
        """
        raw_tickers: dict[str, dict] = await self._exchange.fetch_tickers(symbols)
        result: list[Ticker] = []
        for raw in raw_tickers.values():
            try:
                result.append(Ticker(
                    symbol=raw["symbol"],
                    bid=float(raw.get("bid") or 0),
                    ask=float(raw.get("ask") or 0),
                    last=float(raw.get("last") or 0),
                    volume_24h=float(raw.get("baseVolume") or 0),
                    change_pct_24h=float(raw.get("percentage") or 0),
                    timestamp=datetime.fromtimestamp(
                        (raw.get("timestamp") or 0) / 1000, tz=timezone.utc
                    ),
                ))
            except (TypeError, KeyError) as exc:
                logger.warning("Skipping malformed ticker: %s", exc)
        return result

    # -- OHLCV ----------------------------------------------------------------

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe = Timeframe.H1,
        *,
        since: int | None = None,
        limit: int = 100,
    ) -> list[OHLCV]:
        """
        Fetch historical OHLCV candlestick data.

        Args:
            symbol: Market pair (e.g. 'BTC/USDT').
            timeframe: Candle timeframe.
            since: Start timestamp in ms (optional).
            limit: Max number of candles.

        Returns:
            List of OHLCV schemas, oldest first.
        """
        raw: list[list] = await self._exchange.fetch_ohlcv(
            symbol, timeframe=timeframe.value, since=since, limit=limit
        )
        return [
            OHLCV(
                timestamp=datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5]),
            )
            for candle in raw
        ]

    # -- Order Book -----------------------------------------------------------

    async def fetch_order_book(
        self, symbol: str, *, limit: int = 25
    ) -> OrderBookDepth:
        """
        Fetch L2 order book snapshot.

        Args:
            symbol: Market pair.
            limit: Number of levels per side.

        Returns:
            OrderBookDepth schema.
        """
        raw: dict[str, Any] = await self._exchange.fetch_order_book(symbol, limit=limit)
        return OrderBookDepth(
            symbol=symbol,
            exchange=self._exchange_id,
            bids=[OrderBookLevel(price=b[0], quantity=b[1]) for b in raw.get("bids", [])],
            asks=[OrderBookLevel(price=a[0], quantity=a[1]) for a in raw.get("asks", [])],
            timestamp=datetime.fromtimestamp(
                (raw.get("timestamp") or 0) / 1000, tz=timezone.utc
            ) if raw.get("timestamp") else datetime.now(tz=timezone.utc),
        )

    # -- Trades ---------------------------------------------------------------

    async def fetch_trades(
        self, symbol: str, *, since: int | None = None, limit: int = 50
    ) -> list[Trade]:
        """
        Fetch recent trades for a symbol.

        Args:
            symbol: Market pair.
            since: Start timestamp in ms (optional).
            limit: Max number of trades.

        Returns:
            List of Trade schemas.
        """
        raw_trades: list[dict] = await self._exchange.fetch_trades(
            symbol, since=since, limit=limit
        )
        return [
            Trade(
                symbol=t["symbol"],
                exchange=self._exchange_id,
                price=float(t["price"]),
                amount=float(t["amount"]),
                side=Side.BID if t.get("side") == "buy" else Side.ASK,
                timestamp=datetime.fromtimestamp(
                    (t.get("timestamp") or 0) / 1000, tz=timezone.utc
                ),
            )
            for t in raw_trades
        ]

    # -- Funding Rates (Derivatives) ------------------------------------------

    async def fetch_funding_rate(self, symbol: str) -> dict[str, Any] | None:
        """
        Fetch the current funding rate for a perpetual swap.
        Returns raw dict (formatted by the quant layer).
        Returns None if the exchange does not support funding rates.
        """
        if not self._exchange.has.get("fetchFundingRate"):
            return None
        try:
            return await self._exchange.fetch_funding_rate(symbol)
        except Exception as exc:
            logger.warning("Funding rate unavailable for %s: %s", symbol, exc)
            return None

    # -- Balances (read-only, for portfolio state) ----------------------------

    async def fetch_balances(self) -> dict[str, float]:
        """
        Fetch available balances (requires API key with read permissions).
        Returns a dict of {asset: free_balance}.
        """
        raw: dict[str, Any] = await self._exchange.fetch_balance()
        free: dict[str, Any] = raw.get("free", {})
        return {
            asset: float(bal)
            for asset, bal in free.items()
            if float(bal) > 0
        }

    # -- Positions (read-only, for portfolio state) ---------------------------

    async def fetch_positions(self, symbols: list[str] | None = None) -> list[dict[str, Any]]:
        """
        Fetch open positions (derivatives exchanges only).
        Returns raw dicts — formatted by the portfolio layer.
        Returns empty list if the exchange does not support positions.
        """
        if not self._exchange.has.get("fetchPositions"):
            return []
        try:
            return await self._exchange.fetch_positions(symbols)
        except Exception as exc:
            logger.warning("Positions unavailable: %s", exc)
            return []
