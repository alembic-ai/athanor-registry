"""
Market State Orchestrator — the master data assembly pipeline.

Coordinates all data sources into a single MarketStateSummary,
runs the broadcaster, and manages the lifecycle of the engine.

This is the "main loop" script that ties everything together.
Stateless on a per-cycle basis — each broadcast cycle is independent.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from omni_data.config import OmniDataConfig
from omni_data.integrations.gateway import UniversalDataGateway
from omni_data.integrations.orderflow import compute_orderflow_metrics
from omni_data.quant.indicators import build_indicator_set
from omni_data.quant.derivatives import format_funding_rate
from omni_data.portfolio.injector import build_portfolio_state
from omni_data.broadcaster.ipc import OmniBroadcaster, truncate_state_for_token_budget
from omni_data.broadcaster.charts import render_candlestick_chart
from omni_data.schemas.models import (
    MarketStateSummary,
    Timeframe,
)

logger = logging.getLogger(__name__)


class MarketStateOrchestrator:
    """
    Master orchestrator that assembles MarketStateSummary from all sources.

    Usage:
        config = OmniDataConfig.from_env()
        orchestrator = MarketStateOrchestrator(config)
        await orchestrator.start()
        # Runs until stopped
        await orchestrator.stop()
    """

    def __init__(self, config: OmniDataConfig) -> None:
        self._config = config
        self._gateways: list[UniversalDataGateway] = []
        self._broadcaster: OmniBroadcaster | None = None
        self._running = False

    async def start(self) -> None:
        """Initialize gateways, broadcaster, and start the broadcast loop."""
        logging.basicConfig(level=getattr(logging, self._config.log_level, logging.INFO))

        # Initialize exchange gateways
        for exc_config in self._config.exchanges:
            gw = UniversalDataGateway(
                exc_config.exchange_id,
                sandbox=exc_config.sandbox,
                config={
                    "apiKey": exc_config.api_key or None,
                    "secret": exc_config.api_secret or None,
                    "password": exc_config.password or None,
                    **exc_config.extra,
                },
            )
            await gw.__aenter__()
            self._gateways.append(gw)
            logger.info("Connected to %s (%d symbols)", exc_config.exchange_id, len(gw.symbols))

        # Initialize broadcaster
        if self._config.ipc.transport == "zmq":
            self._broadcaster = OmniBroadcaster(
                self._config.ipc.zmq_bind_address,
                high_water_mark=self._config.ipc.high_water_mark,
            )
            self._broadcaster.start()
            logger.info("ZeroMQ broadcaster started on %s", self._config.ipc.zmq_bind_address)

        # Initialize BSS Translator if enabled
        if self._config.bss_translator.enabled:
            # We must import here inside the try block to gracefully fail if BSS isn't installed
            try:
                from omni_data.bss_translator import OmniBssTranslator, TranslatorConfig
                translator_config = TranslatorConfig(
                    zmq_url=self._config.ipc.zmq_bind_address.replace("*", "127.0.0.1"),
                    bss_root=self._config.bss_translator.bss_root,
                    passive_throttle_sec=self._config.bss_translator.passive_throttle_sec,
                    price_change_pct_1m=self._config.bss_translator.price_change_pct_1m,
                )
                self._bss_translator = OmniBssTranslator(translator_config)
                self._bss_translator_task = asyncio.create_task(self._bss_translator.run())
                logger.info(f"BSS Translator daemon spawned (target: {self._config.bss_translator.bss_root})")
            except ImportError as exc:
                logger.error(f"Cannot enable BSS Translator: module not found. {exc}")

        self._running = True
        logger.info("Orchestrator started. Broadcasting every %.1fs", self._config.broadcast_interval)

        # Main broadcast loop
        try:
            while self._running:
                await self._broadcast_cycle()
                await asyncio.sleep(self._config.broadcast_interval)
        except asyncio.CancelledError:
            logger.info("Orchestrator cancelled.")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully shut down all gateways and the broadcaster."""
        self._running = False

        if hasattr(self, '_bss_translator_task') and self._bss_translator_task:
            self._bss_translator_task.cancel()
            try:
                await self._bss_translator_task
            except asyncio.CancelledError:
                pass
            logger.info("BSS Translator stopped.")

        for gw in self._gateways:
            try:
                await gw.close()
            except Exception as exc:
                logger.warning("Error closing gateway: %s", exc)
        self._gateways.clear()

        if self._broadcaster:
            self._broadcaster.stop()
            self._broadcaster = None

        logger.info("Orchestrator stopped.")

    async def _broadcast_cycle(self) -> None:
        """Execute one full data assembly and broadcast cycle."""
        for symbol in self._config.symbols:
            for gw in self._gateways:
                try:
                    state = await self._assemble_state(gw, symbol)
                    if self._broadcaster:
                        chart_bytes = None
                        if state.ohlcv and len(state.ohlcv) >= 10:
                            try:
                                chart_bytes = render_candlestick_chart(
                                    state.ohlcv,
                                    title=f"{symbol} — {gw.exchange_id}",
                                    style=self._config.charts.style,
                                    figsize=(
                                        self._config.charts.figsize_width,
                                        self._config.charts.figsize_height,
                                    ),
                                    dpi=self._config.charts.dpi,
                                )
                            except Exception as exc:
                                logger.debug("Chart render skipped: %s", exc)

                        await self._broadcaster.publish(
                            state,
                            topic=symbol,
                            include_chart=chart_bytes,
                        )
                except Exception as exc:
                    logger.warning(
                        "Broadcast cycle failed for %s on %s: %s",
                        symbol, gw.exchange_id, exc,
                    )

    async def _assemble_state(
        self,
        gw: UniversalDataGateway,
        symbol: str,
    ) -> MarketStateSummary:
        """
        Assemble a complete MarketStateSummary for a symbol.

        Fetches ticker, OHLCV, orderbook, trades, indicators.
        Each fetch is independent and failure-tolerant.
        """
        ticker = None
        ohlcv = None
        orderbook = None
        recent_trades = None
        orderflow = None
        indicators = None
        funding_rates = None

        # Ticker
        try:
            ticker = await gw.fetch_ticker(symbol)
        except Exception as exc:
            logger.debug("Ticker fetch failed for %s: %s", symbol, exc)

        # OHLCV
        try:
            ohlcv = await gw.fetch_ohlcv(symbol, Timeframe.H1, limit=100)
        except Exception as exc:
            logger.debug("OHLCV fetch failed for %s: %s", symbol, exc)

        # Orderbook
        try:
            orderbook = await gw.fetch_order_book(symbol, limit=25)
        except Exception as exc:
            logger.debug("Orderbook fetch failed for %s: %s", symbol, exc)

        # Recent trades
        try:
            recent_trades = await gw.fetch_trades(symbol, limit=50)
        except Exception as exc:
            logger.debug("Trades fetch failed for %s: %s", symbol, exc)

        # Orderflow
        if orderbook and recent_trades:
            try:
                orderflow = compute_orderflow_metrics(orderbook, recent_trades)
            except Exception as exc:
                logger.debug("Orderflow compute failed: %s", exc)

        # Indicators
        if ohlcv and len(ohlcv) >= 30:
            try:
                indicators = build_indicator_set(symbol, ohlcv)
            except Exception as exc:
                logger.debug("Indicators compute failed: %s", exc)

        # Funding rate
        try:
            raw_funding = await gw.fetch_funding_rate(symbol)
            if raw_funding:
                formatted = format_funding_rate(raw_funding, gw.exchange_id)
                if formatted:
                    funding_rates = [formatted]
        except Exception as exc:
            logger.debug("Funding rate fetch failed: %s", exc)

        return MarketStateSummary(
            symbol=symbol,
            exchange=gw.exchange_id,
            ticker=ticker,
            ohlcv=ohlcv,
            orderbook=orderbook,
            recent_trades=recent_trades,
            orderflow=orderflow,
            indicators=indicators,
            funding_rates=funding_rates,
        )

    async def assemble_single(
        self,
        symbol: str,
        exchange_id: str | None = None,
    ) -> MarketStateSummary | None:
        """
        Public method to assemble a single MarketStateSummary on demand.

        Args:
            symbol: Market pair.
            exchange_id: Specific exchange, or None for the first gateway.

        Returns:
            MarketStateSummary, or None if no gateways are connected.
        """
        for gw in self._gateways:
            if exchange_id and gw.exchange_id != exchange_id:
                continue
            return await self._assemble_state(gw, symbol)
        return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

async def _main() -> None:
    """CLI entry point for running the orchestrator."""
    config = OmniDataConfig.from_env()
    orchestrator = MarketStateOrchestrator(config)
    await orchestrator.start()


def cli() -> None:
    """Synchronous CLI wrapper."""
    asyncio.run(_main())
