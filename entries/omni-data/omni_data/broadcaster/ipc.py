"""
Omni-Broadcaster & IPC Gateway — ZeroMQ state distribution.

Serializes MarketStateSummary into JSON and publishes over
ZeroMQ PUB socket. Any number of downstream subscribers
(agents, skills, swarms) can connect and receive broadcasts.

Design Goals:
    - Zero-coupling: engine doesn't know who/how many are listening
    - Stateless: each broadcast is independent, no session state
    - Micro-compute: serialization + publish costs ~0.1ms per message
    - Multi-modal: JSON text + optional chart image bytes
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any

import zmq
import zmq.asyncio

from omni_data.schemas.models import MarketStateSummary

logger = logging.getLogger(__name__)


class OmniBroadcaster:
    """
    ZeroMQ PUB broadcaster for MarketStateSummary payloads.

    Usage:
        broadcaster = OmniBroadcaster("tcp://*:5555")
        broadcaster.start()
        await broadcaster.publish(state, topic="BTC/USDT")
        broadcaster.stop()
    """

    def __init__(
        self,
        bind_address: str = "tcp://*:5555",
        *,
        high_water_mark: int = 1000,
    ) -> None:
        """
        Initialize the broadcaster.

        Args:
            bind_address: ZeroMQ PUB bind address.
            high_water_mark: Max messages to buffer before dropping.
        """
        self._bind_address = bind_address
        self._hwm = high_water_mark
        self._ctx: zmq.asyncio.Context | None = None
        self._socket: zmq.asyncio.Socket | None = None

    def start(self) -> None:
        """Bind the PUB socket and start accepting connections."""
        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.PUB)
        self._socket.set_hwm(self._hwm)
        self._socket.bind(self._bind_address)
        logger.info("OmniBroadcaster bound to %s", self._bind_address)

    def stop(self) -> None:
        """Close the socket and context."""
        if self._socket:
            self._socket.close()
        if self._ctx:
            self._ctx.term()
        logger.info("OmniBroadcaster stopped.")

    async def publish(
        self,
        state: MarketStateSummary,
        *,
        topic: str = "",
        include_chart: bytes | None = None,
    ) -> None:
        """
        Publish a MarketStateSummary over ZeroMQ.

        The message is a multipart message:
            [topic_bytes, json_bytes, optional_chart_bytes]

        Args:
            state: The MarketStateSummary to broadcast.
            topic: Topic string for subscriber filtering (e.g. "BTC/USDT").
            include_chart: Optional PNG/WebP chart image bytes.
        """
        if not self._socket:
            raise RuntimeError("Broadcaster not started. Call .start() first.")

        payload = state.model_dump_json().encode("utf-8")
        parts: list[bytes] = [topic.encode("utf-8"), payload]

        if include_chart:
            parts.append(include_chart)

        await self._socket.send_multipart(parts)

    async def publish_json(
        self,
        data: dict[str, Any],
        *,
        topic: str = "",
    ) -> None:
        """
        Publish a raw dict payload (for non-schema broadcasts).

        Args:
            data: Dict to serialize and broadcast.
            topic: Topic string for subscriber filtering.
        """
        if not self._socket:
            raise RuntimeError("Broadcaster not started. Call .start() first.")

        payload = json.dumps(data, default=str).encode("utf-8")
        await self._socket.send_multipart([topic.encode("utf-8"), payload])


class OmniSubscriber:
    """
    ZeroMQ SUB subscriber that receives broadcast state.

    Usage:
        sub = OmniSubscriber("tcp://localhost:5555", topics=["BTC/USDT"])
        sub.connect()
        state = await sub.receive()
        sub.disconnect()
    """

    def __init__(
        self,
        connect_address: str = "tcp://localhost:5555",
        *,
        topics: list[str] | None = None,
    ) -> None:
        """
        Initialize the subscriber.

        Args:
            connect_address: ZeroMQ PUB endpoint to connect to.
            topics: List of topic filters (empty list = receive all).
        """
        self._connect_address = connect_address
        self._topics = topics or [""]
        self._ctx: zmq.asyncio.Context | None = None
        self._socket: zmq.asyncio.Socket | None = None

    def connect(self) -> None:
        """Connect to the PUB socket and subscribe to topics."""
        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(self._connect_address)

        for topic in self._topics:
            self._socket.subscribe(topic.encode("utf-8"))

        logger.info(
            "OmniSubscriber connected to %s (topics=%s)",
            self._connect_address, self._topics,
        )

    def disconnect(self) -> None:
        """Close the socket and context."""
        if self._socket:
            self._socket.close()
        if self._ctx:
            self._ctx.term()

    async def receive(self) -> tuple[str, MarketStateSummary, bytes | None]:
        """
        Receive the next broadcast.

        Returns:
            Tuple of (topic, MarketStateSummary, optional_chart_bytes).
        """
        if not self._socket:
            raise RuntimeError("Subscriber not connected. Call .connect() first.")

        parts: list[bytes] = await self._socket.recv_multipart()

        topic = parts[0].decode("utf-8")
        state = MarketStateSummary.model_validate_json(parts[1])
        chart = parts[2] if len(parts) > 2 else None

        return topic, state, chart

    async def receive_json(self) -> tuple[str, dict[str, Any]]:
        """
        Receive a raw JSON broadcast.

        Returns:
            Tuple of (topic, dict).
        """
        if not self._socket:
            raise RuntimeError("Subscriber not connected. Call .connect() first.")

        parts: list[bytes] = await self._socket.recv_multipart()
        topic = parts[0].decode("utf-8")
        data = json.loads(parts[1])
        return topic, data


# ---------------------------------------------------------------------------
# Token-aware summarization utilities
# ---------------------------------------------------------------------------

def truncate_state_for_token_budget(
    state: MarketStateSummary,
    max_chars: int = 8000,
) -> str:
    """
    Serialize a MarketStateSummary to JSON, truncating fields
    to fit within a character budget (proxy for token budget).

    Priority order (highest to lowest):
        1. ticker, indicators, portfolio
        2. orderflow, funding_rates
        3. macro, sentiment, on_chain
        4. ohlcv (truncated to last N candles)
        5. orderbook (truncated to top N levels)
        6. recent_trades, options_chain, liquidations, events

    Args:
        state: The full MarketStateSummary.
        max_chars: Target character limit.

    Returns:
        JSON string within the character budget.
    """
    # Start with full dump
    full_json = state.model_dump_json()

    if len(full_json) <= max_chars:
        return full_json

    # Progressively strip lower-priority fields
    trimmed = state.model_copy()

    # Trim OHLCV to last 20 candles
    if trimmed.ohlcv and len(trimmed.ohlcv) > 20:
        trimmed.ohlcv = trimmed.ohlcv[-20:]

    # Trim orderbook to top 10 levels
    if trimmed.orderbook:
        trimmed.orderbook.bids = trimmed.orderbook.bids[:10]
        trimmed.orderbook.asks = trimmed.orderbook.asks[:10]

    result = trimmed.model_dump_json()
    if len(result) <= max_chars:
        return result

    # Drop lowest priority fields
    for field in ["events", "liquidations", "options_chain", "recent_trades"]:
        setattr(trimmed, field, None)
        result = trimmed.model_dump_json()
        if len(result) <= max_chars:
            return result

    # Final: drop on_chain, sentiment, macro
    for field in ["on_chain", "sentiment", "macro"]:
        setattr(trimmed, field, None)
        result = trimmed.model_dump_json()
        if len(result) <= max_chars:
            return result

    return result[:max_chars]
