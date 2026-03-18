"""
Redis Pub/Sub Broadcaster — alternative IPC transport.

Drop-in replacement for the ZeroMQ broadcaster when Redis
is preferred or already deployed in the infrastructure.
Same interface: publish MarketStateSummary, subscribe by topic.

Requires: redis[hiredis] package + running Redis instance.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from omni_data.schemas.models import MarketStateSummary

logger = logging.getLogger(__name__)


class RedisBroadcaster:
    """
    Redis Pub/Sub broadcaster for MarketStateSummary payloads.

    Usage:
        broadcaster = RedisBroadcaster("redis://localhost:6379")
        await broadcaster.connect()
        await broadcaster.publish(state, channel="omni:BTC/USDT")
        await broadcaster.disconnect()
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        *,
        db: int = 0,
    ) -> None:
        """
        Initialize the Redis broadcaster.

        Args:
            redis_url: Redis connection URL.
            db: Redis database number.
        """
        self._redis_url = redis_url
        self._db = db
        self._redis: Any = None

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis.asyncio as aioredis
        except ImportError as exc:
            raise ImportError(
                "redis[hiredis] package required. "
                "Install with: pip install 'omni-data-market-engine[redis]'"
            ) from exc

        self._redis = aioredis.from_url(
            self._redis_url,
            db=self._db,
            decode_responses=False,
        )
        logger.info("RedisBroadcaster connected to %s", self._redis_url)

    async def disconnect(self) -> None:
        """Close the Redis connection."""
        if self._redis:
            await self._redis.close()
            logger.info("RedisBroadcaster disconnected.")

    async def publish(
        self,
        state: MarketStateSummary,
        *,
        channel: str = "omni:market",
    ) -> int:
        """
        Publish a MarketStateSummary to a Redis channel.

        Args:
            state: MarketStateSummary to broadcast.
            channel: Redis Pub/Sub channel name.

        Returns:
            Number of subscribers that received the message.
        """
        if not self._redis:
            raise RuntimeError("Not connected. Call .connect() first.")

        payload = state.model_dump_json().encode("utf-8")
        count: int = await self._redis.publish(channel, payload)
        return count

    async def publish_json(
        self,
        data: dict[str, Any],
        *,
        channel: str = "omni:market",
    ) -> int:
        """
        Publish a raw dict as JSON to a Redis channel.

        Args:
            data: Dict to serialize.
            channel: Redis Pub/Sub channel name.

        Returns:
            Number of subscribers that received the message.
        """
        if not self._redis:
            raise RuntimeError("Not connected. Call .connect() first.")

        payload = json.dumps(data, default=str).encode("utf-8")
        return await self._redis.publish(channel, payload)

    async def set_latest_state(
        self,
        key: str,
        state: MarketStateSummary,
        *,
        ttl: int = 60,
    ) -> None:
        """
        Store the latest state in a Redis key (for late-joining subscribers).

        This is a Redis advantage over pure Pub/Sub — subscribers can
        fetch the last known state without waiting for the next broadcast.

        Args:
            key: Redis key.
            state: MarketStateSummary to store.
            ttl: Time-to-live in seconds.
        """
        if not self._redis:
            raise RuntimeError("Not connected. Call .connect() first.")

        payload = state.model_dump_json().encode("utf-8")
        await self._redis.set(key, payload, ex=ttl)

    async def get_latest_state(self, key: str) -> MarketStateSummary | None:
        """
        Retrieve the latest stored state from Redis.

        Args:
            key: Redis key.

        Returns:
            MarketStateSummary, or None if not found.
        """
        if not self._redis:
            raise RuntimeError("Not connected. Call .connect() first.")

        data: bytes | None = await self._redis.get(key)
        if data is None:
            return None
        return MarketStateSummary.model_validate_json(data)


class RedisSubscriber:
    """
    Redis Pub/Sub subscriber that receives broadcast state.

    Usage:
        sub = RedisSubscriber("redis://localhost:6379")
        await sub.connect()
        async for channel, state in sub.listen("omni:BTC/USDT"):
            process(state)
        await sub.disconnect()
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        *,
        db: int = 0,
    ) -> None:
        self._redis_url = redis_url
        self._db = db
        self._redis: Any = None
        self._pubsub: Any = None

    async def connect(self) -> None:
        """Connect to Redis and create a Pub/Sub instance."""
        try:
            import redis.asyncio as aioredis
        except ImportError as exc:
            raise ImportError(
                "redis[hiredis] package required."
            ) from exc

        self._redis = aioredis.from_url(
            self._redis_url,
            db=self._db,
            decode_responses=False,
        )
        self._pubsub = self._redis.pubsub()
        logger.info("RedisSubscriber connected to %s", self._redis_url)

    async def disconnect(self) -> None:
        """Unsubscribe and close."""
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()

    async def listen(
        self,
        *channels: str,
    ) -> AsyncIterator[tuple[str, MarketStateSummary]]:
        """
        Subscribe to channels and yield MarketStateSummary payloads.

        Args:
            *channels: Channel names to subscribe to.

        Yields:
            Tuple of (channel_name, MarketStateSummary).
        """
        if not self._pubsub:
            raise RuntimeError("Not connected. Call .connect() first.")

        await self._pubsub.subscribe(*channels)

        async for message in self._pubsub.listen():
            if message.get("type") != "message":
                continue

            channel = message.get("channel", b"").decode("utf-8")
            data = message.get("data", b"")

            try:
                state = MarketStateSummary.model_validate_json(data)
                yield channel, state
            except Exception as exc:
                logger.warning("Failed to parse message on %s: %s", channel, exc)

    async def listen_json(
        self,
        *channels: str,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """
        Subscribe and yield raw JSON dicts.

        Args:
            *channels: Channel names.

        Yields:
            Tuple of (channel_name, dict).
        """
        if not self._pubsub:
            raise RuntimeError("Not connected. Call .connect() first.")

        await self._pubsub.subscribe(*channels)

        async for message in self._pubsub.listen():
            if message.get("type") != "message":
                continue

            channel = message.get("channel", b"").decode("utf-8")
            data = message.get("data", b"")

            try:
                yield channel, json.loads(data)
            except Exception as exc:
                logger.warning("Failed to parse JSON on %s: %s", channel, exc)
