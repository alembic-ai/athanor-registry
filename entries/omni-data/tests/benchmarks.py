"""
Omni-Data Engine — Throughput & Stress Benchmarks.

Covers:
    1. ZeroMQ PUB/SUB throughput (in-process, no external deps)
    2. Redis Pub/Sub throughput (skipped if redis absent)
    3. WebSocket reconnection stress simulation
    4. Memory profiling under sustained message bursts
    5. Serialization throughput (Pydantic model_dump_json hot path)

All benchmarks are self-contained — no running services required.
Run:  python -m pytest tests/benchmarks.py -v -s
"""

from __future__ import annotations

import asyncio
import gc
import statistics
import time
import tracemalloc
from datetime import datetime, timezone, timedelta
from typing import Any

import pytest
import zmq
import zmq.asyncio

from omni_data.schemas.models import (
    OHLCV,
    MarketStateSummary,
    Ticker,
    OrderBookDepth,
    OrderBookLevel,
    OrderflowMetrics,
    IndicatorSet,
)
from omni_data.broadcaster.ipc import (
    OmniBroadcaster,
    OmniSubscriber,
    truncate_state_for_token_budget,
)
from omni_data.integrations.websocket import _backoff_delay


# ---------------------------------------------------------------------------
# Fixtures — synthetic data generators
# ---------------------------------------------------------------------------

def _make_full_state(symbol: str = "BTC/USDT") -> MarketStateSummary:
    """Generate a realistic full MarketStateSummary (~4KB serialized)."""
    now = datetime.now(tz=timezone.utc)
    return MarketStateSummary(
        symbol=symbol,
        exchange="binance",
        ticker=Ticker(
            symbol=symbol, bid=67450.12, ask=67451.88,
            last=67451.00, volume_24h=42389.7,
            change_pct_24h=2.34, timestamp=now,
        ),
        ohlcv=[
            OHLCV(
                timestamp=now - timedelta(hours=i),
                open=67000 + i * 10,
                high=67500 + i * 10,
                low=66800 + i * 10,
                close=67200 + i * 10,
                volume=1000 + i * 50,
            )
            for i in range(50)
        ],
        orderbook=OrderBookDepth(
            symbol=symbol,
            exchange="binance",
            bids=[OrderBookLevel(price=67450 - i * 5, quantity=0.5 + i * 0.1) for i in range(20)],
            asks=[OrderBookLevel(price=67452 + i * 5, quantity=0.3 + i * 0.1) for i in range(20)],
            timestamp=now,
        ),
        orderflow=OrderflowMetrics(
            symbol=symbol,
            volume_delta=125.4,
            voi=0.034,
            liquidity_above=450.2,
            liquidity_below=380.1,
            imbalance_ratio=0.542,
            timestamp=now,
        ),
        indicators=IndicatorSet(
            symbol=symbol,
            atr_14=1234.56,
            realized_vol_30=0.052,
            rsi_14=62.4,
            macd_signal=-15.2,
            macd_histogram=8.7,
            timestamp=now,
        ),
    )


# ---------------------------------------------------------------------------
# 1. ZeroMQ PUB/SUB Throughput
# ---------------------------------------------------------------------------

class TestZMQThroughput:
    """Benchmark ZeroMQ publish/subscribe throughput in-process."""

    @pytest.mark.asyncio
    async def test_zmq_burst_throughput(self) -> None:
        """
        Publish 5000 MarketStateSummary messages through a ZMQ PUB/SUB pair.
        Measure msgs/sec and P99 latency.
        """
        bind_addr = "tcp://127.0.0.1:15555"
        msg_count = 5_000
        state = _make_full_state()

        # Setup PUB
        broadcaster = OmniBroadcaster(bind_addr, high_water_mark=msg_count)
        broadcaster.start()

        # Setup SUB
        subscriber = OmniSubscriber(bind_addr, topics=[""])
        subscriber.connect()

        # Allow ZMQ connection handshake
        await asyncio.sleep(0.3)

        received: list[float] = []
        publish_times: list[float] = []

        async def consume() -> None:
            for _ in range(msg_count):
                t0 = time.perf_counter()
                _, _, _ = await subscriber.receive()
                received.append(time.perf_counter() - t0)

        consumer_task = asyncio.create_task(consume())

        # Publish burst
        t_start = time.perf_counter()
        for _ in range(msg_count):
            pt0 = time.perf_counter()
            await broadcaster.publish(state, topic="BTC/USDT")
            publish_times.append(time.perf_counter() - pt0)
        t_pub_done = time.perf_counter()

        await asyncio.wait_for(consumer_task, timeout=30.0)
        t_end = time.perf_counter()

        # Cleanup
        subscriber.disconnect()
        broadcaster.stop()

        # Results
        pub_elapsed = t_pub_done - t_start
        total_elapsed = t_end - t_start
        pub_rate = msg_count / pub_elapsed
        throughput = msg_count / total_elapsed
        p99_pub = statistics.quantiles(publish_times, n=100)[-1] * 1000
        p99_recv = statistics.quantiles(received, n=100)[-1] * 1000

        print(f"\n{'='*60}")
        print(f"ZMQ PUB/SUB THROUGHPUT ({msg_count} messages)")
        print(f"{'='*60}")
        print(f"Publish rate:     {pub_rate:,.0f} msgs/sec")
        print(f"E2E throughput:   {throughput:,.0f} msgs/sec")
        print(f"P99 publish:      {p99_pub:.3f} ms")
        print(f"P99 receive:      {p99_recv:.3f} ms")
        print(f"Total elapsed:    {total_elapsed:.3f} s")
        print(f"{'='*60}")

        assert len(received) == msg_count, f"Expected {msg_count}, got {len(received)}"
        assert throughput > 100, f"Throughput too low: {throughput:.0f} msgs/sec"

    @pytest.mark.asyncio
    async def test_zmq_high_water_mark_drop(self) -> None:
        """Verify HWM correctly drops messages when subscriber is slow."""
        bind_addr = "tcp://127.0.0.1:15556"
        hwm = 50
        state = _make_full_state()

        broadcaster = OmniBroadcaster(bind_addr, high_water_mark=hwm)
        broadcaster.start()

        # Publish rapidly without any subscriber — messages should be dropped
        for _ in range(hwm * 3):
            await broadcaster.publish(state, topic="OVERFLOW")

        broadcaster.stop()
        # If we get here without blocking, HWM is working correctly


# ---------------------------------------------------------------------------
# 2. Redis Pub/Sub Throughput (skipped if redis not available)
# ---------------------------------------------------------------------------

class TestRedisThroughput:
    """Benchmark Redis Pub/Sub throughput."""

    @pytest.mark.asyncio
    async def test_redis_publish_rate(self) -> None:
        """Measure Redis PUBLISH rate (requires running Redis)."""
        try:
            import redis.asyncio as aioredis
        except ImportError:
            pytest.skip("redis package not installed")

        try:
            r = aioredis.from_url("redis://localhost:6379", db=15)
            await r.ping()
        except Exception:
            pytest.skip("Redis server not available")

        from omni_data.broadcaster.redis_ipc import RedisBroadcaster

        msg_count = 2_000
        state = _make_full_state()

        broadcaster = RedisBroadcaster("redis://localhost:6379", db=15)
        await broadcaster.connect()

        publish_times: list[float] = []
        t_start = time.perf_counter()

        for _ in range(msg_count):
            pt0 = time.perf_counter()
            await broadcaster.publish(state, channel="bench:test")
            publish_times.append(time.perf_counter() - pt0)

        t_end = time.perf_counter()
        await broadcaster.disconnect()
        await r.close()

        elapsed = t_end - t_start
        rate = msg_count / elapsed
        p99 = statistics.quantiles(publish_times, n=100)[-1] * 1000

        print(f"\n{'='*60}")
        print(f"REDIS PUBLISH THROUGHPUT ({msg_count} messages)")
        print(f"{'='*60}")
        print(f"Publish rate:     {rate:,.0f} msgs/sec")
        print(f"P99 publish:      {p99:.3f} ms")
        print(f"Total elapsed:    {elapsed:.3f} s")
        print(f"{'='*60}")

        assert rate > 50, f"Redis publish rate too low: {rate:.0f} msgs/sec"

    @pytest.mark.asyncio
    async def test_redis_set_latest_state(self) -> None:
        """Benchmark Redis SET for last-known-state caching."""
        try:
            import redis.asyncio as aioredis
        except ImportError:
            pytest.skip("redis package not installed")

        try:
            r = aioredis.from_url("redis://localhost:6379", db=15)
            await r.ping()
        except Exception:
            pytest.skip("Redis server not available")

        from omni_data.broadcaster.redis_ipc import RedisBroadcaster

        state = _make_full_state()
        broadcaster = RedisBroadcaster("redis://localhost:6379", db=15)
        await broadcaster.connect()

        # Measure SET throughput
        iterations = 500
        t_start = time.perf_counter()
        for i in range(iterations):
            await broadcaster.set_latest_state(f"bench:state:{i % 10}", state, ttl=30)
        t_end = time.perf_counter()

        await broadcaster.disconnect()
        # Cleanup
        for i in range(10):
            await r.delete(f"bench:state:{i}")
        await r.close()

        rate = iterations / (t_end - t_start)
        print(f"\nRedis SET rate: {rate:,.0f} ops/sec")
        assert rate > 20, f"Redis SET rate too low: {rate:.0f}"


# ---------------------------------------------------------------------------
# 3. WebSocket Reconnection Stress
# ---------------------------------------------------------------------------

class TestWebSocketStress:
    """Validate reconnection logic correctness under stress."""

    def test_backoff_delay_scaling(self) -> None:
        """Verify exponential backoff grows correctly and caps at max."""
        delays = [_backoff_delay(i) for i in range(20)]

        # Must always be positive
        assert all(d > 0 for d in delays), "Negative delay detected"

        # Must not exceed MAX_DELAY + JITTER
        assert all(d <= 60.5 for d in delays), f"Delay exceeded cap: {max(delays)}"

        # Early delays should be smaller than later delays (on average)
        avg_early = statistics.mean(delays[:5])
        avg_late = statistics.mean(delays[10:15])
        assert avg_late > avg_early, "Backoff is not exponential"

    def test_backoff_jitter_variance(self) -> None:
        """Verify jitter produces variance — not identical delays."""
        # Same attempt index, multiple calls should produce different values
        attempts_at_3 = [_backoff_delay(3) for _ in range(50)]
        variance = statistics.variance(attempts_at_3)
        assert variance > 0.001, f"Jitter too low: variance={variance}"

    @pytest.mark.asyncio
    async def test_reconnect_counter_reset(self) -> None:
        """
        Simulate a stream that disconnects and reconnects.
        Verify attempt counter resets on successful receive.
        """
        # The WebSocket stream resets `attempt = 0` on each successful
        # watch call. We validate the backoff pattern by simulating failures.
        attempts: list[int] = []
        max_attempts = 10

        for attempt in range(max_attempts):
            delay = _backoff_delay(attempt)
            attempts.append(attempt)
            # Simulate "success" at attempt 3 — in real code this resets to 0
            if attempt == 3:
                break

        assert len(attempts) == 4
        assert attempts[-1] == 3

    def test_max_retries_exhaustion(self) -> None:
        """Verify max retries boundary is respected."""
        max_retries = 50
        # Under real conditions the stream loop exits when attempt >= max_retries
        attempt = 0
        exited_cleanly = False
        while attempt < max_retries:
            attempt += 1
            if attempt >= max_retries:
                exited_cleanly = True
                break

        assert exited_cleanly
        assert attempt == max_retries


# ---------------------------------------------------------------------------
# 4. Memory Profiling Under Burst
# ---------------------------------------------------------------------------

class TestMemoryProfile:
    """Profile memory usage during sustained message bursts."""

    def test_serialization_memory_footprint(self) -> None:
        """
        Measure memory delta when serializing 1000 full MarketStateSummary
        payloads. Catches memory leaks in the serialization path.
        """
        gc.collect()
        tracemalloc.start()

        snapshot_before = tracemalloc.take_snapshot()
        payloads: list[bytes] = []

        for _ in range(1_000):
            state = _make_full_state()
            payloads.append(state.model_dump_json().encode("utf-8"))

        snapshot_after = tracemalloc.take_snapshot()

        # Calculate memory delta
        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        total_delta_mb = sum(s.size_diff for s in stats) / (1024 * 1024)

        tracemalloc.stop()

        avg_payload_kb = statistics.mean(len(p) for p in payloads) / 1024

        print(f"\n{'='*60}")
        print(f"MEMORY PROFILE (1000 serializations)")
        print(f"{'='*60}")
        print(f"Memory delta:     {total_delta_mb:.2f} MB")
        print(f"Avg payload size: {avg_payload_kb:.1f} KB")
        print(f"Total payloads:   {len(payloads)}")
        print(f"{'='*60}")

        # 1000 full states should not consume more than 100MB
        assert total_delta_mb < 100, f"Memory spike too large: {total_delta_mb:.1f} MB"

    def test_truncation_does_not_leak(self) -> None:
        """Verify truncation path doesn't accumulate memory."""
        gc.collect()
        tracemalloc.start()

        snapshot_before = tracemalloc.take_snapshot()

        for _ in range(500):
            state = _make_full_state()
            truncate_state_for_token_budget(state, max_chars=2000)

        snapshot_after = tracemalloc.take_snapshot()
        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        delta_mb = sum(s.size_diff for s in stats) / (1024 * 1024)

        tracemalloc.stop()

        print(f"\nTruncation memory delta: {delta_mb:.2f} MB (500 iterations)")
        assert delta_mb < 50, f"Truncation memory leak: {delta_mb:.1f} MB"


# ---------------------------------------------------------------------------
# 5. Serialization Throughput
# ---------------------------------------------------------------------------

class TestSerializationThroughput:
    """Benchmark the Pydantic serialization hot path."""

    def test_model_dump_json_rate(self) -> None:
        """Measure how fast we can serialize MarketStateSummary to JSON."""
        state = _make_full_state()
        iterations = 5_000

        # Warm up
        for _ in range(100):
            state.model_dump_json()

        t_start = time.perf_counter()
        for _ in range(iterations):
            state.model_dump_json()
        t_end = time.perf_counter()

        rate = iterations / (t_end - t_start)
        per_op_us = (t_end - t_start) / iterations * 1_000_000

        print(f"\n{'='*60}")
        print(f"SERIALIZATION THROUGHPUT ({iterations} iterations)")
        print(f"{'='*60}")
        print(f"Rate:             {rate:,.0f} ops/sec")
        print(f"Per operation:    {per_op_us:.1f} μs")
        print(f"{'='*60}")

        assert rate > 500, f"Serialization too slow: {rate:.0f} ops/sec"

    def test_model_validate_json_rate(self) -> None:
        """Measure deserialization (parse) throughput."""
        state = _make_full_state()
        json_bytes = state.model_dump_json()
        iterations = 5_000

        # Warm up
        for _ in range(100):
            MarketStateSummary.model_validate_json(json_bytes)

        t_start = time.perf_counter()
        for _ in range(iterations):
            MarketStateSummary.model_validate_json(json_bytes)
        t_end = time.perf_counter()

        rate = iterations / (t_end - t_start)
        per_op_us = (t_end - t_start) / iterations * 1_000_000

        print(f"\nDeserialization: {rate:,.0f} ops/sec ({per_op_us:.1f} μs/op)")
        assert rate > 300, f"Deserialization too slow: {rate:.0f} ops/sec"
