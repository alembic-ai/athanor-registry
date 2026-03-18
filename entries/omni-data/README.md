# Omni-Data Market Engine (Alembic AI Module)

## Philosophy: THIS IS ONLY DATA.

This module represents the exhaustive, model-agnostic *"Human Layer"* sensory pipeline within the **Alembic AI Studios** ecosystem (The Open Athanor & BSS Swarm OS architecture).

**CRITICAL RULE:** There are **NO** skills, **NO** inferences, and **NO** agent execution layers inside this entire directory. This pipeline computes nothing related to trading logic or reasoning. It does not spawn language models. It does not trade.

The sole absolute purpose of this module is to **INJECT DATA** into the wider ecosystem so that your agents — armed with independent prompts, skills, and tasks across your distributed swarm — actually have a mathematically sound reality to execute against.

---

## What it Does (The inputs)

It silently connects to global financial networks in the background, consuming:
*   **CEX Data:** High-frequency CCXT WebSockets (Full Orderbooks, BBO, Trades, OHLCV)
*   **Alternative Data:** SEC EDGAR v2 API sweeps for sudden 8-K volatility, On-chain token unlocks (TokenTerminal/Glassnode), and macro news sentiment wrappers.
*   **TradFi Bridges:** Polygon & Alpha Vantage adapters for equity and index correlation.

It then normalizes the chaos. It runs rapid vector math on the inputs to generate baseline quantitative views (RSI, MACD, Orderflow Imbalances, Funding Rates) so your language models don't have to waste parameter context tokens doing math.

## How it Plugs In (The outputs)

The result is a unified `MarketStateSummary` JSON object that is seamlessly injected into the Alembic AI ecosystem via two parallel, highly scalable mechanisms:

### 1. The BSS Plug-and-Play (For Swarm Agents)
Omni-Data natively runs a `bss_translator.py` daemon.
*   When a routine tick drops, an ambient blink (`..`) is fired into the BSS local filesystem `/active/` graph.
*   When a violent market threshold is crossed, a massive handoff blink (`~!`) is fired into the BSS `/relay/` graph, physically waking dormant Swarm agents.
*   **The 1-to-1 Base36 Offload:** To prevent token-bombing, the engine adheres rigidly to BSS linkage principles. The actual 10,000-line JSON dataset is registered into `/artifacts/` using a prefixed *Base36 + Author* namespace matching its parent Blink ID. An inference agent can rapidly triage the 17-character BSS filename, and dynamically pull the massive quantitative JSON only if its algorithmic skill requires it.

### 2. The ZeroMQ Pub/Sub (For Athanor Visualizer)
Omni-Data instantly broadcasts its state over a high-throughput raw ZMQ port (Default: `tcp://*:5555`).
*   This allows the Electron-based Athanor UX UI to physically render what the swarm is seeing in real-time.
*   The architecture scales elastically. One laptop can listen to this stream, or a mega-cluster of GPU nodes can subscribe 100 disparate agents directly to the exact same broadcast without skipping a tick.

---

## Integration Guide

To plug this module into your existing Swarm:

1. Setup the isolated environment (dependencies listed in `module.json`).
2. Define your external keys in `.env` (`OMNI_EXCHANGES=binance`).
3. Connect it to BSS via `OMNI_ENABLE_BSS_TRANSLATOR=true` and pointing `OMNI_BSS_ROOT=/path/to/your/bss`.
4. Run the orchestrator: `python -m omni_data.orchestrator`.

The orchestration boundary is complete. Connect your **Skills**, **Agents**, and **Inferences** downstream. The data is waiting.

## License

CC-BY-4.0
