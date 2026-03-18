"""
Omni-Data -> BSS Translation Engine

This daemon consumes the Omni-Data ZeroMQ broadcast stream and translates
MarketStateSummary payloads directly into BSS Blinks and attached Artifacts,
enabling true event-driven, hardware-elastic swarm execution.

It avoids the "adapter polling" anti-pattern by proactively pushing state
into the BSS filesystem graph when defined market thresholds are crossed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import zmq
import zmq.asyncio

# Required for parsing config and data
try:
    from omni_data.schemas.models import MarketStateSummary
except ImportError:
    # Handle the case where the daemon runs strictly in the BSS environment
    # and might not have the full omni_data package installed.
    # We fallback to standard dict processing if Pydantic isn't available.
    pass

from src.bss.environment import BSSEnvironment
from src.bss.blink_file import BlinkFile, write as write_blink

logger = logging.getLogger("bss.omni_translator")

# Standard Blink syntax for the Omni-Data Oracle
AUTHOR_SIGIL = "Z"  # Using Z for machine/sensory Oracles

# Thresholds for triggering an active handoff blink vs a passive artifact
@dataclass
class TranslatorConfig:
    zmq_url: str = "tcp://127.0.0.1:5555"
    bss_root: str = "./.bss_test"
    topics: list[str] = None
    
    # Throttle: minimum seconds between writing passive market states
    passive_throttle_sec: float = 60.0
    
    # Thresholds: if delta exceeds these, write an active ! Handoff blink
    volatility_spike_threshold: float = 0.5
    price_change_pct_1m: float = 1.0


class OmniBssTranslator:
    def __init__(self, config: TranslatorConfig):
        self.config = config
        self.bss = BSSEnvironment.init(Path(config.bss_root).resolve())
        self._last_passive_write: dict[str, float] = {}
        self._last_prices: dict[str, dict] = {}
        
        self.ctx = zmq.asyncio.Context()
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(self.config.zmq_url)
        
        topics = self.config.topics or [""] # Default to all topics
        for topic in topics:
            self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

    async def run(self):
        """Main ingest loop."""
        logger.info(f"Translator connected to {self.config.zmq_url}, writing to {self.config.bss_root}")
        
        try:
            while True:
                parts = await self.sub.recv_multipart()
                
                if len(parts) >= 2:
                    topic = parts[0]
                    msg = parts[1]
                else:
                    logger.error("Received incomplete multipart message")
                    continue
                
                try:
                    payload = json.loads(msg.decode('utf-8'))
                    await self._process_payload(topic.decode('utf-8'), payload)
                except json.JSONDecodeError:
                    logger.error("Received malformed JSON payload")
                except Exception as e:
                    logger.error(f"Error processing payload: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Translator shutting down...")
        finally:
            self.sub.close()
            self.ctx.term()

    async def _process_payload(self, topic: str, payload: dict):
        """Evaluate market state and generate blinks if necessary."""
        symbol = payload.get("symbol", "UNKNOWN")
        now = datetime.now(timezone.utc).timestamp()
        
        # 1. Threshold Evaluation
        is_event = self._evaluate_thresholds(symbol, payload)
        
        last_write = self._last_passive_write.get(symbol, 0)
        is_throttled = (now - last_write) < self.config.passive_throttle_sec
        
        if not is_event and is_throttled:
            return  # Skip routine ticks to prevent BSS bloat
            
        # 2. Write the JSON payload as a temporary artifact
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tf:
            json.dump(payload, tf, indent=2)
            tmp_path = Path(tf.name)
            
        try:
            sequence = self.bss.next_sequence()
            
            # Action: ~! (Handoff) if critical event, else .. (Passive info)
            action = "~!" if is_event else ".."
            
            # Urgency: !^ (Critical/Soon) if event, else =. (Normal/Whenever)
            urgency = "!^" if is_event else "=."
            
            # Construct blink ID: Seq(5) + Auth(1) + Act(2) + Rel(1) + Conf(1) + Cog(1) + Dom(1) + Sub(1) + Scope(1) + Mat(1) + Urg(2)
            # O: Author (Oracle)
            # ^: Relational (Origin / Seed) - required if Born from: Origin
            relational = "^"
            # !: Confidence (High - it's hard data)
            # .: Cognitive (Resolution / Data point)
            # $: Domain (Finance)
            # &: Subdomain (Analyzing/Data)
            # -: Scope (Local to this symbol)
            # !: Maturity (Complete)
            blink_id = f"{sequence}{AUTHOR_SIGIL}{action}{relational}!.$&-!{urgency}"
            
            # 3. Register the Artifact
            slug = f"market-{symbol.lower().replace('/', '-')}"
            artifact_path = self.bss.register_artifact(blink_id, tmp_path, slug)
            
            # 4. Generate Natural Language Summary
            summary = self._generate_summary(payload, is_event, artifact_path.name)
            
            # 5. Write the Blink
            blink = BlinkFile(
                blink_id=blink_id,
                born_from=["Origin"], # In a real implementation, we might track event thread lineages
                summary=summary,
                lineage=[blink_id],
                links=[]
            )
            
            # Handoffs go to /relay/ to wake up inference agents. Routine data goes to /active/.
            target_dir = self.bss.relay_dir if is_event else self.bss.active_dir
            write_blink(blink, target_dir)
            
            self._last_passive_write[symbol] = now
            logger.info(f"Wrote blink {blink_id} for {symbol} (Event: {is_event})")
            
        finally:
            if tmp_path.exists():
                os.unlink(tmp_path)

    def _evaluate_thresholds(self, symbol: str, payload: dict) -> bool:
        """Determine if this tick crosses actionable thresholds."""
        ticker = payload.get("ticker", {})
        price = ticker.get("last", 0)
        
        if not price:
            return False
            
        if symbol not in self._last_prices:
            self._last_prices[symbol] = {"price": price, "ts": datetime.now().timestamp()}
            return False # Need baseline
            
        baseline = self._last_prices[symbol]
        pct_change = abs((price - baseline["price"]) / baseline["price"]) * 100
        
        # Simple threshold logic for the prototype
        if pct_change >= self.config.price_change_pct_1m:
            self._last_prices[symbol] = {"price": price, "ts": datetime.now().timestamp()}
            return True
            
        return False

    def _generate_summary(self, payload: dict, is_event: bool, artifact_filename: str) -> str:
        """Convert the raw JSON into dense natural language for the LLM context."""
        symbol = payload.get("symbol", "UNKNOWN")
        ticker = payload.get("ticker", {})
        price = ticker.get("last", 0)
        
        if is_event:
            return (
                f"MARKET EVENT ALARM: Volatility threshold crossed for {symbol}. "
                f"Current Price is {price}. "
                f"BSS Inference Agents: review the attached structured artifact '{artifact_filename}' "
                f"and evaluate whether current portfolio positions require immediate adjustment."
            )
        else:
            return (
                f"Routine market state update for {symbol} at price {price}. "
                f"Structured data snapshot preserved in localized artifact '{artifact_filename}' for quantitative analysis."
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = TranslatorConfig(
        bss_root=os.environ.get("BSS_ROOT", "./.bss_env")
    )
    translator = OmniBssTranslator(config)
    asyncio.run(translator.run())
