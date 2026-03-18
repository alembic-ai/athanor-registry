"""
Omni-Data Configuration Module — centralized settings.

Environment-based configuration for all API keys, IPC endpoints,
and runtime options. Uses Pydantic Settings for validation and
.env file support.

All sensitive values come from environment variables.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, Field


class ExchangeConfig(BaseModel):
    """Configuration for a single exchange connection."""
    exchange_id: str
    sandbox: bool = False
    api_key: str = ""
    api_secret: str = ""
    password: str = ""
    rate_limit: bool = True
    extra: dict[str, Any] = Field(default_factory=dict)


class IPCConfig(BaseModel):
    """IPC transport configuration."""
    transport: str = Field(default="zmq", description="'zmq' or 'redis'")
    zmq_bind_address: str = "tcp://*:5555"
    zmq_connect_address: str = "tcp://localhost:5555"
    redis_url: str = "redis://localhost:6379"
    redis_db: int = 0
    high_water_mark: int = 1000


class APIKeysConfig(BaseModel):
    """External API keys — all sourced from environment variables."""
    fred_api_key: str = Field(
        default_factory=lambda: os.environ.get("FRED_API_KEY", "")
    )
    glassnode_api_key: str = Field(
        default_factory=lambda: os.environ.get("GLASSNODE_API_KEY", "")
    )
    token_terminal_api_key: str = Field(
        default_factory=lambda: os.environ.get("TOKEN_TERMINAL_API_KEY", "")
    )
    newsapi_key: str = Field(
        default_factory=lambda: os.environ.get("NEWSAPI_KEY", "")
    )
    lunarcrush_api_key: str = Field(
        default_factory=lambda: os.environ.get("LUNARCRUSH_API_KEY", "")
    )
    polygon_api_key: str = Field(
        default_factory=lambda: os.environ.get("POLYGON_API_KEY", "")
    )
    alphavantage_api_key: str = Field(
        default_factory=lambda: os.environ.get("ALPHAVANTAGE_API_KEY", "")
    )
    sec_user_agent: str = Field(
        default_factory=lambda: os.environ.get(
            "SEC_USER_AGENT",
            "OmniDataMarketEngine/0.1.0 (contact@alembic.ai)"
        )
    )


class ReplayConfig(BaseModel):
    """Replay engine configuration."""
    data_directory: str = "data/"
    default_format: str = "parquet"
    default_speed: float = 10.0


class ChartConfig(BaseModel):
    """Chart rendering configuration."""
    style: str = "nightclouds"
    figsize_width: int = 12
    figsize_height: int = 6
    dpi: int = 100
    orderbook_levels: int = 20


class BssTranslatorConfig(BaseModel):
    """Configuration for the native BSS translation daemon."""
    enabled: bool = Field(default=False, description="Enable native BSS blink translation")
    bss_root: str = Field(default="./.bss_env", description="Root directory of the BSS environment")
    passive_throttle_sec: float = 60.0
    price_change_pct_1m: float = 1.0


class OmniDataConfig(BaseModel):
    """
    Root configuration for the entire Omni-Data Market Engine.

    Usage:
        config = OmniDataConfig(
            exchanges=[
                ExchangeConfig(exchange_id="binance"),
                ExchangeConfig(exchange_id="kraken"),
            ],
        )
    """
    exchanges: list[ExchangeConfig] = Field(default_factory=list)
    ipc: IPCConfig = Field(default_factory=IPCConfig)
    api_keys: APIKeysConfig = Field(default_factory=APIKeysConfig)
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    charts: ChartConfig = Field(default_factory=ChartConfig)
    bss_translator: BssTranslatorConfig = Field(default_factory=BssTranslatorConfig)

    # Runtime options
    symbols: list[str] = Field(
        default_factory=lambda: ["BTC/USDT"],
        description="Default symbols to stream/broadcast.",
    )
    broadcast_interval: float = Field(
        default=5.0,
        description="Seconds between full state broadcasts.",
    )
    max_token_budget: int = Field(
        default=8000,
        description="Default character limit for token-aware truncation.",
    )
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> OmniDataConfig:
        """
        Build config from environment variables with sensible defaults.

        This is the recommended way to construct config in production.
        """
        exchanges_str = os.environ.get("OMNI_EXCHANGES", "binance")
        exchange_ids = [e.strip() for e in exchanges_str.split(",") if e.strip()]

        exchanges = []
        for eid in exchange_ids:
            prefix = eid.upper()
            exchanges.append(ExchangeConfig(
                exchange_id=eid,
                api_key=os.environ.get(f"{prefix}_API_KEY", ""),
                api_secret=os.environ.get(f"{prefix}_API_SECRET", ""),
                password=os.environ.get(f"{prefix}_PASSWORD", ""),
                sandbox=os.environ.get(f"{prefix}_SANDBOX", "").lower() == "true",
            ))

        return cls(
            exchanges=exchanges,
            ipc=IPCConfig(
                transport=os.environ.get("OMNI_IPC_TRANSPORT", "zmq"),
                zmq_bind_address=os.environ.get("OMNI_ZMQ_BIND", "tcp://*:5555"),
                redis_url=os.environ.get("OMNI_REDIS_URL", "redis://localhost:6379"),
            ),
            bss_translator=BssTranslatorConfig(
                enabled=os.environ.get("OMNI_ENABLE_BSS_TRANSLATOR", "false").lower() == "true",
                bss_root=os.environ.get("OMNI_BSS_ROOT", "./.bss_env"),
            ),
            symbols=os.environ.get("OMNI_SYMBOLS", "BTC/USDT").split(","),
            broadcast_interval=float(os.environ.get("OMNI_BROADCAST_INTERVAL", "5.0")),
            max_token_budget=int(os.environ.get("OMNI_MAX_TOKENS", "8000")),
            log_level=os.environ.get("OMNI_LOG_LEVEL", "INFO"),
        )
