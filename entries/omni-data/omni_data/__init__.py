"""
Omni-Data Market Engine — The "All-Seeing Eye"

Exhaustive, model-agnostic, script-only financial data ingestion
and normalization engine for Open Athanor.

Subpackages:
    integrations/   Exchange connectivity (CCXT-powered, read-only)
    alternative/    Macro, on-chain, sentiment, regulatory data
    quant/          Options, derivatives, and risk calculators
    portfolio/      Portfolio state injection and PnL tracking
    broadcaster/    IPC gateway and multi-modal data formatting
    replay/         Deterministic walk-forward replay engine
    schemas/        Pydantic data contracts for all layers
"""

__version__ = "0.1.0"
__all__ = [
    "integrations",
    "alternative",
    "quant",
    "portfolio",
    "broadcaster",
    "replay",
    "schemas",
]
