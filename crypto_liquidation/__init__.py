"""
crypto_liquidation - Unified Real-time Cryptocurrency Liquidation WebSocket Library.

Supports Binance Futures (USDT-M), Bybit Linear (v5), and OKX (v5 SWAP).
"""

from .models import LiquidationEvent, ExchangeType, PositionSide, OrderSide
from .streamer import LiquidationStream
from .utils import normalize_symbol
from .gemini import fetch_live_liquidations, format_events_for_gemini, get_liquidation_tools

__version__ = "1.0.0"
__all__ = [
    "LiquidationStream",
    "LiquidationEvent",
    "ExchangeType",
    "PositionSide",
    "OrderSide",
    "normalize_symbol",
    "fetch_live_liquidations",
    "format_events_for_gemini",
    "get_liquidation_tools",
]
