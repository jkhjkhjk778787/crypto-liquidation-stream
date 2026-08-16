"""
Data models and type definitions for normalized crypto liquidation events.
Optimized with `slots=True` and `frozen=True` for low memory footprint and high throughput.
"""

import sys
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Dict, Any
import time

try:
    import orjson
    def _to_json(data: dict) -> str:
        return orjson.dumps(data).decode("utf-8")
except ImportError:
    import json
    def _to_json(data: dict) -> str:
        return json.dumps(data, ensure_ascii=False)


class ExchangeType(str, Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"


class PositionSide(str, Enum):
    """The position side that was liquidated."""
    LONG = "long"
    SHORT = "short"
    UNKNOWN = "unknown"


class OrderSide(str, Enum):
    """The market order side executed to liquidate the position."""
    SELL = "sell"  # Forced sell to liquidate a Long position
    BUY = "buy"    # Forced buy to liquidate a Short position


# Use slots=True on Python 3.10+ for ~60% memory reduction and faster attribute access
_dataclass_kwargs = {"slots": True, "frozen": True} if sys.version_info >= (3, 10) else {"frozen": True}


@dataclass(**_dataclass_kwargs)
class LiquidationEvent:
    """
    Normalized, exchange-agnostic representation of a single liquidation event.

    Attributes:
        exchange (str): Source exchange name ("binance", "bybit", "okx").
        symbol (str): Normalized unified pair symbol, e.g. "BTCUSDT", "ETHUSDT".
        timestamp (int): Event execution timestamp in Unix milliseconds (UTC).
        side (OrderSide): Market order side executed ("sell" for Long liq, "buy" for Short liq).
        pos_side (PositionSide): Liquidated position side ("long" or "short").
        price (float): Liquidation / bankruptcy execution price in USD/USDT.
        amount (float): Liquidated asset volume/quantity in base coin units.
        notional_usd (float): Total liquidation value in USD/USDT (price * amount).
        raw (Optional[dict]): Original raw JSON payload received from WebSocket (optional).
    """
    exchange: str
    symbol: str
    timestamp: int
    side: OrderSide
    pos_side: PositionSide
    price: float
    amount: float
    notional_usd: float
    raw: Optional[Dict[str, Any]] = field(default=None, repr=False)

    @property
    def is_long_liquidation(self) -> bool:
        """Returns True if a Long position was liquidated (forced sell order)."""
        return self.pos_side == PositionSide.LONG or self.side == OrderSide.SELL

    @property
    def is_short_liquidation(self) -> bool:
        """Returns True if a Short position was liquidated (forced buy order)."""
        return self.pos_side == PositionSide.SHORT or self.side == OrderSide.BUY

    @property
    def datetime_iso(self) -> str:
        """Returns human-readable ISO formatted UTC time string."""
        return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(self.timestamp / 1000.0)) + f".{self.timestamp % 1000:03d}Z"

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to a standard Python dictionary."""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "datetime_iso": self.datetime_iso,
            "side": self.side.value if isinstance(self.side, Enum) else self.side,
            "pos_side": self.pos_side.value if isinstance(self.pos_side, Enum) else self.pos_side,
            "price": self.price,
            "amount": self.amount,
            "notional_usd": self.notional_usd,
            "raw": self.raw,
        }

    def to_json(self) -> str:
        """Serialize event to a valid JSON string using fast orjson if available."""
        return _to_json(self.to_dict())
