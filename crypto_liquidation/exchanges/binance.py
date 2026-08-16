"""
Binance USDT-M Futures Native WebSocket Liquidation Worker.
"""

import logging
import websockets
from typing import Optional, List
from .base import BaseLiquidationWorker
from ..models import LiquidationEvent, OrderSide, PositionSide
from ..utils import to_ms, normalize_symbol, fast_json_loads

logger = logging.getLogger("crypto_liquidation")

BINANCE_FUTURES_WS_URL = "wss://fstream.binance.com/market/ws/!forceOrder@arr"


class BinanceLiquidationWorker(BaseLiquidationWorker):
    """
    Connects to Binance Futures All-Market Liquidation Order stream (!forceOrder@arr).
    """

    @property
    def exchange_name(self) -> str:
        return "binance"

    async def _stream_loop(self) -> None:
        async with websockets.connect(
            BINANCE_FUTURES_WS_URL,
            ping_interval=15,
            ping_timeout=10,
            max_queue=None,
        ) as ws:
            logger.info(f"[{self.exchange_name}] WebSocket connected successfully.")
            async for raw_msg in ws:
                if not self.is_running:
                    break

                try:
                    msg = fast_json_loads(raw_msg)
                    if "data" in msg:
                        msg = msg["data"]

                    if msg.get("e") != "forceOrder":
                        continue

                    order = msg.get("o", {})
                    raw_symbol = order.get("s", "")
                    symbol = normalize_symbol(raw_symbol)

                    # Symbol filtering if specified
                    if self.normalized_symbols and symbol not in self.normalized_symbols:
                        continue

                    price = float(order.get("ap") or order.get("p") or 0.0)
                    amount = float(order.get("q") or 0.0)
                    order_side_raw = order.get("S", "SELL")

                    # In Binance: SELL means long position liquidated, BUY means short position liquidated
                    if order_side_raw == "SELL":
                        side = OrderSide.SELL
                        pos_side = PositionSide.LONG
                    else:
                        side = OrderSide.BUY
                        pos_side = PositionSide.SHORT

                    event = LiquidationEvent(
                        exchange=self.exchange_name,
                        symbol=symbol,
                        timestamp=to_ms(msg.get("E")),
                        side=side,
                        pos_side=pos_side,
                        price=price,
                        amount=amount,
                        notional_usd=price * amount,
                        raw=msg if self.include_raw else None,
                    )

                    self.push_event_nowait(event)

                except Exception as e:
                    logger.debug(f"[{self.exchange_name}] Failed to parse message: {e}")
