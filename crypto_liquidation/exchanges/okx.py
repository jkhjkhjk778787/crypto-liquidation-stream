"""
OKX v5 Public WebSocket Liquidation Worker.
"""

import json
import logging
import websockets
from typing import Optional, List
from .base import BaseLiquidationWorker
from ..models import LiquidationEvent, OrderSide, PositionSide
from ..utils import to_ms, normalize_symbol, extract_base_quote, fast_json_loads

logger = logging.getLogger("crypto_liquidation")

OKX_V5_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


class OKXLiquidationWorker(BaseLiquidationWorker):
    """
    Connects to OKX v5 Public WebSocket liquidation-orders channel.
    """

    @property
    def exchange_name(self) -> str:
        return "okx"

    async def _stream_loop(self) -> None:
        if self.normalized_symbols:
            args = []
            for s in self.normalized_symbols:
                base, quote = extract_base_quote(s)
                args.append({"channel": "liquidation-orders", "instType": "SWAP", "uly": f"{base}-{quote}"})
        else:
            # Global subscription to all SWAP liquidation events
            args = [{"channel": "liquidation-orders", "instType": "SWAP"}]

        async with websockets.connect(
            OKX_V5_PUBLIC_WS_URL,
            ping_interval=20,
            max_queue=None,
        ) as ws:
            # OKX accepts subscribe list in chunks of 20
            chunk_size = 20
            for i in range(0, len(args), chunk_size):
                chunk = args[i : i + chunk_size]
                await ws.send(json.dumps({"op": "subscribe", "args": chunk}))

            logger.info(f"[{self.exchange_name}] Subscribed to liquidation-orders.")

            async for raw_msg in ws:
                if not self.is_running:
                    break

                if raw_msg == "pong":
                    continue

                try:
                    msg = fast_json_loads(raw_msg)
                    if msg.get("arg", {}).get("channel") != "liquidation-orders":
                        continue

                    data_list = msg.get("data", [])
                    for data in data_list:
                        inst_id = data.get("instId", "")
                        uly = data.get("uly", "")
                        raw_sym = inst_id or uly
                        symbol = normalize_symbol(raw_sym)

                        if self.normalized_symbols and symbol not in self.normalized_symbols:
                            continue

                        details = data.get("details", [])
                        for detail in details:
                            price = float(detail.get("bkPx", 0) or 0.0)
                            amount = float(detail.get("sz", 0) or 0.0)
                            if price <= 0 or amount <= 0:
                                continue

                            pos_side_raw = detail.get("posSide", "long").lower()
                            if pos_side_raw == "long":
                                side = OrderSide.SELL
                                pos_side = PositionSide.LONG
                            else:
                                side = OrderSide.BUY
                                pos_side = PositionSide.SHORT

                            event = LiquidationEvent(
                                exchange=self.exchange_name,
                                symbol=symbol,
                                timestamp=to_ms(detail.get("ts")),
                                side=side,
                                pos_side=pos_side,
                                price=price,
                                amount=amount,
                                notional_usd=price * amount,
                                raw=detail if self.include_raw else None,
                            )
                            self.push_event_nowait(event)

                except Exception as e:
                    logger.debug(f"[{self.exchange_name}] Parse error: {e}")
