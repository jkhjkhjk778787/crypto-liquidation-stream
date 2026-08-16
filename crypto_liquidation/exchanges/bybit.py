"""
Bybit Linear (v5) Native WebSocket Liquidation Worker.
"""

import asyncio
import json
import logging
import urllib.request
import websockets
from typing import Optional, List, Set
from .base import BaseLiquidationWorker
from ..models import LiquidationEvent, OrderSide, PositionSide
from ..utils import to_ms, normalize_symbol, fast_json_loads

logger = logging.getLogger("crypto_liquidation")

BYBIT_V5_LINEAR_WS_URL = "wss://stream.bybit.com/v5/public/linear"


class BybitLiquidationWorker(BaseLiquidationWorker):
    """
    Connects to Bybit v5 Linear Public WebSocket (allLiquidation.{symbol}).
    """

    @property
    def exchange_name(self) -> str:
        return "bybit"

    def _fetch_active_symbols(self, limit: int = 150) -> List[str]:
        """Fetch top traded linear USDT perpetual symbols if none specified."""
        try:
            req = urllib.request.Request(
                "https://api.bybit.com/v5/market/tickers?category=linear",
                headers={"User-Agent": "crypto-liquidation-stream"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = fast_json_loads(resp.read().decode("utf-8"))
            tickers = data.get("result", {}).get("list", [])
            sorted_tickers = sorted(
                [d for d in tickers if d.get("symbol", "").endswith("USDT")],
                key=lambda x: float(x.get("turnover24h", 0) or 0),
                reverse=True,
            )
            return [d["symbol"] for d in sorted_tickers[:limit]]
        except Exception as e:
            logger.warning(f"[{self.exchange_name}] Failed to fetch active tickers: {e}. Fallback to default symbols.")
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", "ADAUSDT", "AVAXUSDT", "SUIUSDT", "NEARUSDT"]

    async def _stream_loop(self) -> None:
        target_symbols = list(self.normalized_symbols) if self.normalized_symbols else self._fetch_active_symbols()
        topics = [f"allLiquidation.{s}" for s in target_symbols]

        async with websockets.connect(
            BYBIT_V5_LINEAR_WS_URL,
            ping_interval=20,
            max_queue=None,
        ) as ws:
            # Application-level keepalive ping task
            async def _ping_loop():
                while self.is_running:
                    await asyncio.sleep(20)
                    try:
                        await ws.send('{"op":"ping"}')
                    except Exception:
                        break

            ping_task = asyncio.create_task(_ping_loop())

            try:
                # Bybit allows up to 10 args per subscribe command
                chunk_size = 10
                for i in range(0, len(topics), chunk_size):
                    chunk = topics[i : i + chunk_size]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))

                logger.info(f"[{self.exchange_name}] Subscribed to {len(topics)} symbols.")

                async for raw_msg in ws:
                    if not self.is_running:
                        break

                    try:
                        msg = fast_json_loads(raw_msg)
                        topic = msg.get("topic", "")
                        if not topic.startswith("allLiquidation."):
                            continue

                        data_list = msg.get("data", [])
                        if isinstance(data_list, dict):
                            data_list = [data_list]

                        for item in data_list:
                            raw_sym = item.get("symbol") or item.get("s") or topic.replace("allLiquidation.", "")
                            symbol = normalize_symbol(raw_sym)

                            if self.normalized_symbols and symbol not in self.normalized_symbols:
                                continue

                            price = float(item.get("p") or item.get("price") or 0.0)
                            amount = float(item.get("v") or item.get("size") or 0.0)
                            side_raw = item.get("S") or item.get("side") or "Buy"

                            # In Bybit v5: 'Buy' indicates a bankrupt Long position (forced sell market order)
                            if side_raw == "Buy":
                                side = OrderSide.SELL
                                pos_side = PositionSide.LONG
                            else:
                                side = OrderSide.BUY
                                pos_side = PositionSide.SHORT

                            ts_raw = item.get("T") or item.get("updatedTime") or msg.get("ts")
                            event = LiquidationEvent(
                                exchange=self.exchange_name,
                                symbol=symbol,
                                timestamp=to_ms(ts_raw),
                                side=side,
                                pos_side=pos_side,
                                price=price,
                                amount=amount,
                                notional_usd=price * amount,
                                raw=item if self.include_raw else None,
                            )
                            self.push_event_nowait(event)

                    except Exception as e:
                        logger.debug(f"[{self.exchange_name}] Parse error: {e}")

            finally:
                ping_task.cancel()
