# 🌊 Crypto Liquidation Stream

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![WebSocket](https://img.shields.io/badge/WebSocket-Native-brightgreen.svg)]()

A unified, high-performance, asynchronous WebSocket client for streaming real-time liquidation data across **Binance Futures (USDT-M)**, **Bybit Linear (v5)**, and **OKX SWAP (v5)**.

Designed for quantitative trading bots, footprint orderflow terminals, risk managers, and AI trading agents.

---

## ✨ Key Features

- ⚡ **Native High-Frequency WebSockets**: Zero overhead, direct native WebSocket connections without bulky wrappers.
- 🔄 **Unified Event Model**: Standardized `LiquidationEvent` across all exchanges with normalized symbols, prices, timestamps, and position side interpretation.
- 🔁 **Resilient Auto-Reconnect**: Built-in exponential backoff and application-level keepalive pings (`ping/pong`).
- 🤖 **AI & Agent-Friendly**: Fully typed with type annotations, async iterator (`async for`), callback pattern, and `llms.txt` integration.
- 🎯 **Flexible Filtering**: Easily filter by symbols (e.g. `BTCUSDT`, `ETHUSDT`) or minimum liquidation USD threshold.
- 💻 **CLI Terminal Included**: Instant terminal live monitoring tool out of the box.

---

## 📦 Installation

```bash
git clone https://github.com/<username>/crypto-liquidation-stream.git
cd crypto-liquidation-stream
pip install -e .
```

Or install directly via requirements:
```bash
pip install websockets>=12.0
```

---

## 🚀 Quickstart

### 1. Simple Async Iterator (`async for`)

```python
import asyncio
from crypto_liquidation import LiquidationStream

async def main():
    # Connect to Binance, Bybit, and OKX for BTC & ETH liquidations
    async with LiquidationStream(symbols=["BTCUSDT", "ETHUSDT"], min_notional_usd=500.0) as stream:
        async for event in stream:
            direction = "🔴 LONG LIQ (SELL)" if event.is_long_liquidation else "🟢 SHORT LIQ (BUY)"
            print(f"[{event.datetime_iso}] [{event.exchange.upper()}] {event.symbol} | {direction} | Price: ${event.price:,.2f} | Size: ${event.notional_usd:,.2f}")

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Event-Driven Callbacks

```python
import asyncio
from crypto_liquidation import LiquidationStream, LiquidationEvent

async def on_large_liquidation(e: LiquidationEvent):
    if e.notional_usd >= 50_000:
        print(f"🚨 [WHALE ALERT] ${e.notional_usd:,.2f} liquidated on {e.exchange.upper()} ({e.symbol})!")

async def main():
    stream = LiquidationStream(exchanges=["binance", "bybit", "okx"])
    stream.add_callback(on_large_liquidation)
    
    await stream.start()
    await asyncio.sleep(60)
### 3. High-Throughput Micro-Batching (DB Ingestion)

```python
import asyncio
from crypto_liquidation import LiquidationStream

async def main():
    stream = LiquidationStream(include_raw=False)
    await stream.start()
    # Collects up to 100 events or flushes every 20ms
    async for batch in stream.stream_batches(max_batch_size=100, max_interval_ms=20):
        # Bulk insert into ClickHouse / Redis / PostgreSQL
        print(f"Bulk inserting {len(batch)} liquidation records to DB")

if __name__ == "__main__":
    asyncio.run(main())
```

### 4. 🤖 Google Gemini Tool / Function Calling Integration

```python
from google import genai
from crypto_liquidation.gemini import get_liquidation_tools

client = genai.Client()
# Gemini directly calls fetch_live_liquidations to analyze real-time orderflow
response = client.models.generate_content(
    model="gemini-2.0-flash",
    contents="Check if there is a liquidation cascade on BTC or ETH right now.",
    config={"tools": get_liquidation_tools()}
)
print(response.text)
```

---

## 📊 Normalized Data Model (`LiquidationEvent`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `exchange` | `str` | `"binance"`, `"bybit"`, or `"okx"` |
| `symbol` | `str` | Unified uppercase pair name, e.g. `"BTCUSDT"` |
| `timestamp` | `int` | Event timestamp in milliseconds (UTC) |
| `datetime_iso` | `str` | ISO 8601 UTC string (`2026-08-16T08:59:03.123Z`) |
| `side` | `OrderSide` | `OrderSide.SELL` (Long liq) or `OrderSide.BUY` (Short liq) |
| `pos_side` | `PositionSide`| `PositionSide.LONG` or `PositionSide.SHORT` |
| `price` | `float` | Liquidation / bankruptcy execution price (USD) |
| `amount` | `float` | Executed quantity in base coin units |
| `notional_usd`| `float` | Total liquidation value in USD (`price * amount`) |
| `is_long_liquidation` | `bool` | Convenience property returning True if Long position was liquidated |
| `is_short_liquidation` | `bool` | Convenience property returning True if Short position was liquidated |
| `raw` | `dict` | Original unparsed JSON message from the exchange |

---

## 🖥️ Command Line Interface (CLI)

Run live liquidation monitor directly from your terminal:

```bash
# Monitor all 3 exchanges with table format
python -m crypto_liquidation --format table

# Monitor specific symbols with minimum $1,000 liquidation filter
python -m crypto_liquidation --symbols BTCUSDT,ETHUSDT,SOLUSDT --min-usd 1000

# Output as JSON stream
python -m crypto_liquidation --format json
```

---

## 📡 Exchange Protocol Details

| Exchange | WebSocket Endpoint | Channel / Stream | Direction Mapping |
| :--- | :--- | :--- | :--- |
| **Binance** | `wss://fstream.binance.com/market/ws/!forceOrder@arr` | `!forceOrder@arr` | `SELL` ➔ Long Liq<br>`BUY` ➔ Short Liq |
| **Bybit** | `wss://stream.bybit.com/v5/public/linear` | `allLiquidation.{symbol}` | `Buy` ➔ Long Liq (`sell`)<br>`Sell` ➔ Short Liq (`buy`) |
| **OKX** | `wss://ws.okx.com:8443/ws/v5/public` | `liquidation-orders` (`instType: SWAP`) | `posSide: "long"` ➔ Long Liq (`sell`)<br>`posSide: "short"` ➔ Short Liq (`buy`) |

---

## 🧪 Testing

```bash
pytest tests/
```

---

## 📄 License

MIT License. Free for personal and commercial quantitative trading use.
