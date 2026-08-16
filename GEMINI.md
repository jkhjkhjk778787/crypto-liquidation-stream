# GEMINI.md: Google Gemini & Antigravity Agent Guidelines

> **Project**: `crypto-liquidation-stream`  
> **Primary Purpose**: High-performance, unified real-time cryptocurrency liquidation WebSocket stream for Binance, Bybit, and OKX derivatives markets.

---

## 🤖 Gemini Agent Quick Reference

### 1. Essential Imports
```python
from crypto_liquidation import LiquidationStream, LiquidationEvent, OrderSide, PositionSide
from crypto_liquidation.gemini import fetch_live_liquidations, format_events_for_gemini, get_liquidation_tools
```

### 2. Core Architectural Principles
1. **Zero External DB Dependency**:
   - The core streamer runs in-memory with native `websockets` and `asyncio`.
   - Use `async with LiquidationStream(...) as stream:` for zero-leak async iteration.
2. **Exchange Side Mapping Standard**:
   - `OrderSide.SELL` / `is_long_liquidation == True` ➔ **Long Position Liquidated** (forced market sell).
   - `OrderSide.BUY` / `is_short_liquidation == True` ➔ **Short Position Liquidated** (forced market buy).
3. **Symbol Normalization**:
   - Always input standard pair strings (e.g., `"BTCUSDT"`, `"ETHUSDT"`). Symbols with slashes (`"BTC/USDT"`) or swap suffixes (`"BTC-USDT-SWAP"`) are automatically normalized.

---

## 🛠️ Gemini Tool / Function Calling Definitions

Gemini can invoke `fetch_live_liquidations` to capture real-time orderflow bursts:

```python
from google import genai
from crypto_liquidation.gemini import get_liquidation_tools

client = genai.Client()
response = client.models.generate_content(
    model="gemini-2.0-flash",
    contents="Check if there is a liquidation cascade on BTC or ETH right now.",
    config={"tools": get_liquidation_tools()}
)
```

---

## 📈 Gemini Analysis Rules for Crypto Liquidations

When analyzing liquidation data to generate trading insights or market commentary:
1. **Absorption vs. Reversal**:
   - Large long liquidation clusters alone do **not** guarantee an immediate market bottom. Verify if spot taker buy CVD is actively absorbing the sell-off before suggesting bullish reversal.
2. **Liquidation Cascades**:
   - Consecutive liquidation clusters breaking critical support levels often trigger rapid cascade sell-offs (Long Squeeze).
3. **Multi-Exchange Divergence**:
   - Cross-verify Binance, Bybit, and OKX volumes to distinguish isolated exchange anomalies from market-wide volatility.

---

## 📂 Project Directory Structure

```
crypto-liquidation-stream/
├── GEMINI.md                   # This file (Gemini instruction manifest)
├── llms.txt                    # Ultra-compact API guide for LLMs
├── AGENTS.md                   # Agentic coding conventions
├── crypto_liquidation/
│   ├── models.py               # LiquidationEvent dataclass (slots=True, frozen=True)
│   ├── streamer.py             # LiquidationStream (async for & stream_batches)
│   ├── utils.py                # LRU-cached symbol normalization & fast_json_loads
│   ├── gemini.py               # Gemini Function Calling tools & prompt formatters
│   ├── cli.py                  # Terminal monitor CLI
│   └── exchanges/
│       ├── base.py             # Auto-reconnect & queue overflow protector
│       ├── binance.py          # Binance USDT-M Futures (!forceOrder@arr)
│       ├── bybit.py            # Bybit Linear v5 (allLiquidation.{symbol})
│       └── okx.py              # OKX SWAP v5 (liquidation-orders)
├── examples/
│   ├── 01_async_iterator.py    # Basic streaming
│   ├── 02_callback_mode.py     # Event callbacks
│   ├── 03_batch_streaming.py   # High-throughput DB batching
│   └── 04_gemini_tool_call.py  # Gemini SDK integration
└── tests/
    └── test_stream.py          # Pytest suite
```
