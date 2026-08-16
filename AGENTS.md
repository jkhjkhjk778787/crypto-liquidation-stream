# Agent Guidelines: crypto-liquidation-stream

This repository provides a unified WebSocket streaming interface for cryptocurrency liquidations across Binance, Bybit, and OKX.

## Architecture Guidelines
- **Core Library**: Located in `crypto_liquidation/`.
- **Zero Heavy Dependencies**: Only requires standard Python 3.9+ and `websockets`. Avoid adding heavy third-party packages to the core library.
- **Unified Event Model**: All exchange workers MUST emit normalized `LiquidationEvent` instances into the shared `asyncio.Queue`.
- **Exchange Workers**:
  - `BinanceLiquidationWorker`: Uses `wss://fstream.binance.com/market/ws/!forceOrder@arr` (2026 `/market/ws/` format).
  - `BybitLiquidationWorker`: Uses `wss://stream.bybit.com/v5/public/linear` with `allLiquidation.{symbol}` and periodic keepalive ping `{"op": "ping"}`.
  - `OKXLiquidationWorker`: Uses `wss://ws.okx.com:8443/ws/v5/public` with `channel: liquidation-orders, instType: SWAP`.
- **Side Interpretation Standard**:
  - Forced Market SELL = Liquidated LONG Position.
  - Forced Market BUY = Liquidated SHORT Position.

## Testing
Run test suite with:
```bash
pytest tests/
```
