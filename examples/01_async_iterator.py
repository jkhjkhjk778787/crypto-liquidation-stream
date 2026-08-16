"""
Example 1: Streaming liquidations using Python's native `async for` iterator.
"""

import asyncio
from crypto_liquidation import LiquidationStream

async def main():
    print("Starting liquidation stream for BTCUSDT and ETHUSDT across Binance, Bybit, and OKX...")
    
    # Initialize stream
    stream = LiquidationStream(
        exchanges=["binance", "bybit", "okx"],
        symbols=["BTCUSDT", "ETHUSDT"],
        min_notional_usd=100.0,  # Filter out tiny dust liquidations
    )

    async with stream:
        async for event in stream:
            direction = "🔴 LONG LIQUIDATED (Forced SELL)" if event.is_long_liquidation else "🟢 SHORT LIQUIDATED (Forced BUY)"
            print(f"[{event.datetime_iso}] [{event.exchange.upper()}] {event.symbol} | {direction}")
            print(f"  └ Price: ${event.price:,.2f} | Amount: {event.amount} | Value: ${event.notional_usd:,.2f}\n")

if __name__ == "__main__":
    asyncio.run(main())
