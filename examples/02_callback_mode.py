"""
Example 2: Event-driven liquidation monitoring using callback handlers.
"""

import asyncio
from crypto_liquidation import LiquidationStream, LiquidationEvent

async def on_large_liquidation(event: LiquidationEvent):
    """Async callback triggered when a whale liquidation occurs."""
    if event.notional_usd >= 10_000:
        print(f"🚨 [WHALE ALERT] ${event.notional_usd:,.2f} liquidated on {event.exchange.upper()} ({event.symbol})!")

def on_any_liquidation(event: LiquidationEvent):
    """Synchronous logging callback."""
    print(f"⚡ [{event.exchange.upper()}] {event.symbol} {event.side.value.upper()} | ${event.notional_usd:,.2f}")

async def main():
    stream = LiquidationStream(
        exchanges=["binance", "bybit", "okx"],
        min_notional_usd=50.0,
    )
    
    stream.add_callback(on_any_liquidation)
    stream.add_callback(on_large_liquidation)

    await stream.start()
    print("Stream running with callbacks registered. Listening for 20 seconds...")
    await asyncio.sleep(20)
    await stream.stop()

if __name__ == "__main__":
    asyncio.run(main())
