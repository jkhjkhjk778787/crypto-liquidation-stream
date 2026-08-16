"""
Example 3: High-throughput Micro-Batching for database bulk-insert (Redis/ClickHouse/DB).
"""

import asyncio
from crypto_liquidation import LiquidationStream

async def main():
    print("Starting high-throughput micro-batch liquidation stream...")
    
    stream = LiquidationStream(
        exchanges=["binance", "bybit", "okx"],
        min_notional_usd=0.0,
        include_raw=False,  # Keep memory footprint minimal
    )

    await stream.start()
    
    count_batches = 0
    total_events = 0

    try:
        # Yields a list of events every 50ms or when 50 items are accumulated
        async for batch in stream.stream_batches(max_batch_size=50, max_interval_ms=50):
            count_batches += 1
            total_events += len(batch)
            print(f"📦 [Batch #{count_batches}] Received {len(batch)} liquidation events (Total: {total_events})")
            for event in batch[:3]:  # Print first 3 events of batch
                print(f"   • [{event.exchange.upper()}] {event.symbol} {event.side.value.upper()} | ${event.notional_usd:,.2f}")
            if len(batch) > 3:
                print(f"   • ... and {len(batch) - 3} more")

            if total_events >= 10:
                print("Captured 10+ batch events. Stopping demo.")
                break
    finally:
        await stream.stop()

if __name__ == "__main__":
    asyncio.run(main())
