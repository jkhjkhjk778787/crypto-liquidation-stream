"""
Command Line Interface for live crypto liquidation monitoring.
"""

import argparse
import asyncio
import sys
import logging
from .streamer import LiquidationStream
from .models import LiquidationEvent


def format_event(event: LiquidationEvent, fmt: str = "compact") -> str:
    if fmt == "json":
        return event.to_json()
    
    side_kr = "롱 청산(SELL)" if event.is_long_liquidation else "숏 청산(BUY)"
    side_color = "\033[91m" if event.is_long_liquidation else "\033[92m"
    reset_color = "\033[0m"
    
    if fmt == "table":
        return f"{event.datetime_iso} | {event.exchange.upper():<7} | {event.symbol:<12} | {side_color}{side_kr:<13}{reset_color} | Price: ${event.price:>11,.4f} | Qty: {event.amount:>10,.2f} | Size: ${event.notional_usd:>10,.2f}"
    
    # Compact default
    return f"[{event.exchange.upper()}] {event.symbol} {side_color}{side_kr}{reset_color} @ ${event.price:,.4f} | 규모: ${event.notional_usd:,.2f}"


async def async_main():
    parser = argparse.ArgumentParser(
        description="Real-time multi-exchange cryptocurrency liquidation stream terminal."
    )
    parser.add_argument(
        "--exchanges", "-e",
        type=str,
        default="binance,bybit,okx",
        help="Comma-separated exchanges to stream: binance,bybit,okx (default: all)",
    )
    parser.add_argument(
        "--symbols", "-s",
        type=str,
        default=None,
        help="Comma-separated symbols to track, e.g. 'BTCUSDT,ETHUSDT' (default: all)",
    )
    parser.add_argument(
        "--min-usd", "-m",
        type=float,
        default=0.0,
        help="Minimum liquidation size in USD to display (default: 0.0)",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["compact", "table", "json"],
        default="table",
        help="Output display format (default: table)",
    )
    parser.add_argument(
        "--duration", "-d",
        type=int,
        default=0,
        help="Execution duration in seconds (0 = run indefinitely)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose debug logging",
    )

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(message)s")

    exchanges = [e.strip().lower() for e in args.exchanges.split(",") if e.strip()]
    symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None

    print("\n" + "="*80)
    print("🚀 Crypto Liquidation Stream Terminal")
    print(f"• Exchanges : {', '.join([e.upper() for e in exchanges])}")
    print(f"• Symbols   : {', '.join(symbols) if symbols else 'ALL (전체 시장 청산)'}")
    print(f"• Min Size  : ${args.min_usd:,.2f}")
    print(f"• Format    : {args.format}")
    print("="*80 + "\n")

    stream = LiquidationStream(
        exchanges=exchanges,
        symbols=symbols,
        min_notional_usd=args.min_usd,
    )

    count = 0
    total_volume = 0.0

    async def _runner():
        nonlocal count, total_volume
        async with stream:
            async for event in stream:
                count += 1
                total_volume += event.notional_usd
                print(format_event(event, args.format))

    try:
        if args.duration > 0:
            await asyncio.wait_for(_runner(), timeout=args.duration)
        else:
            await _runner()
    except (asyncio.TimeoutError, KeyboardInterrupt):
        pass
    finally:
        await stream.stop()
        print("\n" + "="*80)
        print(f"🏁 스트림 종료: 총 {count}건 청산 포착 (누적 청산 규모: ${total_volume:,.2f})")
        print("="*80 + "\n")


def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
