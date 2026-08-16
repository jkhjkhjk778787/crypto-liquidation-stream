"""
Google Gemini & GenAI SDK Integration Helper.

Provides:
1. Ready-to-use Function Calling Tool declarations for Gemini 2.0 / 3.0.
2. Context-optimized prompt formatters for Gemini 1M-2M token context window.
3. Quantitative liquidation analysis rules aligned with orderflow trading protocols.
"""

import asyncio
import time
from typing import List, Optional, Dict, Any, Union
from .streamer import LiquidationStream
from .models import LiquidationEvent


def format_events_for_gemini(
    events: List[LiquidationEvent],
    title: str = "Real-Time Liquidation Snapshot",
) -> str:
    """
    Formats a collection of LiquidationEvents into high-signal Markdown optimized
    for Gemini's context window with zero token bloat.
    """
    if not events:
        return f"### 📊 {title}\n*No liquidation events captured during this window.*"

    total_long_liq = sum(e.notional_usd for e in events if e.is_long_liquidation)
    total_short_liq = sum(e.notional_usd for e in events if e.is_short_liquidation)
    total_liq = total_long_liq + total_short_liq

    long_count = sum(1 for e in events if e.is_long_liquidation)
    short_count = sum(1 for e in events if e.is_short_liquidation)

    largest_liq = max(events, key=lambda e: e.notional_usd)

    lines = [
        f"### 📊 {title} (Total: ${total_liq:,.2f})",
        f"- **Long Liquidations (Forced Sell)**: ${total_long_liq:,.2f} ({long_count} orders)",
        f"- **Short Liquidations (Forced Buy)**: ${total_short_liq:,.2f} ({short_count} orders)",
        f"- **Largest Liquidation**: [{largest_liq.exchange.upper()}] {largest_liq.symbol} ${largest_liq.notional_usd:,.2f} @ ${largest_liq.price:,.4f}",
        "",
        "| Time (UTC) | Exchange | Symbol | Side | Price | Amount | Notional (USD) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    # Show top 50 events sorted by size or recency
    sorted_events = sorted(events, key=lambda x: x.notional_usd, reverse=True)[:50]
    for e in sorted_events:
        side_label = "🔴 LONG (SELL)" if e.is_long_liquidation else "🟢 SHORT (BUY)"
        t_str = time.strftime('%H:%M:%S', time.gmtime(e.timestamp / 1000.0))
        lines.append(f"| {t_str} | {e.exchange.upper()} | {e.symbol} | {side_label} | ${e.price:,.4f} | {e.amount:,.2f} | ${e.notional_usd:,.2f} |")

    return "\n".join(lines)


async def fetch_live_liquidations(
    symbols: Optional[List[str]] = None,
    duration_sec: int = 5,
    min_notional_usd: float = 0.0,
    exchanges: Optional[List[str]] = None,
) -> str:
    """
    Captures live liquidation events across Binance, Bybit, and OKX for a short duration
    and returns an AI-analyzable summary report.

    Args:
        symbols: Optional list of coin symbols to monitor, e.g. ["BTCUSDT", "ETHUSDT"].
                 If omitted, monitors the entire derivatives market.
        duration_sec: Sampling window in seconds (default: 5).
        min_notional_usd: Minimum USD notional size filter (default: 0.0).
        exchanges: List of target exchanges (default: ["binance", "bybit", "okx"]).

    Returns:
        Formatted Markdown liquidation report for Gemini analysis.
    """
    stream = LiquidationStream(
        exchanges=exchanges or ["binance", "bybit", "okx"],
        symbols=symbols,
        min_notional_usd=min_notional_usd,
        include_raw=False,
    )

    events: List[LiquidationEvent] = []
    await stream.start()

    async def _collect():
        async for event in stream:
            events.append(event)

    collector_task = asyncio.create_task(_collect())
    try:
        await asyncio.sleep(max(1, duration_sec))
    finally:
        collector_task.cancel()
        await stream.stop()

    sym_str = ", ".join(symbols) if symbols else "Entire Market"
    return format_events_for_gemini(events, title=f"Live Liquidation Window ({duration_sec}s for {sym_str})")


def get_gemini_analysis_prompt() -> str:
    """
    Returns high-priority system prompt guidelines for Gemini when briefing crypto orderflow.
    """
    return """
# Real-Time Crypto Liquidation Analysis Directives

When evaluating crypto liquidation orderflow:
1. **Absorption Bias Caution**: Massive long liquidation absorption does NOT guarantee an immediate price bottom. Only signal bullish confirmation if spot taker CVD aggressively surges and breaks key resistance.
2. **Price vs Volume Efficiency**: If massive buy volume is absorbed without price advancement, treat it as hidden iceberg short defense (Short Trap).
3. **Cascading Breakdown**: Repeated tests of support levels weaken structural depth, precipitating rapid multi-exchange liquidation cascades.
""".strip()


def get_liquidation_tools() -> List[Dict[str, Any]]:
    """
    Returns OpenAPI / Function Calling JSON Schema definitions for Google Gemini APIs.
    """
    return [
        {
            "name": "fetch_live_liquidations",
            "description": "Stream live high-frequency liquidation events from Binance, Bybit, and OKX for real-time market orderflow analysis.",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of cryptocurrency pair symbols to track, e.g. ['BTCUSDT', 'ETHUSDT']. If empty, streams all pairs.",
                    },
                    "duration_sec": {
                        "type": "integer",
                        "description": "Sampling duration in seconds (typically 3 to 15 seconds). Default is 5.",
                    },
                    "min_notional_usd": {
                        "type": "number",
                        "description": "Minimum liquidation value in USD to filter out noise/dust orders. Default is 0.0.",
                    },
                },
            },
        }
    ]
