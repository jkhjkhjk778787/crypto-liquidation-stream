"""
Utility functions for symbol normalization, timestamp formatting, and fast JSON loading.
Optimized with LRU caching and optional orjson/ujson acceleration.
"""

import functools
import time
from typing import Tuple, Any, Union

# Fast JSON parser fallback chain: orjson -> ujson -> standard json
try:
    import orjson
    def fast_json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)
except ImportError:
    try:
        import ujson
        def fast_json_loads(data: Union[str, bytes]) -> Any:
            return ujson.loads(data)
    except ImportError:
        import json
        def fast_json_loads(data: Union[str, bytes]) -> Any:
            return json.loads(data)


def to_ms(ts: Any = None) -> int:
    """
    Safely convert various timestamp formats (seconds, milliseconds, float, string)
    to a standardized 13-digit Unix millisecond integer.
    """
    if ts is None:
        return int(time.time() * 1000)
    try:
        ts_int = int(float(ts))
        if ts_int < 10**11:  # seconds timestamp (e.g. 1786838342)
            return ts_int * 1000
        return ts_int
    except (ValueError, TypeError):
        return int(time.time() * 1000)


@functools.lru_cache(maxsize=2048)
def extract_base_quote(raw_symbol: str) -> Tuple[str, str]:
    """
    Extract base currency and quote currency from arbitrary symbol strings.
    Results are cached in memory for zero-allocation $O(1)$ lookups in hot paths.
    """
    clean = raw_symbol.upper().strip()
    
    # Remove exchange-specific suffixes
    clean = clean.replace("-SWAP", "").replace(":USDT", "").replace(":USD", "")
    
    if "/" in clean:
        parts = clean.split("/")
        return parts[0], parts[1]
    elif "-" in clean:
        parts = clean.split("-")
        return parts[0], parts[1]
    elif "_" in clean:
        parts = clean.split("_")
        return parts[0], parts[1]
    
    # Common quote currencies
    for quote in ("USDT", "USDC", "USD", "BUSD", "EUR"):
        if clean.endswith(quote) and len(clean) > len(quote):
            return clean[:-len(quote)], quote
            
    return clean, "USDT"


@functools.lru_cache(maxsize=2048)
def normalize_symbol(symbol: str) -> str:
    """
    Normalizes any input symbol format into standard unified compact uppercase format (e.g. 'BTCUSDT').
    Cached with LRU cache.
    """
    base, quote = extract_base_quote(symbol)
    return f"{base}{quote}"


@functools.lru_cache(maxsize=2048)
def format_for_exchange(symbol: str, exchange: str) -> str:
    """
    Convert a unified symbol into the specific wire format expected by the target exchange.
    """
    base, quote = extract_base_quote(symbol)
    ex = exchange.lower()
    
    if ex in ("binance", "binanceusdm"):
        return f"{base}{quote}"
    elif ex in ("bybit", "bybitlinear"):
        return f"{base}{quote}"
    elif ex in ("okx", "okxswap"):
        return f"{base}-{quote}"
    return f"{base}{quote}"
