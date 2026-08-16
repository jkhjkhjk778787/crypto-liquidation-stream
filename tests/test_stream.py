"""
Automated test suite for crypto_liquidation library.
"""

import asyncio
from crypto_liquidation import (
    LiquidationStream,
    LiquidationEvent,
    normalize_symbol,
    OrderSide,
    PositionSide,
)
from crypto_liquidation.utils import fast_json_loads


def test_symbol_normalization():
    assert normalize_symbol("BTC/USDT") == "BTCUSDT"
    assert normalize_symbol("BTC-USDT-SWAP") == "BTCUSDT"
    assert normalize_symbol("eth/usdt:usdt") == "ETHUSDT"
    assert normalize_symbol("SOL_USDT") == "SOLUSDT"


def test_fast_json_loads():
    data = '{"symbol": "BTCUSDT", "price": 95000.5, "amount": 10}'
    parsed = fast_json_loads(data)
    assert parsed["symbol"] == "BTCUSDT"
    assert parsed["price"] == 95000.5


def test_liquidation_event_model():
    event = LiquidationEvent(
        exchange="binance",
        symbol="BTCUSDT",
        timestamp=1786838342000,
        side=OrderSide.SELL,
        pos_side=PositionSide.LONG,
        price=95000.0,
        amount=1.5,
        notional_usd=142500.0,
        raw=None,
    )
    assert event.is_long_liquidation is True
    assert event.is_short_liquidation is False
    assert event.notional_usd == 142500.0
    
    d = event.to_dict()
    assert d["exchange"] == "binance"
    assert d["side"] == "sell"
    assert d["pos_side"] == "long"
    assert d["raw"] is None


def test_live_stream_smoke():
    """Verify that LiquidationStream starts and connects to Binance, Bybit, and OKX without errors."""
    async def _async_test():
        stream = LiquidationStream(
            exchanges=["binance", "bybit", "okx"],
            symbols=["BTCUSDT"],
            include_raw=False,
        )
        
        await stream.start()
        assert len(stream._workers) == 3
        assert stream._is_running is True
        
        await asyncio.sleep(2)
        await stream.stop()
        assert stream._is_running is False

    asyncio.run(_async_test())


def test_stream_batches_smoke():
    """Verify that stream_batches generator initializes and tears down cleanly."""
    async def _async_batch_test():
        stream = LiquidationStream(
            exchanges=["binance"],
            symbols=["BTCUSDT"],
        )
        await stream.start()
        
        # Test iteration for 1 second
        async def _consume():
            async for batch in stream.stream_batches(max_batch_size=10, max_interval_ms=50):
                assert isinstance(batch, list)

        consumer = asyncio.create_task(_consume())
        await asyncio.sleep(1)
        consumer.cancel()
        await stream.stop()

    asyncio.run(_async_batch_test())
