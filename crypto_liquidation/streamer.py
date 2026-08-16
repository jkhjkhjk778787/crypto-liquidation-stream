"""
Unified multi-exchange LiquidationStream orchestrator.

Supports async iterator (`async for`), micro-batching (`stream_batches`),
callback handlers, and context managers.
"""

import asyncio
import inspect
import logging
import time
from typing import List, Optional, Union, Callable, Dict, Set, AsyncIterator, Any
from .models import LiquidationEvent, ExchangeType
from .exchanges import (
    BaseLiquidationWorker,
    BinanceLiquidationWorker,
    BybitLiquidationWorker,
    OKXLiquidationWorker,
)
from .utils import normalize_symbol

logger = logging.getLogger("crypto_liquidation")


class LiquidationStream:
    """
    Unified high-level orchestrator for streaming real-time liquidation data
    across Binance, Bybit, and OKX.

    Usage Example (Single-event Async Iterator):
    --------------------------------------------
    ```python
    from crypto_liquidation import LiquidationStream

    async def main():
        async with LiquidationStream(symbols=["BTCUSDT"], min_notional_usd=100.0) as stream:
            async for liq in stream:
                print(f"[{liq.exchange}] {liq.symbol} {liq.side.value.upper()}: ${liq.notional_usd:,.2f}")
    ```

    Usage Example (High-Throughput Micro-Batching):
    ----------------------------------------------
    ```python
    async def main():
        stream = LiquidationStream()
        await stream.start()
        # Collects up to 100 events or flushes every 20ms
        async for batch in stream.stream_batches(max_batch_size=100, max_interval_ms=20):
            # Batch write to DB (Redis, ClickHouse, Postgres)
            print(f"Flushing {len(batch)} liquidation records to database")
    ```
    """

    EXCHANGE_WORKERS = {
        "binance": BinanceLiquidationWorker,
        "binanceusdm": BinanceLiquidationWorker,
        "bybit": BybitLiquidationWorker,
        "bybitlinear": BybitLiquidationWorker,
        "okx": OKXLiquidationWorker,
        "okxswap": OKXLiquidationWorker,
    }

    def __init__(
        self,
        exchanges: Optional[List[Union[str, ExchangeType]]] = None,
        symbols: Optional[List[str]] = None,
        min_notional_usd: float = 0.0,
        include_raw: bool = False,
        queue_maxsize: int = 50000,
    ):
        """
        Initialize the multi-exchange liquidation stream.

        Args:
            exchanges: List of exchange names (default: ["binance", "bybit", "okx"]).
            symbols: Optional list of symbols to filter (e.g. ["BTCUSDT", "ETH/USDT"]).
                     If None or empty, streams all available liquidations.
            min_notional_usd: Minimum USD notional size filter (default: 0.0, all events).
            include_raw: Whether to attach original raw exchange payload to events (default: False).
            queue_maxsize: Maximum size of the internal async queue buffer.
        """
        self.exchanges = [str(e).lower() for e in (exchanges or ["binance", "bybit", "okx"])]
        self.raw_symbols = symbols or []
        self.symbols = [normalize_symbol(s) for s in self.raw_symbols]
        self.min_notional_usd = float(min_notional_usd)
        self.include_raw = include_raw

        self._queue: asyncio.Queue[LiquidationEvent] = asyncio.Queue(maxsize=queue_maxsize)
        self._workers: List[BaseLiquidationWorker] = []
        self._callbacks: List[Callable[[LiquidationEvent], Any]] = []
        self._is_running = False
        self._callback_task: Optional[asyncio.Task] = None

    def add_callback(self, callback: Callable[[LiquidationEvent], Any]) -> None:
        """Register a sync or async callback to be executed for each liquidation event."""
        self._callbacks.append(callback)

    async def start(self) -> "LiquidationStream":
        """Start all exchange worker connections."""
        if self._is_running:
            return self

        self._is_running = True
        self._workers = []

        seen_workers: Set[Any] = set()
        for ex in self.exchanges:
            worker_cls = self.EXCHANGE_WORKERS.get(ex)
            if not worker_cls:
                logger.warning(f"Unsupported exchange requested: '{ex}'. Skipping.")
                continue

            if worker_cls in seen_workers:
                continue
            seen_workers.add(worker_cls)

            worker = worker_cls(
                symbols=self.symbols,
                queue=self._queue,
                include_raw=self.include_raw,
            )
            self._workers.append(worker)
            await worker.start()

        if self._callbacks:
            self._callback_task = asyncio.create_task(self._process_callbacks())

        logger.info(f"LiquidationStream started for {len(self._workers)} exchanges.")
        return self

    async def stop(self) -> None:
        """Stop all workers and teardown background tasks."""
        self._is_running = False

        # Stop workers
        for worker in self._workers:
            await worker.stop()
        self._workers.clear()

        # Stop callback task
        if self._callback_task and not self._callback_task.done():
            self._callback_task.cancel()
            try:
                await self._callback_task
            except asyncio.CancelledError:
                pass

        logger.info("LiquidationStream stopped.")

    async def _process_callbacks(self) -> None:
        """Internal worker executing registered callbacks."""
        while self._is_running:
            try:
                event = await self._queue.get()
                if event.notional_usd < self.min_notional_usd:
                    self._queue.task_done()
                    continue

                for cb in self._callbacks:
                    try:
                        if inspect.iscoroutinefunction(cb):
                            await cb(event)
                        else:
                            cb(event)
                    except Exception as e:
                        logger.error(f"Error in liquidation callback: {e}", exc_info=True)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Callback processor loop error: {e}")

    async def get(self) -> LiquidationEvent:
        """
        Fetch the next single LiquidationEvent from the stream.
        Automatically starts stream if not already started.
        """
        if not self._is_running:
            await self.start()

        while True:
            event = await self._queue.get()
            self._queue.task_done()
            if event.notional_usd >= self.min_notional_usd:
                return event

    async def stream_batches(
        self,
        max_batch_size: int = 100,
        max_interval_ms: int = 20,
    ) -> AsyncIterator[List[LiquidationEvent]]:
        """
        High-throughput micro-batching async generator.
        Yields batches of LiquidationEvents whenever `max_batch_size` items are reached
        OR `max_interval_ms` milliseconds have elapsed.

        Ideal for database bulk inserts (ClickHouse, Redis pipeline, Postgres) and ML ingestion.
        """
        if not self._is_running:
            await self.start()

        interval_sec = max_interval_ms / 1000.0
        current_batch: List[LiquidationEvent] = []
        last_flush_time = time.monotonic()

        while self._is_running:
            try:
                # Wait for next item with timeout based on remaining window
                remaining = max(0.001, interval_sec - (time.monotonic() - last_flush_time))
                try:
                    event = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    self._queue.task_done()
                    if event.notional_usd >= self.min_notional_usd:
                        current_batch.append(event)
                except asyncio.TimeoutError:
                    pass

                # Check flush condition
                now = time.monotonic()
                if (now - last_flush_time >= interval_sec) or (len(current_batch) >= max_batch_size):
                    if current_batch:
                        yield current_batch
                        current_batch = []
                    last_flush_time = now

            except asyncio.CancelledError:
                if current_batch:
                    yield current_batch
                break

    def __aiter__(self) -> AsyncIterator[LiquidationEvent]:
        return self

    async def __anext__(self) -> LiquidationEvent:
        if not self._is_running:
            await self.start()
        try:
            return await self.get()
        except asyncio.CancelledError:
            raise StopAsyncIteration

    async def __aenter__(self) -> "LiquidationStream":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
