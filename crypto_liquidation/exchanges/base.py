"""
Abstract Base Worker for Exchange WebSocket connections with auto-reconnection and backoff.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, Set, List
from ..models import LiquidationEvent

logger = logging.getLogger("crypto_liquidation")


class BaseLiquidationWorker(ABC):
    """
    Base async worker managing WebSocket lifecycle, backoff, and event queueing.
    """

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        queue: Optional[asyncio.Queue] = None,
        include_raw: bool = False,
    ):
        self.raw_symbols = symbols or []
        self.normalized_symbols: Set[str] = {
            s.upper().replace("/", "").replace("-", "").replace(":USDT", "")
            for s in self.raw_symbols
        } if self.raw_symbols else set()
        self.queue: asyncio.Queue[LiquidationEvent] = queue or asyncio.Queue()
        self.include_raw = include_raw
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.backoff_min = 1.0
        self.backoff_max = 30.0

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Returns the name of the exchange."""
        pass

    @abstractmethod
    async def _stream_loop(self) -> None:
        """Internal loop handling connection, subscription, and message parsing."""
        pass

    async def start(self) -> None:
        """Start the worker background task."""
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._run_with_retry())

    async def stop(self) -> None:
        """Gracefully stop the worker."""
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_with_retry(self) -> None:
        """Run the streaming loop with exponential backoff reconnection."""
        backoff = self.backoff_min
        while self.is_running:
            try:
                logger.info(f"[{self.exchange_name}] Connecting WebSocket...")
                await self._stream_loop()
                backoff = self.backoff_min  # Reset backoff on clean return
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[{self.exchange_name}] WebSocket error: {e}. Reconnecting in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.backoff_max)

    def push_event_nowait(self, event: LiquidationEvent) -> None:
        """
        Fast non-blocking event push. If queue is full, drops the oldest item
        to prevent blocking high-frequency WebSocket receivers.
        """
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                pass
            try:
                self.queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def push_event(self, event: LiquidationEvent) -> None:
        """Emit an event to the shared consumer queue."""
        self.push_event_nowait(event)
