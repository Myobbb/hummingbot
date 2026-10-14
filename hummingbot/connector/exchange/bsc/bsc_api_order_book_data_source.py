import asyncio
import time
from typing import Any, Dict, List, Optional

from hummingbot.connector.exchange.bsc import bsc_constants as CONSTANTS
from hummingbot.connector.exchange.bsc.bsc_quoter import POLL_S
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


class BscAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """The chain's books, polled (P2-DEX step 3, 2026-10-14). listen_for_subscriptions, which the tracker runs where a socket
    would be, runs the connector's poll every POLL_S: one eth_call (bsc_quoter.BscQuoter). Each poll hands the tracker a
    SNAPSHOT per trading pair, never a diff (the runbook: never mix the two). A pair with no book gets an empty one: a market
    that is off looks empty, never dropped. No good poll for STALE_S, or a block older than that: every book goes empty, so a
    DEX price is never kept alive (P1's B7)."""

    def __init__(self, trading_pairs: List[str], connector):
        super().__init__(trading_pairs)
        self._connector = connector
        self._books: Dict[str, tuple] = {}            # trading pair -> (bids, asks), [[price, qty], ...], of the last good poll
        self._update_id = int(time.time() * 1e3)
        self._first_poll = asyncio.Event()
        self._stale = False
        self._last_summary = time.time()
        self.last_ok = 0.0
        self.last_block: Optional[int] = None
        self.last_error: Optional[str] = None
        self.polls = 0
        self.failed = 0

    def reset(self):
        """a (re)start never shows an earlier run's books: each pair's first snapshot waits for a fresh poll (HMB's network
        iterator stops and starts the connector on every reconnect)"""
        self._books, self._stale = {}, False
        self._first_poll.clear()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """the book's mid for every pair asked (NaN without a two-sided book): the tracker re-asks at once on a missing pair"""
        out = {}
        for trading_pair in trading_pairs:
            bids, asks = self._books.get(trading_pair, ([], []))
            out[trading_pair] = (bids[0][0] + asks[0][0]) / 2 if bids and asks else float("nan")
        return out

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        try:
            await asyncio.wait_for(self._first_poll.wait(), timeout=CONSTANTS.FIRST_POLL_WAIT_S)
        except asyncio.TimeoutError:
            pass
        return self._snapshot(trading_pair)

    def _snapshot(self, trading_pair: str) -> OrderBookMessage:
        self._update_id += 1
        bids, asks = self._books.get(trading_pair, ([], []))
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT,
                                {"trading_pair": trading_pair, "update_id": self._update_id, "bids": bids, "asks": asks},
                                timestamp=time.time())

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        message_queue.put_nowait(raw_message)         # already an OrderBookMessage

    async def listen_for_subscriptions(self):
        """the poll loop, in place of a socket"""
        while True:
            t0 = time.time()
            try:
                last = await self._connector._poll_books()
                age = t0 - last["ts"] if last.get("ts") else 0.0
                if age > CONSTANTS.STALE_S:
                    raise RuntimeError(f"block {last.get('block')} is {age:.1f} s old")
                self._books = self._connector._books_by_pair(last)
                self.last_ok, self.last_block = time.time(), last.get("block")
                self.polls += 1
                self._first_poll.set()
                if self._stale:
                    self._stale = False
                    self.logger().info(f"bsc: books back (block {self.last_block})")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.failed += 1
                self.last_error = self._connector._mask(f"{type(e).__name__}: {e}")[:300]
                if time.time() - self.last_ok > CONSTANTS.STALE_S and not self._stale:
                    self._stale, self._books = True, {}
                    self.logger().warning(f"bsc: no fresh poll for {CONSTANTS.STALE_S:.0f} s, every book is empty: {self.last_error}")
            queue = self._message_queue[self._snapshot_messages_queue_key]
            for trading_pair in list(self._trading_pairs):
                queue.put_nowait(self._snapshot(trading_pair))
            self._summary()
            await asyncio.sleep(max(0.0, POLL_S - (time.time() - t0)))

    def _summary(self):
        now = time.time()
        if now - self._last_summary < CONSTANTS.SUMMARY_EVERY_S:
            return
        self._last_summary = now
        two_sided = sum(1 for bids, asks in self._books.values() if bids and asks)
        q = self._connector._quoter.stats
        self.logger().debug(f"bsc: {self.polls} polls, {self.failed} failed, {two_sided}/{len(self._trading_pairs)} two-sided books, "
                            f"block {self.last_block}, RU {q.get('ru')}, elections {q.get('elections')}, switches {q.get('switches')}")

    async def _subscribe_single_trading_pair(self, ws, trading_pair: str):
        """a runtime-added pair is in the next poll (its CA's payload must be in the pool directory)"""
        return

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("bsc is polled, not streamed")

    async def _subscribe_channels(self, ws):
        raise NotImplementedError("bsc is polled, not streamed")
