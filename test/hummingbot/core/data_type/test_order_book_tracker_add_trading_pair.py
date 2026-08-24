"""
Regression tests for OrderBookTracker.add_trading_pair (the runtime `control create` /
`control add_market` path).

The defect these cover: the snapshot used to be fetched BEFORE the websocket subscription
existed, so every update inside that window was lost outright. On a connector with a purely
incremental feed that never routes a WS snapshot (Binance, KuCoin) a level deleted inside the
window then survives in the local book until the hourly FULL_ORDER_BOOK_RESET_DELTA_SECONDS
REST refresh.

`test_level_deleted_during_snapshot_window_is_not_lost` is the one that matters: it fails
against the old ordering and passes against the new one. `test_old_ordering_loses_the_update`
pins that the scenario really does break under the old ordering, so the first test cannot
quietly stop proving anything.
"""
import asyncio
import unittest
from typing import Dict, List, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource

TRADING_PAIR = "BB-USDT"


class FakeExchange:
    """Authoritative book. Every mutation bumps the sequence and is pushed to subscribers only."""

    def __init__(self):
        self.bids: Dict[float, float] = {0.00982: 50000.0, 0.00981: 20000.0}
        self.asks: Dict[float, float] = {0.00984: 30000.0}
        self.uid = 100
        self.subscribers: List[asyncio.Queue] = []

    def push(self, bids: List[List[float]], asks: List[List[float]]):
        self.uid += 1
        for price, amount in bids:
            self.bids.pop(price, None) if amount == 0 else self.bids.__setitem__(price, amount)
        for price, amount in asks:
            self.asks.pop(price, None) if amount == 0 else self.asks.__setitem__(price, amount)
        message = OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": TRADING_PAIR,
            "update_id": self.uid,
            "bids": [[str(p), str(a)] for p, a in bids],
            "asks": [[str(p), str(a)] for p, a in asks],
        }, timestamp=float(self.uid))
        # A subscriber only receives what is published while it is subscribed — same as a WS.
        for queue in self.subscribers:
            queue.put_nowait(message)

    def snapshot_message(self) -> OrderBookMessage:
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": TRADING_PAIR,
            "update_id": self.uid,
            "bids": [[str(p), str(a)] for p, a in sorted(self.bids.items(), reverse=True)],
            "asks": [[str(p), str(a)] for p, a in sorted(self.asks.items())],
        }, timestamp=float(self.uid))

    @property
    def best_bid(self) -> float:
        return max(self.bids)


class FakeDataSource(OrderBookTrackerDataSource):
    """Minimal data source: the only realistic parts are when the WS starts delivering and
    that the snapshot request takes a round trip during which the exchange keeps moving."""

    def __init__(self, exchange: FakeExchange, during_snapshot=None):
        super().__init__([])
        self._exchange = exchange
        self._during_snapshot = during_snapshot
        self._ws_queue: Optional[asyncio.Queue] = None
        self.subscribed = False

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        self._ws_queue = asyncio.Queue()
        self._exchange.subscribers.append(self._ws_queue)
        self.subscribed = True
        return True

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = self._exchange.snapshot_message()   # the exchange builds it now...
        if self._during_snapshot is not None:
            self._during_snapshot()                    # ...and keeps trading while it is in flight
        await asyncio.sleep(0.05)                      # REST round trip
        return snapshot

    async def listen_for_order_book_diffs(self, ev_loop, output: asyncio.Queue):
        while True:
            if self._ws_queue is None:
                await asyncio.sleep(0.005)
                continue
            output.put_nowait(await self._ws_queue.get())

    async def listen_for_trades(self, ev_loop, output):
        await asyncio.Event().wait()

    async def listen_for_order_book_snapshots(self, ev_loop, output):
        await asyncio.Event().wait()

    async def listen_for_subscriptions(self):
        await asyncio.Event().wait()

    async def get_last_traded_prices(self, trading_pairs, domain=None):
        return {}


class OrderBookTrackerAddTradingPairTest(unittest.IsolatedAsyncioTestCase):

    async def _run(self, tracker: OrderBookTracker, data_source: FakeDataSource,
                   exchange: FakeExchange, old_ordering: bool) -> OrderBook:
        tracker.start()
        await tracker.wait_ready()
        try:
            if old_ordering:
                # Reproduce the pre-fix sequence: snapshot first, subscription afterwards.
                data_source.add_trading_pair(TRADING_PAIR)
                snapshot = await data_source._order_book_snapshot(TRADING_PAIR)
                await data_source.subscribe_to_trading_pair(TRADING_PAIR)
                book = tracker.order_book_create_function()
                book.apply_snapshot(snapshot.bids, snapshot.asks, snapshot.update_id)
                tracker._order_books[TRADING_PAIR] = book
                tracker._tracking_message_queues[TRADING_PAIR] = asyncio.Queue()
                tracker._tracking_tasks[TRADING_PAIR] = asyncio.ensure_future(
                    tracker._track_single_book(TRADING_PAIR))
            else:
                self.assertTrue(await tracker.add_trading_pair(TRADING_PAIR))

            # Normal trading afterwards, on a level untouched by the window.
            exchange.push(bids=[], asks=[[0.00984, 25000.0]])
            await asyncio.sleep(0.15)
            return tracker.order_books[TRADING_PAIR]
        finally:
            tracker.stop()

    async def test_level_deleted_during_snapshot_window_is_not_lost(self):
        """The top bid is swept while the snapshot is in flight — the book must follow."""
        exchange = FakeExchange()
        # The sweep happens after the exchange built the snapshot, before the response lands.
        data_source = FakeDataSource(exchange, during_snapshot=lambda: exchange.push(
            bids=[[0.00982, 0.0]], asks=[]))
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])

        book = await self._run(tracker, data_source, exchange, old_ordering=False)

        self.assertEqual(0.00981, exchange.best_bid)
        self.assertAlmostEqual(exchange.best_bid, book.get_price(False), places=8,
                               msg="the swept 0.00982 bid survived in the local book")

    async def test_old_ordering_loses_the_update(self):
        """Pins that the scenario is genuinely destructive under the pre-fix ordering."""
        exchange = FakeExchange()
        data_source = FakeDataSource(exchange, during_snapshot=lambda: exchange.push(
            bids=[[0.00982, 0.0]], asks=[]))
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])

        book = await self._run(tracker, data_source, exchange, old_ordering=True)

        self.assertEqual(0.00981, exchange.best_bid)
        self.assertAlmostEqual(0.00982, book.get_price(False), places=8,
                               msg="expected the old ordering to keep the phantom top bid")

    async def test_snapshot_and_stream_agree_when_nothing_happens_in_the_window(self):
        """The quiet case must be untouched: no diffs to replay, book == snapshot."""
        exchange = FakeExchange()
        data_source = FakeDataSource(exchange)
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])

        book = await self._run(tracker, data_source, exchange, old_ordering=False)

        self.assertAlmostEqual(exchange.best_bid, book.get_price(False), places=8)
        self.assertAlmostEqual(min(exchange.asks), book.get_price(True), places=8)

    async def test_snapshot_failure_falls_back_to_the_buffered_stream(self):
        """A failing snapshot must still leave a tracked, live book (previous behaviour)."""
        exchange = FakeExchange()
        data_source = FakeDataSource(exchange)

        async def failing_snapshot(trading_pair):
            raise IOError("snapshot endpoint down")

        data_source._order_book_snapshot = failing_snapshot
        tracker = OrderBookTracker(data_source=data_source, trading_pairs=[])

        tracker.start()
        await tracker.wait_ready()
        try:
            self.assertTrue(await tracker.add_trading_pair(TRADING_PAIR))
            self.assertTrue(data_source.subscribed)
            exchange.push(bids=[[0.00979, 12000.0]], asks=[])
            await asyncio.sleep(0.15)
            book = tracker.order_books[TRADING_PAIR]
            self.assertAlmostEqual(0.00979, book.get_price(False), places=8,
                                   msg="book was not populated from the websocket stream")
        finally:
            tracker.stop()

    def test_diffs_not_in_snapshot_filter(self):
        def diff(update_id):
            return OrderBookMessage(OrderBookMessageType.DIFF, {
                "trading_pair": TRADING_PAIR, "update_id": update_id,
                "bids": [], "asks": []}, timestamp=1.0)

        snapshot = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": TRADING_PAIR, "update_id": 100,
            "bids": [], "asks": []}, timestamp=1.0)

        parked = [diff(98), diff(100), diff(101), diff(102)]
        kept = OrderBookTracker._diffs_not_in_snapshot(parked, snapshot)
        self.assertEqual([101, 102], [m.update_id for m in kept])

        # Unusable snapshot id -> replay everything rather than silently dropping the window.
        unusable = OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": TRADING_PAIR, "update_id": 0,
            "bids": [], "asks": []}, timestamp=1.0)
        self.assertEqual(4, len(OrderBookTracker._diffs_not_in_snapshot(parked, unusable)))

        # Unusable diff id -> keep it, same reason.
        none_id = OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": TRADING_PAIR, "update_id": None,
            "bids": [], "asks": []}, timestamp=1.0)
        self.assertEqual(1, len(OrderBookTracker._diffs_not_in_snapshot([none_id], snapshot)))


if __name__ == "__main__":
    unittest.main()
