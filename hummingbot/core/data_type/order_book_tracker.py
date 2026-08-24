import asyncio
import logging
import time
from collections import defaultdict, deque
from enum import Enum
from typing import Deque, Dict, List, Optional, Tuple

import pandas as pd

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.event.events import OrderBookTradeEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger


class OrderBookTrackerDataSourceType(Enum):
    REMOTE_API = 2
    EXCHANGE_API = 3


class OrderBookTracker:
    PAST_DIFF_WINDOW_SIZE: int = 32
    _obt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._obt_logger is None:
            cls._obt_logger = logging.getLogger(__name__)
        return cls._obt_logger

    def __init__(self, data_source: OrderBookTrackerDataSource, trading_pairs: List[str], domain: Optional[str] = None):
        self._domain: Optional[str] = domain
        self._data_source: OrderBookTrackerDataSource = data_source
        self._trading_pairs: List[str] = trading_pairs
        self._order_books_initialized: asyncio.Event = asyncio.Event()
        self._tracking_tasks: Dict[str, asyncio.Task] = {}
        self._order_books: Dict[str, OrderBook] = {}
        self._tracking_message_queues: Dict[str, asyncio.Queue] = {}
        self._past_diffs_windows: Dict[str, Deque] = defaultdict(lambda: deque(maxlen=self.PAST_DIFF_WINDOW_SIZE))
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._saved_message_queues: Dict[str, Deque[OrderBookMessage]] = defaultdict(lambda: deque(maxlen=1000))

        self._emit_trade_event_task: Optional[asyncio.Task] = None
        self._init_order_books_task: Optional[asyncio.Task] = None
        self._order_book_diff_listener_task: Optional[asyncio.Task] = None
        self._order_book_trade_listener_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_listener_task: Optional[asyncio.Task] = None
        self._order_book_diff_router_task: Optional[asyncio.Task] = None
        self._order_book_snapshot_router_task: Optional[asyncio.Task] = None
        self._update_last_trade_prices_task: Optional[asyncio.Task] = None
        self._order_book_stream_listener_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        return self._data_source

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_books

    @property
    def ready(self) -> bool:
        return self._order_books_initialized.is_set()

    @property
    def snapshot(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        return {
            trading_pair: order_book.snapshot
            for trading_pair, order_book in self._order_books.items()
        }

    def start(self):
        self.stop()
        self._init_order_books_task = safe_ensure_future(
            self._init_order_books()
        )
        self._emit_trade_event_task = safe_ensure_future(
            self._emit_trade_event_loop()
        )
        self._order_book_diff_listener_task = safe_ensure_future(
            self._data_source.listen_for_order_book_diffs(self._ev_loop, self._order_book_diff_stream)
        )
        self._order_book_trade_listener_task = safe_ensure_future(
            self._data_source.listen_for_trades(self._ev_loop, self._order_book_trade_stream)
        )
        self._order_book_snapshot_listener_task = safe_ensure_future(
            self._data_source.listen_for_order_book_snapshots(self._ev_loop, self._order_book_snapshot_stream)
        )
        self._order_book_stream_listener_task = safe_ensure_future(
            self._data_source.listen_for_subscriptions()
        )
        self._order_book_diff_router_task = safe_ensure_future(
            self._order_book_diff_router()
        )
        self._order_book_snapshot_router_task = safe_ensure_future(
            self._order_book_snapshot_router()
        )
        self._update_last_trade_prices_task = safe_ensure_future(
            self._update_last_trade_prices_loop()
        )

    def stop(self):
        if self._init_order_books_task is not None:
            self._init_order_books_task.cancel()
            self._init_order_books_task = None
        if self._emit_trade_event_task is not None:
            self._emit_trade_event_task.cancel()
            self._emit_trade_event_task = None
        if self._order_book_diff_listener_task is not None:
            self._order_book_diff_listener_task.cancel()
            self._order_book_diff_listener_task = None
        if self._order_book_snapshot_listener_task is not None:
            self._order_book_snapshot_listener_task.cancel()
            self._order_book_snapshot_listener_task = None
        if self._order_book_trade_listener_task is not None:
            self._order_book_trade_listener_task.cancel()
            self._order_book_trade_listener_task = None

        if self._order_book_diff_router_task is not None:
            self._order_book_diff_router_task.cancel()
            self._order_book_diff_router_task = None
        if self._order_book_snapshot_router_task is not None:
            self._order_book_snapshot_router_task.cancel()
            self._order_book_snapshot_router_task = None
        if self._update_last_trade_prices_task is not None:
            self._update_last_trade_prices_task.cancel()
            self._update_last_trade_prices_task = None
        if self._order_book_stream_listener_task is not None:
            self._order_book_stream_listener_task.cancel()
        if len(self._tracking_tasks) > 0:
            for _, task in self._tracking_tasks.items():
                task.cancel()
            self._tracking_tasks.clear()
        self._order_books_initialized.clear()

    async def wait_ready(self):
        await self._order_books_initialized.wait()

    async def _update_last_trade_prices_loop(self):
        '''
        Updates last trade price for all order books through REST API, it is to initiate last_trade_price and as
        fall-back mechanism for when the web socket update channel fails.

        Monitors both trade events AND order book diff updates. If BOTH streams have not received updates
        in 3 minutes, triggers REST fallback. This prevents false positives on low-volume trading pairs
        where order book updates are streaming but no trades occur.
        '''
        await self._order_books_initialized.wait()
        while True:
            try:
                # Check if BOTH trade stream AND orderbook diff stream are stale (no updates in 3 minutes)
                # This avoids false positives on low-volume pairs where order books update but trades are rare
                current_time = time.perf_counter()
                three_minutes_ago = current_time - (60. * 3)
                five_seconds_ago = current_time - 5

                outdateds = [
                    t_pair for t_pair, o_book in self._order_books.items()
                    if (o_book.last_applied_trade < three_minutes_ago and
                        o_book.last_applied_diff < three_minutes_ago and
                        o_book.last_trade_price_rest_updated < five_seconds_ago)
                ]

                if outdateds:
                    args = {"trading_pairs": outdateds}
                    if self._domain is not None:
                        args["domain"] = self._domain
                    last_prices = await self._data_source.get_last_traded_prices(**args)
                    for trading_pair, last_price in last_prices.items():
                        self._order_books[trading_pair].last_trade_price = last_price
                        self._order_books[trading_pair].last_trade_price_rest_updated = time.perf_counter()
                else:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching last trade price.", exc_info=True)
                await asyncio.sleep(30)

    async def _initial_order_book_for_trading_pair(self, trading_pair: str) -> OrderBook:
        return await self._data_source.get_new_order_book(trading_pair)

    async def _init_order_books(self):
        """
        Initialize order books with timeout and error recovery.
        
        Note: Even if initial snapshot fetch fails/times out, we still set the ready flag
        because WebSocket snapshots will arrive soon and populate the orderbooks.
        """
        failed_pairs = []
        for index, trading_pair in enumerate(self._trading_pairs):
            try:
                # Add timeout to prevent hanging on slow/failed snapshot requests
                self._order_books[trading_pair] = await asyncio.wait_for(
                    self._initial_order_book_for_trading_pair(trading_pair),
                    timeout=30.0  # 30 second timeout per trading pair
                )
                self._tracking_message_queues[trading_pair] = asyncio.Queue()
                self._tracking_tasks[trading_pair] = safe_ensure_future(self._track_single_book(trading_pair))
                self.logger().info(f"Initialized order book for {trading_pair}. "
                                   f"{index + 1}/{len(self._trading_pairs)} completed.")
            except asyncio.TimeoutError:
                # Snapshot fetch timed out - create empty orderbook, WebSocket will populate it
                self.logger().warning(
                    f"Timeout initializing order book for {trading_pair}. "
                    f"Creating empty orderbook, will be populated by WebSocket."
                )
                self._order_books[trading_pair] = self.order_book_create_function()
                self._tracking_message_queues[trading_pair] = asyncio.Queue()
                self._tracking_tasks[trading_pair] = safe_ensure_future(self._track_single_book(trading_pair))
                failed_pairs.append(trading_pair)
            except Exception as e:
                # Any other error - create empty orderbook
                self.logger().error(
                    f"Error initializing order book for {trading_pair}: {e}. "
                    f"Creating empty orderbook, will be populated by WebSocket.",
                    exc_info=True
                )
                self._order_books[trading_pair] = self.order_book_create_function()
                self._tracking_message_queues[trading_pair] = asyncio.Queue()
                self._tracking_tasks[trading_pair] = safe_ensure_future(self._track_single_book(trading_pair))
                failed_pairs.append(trading_pair)
            
            await self._sleep(delay=1)
        
        # CRITICAL: Always set the flag, even if some snapshots failed
        # WebSocket will provide snapshots soon, so connector can still operate
        self._order_books_initialized.set()
        
        if failed_pairs:
            self.logger().info(
                f"Order book tracker ready. {len(failed_pairs)}/{len(self._trading_pairs)} pairs "
                f"initialized with empty orderbooks (will be populated by WebSocket): {failed_pairs}"
            )
        else:
            self.logger().info(
                f"Order book tracker ready. All {len(self._trading_pairs)} pairs initialized successfully."
            )

    async def _order_book_diff_router(self):
        """
        Routes the real-time order book diff messages to the correct order book.
        """
        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0

        while True:
            try:
                ob_message: OrderBookMessage = await self._order_book_diff_stream.get()
                trading_pair: str = ob_message.trading_pair

                if trading_pair not in self._tracking_message_queues:
                    messages_queued += 1
                    # Save diff messages received before snapshots are ready
                    self._saved_message_queues[trading_pair].append(ob_message)
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                # Check the order book's initial update ID. If it's larger, don't bother.
                order_book: OrderBook = self._order_books[trading_pair]

                # OPTIMISTIC: Route message even if sequence seems stale
                # Hard rejection caused orderbook freezes when sequences got misaligned
                # Trust the exchange WS stream and apply updates optimistically
                if order_book.snapshot_uid > ob_message.update_id:
                    messages_rejected += 1
                    # Still route the message - let OrderBook handle it
                    # The exchange stream is authoritative
                await message_queue.put(ob_message)
                messages_accepted += 1

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(f"Diff messages processed: {messages_accepted}, "
                                        f"rejected: {messages_rejected}, queued: {messages_queued}")
                    messages_accepted = 0
                    messages_rejected = 0
                    messages_queued = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    async def _order_book_snapshot_router(self):
        """
        Route the real-time order book snapshot messages to the correct order book.
        """
        await self._order_books_initialized.wait()
        while True:
            try:
                ob_message: OrderBookMessage = await self._order_book_snapshot_stream.get()
                trading_pair: str = ob_message.trading_pair
                if trading_pair not in self._tracking_message_queues:
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
                await message_queue.put(ob_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 5 seconds.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _track_single_book(self, trading_pair: str):
        """
        Process orderbook messages for a single trading pair.

        IMPORTANT: ALL diff messages must be processed in order - skipping diffs
        would corrupt the orderbook since diffs are incremental (not idempotent).

        Handles three message types:
        - DIFF:     incremental update applied directly to the live order book
        - SNAPSHOT: full book reset via restore_from_snapshot_and_diffs, which
                    replays only post-snapshot DIFFs from past_diffs_window.
                    past_diffs_window is cleared after each snapshot.
        """
        past_diffs_window = self._past_diffs_windows[trading_pair]
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]

        # Resolve raw-method capability once per task (avoids hasattr on every message)
        use_raw = hasattr(order_book, 'apply_diffs_raw')

        last_message_timestamp: float = time.time()
        last_queue_warning_ts: float = 0.0
        diff_messages_accepted: int = 0

        def _apply_diff(msg: OrderBookMessage) -> None:
            raw_bids = msg.content.get("bids")
            raw_asks = msg.content.get("asks")
            if use_raw and raw_bids is not None and raw_asks is not None:
                order_book.apply_diffs_raw(raw_bids, raw_asks, msg.update_id)
            else:
                order_book.apply_diffs(msg.bids, msg.asks, msg.update_id)

        def _apply_snapshot(msg: OrderBookMessage) -> None:
            order_book.restore_from_snapshot_and_diffs(msg, list(past_diffs_window))
            past_diffs_window.clear()  # stale pre-snapshot DIFFs already absorbed

        while True:
            try:
                saved_messages: Deque[OrderBookMessage] = self._saved_message_queues[trading_pair]

                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                # Process the message
                if message.type is OrderBookMessageType.DIFF:
                    _apply_diff(message)
                    past_diffs_window.append(message)
                    diff_messages_accepted += 1

                    # Batch drain: process remaining queue without yielding to event loop
                    while not message_queue.empty():
                        try:
                            batch_msg = message_queue.get_nowait()
                            if batch_msg.type is OrderBookMessageType.DIFF:
                                _apply_diff(batch_msg)
                                past_diffs_window.append(batch_msg)
                                diff_messages_accepted += 1
                            elif batch_msg.type is OrderBookMessageType.SNAPSHOT:
                                _apply_snapshot(batch_msg)
                                break
                        except asyncio.QueueEmpty:
                            break

                    # Periodic logging (once per minute)
                    now = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        # Check queue depth only during periodic logging (not every message)
                        queue_size = message_queue.qsize()
                        if queue_size > 5 and now - last_queue_warning_ts > 60.0:
                            self.logger().warning(
                                f"Orderbook queue for {trading_pair} backed up: {queue_size} pending"
                            )
                            last_queue_warning_ts = now
                        self.logger().debug(f"Processed {diff_messages_accepted} order book diffs for {trading_pair}.")
                        diff_messages_accepted = 0
                    last_message_timestamp = now

                elif message.type is OrderBookMessageType.SNAPSHOT:
                    _apply_snapshot(message)
                    # Drain any messages queued behind this snapshot without yielding to
                    # the event loop — applies subsequent DIFFs immediately
                    while not message_queue.empty():
                        try:
                            batch_msg = message_queue.get_nowait()
                            if batch_msg.type is OrderBookMessageType.DIFF:
                                _apply_diff(batch_msg)
                                past_diffs_window.append(batch_msg)
                                diff_messages_accepted += 1
                            elif batch_msg.type is OrderBookMessageType.SNAPSHOT:
                                # Back-to-back snapshots: newer one wins
                                _apply_snapshot(batch_msg)
                                break
                        except asyncio.QueueEmpty:
                            break

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error tracking order book for {trading_pair}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error tracking order book. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)


    async def _emit_trade_event_loop(self):
        last_message_timestamp: float = time.time()
        messages_accepted: int = 0
        messages_rejected: int = 0
        await self._order_books_initialized.wait()
        while True:
            try:
                trade_message: OrderBookMessage = await self._order_book_trade_stream.get()
                trading_pair: str = trade_message.trading_pair

                if trading_pair not in self._order_books:
                    messages_rejected += 1
                    continue

                order_book: OrderBook = self._order_books[trading_pair]
                order_book.apply_trade(OrderBookTradeEvent(
                    trading_pair=trade_message.trading_pair,
                    timestamp=trade_message.timestamp,
                    price=float(trade_message.content["price"]),
                    amount=float(trade_message.content["amount"]),
                    trade_id=trade_message.trade_id,
                    type=TradeType.SELL if
                    trade_message.content["trade_type"] == float(TradeType.SELL.value) else TradeType.BUY
                ))

                messages_accepted += 1

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug(f"Trade messages processed: {messages_accepted}, rejected: {messages_rejected}")
                    messages_accepted = 0
                    messages_rejected = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    @staticmethod
    async def _sleep(delay: float):
        await asyncio.sleep(delay=delay)

    # === Dynamic Trading Pair Management ===

    @staticmethod
    def _diffs_not_in_snapshot(parked: List[OrderBookMessage],
                               snapshot_msg: OrderBookMessage) -> List[OrderBookMessage]:
        """
        Of the diffs buffered while a snapshot was in flight, keep the ones the snapshot does
        not already contain.

        On the connectors this matters most for, the diff and snapshot update_ids come from the
        same exchange sequence (Binance `lastUpdateId`/`u`, KuCoin `sequence`/`sequenceEnd`,
        MEXC `lastUpdateId`/`r`), so the comparison is exact. Where a connector numbers the two
        differently the result degrades safely in *both* directions: every diff looks newer and
        they are all replayed (which is what `_init_order_books` and the hourly
        `FULL_ORDER_BOOK_RESET_DELTA_SECONDS` refresh already do), or none does and the book is
        the fresh snapshot alone — at most one REST round-trip stale, and corrected by the next
        live diff. A diff whose update_id is missing/non-numeric is always replayed.

        This filtering deliberately is *not* delegated to
        `OrderBook.restore_from_snapshot_and_diffs`: its `bisect_right` relies on
        `OrderBookMessage.__lt__`, whose final clause is `or self.has_update_id`, so
        `snapshot < diff` evaluates True for every diff and the replay position is always 0.
        Pre-filtering here is correct whether or not that comparison is ever changed.
        """
        snapshot_uid = snapshot_msg.update_id
        if not isinstance(snapshot_uid, int) or snapshot_uid <= 0:
            return list(parked)
        newer: List[OrderBookMessage] = []
        for message in parked:
            update_id = message.update_id
            if not isinstance(update_id, int) or update_id > snapshot_uid:
                newer.append(message)
        return newer

    async def add_trading_pair(self, trading_pair: str) -> bool:
        """
        Dynamically add a new trading pair to the order book tracker.

        Order of operations — **subscribe first, snapshot second**:

        1. register the pair with the data source
        2. subscribe to the websocket
        3. fetch the REST snapshot
        4. build the book from the snapshot, replaying the diffs that arrived under it

        Steps 2 and 3 used to run the other way round, which left the pair unsubscribed while
        its snapshot was in flight. Every update inside that window was lost outright, and on a
        connector whose feed is purely incremental and which never routes a WS snapshot
        (Binance and KuCoin — the others either subscribe incrementally or push snapshots) a
        level deleted in the window survives in the local book until the hourly
        `FULL_ORDER_BOOK_RESET_DELTA_SECONDS` REST refresh. Measured window on live Binance
        runtime adds: 88-402 ms.

        Subscribing first closes it: the pair has no entry in `_tracking_message_queues` yet, so
        `_order_book_diff_router` parks its diffs in `_saved_message_queues` (a bounded deque)
        instead of dropping them, and step 4 replays the ones the snapshot does not already
        cover. This is the same protection the startup path gets for free, because
        `listen_for_subscriptions()` is already running when `_init_order_books()` fetches its
        snapshots.

        :param trading_pair: The trading pair to add (e.g. "BTC-USDT")
        :return: True if successfully added, False otherwise
        """
        if trading_pair in self._order_books:
            self.logger().warning(f"Trading pair {trading_pair} already tracked.")
            return False

        if trading_pair in self._trading_pairs:
            self.logger().warning(
                f"Trading pair {trading_pair} in list but not initialized. "
                f"Initializing now."
            )
        else:
            self._trading_pairs.append(trading_pair)

        try:
            # Add to data source's trading pairs
            self._data_source.add_trading_pair(trading_pair)

            # --- 1. Subscribe FIRST, so the snapshot window is buffered, not lost -----------
            subscribed = await self._data_source.subscribe_to_trading_pair(trading_pair)
            if subscribed:
                self.logger().info(
                    f"Successfully added and subscribed to trading pair: {trading_pair}"
                )
            else:
                self.logger().info(
                    f"Added trading pair {trading_pair}. "
                    f"Will subscribe on next websocket reconnection."
                )

            # --- 2. Then the snapshot (diffs are accumulating in _saved_message_queues) -----
            snapshot_msg: Optional[OrderBookMessage] = None
            try:
                snapshot_msg = await asyncio.wait_for(
                    self._data_source.get_order_book_snapshot_message(trading_pair),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                self.logger().warning(
                    f"Timeout fetching order book snapshot for {trading_pair}. "
                    f"Starting from the buffered websocket diffs instead."
                )
            except Exception as e:
                self.logger().warning(
                    f"Error fetching order book snapshot for {trading_pair}: {e}. "
                    f"Starting from the buffered websocket diffs instead."
                )

            # --- 3. Build the book and hand it to the tracking task ------------------------
            # No `await` from here to the end of the block: the diff router runs as a separate
            # task and without an await point it cannot append to the deque while it is being
            # drained, so nothing can slip between the drain and _track_single_book taking over.
            order_book: OrderBook = self.order_book_create_function()
            if snapshot_msg is not None:
                parked: List[OrderBookMessage] = list(self._saved_message_queues[trading_pair])
                self._saved_message_queues[trading_pair].clear()
                replay = self._diffs_not_in_snapshot(parked, snapshot_msg)
                order_book.restore_from_snapshot_and_diffs(snapshot_msg, replay)
                self.logger().info(
                    f"Initialized order book for {trading_pair} "
                    f"(snapshot uid {snapshot_msg.update_id}, replayed {len(replay)} of "
                    f"{len(parked)} buffered diff(s))"
                )
            else:
                # No snapshot: leave the buffered diffs where they are — _track_single_book
                # drains _saved_message_queues before its own queue, so the book is populated
                # from the websocket exactly as it was before this change.
                self.logger().warning(
                    f"Created empty order book for {trading_pair}; it will be populated by "
                    f"the websocket stream."
                )

            self._order_books[trading_pair] = order_book
            self._tracking_message_queues[trading_pair] = asyncio.Queue()
            self._tracking_tasks[trading_pair] = safe_ensure_future(
                self._track_single_book(trading_pair)
            )

            return True

        except Exception as e:
            self.logger().error(
                f"Failed to add trading pair {trading_pair}: {e}",
                exc_info=True
            )
            # Clean up partial state
            self._order_books.pop(trading_pair, None)
            self._tracking_message_queues.pop(trading_pair, None)
            task = self._tracking_tasks.pop(trading_pair, None)
            if task:
                task.cancel()
            return False

    def has_order_book(self, trading_pair: str) -> bool:
        """Check if an order book exists for the given trading pair."""
        return trading_pair in self._order_books

    @property
    def order_book_create_function(self):
        """Get the order book creation function from the data source."""
        return self._data_source.order_book_create_function