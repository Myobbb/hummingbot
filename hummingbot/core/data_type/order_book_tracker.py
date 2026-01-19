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
        """
        past_diffs_window = self._past_diffs_windows[trading_pair]
        message_queue: asyncio.Queue = self._tracking_message_queues[trading_pair]
        order_book: OrderBook = self._order_books[trading_pair]
        
        last_message_timestamp: float = time.time()
        last_queue_warning_ts: float = 0.0
        diff_messages_accepted: int = 0

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
                    # Use optimized raw method if available (bypasses OrderBookRow allocation)
                    raw_bids = message.content.get("bids")
                    raw_asks = message.content.get("asks")
                    if raw_bids is not None and raw_asks is not None and hasattr(order_book, 'apply_diffs_raw'):
                        order_book.apply_diffs_raw(raw_bids, raw_asks, message.update_id)
                    else:
                        order_book.apply_diffs(message.bids, message.asks, message.update_id)
                    past_diffs_window.append(message)
                    diff_messages_accepted += 1

                    # Batch processing: drain queue without await to reduce loop overhead
                    while not message_queue.empty():
                        try:
                            batch_msg = message_queue.get_nowait()
                            if batch_msg.type is OrderBookMessageType.DIFF:
                                raw_bids = batch_msg.content.get("bids")
                                raw_asks = batch_msg.content.get("asks")
                                if raw_bids is not None and raw_asks is not None and hasattr(order_book, 'apply_diffs_raw'):
                                    order_book.apply_diffs_raw(raw_bids, raw_asks, batch_msg.update_id)
                                else:
                                    order_book.apply_diffs(batch_msg.bids, batch_msg.asks, batch_msg.update_id)
                                past_diffs_window.append(batch_msg)
                                diff_messages_accepted += 1
                            elif batch_msg.type is OrderBookMessageType.SNAPSHOT:
                                order_book.restore_from_snapshot_and_diffs(batch_msg, list(past_diffs_window))
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
                    order_book.restore_from_snapshot_and_diffs(message, list(past_diffs_window))

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

    async def add_trading_pair(self, trading_pair: str) -> bool:
        """
        Dynamically add a new trading pair to the order book tracker.
        
        This method:
        1. Adds the pair to the data source's trading pairs list
        2. Creates an order book for the pair
        3. Sets up tracking message queue and task
        4. Subscribes to the websocket for the pair
        
        :param trading_pair: The trading pair to add (e.g., "BTC-USDT")
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

            # Initialize order book (with timeout to avoid hanging)
            try:
                self._order_books[trading_pair] = await asyncio.wait_for(
                    self._initial_order_book_for_trading_pair(trading_pair),
                    timeout=30.0
                )
                self.logger().info(f"Initialized order book for {trading_pair}")
            except asyncio.TimeoutError:
                self.logger().warning(
                    f"Timeout initializing order book for {trading_pair}. "
                    f"Creating empty orderbook, will be populated by WebSocket."
                )
                self._order_books[trading_pair] = self.order_book_create_function()
            except Exception as e:
                self.logger().warning(
                    f"Error initializing order book for {trading_pair}: {e}. "
                    f"Creating empty orderbook, will be populated by WebSocket."
                )
                self._order_books[trading_pair] = self.order_book_create_function()

            # Set up tracking queue and task
            self._tracking_message_queues[trading_pair] = asyncio.Queue()
            self._tracking_tasks[trading_pair] = safe_ensure_future(
                self._track_single_book(trading_pair)
            )

            # Subscribe to websocket for this pair
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