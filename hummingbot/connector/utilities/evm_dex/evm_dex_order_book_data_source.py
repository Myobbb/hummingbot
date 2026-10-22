import asyncio
import time
from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


class _Idle(Exception):
    """the connector's poll had nothing to quote and made no call: not a failure"""


class _NoAnswer(RuntimeError):
    """no poll came before the books on show turned STALE_S old"""


class EvmDexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """The chain's books, polled (P2-DEX step 3, 2026-10-14; the EVM core since 2026-10-22: every constant is the connector's
    spec's, `connector.C`; each chain subclasses it, so its logger is named for the chain). listen_for_subscriptions, which the
    tracker runs where a socket would be, takes each poll the feed process makes, once a block on its own block clock (since
    2026-10-19; every POLL_S without it): one eth_call (evm_dex_quoter.EvmDexQuoter, in evm_dex_feed's child process,
    2026-10-15). Each poll hands the tracker a SNAPSHOT per trading pair, never a diff (the
    runbook: never mix the two). A pair with no book gets an empty one: a market that is off looks empty, never dropped.
    Every book goes empty once the block it was priced at is STALE_S old, however its poll ended (a failed call, an answer
    from an old block, a node too slow to answer: a poll is never waited for past that moment), so a DEX price is never kept
    alive (P1's B7). With no DEX pair to quote there is no feed process and no call (idle, Pavel 2026-10-15): nothing is on
    show, no stale clock runs, and the loop only looks again each POLL_S."""

    def __init__(self, trading_pairs: List[str], connector):
        super().__init__(trading_pairs)
        self._connector = connector
        self.C = connector.C                          # the chain's spec
        self._books: Dict[str, tuple] = {}            # trading pair -> (bids, asks), [[price, qty], ...], of the last good poll
        self._pushed: Dict[str, tuple] = {}           # trading pair -> the (bids, asks) last handed to the tracker
        self._update_id = int(time.time() * 1e3)
        self._first_poll = asyncio.Event()
        self._stale = False
        self._last_summary = time.time()
        self.last_ok = 0.0
        self.book_ts = time.time()                    # the timestamp of the block the books on show were priced at (a
                                                      # start counts as one: no good poll within STALE_S of it is an outage)
        self.last_block: Optional[int] = None
        self.last_error: Optional[str] = None
        self._erring = False                          # a poll failed since the last good one (its error is the outage's)
        self.polls = 0
        self.failed = 0

    def reset(self):
        """a (re)start never shows an earlier run's books: each pair's first snapshot waits for a fresh poll (HMB's network
        iterator stops and starts the connector on every reconnect)"""
        self._books, self._stale, self.book_ts = {}, False, time.time()
        self._pushed = {}
        self._first_poll.clear()

    def hide(self, trading_pair: str) -> int:
        """a pair's book emptied at once (its DEX order failed: evm_dex_exchange._after_failure), until a newer block's poll shows
        it -> the update id of the empty snapshot, which the connector also applies to the tracker's book directly"""
        if trading_pair in self._books:
            self._books[trading_pair] = ([], [])
            self._push()
        return self._update_id

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """the book's mid for every pair asked (NaN without a two-sided book): the tracker re-asks at once on a missing pair"""
        out = {}
        for trading_pair in trading_pairs:
            bids, asks = self._books.get(trading_pair, ([], []))
            out[trading_pair] = (bids[0][0] + asks[0][0]) / 2 if bids and asks else float("nan")
        return out

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        try:
            await asyncio.wait_for(self._first_poll.wait(), timeout=self.C.FIRST_POLL_WAIT_S)
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

    def _life(self) -> Optional[float]:
        """how long the books on show stay fresh: until the block they were priced at is STALE_S old (None: none on show)"""
        return None if self._stale else self.book_ts + self.C.STALE_S - time.time()

    def _expire(self) -> bool:
        """the books on show go empty once the block they were priced at is STALE_S old, whatever the poll is doing (audit
        2026-10-15: they went empty only STALE_S after the last good poll, ~10 s in all, and never while a poll hung).
        -> True when they just did; the empty books are handed to the tracker at once"""
        life = self._life()
        if life is None or life > 0:
            return False
        self._stale, self._books = True, {}
        self.logger().warning(f"{self.C.EXCHANGE_NAME}: no fresh poll for {self.C.STALE_S:.0f} s, every book is empty: "
                              f"{self.last_error}")
        self._push()
        return True

    def _push(self):
        """one SNAPSHOT per trading pair whose book on show changed (Pavel 2026-10-15: the DEX must not hold HMB's loop; a
        quiet pool's book is the same every poll, and the tracker's book for it is already that one). Nothing reads a
        book's snapshot time: a DEX book's age is this data source's (STALE_S, _expire)"""
        queue = self._message_queue[self._snapshot_messages_queue_key]
        for trading_pair in list(self._trading_pairs):
            book = self._books.get(trading_pair, ([], []))
            if self._pushed.get(trading_pair) != book:
                self._pushed[trading_pair] = book
                queue.put_nowait(self._snapshot(trading_pair))

    async def listen_for_subscriptions(self):
        """the poll loop, in place of a socket: each of the feed's polls is pushed the moment it comes (the feed keeps its
        own cadence, so a good poll is followed at once by the wait for the next; only an idle connector or a failure waits
        out the rest of POLL_S here). A poll is never waited for past the moment the books on show turn stale, and the loop
        never sleeps past it either; each node call has its own timeout (RPC_TIMEOUT_S)"""
        while True:
            t0 = time.time()
            self._expire()                                    # they may have turned stale while the loop slept
            wait = True
            try:
                poll, life = self._connector._poll_books(), self._life()
                if life is None:
                    last = await poll                         # no book on show: each node call's own timeout bounds it
                else:
                    try:
                        last = await asyncio.wait_for(poll, timeout=life)
                    except asyncio.TimeoutError:
                        raise _NoAnswer(f"no answer before the books turned {self.C.STALE_S:.0f} s old") from None
                if last is None:                              # idle: no DEX pair to quote, no call made (Pavel 2026-10-15):
                    self._books, self._stale, self.book_ts = {}, False, time.time()    # nothing on show, no stale clock
                    raise _Idle()
                age = t0 - last["ts"] if last.get("ts") else 0.0
                if age > self.C.STALE_S:
                    raise RuntimeError(f"block {last.get('block')} is {age:.1f} s old")
                self._books = self._connector._books_by_pair(last)
                self.last_ok, self.last_block, self.book_ts = time.time(), last.get("block"), last.get("ts") or t0
                self.polls += 1
                self._first_poll.set()
                wait, self._erring = False, False
                if self._stale:
                    self._stale = False
                    self.logger().info(f"{self.C.EXCHANGE_NAME}: books back (block {self.last_block})")
            except asyncio.CancelledError:
                raise
            except _Idle:
                pass
            except Exception as e:
                self.failed += 1
                # the feed's polls come on its own clock, so the last wait before the books expire may end first: that says
                # nothing new when a poll already failed since the last good one (its error is why the books expire)
                if not (isinstance(e, _NoAnswer) and self._erring):
                    self.last_error = self._connector._mask(f"{type(e).__name__}: {e}")[:300]
                self._erring = True
            if not self._expire():
                self._push()
            self._summary()
            if wait:
                life = self._life()
                await asyncio.sleep(max(0.0, min(self.C.POLL_S - (time.time() - t0),
                                                 life if life is not None else self.C.POLL_S)))

    def _summary(self):
        now = time.time()
        if now - self._last_summary < self.C.SUMMARY_EVERY_S:
            return
        self._last_summary = now
        two_sided = sum(1 for bids, asks in self._books.values() if bids and asks)
        q = self._connector._feed_stats
        self.logger().debug(f"{self.C.EXCHANGE_NAME}: {self.polls} polls, {self.failed} failed, {two_sided}/{len(self._trading_pairs)} "
                            f"two-sided books, "
                            f"block {self.last_block}, RU at BlockPI's rate {q.get('ru')}, elections {q.get('elections')}, switches {q.get('switches')}")

    async def _subscribe_single_trading_pair(self, ws, trading_pair: str):
        """a runtime-added pair is in the next poll (its CA's payload must be in the pool directory)"""
        return

    async def _connected_websocket_assistant(self):
        raise NotImplementedError(f"{self.C.EXCHANGE_NAME} is polled, not streamed")

    async def _subscribe_channels(self, ws):
        raise NotImplementedError(f"{self.C.EXCHANGE_NAME} is polled, not streamed")
