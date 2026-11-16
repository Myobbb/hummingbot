"""How a websocket stream's drops reach the log (2026-11-14).

HMB's log file keeps INFO and above. A transient drop (the stream is back within SLOW_S, nothing to do) is routine, so
it goes to DEBUG. INFO gets only the drops someone should look at:
  * a recovery that took SLOW_S or longer: how long, how many attempts failed, and why it dropped;
  * a stream that dropped RECONNECTS_PER_HOUR_INFO times within an hour (failed reconnect attempts count): one line,
    then at most one an hour while that lasts.
Both lines carry the stream's drop count since it started. A close we cause ourselves (reconnecting to add a pair, a
proactive reconnect) is not a drop: DEBUG, not counted.
"""
import logging
import time
from collections import deque
from typing import Deque, Optional

RECONNECTS_PER_HOUR_INFO = 6   # the one knob
SLOW_S = 30.0                  # "back within seconds"; a recovery this slow is reported
_HOUR = 3600.0


def _utc(ts: float) -> str:
    return time.strftime("%m-%d %H:%M", time.gmtime(ts))


class StreamReconnects:
    """One stream's drops and recoveries. The listen loop calls `closed()` when the socket drops (or a reconnect
    attempt fails) and `up()` once it is subscribed again; `level` gives the connector's own "subscribed" line INFO on
    the stream's first connect and DEBUG on every reconnect."""

    __slots__ = ("_log", "_name", "_window", "_since", "drops", "connects", "_down_at", "_failed", "_why", "_storm_at",
                 "_ours")

    def __init__(self, logger: logging.Logger, name: str):
        self._log = logger
        self._name = name
        self._window: Deque[float] = deque()
        self._since = time.time()
        self.drops = 0            # drops and failed reconnect attempts since start
        self.connects = 0         # successful subscribes since start
        self._down_at: Optional[float] = None
        self._failed = 0          # failed attempts in the current outage
        self._why = ""
        self._storm_at = float("-inf")
        self._ours: Optional[str] = None

    @property
    def level(self) -> int:
        return logging.INFO if self.connects == 0 else logging.DEBUG

    @property
    def since_utc(self) -> str:
        """When this stream started, for a mechanism line's "N since <time>" counter."""
        return _utc(self._since)

    def ours(self, why: str) -> None:
        """The next close is one we cause: it is logged at DEBUG and not counted as a drop."""
        self._ours = why

    def closed(self, why) -> None:
        now = time.time()
        if self._ours is not None:
            self._log.debug("%s closed by us (%s); reconnecting", self._name, self._ours)
            self._ours = None
            return
        self.drops += 1
        if self._down_at is None:
            self._down_at, self._failed = now, 0
        else:
            self._failed += 1
        self._why = why
        window = self._window
        window.append(now)
        while now - window[0] > _HOUR:
            window.popleft()
        if len(window) >= RECONNECTS_PER_HOUR_INFO and now - self._storm_at >= _HOUR:
            self._storm_at = now
            down = (f"; down since {_utc(self._down_at)} UTC, {self._failed} reconnect attempt(s) failed"
                    if self._failed else "")
            self._log.info("%s dropped %d times in the last hour (%d since %s UTC)%s; the last: %s",
                           self._name, len(window), self.drops, _utc(self._since), down, why)
        else:
            self._log.debug("%s closed (%s); reconnecting (drop %d since %s UTC)",
                            self._name, why, self.drops, _utc(self._since))

    def up(self, report: bool = True) -> None:
        """`report=False`: the caller logs its own line for a recovery that needed retries."""
        self.connects += 1
        down_at = self._down_at
        if down_at is None:
            return
        self._down_at = None
        took = time.time() - down_at
        if report and took >= SLOW_S:
            self._log.info("%s back after %.0f s, %d reconnect attempt(s) failed (it dropped: %s; drop %d since %s UTC)",
                           self._name, took, self._failed, self._why, self.drops, _utc(self._since))
        else:
            self._log.debug("%s back after %.1f s", self._name, took)


def stream_reconnects(owner, name: str) -> StreamReconnects:
    """The owner's StreamReconnects for `name`, made on first use (owners need no __init__ change)."""
    streams = owner.__dict__.get("_stream_reconnects_by_name")
    if streams is None:
        streams = owner.__dict__["_stream_reconnects_by_name"] = {}
    tracker = streams.get(name)
    if tracker is None:
        venue = type(owner).__module__.split(".")
        venue = venue[3] if len(venue) > 4 and venue[1] == "connector" else type(owner).__name__
        tracker = streams[name] = StreamReconnects(owner.logger(), f"{venue} {name}")
    return tracker
