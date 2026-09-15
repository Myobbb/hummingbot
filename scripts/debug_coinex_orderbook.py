#!/usr/bin/env python3
"""
Phase-1 live validation for the CoinEx spot connector.

Drives the REAL CoinexAPIOrderBookDataSource against live CoinEx, rather than reimplementing the
protocol, so what passes here is the code that ships. A minimal stub stands in for the connector
purely to supply the symbol <-> trading-pair mapping (that mapping is the exchange class's job and
arrives in Phase 2).

Checks performed:
  1. GET /spot/market  -> symbol map + trading rules sanity
  2. REST snapshot via the data source's own _order_book_snapshot()
  3. WS depth.update  -> gzip decode, is_full=True, bid < ask, update cadence
  4. WS deals.update  -> trade messages parse
  5. REST vs WS cross-check on the same market

Usage:
    python3 scripts/debug_coinex_orderbook.py [SECONDS] [PAIR ...]
    python3 scripts/debug_coinex_orderbook.py 20 BTC-USDT CARDS-USDT
"""
import asyncio
import statistics
import sys
import time
from decimal import Decimal
from typing import Dict, List

from hummingbot.connector.exchange.coinex import (
    coinex_constants as CONSTANTS,
    coinex_utils as utils,
    coinex_web_utils as web_utils,
)
from hummingbot.connector.exchange.coinex.coinex_api_order_book_data_source import CoinexAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod

DEFAULT_PAIRS = ["BTC-USDT", "CARDS-USDT"]


class _StubConnector:
    """Supplies only the symbol mapping the data source needs. Phase 2 replaces this."""

    def __init__(self, symbol_map: Dict[str, str]):
        self._symbol_to_pair = symbol_map
        self._pair_to_symbol = {v: k for k, v in symbol_map.items()}

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        return self._pair_to_symbol[trading_pair]

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        return self._symbol_to_pair[symbol]

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        return {}


async def fetch_market_map(api_factory) -> Dict[str, Dict]:
    rest = await api_factory.get_rest_assistant()
    resp = await rest.execute_request(
        url=web_utils.public_rest_url(CONSTANTS.PUBLIC_MARKET_ENDPOINT),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.PUBLIC_MARKET_ENDPOINT,
    )
    if web_utils.is_error_response(resp):
        raise SystemExit(f"GET /spot/market failed: {resp}")
    return {m["market"]: m for m in resp["data"]}


async def main() -> int:
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    pairs = sys.argv[2:] or DEFAULT_PAIRS
    failures: List[str] = []

    api_factory = web_utils.build_api_factory()

    # --- 1. market map + trading rules -------------------------------------------------------
    markets = await fetch_market_map(api_factory)
    valid = {k: v for k, v in markets.items() if utils.is_exchange_information_valid(v)}
    print(f"[1] GET /spot/market: {len(markets)} markets, {len(valid)} tradable after filtering")

    symbol_map: Dict[str, str] = {}
    for pair in pairs:
        base, quote = pair.split("-")
        symbol = f"{base}{quote}"
        if symbol not in markets:
            failures.append(f"{pair}: symbol {symbol} not listed on CoinEx")
            continue
        info = markets[symbol]
        symbol_map[symbol] = pair
        price_inc = Decimal(1).scaleb(-int(info["quote_ccy_precision"]))
        amount_inc = Decimal(1).scaleb(-int(info["base_ccy_precision"]))
        print(f"    {pair:<12} min_amount={info['min_amount']:<12} price_inc={price_inc} "
              f"amount_inc={amount_inc} maker={info['maker_fee_rate']} taker={info['taker_fee_rate']}")

    if not symbol_map:
        print("FATAL: no usable markets among the requested pairs")
        return 1

    connector = _StubConnector(symbol_map)
    ds = CoinexAPIOrderBookDataSource(
        trading_pairs=list(symbol_map.values()), connector=connector, api_factory=api_factory
    )

    # --- 2. REST snapshot through the shipped method ------------------------------------------
    rest_books = {}
    for pair in symbol_map.values():
        snap = await ds._order_book_snapshot(pair)
        bids, asks = snap.content["bids"], snap.content["asks"]
        rest_books[pair] = (bids, asks)
        top_bid = Decimal(bids[0][0]) if bids else None
        top_ask = Decimal(asks[0][0]) if asks else None
        print(f"[2] REST snapshot {pair:<12} bid={top_bid} ask={top_ask} depth={len(bids)}x{len(asks)}")
        if top_bid is not None and top_ask is not None and top_bid >= top_ask:
            failures.append(f"{pair}: REST book crossed (bid {top_bid} >= ask {top_ask})")

    # --- 3/4. live WS ---------------------------------------------------------------------------
    snap_q: asyncio.Queue = asyncio.Queue()
    trade_q: asyncio.Queue = asyncio.Queue()
    tasks = [
        asyncio.create_task(ds.listen_for_subscriptions()),
        asyncio.create_task(ds.listen_for_order_book_snapshots(None, snap_q)),
        asyncio.create_task(ds.listen_for_trades(None, trade_q)),
    ]

    print(f"[3] listening {duration}s on the live WebSocket ...")
    arrivals: Dict[str, List[float]] = {p: [] for p in symbol_map.values()}
    ws_books: Dict[str, tuple] = {}
    counters = {"trades": 0}

    # Dedicated consumer per queue. Racing two get() calls with asyncio.wait would orphan the
    # loser each iteration, and an orphaned get() can swallow a message that is then lost.
    async def drain_snapshots():
        while True:
            msg = await snap_q.get()
            pair = msg.content["trading_pair"]
            arrivals.setdefault(pair, []).append(time.time())
            bids, asks = msg.content["bids"], msg.content["asks"]
            ws_books[pair] = (bids, asks)
            if bids and asks and Decimal(bids[0][0]) >= Decimal(asks[0][0]):
                failures.append(f"{pair}: WS book crossed (bid {bids[0][0]} >= ask {asks[0][0]})")

    async def drain_trades():
        while True:
            await trade_q.get()
            counters["trades"] += 1

    tasks += [asyncio.create_task(drain_snapshots()), asyncio.create_task(drain_trades())]
    await asyncio.sleep(duration)

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    trades = counters["trades"]

    for pair, ts in arrivals.items():
        if not ts:
            failures.append(f"{pair}: no depth.update received in {duration}s")
            continue
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        median_gap = statistics.median(gaps) if gaps else float("nan")
        print(f"[3] {pair:<12} {len(ts):>4} snapshots, median gap {median_gap * 1e3:.0f}ms "
              f"(CoinEx documents ~200ms)")
    print(f"[4] trade messages parsed: {trades}")

    # --- 5. REST vs WS cross-check ---------------------------------------------------------------
    for pair, (wbids, wasks) in ws_books.items():
        rbids, rasks = rest_books.get(pair, ([], []))
        if not (wbids and rbids):
            continue
        wp, rp = Decimal(wbids[0][0]), Decimal(rbids[0][0])
        drift = abs(wp - rp) / rp if rp else Decimal(0)
        flag = "ok" if drift < Decimal("0.02") else "SUSPECT"
        print(f"[5] {pair:<12} REST bid {rp} vs WS bid {wp} -> {drift * 100:.3f}% drift [{flag}]")
        if drift >= Decimal("0.02"):
            failures.append(f"{pair}: REST/WS top-of-book drift {drift * 100:.2f}%")

    print()
    if failures:
        print(f"FAILED ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
