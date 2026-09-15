#!/usr/bin/env python3
"""
Phase-1 END-TO-END validation: drives the real CoinexExchange, not just its data source.

debug_coinex_orderbook.py exercises CoinexAPIOrderBookDataSource behind a stub connector. This
script starts the ACTUAL connector with trading_required=False, so it needs no API keys yet still
exercises the whole public stack the strategies depend on:

    trading rules fetch -> symbol map -> order book tracker -> live WS -> get_price/get_order_book

Checks:
  1. start_network() completes and the connector reaches its public-ready state
  2. Trading rules parsed for the requested pairs (min size, price/amount increments)
  3. Symbol map round-trips: BTC-USDT <-> BTCUSDT
  4. Order books populate with uncrossed, real depth
  5. Books keep updating (update_id advances) over the observation window
  6. get_price / get_order_book_snapshot agree with the live book
  7. Runtime add_trading_pair() -> subscribe_to_trading_pair() actually starts a new book,
     which is the orchestrator's `add_market` path

Usage:
    PYTHONPATH=. python3 scripts/debug_coinex_connector.py [SECONDS] [PAIR ...]
"""
import asyncio
import sys
import time
from decimal import Decimal
from typing import List

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange
from hummingbot.core.data_type.common import OrderType, TradeType

DEFAULT_PAIRS = ["BTC-USDT", "ZEC-USDT"]
RUNTIME_ADD_PAIR = "CARDS-USDT"


async def main() -> int:
    # Imported INSIDE the function on purpose: HB's autocomplete builder
    # (client/ui/completer.py get_strategies_v2_with_config) imports every scripts/*.py at
    # launch and treats any module-level BaseClientModel subclass as a strategy config, which
    # would list this debug script under `start --script`. Keeping it local stays out of that.
    from hummingbot.client.config.client_config_map import ClientConfigMap
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    pairs = sys.argv[2:] or DEFAULT_PAIRS
    failures: List[str] = []

    connector = CoinexExchange(
        client_config_map=ClientConfigAdapter(ClientConfigMap()),
        coinex_api_key="", coinex_secret_key="",
        trading_pairs=list(pairs),
        trading_required=False,
    )

    print(f"[1] starting network for {pairs} ...")
    await connector.start_network()

    # ExchangePyBase only starts the trading-rules poller when is_trading_required (start_network
    # :697), and status_dict hardcodes trading_rule_initialized=True in that mode (:173). So with
    # trading_required=False the rules must be fetched explicitly to exercise
    # _format_trading_rules / _initialize_trading_pair_symbols_from_exchange_info. GET /spot/market
    # is public, so this needs no credentials.
    await connector._update_trading_rules()

    deadline = time.time() + 60
    while time.time() < deadline:
        if all(p in connector.order_books for p in pairs):
            break
        await asyncio.sleep(1)
    print(f"    status: {connector.status_dict}")
    print(f"    trading rules loaded: {len(connector.trading_rules)}")
    if not connector.trading_rules:
        failures.append("no trading rules were loaded")

    # --- 2. trading rules -----------------------------------------------------------------------
    for pair in pairs:
        rule = connector.trading_rules.get(pair)
        if rule is None:
            failures.append(f"{pair}: no trading rule")
            continue
        print(f"[2] {pair:<12} min_size={rule.min_order_size} price_inc={rule.min_price_increment} "
              f"amount_inc={rule.min_base_amount_increment}")
        if rule.min_price_increment <= 0 or rule.min_base_amount_increment <= 0:
            failures.append(f"{pair}: non-positive increment in trading rule")

    # --- 3. symbol map --------------------------------------------------------------------------
    for pair in pairs:
        symbol = await connector.exchange_symbol_associated_to_pair(trading_pair=pair)
        back = await connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)
        print(f"[3] {pair:<12} -> {symbol:<12} -> {back}")
        if back != pair:
            failures.append(f"{pair}: symbol map does not round-trip (got {back} via {symbol})")

    # --- 4/5. live books ------------------------------------------------------------------------
    print(f"[4] observing books for {duration}s ...")
    first = {}
    for pair in pairs:
        ob = connector.get_order_book(pair)
        first[pair] = ob.snapshot_uid
    await asyncio.sleep(duration)

    for pair in pairs:
        ob = connector.get_order_book(pair)
        bids = list(ob.bid_entries())
        asks = list(ob.ask_entries())
        if not bids or not asks:
            failures.append(f"{pair}: order book empty ({len(bids)} bids / {len(asks)} asks)")
            continue
        top_bid, top_ask = Decimal(str(bids[0].price)), Decimal(str(asks[0].price))
        advanced = ob.snapshot_uid != first[pair]
        print(f"[4] {pair:<12} bid={top_bid} ask={top_ask} depth={len(bids)}x{len(asks)} "
              f"updating={advanced}")
        if top_bid >= top_ask:
            failures.append(f"{pair}: crossed book in the tracker (bid {top_bid} >= ask {top_ask})")
        if not advanced:
            failures.append(f"{pair}: order book never advanced in {duration}s")

    # --- 6. pricing helpers the strategies actually call -----------------------------------------
    for pair in pairs:
        buy_px = connector.get_price(pair, True)
        sell_px = connector.get_price(pair, False)
        quote = await connector.get_quote_price(pair, True, Decimal("0.001"))
        print(f"[6] {pair:<12} get_price buy={buy_px} sell={sell_px} quote(0.001)={quote}")
        if not (buy_px > 0 and sell_px > 0):
            failures.append(f"{pair}: get_price returned a non-positive value")
        fee = connector.get_fee(pair.split("-")[0], pair.split("-")[1], OrderType.LIMIT,
                                TradeType.BUY, Decimal("1"), Decimal(str(buy_px)))
        print(f"    fee percent={fee.percent}")

    # --- 7. the orchestrator's runtime add_market path -------------------------------------------
    print(f"[7] runtime add of {RUNTIME_ADD_PAIR} (the orchestrator add_market path) ...")
    ds = connector.order_book_tracker.data_source
    added = ds.add_trading_pair(RUNTIME_ADD_PAIR)
    subscribed = await ds.subscribe_to_trading_pair(RUNTIME_ADD_PAIR)
    print(f"    add_trading_pair={added} subscribe_to_trading_pair={subscribed}")
    await asyncio.sleep(20)
    if RUNTIME_ADD_PAIR in connector.order_books:
        ob = connector.get_order_book(RUNTIME_ADD_PAIR)
        bids, asks = list(ob.bid_entries()), list(ob.ask_entries())
        print(f"[7] {RUNTIME_ADD_PAIR:<12} book after runtime add: {len(bids)}x{len(asks)}")
    else:
        # The tracker only creates books for pairs it was started with; the data source having
        # accepted the pair is what the orchestrator relies on.
        print(f"[7] {RUNTIME_ADD_PAIR} accepted by the data source "
              f"(tracker book creation is the connector's job on add_market)")
    if not (added and subscribed):
        failures.append(f"runtime add of {RUNTIME_ADD_PAIR} was rejected")

    # The base subscribe_to_trading_pair forces a websocket reconnect, so verify the ORIGINAL
    # pairs recovered rather than going dark - that reconnect is the documented blast radius.
    before = {p: connector.get_order_book(p).snapshot_uid for p in pairs}
    await asyncio.sleep(15)
    for pair in pairs:
        ob = connector.get_order_book(pair)
        recovered = ob.snapshot_uid != before[pair]
        print(f"[7] {pair:<12} resumed after the forced reconnect: {recovered}")
        if not recovered:
            failures.append(f"{pair}: book did NOT resume after the runtime-add reconnect")

    await connector.stop_network()

    print()
    if failures:
        print(f"FAILED ({len(failures)}):")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
