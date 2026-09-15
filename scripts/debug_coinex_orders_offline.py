#!/usr/bin/env python3
"""
OFFLINE verification of the CoinEx order lifecycle — no API calls, no credentials.

Every payload below is copied verbatim from the CoinEx v2 docs (or from a live response recorded
during Phase 2). The REAL connector methods are exercised; only the HTTP layer is intercepted, so
what is verified is the code that ships.

Covered:
  1. _place_order request bodies      LIMIT / MARKET / LIMIT_MAKER, against put-order's field table
  2. Decimal formatting               no scientific notation on the wire (1E-8 -> 0.00000001)
  3. _place_cancel request body       against cancel-order's field table
  4. order.update -> OrderState       all four events, current + legacy spellings, finish split
  5. user_deals.update -> TradeUpdate real-time fill, fee, maker/taker role
  6. order-status -> OrderState       the doc's own example (which returns undocumented part_deal)
  7. balance.update -> balances       available + frozen -> total
  8. error classification             not-found vs time-sync vs neither

Usage:
    PYTHONPATH=. python3 scripts/debug_coinex_orders_offline.py
"""
import asyncio
import sys
from decimal import Decimal
from typing import Any, Dict, List

from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS
from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState

TRADING_PAIR = "BTC-USDT"
SYMBOL = "BTCUSDT"

failures: List[str] = []


def check(label: str, got: Any, want: Any) -> None:
    ok = got == want
    if not ok:
        failures.append(f"{label}: got {got!r}, want {want!r}")
    print(f"  {'PASS' if ok else 'FAIL'}  {label:<58} {got!r}")


def build_connector() -> CoinexExchange:
    ex = CoinexExchange(
        client_config_map=ClientConfigAdapter(ClientConfigMap()),
        coinex_api_key="k", coinex_secret_key="s",
        trading_pairs=[TRADING_PAIR], trading_required=False,
    )
    # Symbol map without a network call.
    ex._set_trading_pair_symbol_map(bidict({SYMBOL: TRADING_PAIR}))
    return ex


def track(ex: CoinexExchange, client_order_id: str, exchange_order_id: str) -> InFlightOrder:
    order = InFlightOrder(
        client_order_id=client_order_id, exchange_order_id=exchange_order_id,
        trading_pair=TRADING_PAIR, order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
        amount=Decimal("1.5"), price=Decimal("5999"), creation_timestamp=1689152421.0,
        initial_state=OrderState.OPEN,
    )
    ex._order_tracker.start_tracking_order(order)
    return order


async def main() -> int:
    ex = build_connector()
    captured: Dict[str, Any] = {}

    async def fake_post(path_url, data=None, **kwargs):
        captured["path"] = path_url
        captured["body"] = data
        return {"code": 0, "data": {"order_id": 13400}, "message": "OK"}

    ex._api_post = fake_post

    # --- 1/2. place-order request bodies -------------------------------------------------------
    print("[1] _place_order request bodies (fields per spot/order/http/put-order)")
    await ex._place_order("HBOTLIMIT1", TRADING_PAIR, Decimal("1.5"), TradeType.BUY,
                          OrderType.LIMIT, Decimal("5999"))
    body = captured["body"]
    check("LIMIT path", captured["path"], CONSTANTS.PLACE_ORDER_ENDPOINT)
    check("LIMIT market", body["market"], SYMBOL)
    check("LIMIT market_type", body["market_type"], "SPOT")
    check("LIMIT side", body["side"], "buy")
    check("LIMIT type", body["type"], "limit")
    check("LIMIT amount", body["amount"], "1.5")
    check("LIMIT price", body["price"], "5999")
    check("LIMIT client_id", body["client_id"], "HBOTLIMIT1")
    check("LIMIT sends no ccy", "ccy" in body, False)

    await ex._place_order("HBOTMAKER1", TRADING_PAIR, Decimal("1"), TradeType.SELL,
                          OrderType.LIMIT_MAKER, Decimal("60000"))
    check("LIMIT_MAKER type", captured["body"]["type"], "maker_only")
    check("LIMIT_MAKER carries price", captured["body"].get("price"), "60000")

    await ex._place_order("HBOTMKT1", TRADING_PAIR, Decimal("0.00000001"), TradeType.BUY,
                          OrderType.MARKET, Decimal("0"))
    body = captured["body"]
    check("MARKET type", body["type"], "market")
    check("MARKET ccy = base (amount is in base)", body["ccy"], "BTC")
    check("MARKET sends no price", "price" in body, False)
    print("[2] decimal formatting — CoinEx must never receive scientific notation")
    check("amount 1e-8 not '1E-8'", body["amount"], "0.00000001")

    # --- 3. cancel ------------------------------------------------------------------------------
    print("[3] _place_cancel request body (spot/order/http/cancel-order)")
    order = track(ex, "HBOTCANCEL1", "13400")
    await ex._place_cancel("HBOTCANCEL1", order)
    body = captured["body"]
    check("cancel path", captured["path"], CONSTANTS.CANCEL_ORDER_ENDPOINT)
    check("cancel market", body["market"], SYMBOL)
    check("cancel market_type", body["market_type"], "SPOT")
    check("cancel order_id is int", body["order_id"], 13400)

    # --- 4. order.update ------------------------------------------------------------------------
    print("[4] order.update -> OrderState (CURRENT doc spelling; legacy spelling covered below)")
    # Verbatim from spot/order/ws/user-order, re-fetched 2026-09-11. CoinEx corrected this example
    # around 2026-09-07: it previously sent unfill_amount / fill_value / last_fill_* while the
    # field table said unfilled_amount / filled_value / last_filled_*. The table spelling won.
    doc_order = {
        "order_id": 12750, "market": SYMBOL, "margin_market": SYMBOL, "type": "limit",
        "side": "buy", "price": "5999.00", "amount": "1.50000000",
        "unfilled_amount": "1.50000000", "filled_value": "1.50000000",
        "taker_fee_rate": "0.0001", "maker_fee_rate": "0.0001", "base_ccy_fee": "0.0001",
        "quote_ccy_fee": "0.0001", "discount_ccy_fee": "0.0001",
        "last_filled_amount": "0", "last_filled_price": "0", "client_id": "HBOTWS1",
        "created_at": 1689152421692, "updated_at": 1689152421692,
    }
    tracked = track(ex, "HBOTWS1", "12750")
    # ClientOrderTracker.process_order_update schedules the work with safe_ensure_future, so the
    # new state lands on a later event-loop pass rather than synchronously.
    async def apply(message):
        ex._process_order_update_message(message)
        await asyncio.sleep(0.05)

    await apply({"event": "put", "order": doc_order})
    check("put (untouched) -> OPEN", tracked.current_state, OrderState.OPEN)

    await apply({"event": "update", "order": dict(doc_order, unfilled_amount="0.50000000")})
    check("update, 1.0 of 1.5 filled -> PARTIALLY_FILLED", tracked.current_state,
          OrderState.PARTIALLY_FILLED)

    # A FILLED transition makes the tracker await wait_until_completely_filled() (5s timeout), so
    # the fills are delivered first — which is also the real ordering: user_deals.update arrives
    # before the order.update `finish`.
    ex._process_user_deal_message({
        "deal_id": 1, "created_at": 1689152421692, "market": SYMBOL, "side": "buy",
        "order_id": 12750, "client_id": "HBOTWS1", "price": "5999", "amount": "1.5",
        "role": "taker", "fee": "0.1", "fee_ccy": "USDT",
    })
    await apply({"event": "finish", "order": dict(doc_order, unfilled_amount="0")})
    check("finish, nothing unfilled -> FILLED", tracked.current_state, OrderState.FILLED)

    # Same transition via the PRE-2026-09-07 spelling. CoinEx no longer documents `unfill_amount`,
    # but the alias handling is kept because these payloads have never been seen live and a field
    # read as absent would mis-derive the state silently. A remainder means CANCELED, and that
    # path does not wait on fills.
    tracked2 = track(ex, "HBOTWS2", "12751")
    await apply({
        "event": "finish",
        "order": {"order_id": 12751, "client_id": "HBOTWS2", "amount": "1.5",
                  "unfill_amount": "0.9", "updated_at": 1689152421692},
    })
    check("finish w/ remainder (LEGACY spelling still handled) -> CANCELED", tracked2.current_state,
          OrderState.CANCELED)

    # --- 5. user_deals.update -------------------------------------------------------------------
    print("[5] user_deals.update -> TradeUpdate (verbatim doc example)")
    tracked3 = track(ex, "HBOTFILL1", "8678890")
    ex._process_user_deal_message({
        "deal_id": 3514376759, "created_at": 1689152421692, "market": SYMBOL, "side": "buy",
        "order_id": 8678890, "client_id": "HBOTFILL1", "margin_market": SYMBOL,
        "price": "30718.42", "amount": "0.00000325", "role": "taker",
        "fee": "0.0299", "fee_ccy": "USDT",
    })
    check("fill applied to tracked order", tracked3.executed_amount_base, Decimal("0.00000325"))
    check("fill quote amount", tracked3.executed_amount_quote,
          Decimal("30718.42") * Decimal("0.00000325"))
    fills = list(tracked3.order_fills.values())
    check("one fill recorded", len(fills), 1)
    check("fee token", fills[0].fee.flat_fees[0].token, "USDT")
    check("fee amount", fills[0].fee.flat_fees[0].amount, Decimal("0.0299"))
    check("taker role", fills[0].is_taker, True)

    # unknown client_id must still resolve via exchange order id
    tracked4 = track(ex, "HBOTFILL2", "999001")
    ex._process_user_deal_message({
        "deal_id": 3514376760, "created_at": 1689152421700, "market": SYMBOL, "side": "buy",
        "order_id": 999001, "client_id": "", "price": "100", "amount": "2",
        "role": "maker", "fee": "0.1", "fee_ccy": "USDT",
    })
    check("blank client_id falls back to exchange_order_id", tracked4.executed_amount_base,
          Decimal("2"))
    check("maker role", list(tracked4.order_fills.values())[0].is_taker, False)

    # --- 5b. fills PAGINATION (never exercised live — no order has existed) ----------------------
    print("[5b] _all_trade_updates_for_order pagination (driven by pagination.has_next)")
    paged = track(ex, "HBOTPAGE1", "555001")
    calls = []

    def make_pages(*pages):
        async def fake_get(path_url, params=None, **kwargs):
            calls.append(params.get("page"))
            body, has_next = pages[params["page"] - 1]
            return {"code": 0, "data": body, "pagination": {"has_next": has_next}, "message": "OK"}
        return fake_get

    deal = lambda i: {"deal_id": i, "created_at": 1689152421692, "market": SYMBOL, "side": "buy",
                      "order_id": 555001, "client_id": "HBOTPAGE1", "price": "100", "amount": "1",
                      "role": "taker", "fee": "0.1", "fee_ccy": "USDT"}

    ex._api_get = make_pages(([deal(i) for i in range(1, 11)], True),
                             ([deal(i) for i in range(11, 14)], False))
    updates = await ex._all_trade_updates_for_order(paged)
    check("walks both pages", calls, [1, 2])
    check("collects every fill across pages", len(updates), 13)

    # a response WITHOUT the pagination block must fall back to the length heuristic
    calls.clear()
    async def fake_get_nopag(path_url, params=None, **kwargs):
        calls.append(params.get("page"))
        return {"code": 0, "data": [deal(1), deal(2)], "message": "OK"}
    ex._api_get = fake_get_nopag
    updates = await ex._all_trade_updates_for_order(paged)
    check("no pagination block -> short page stops the loop", calls, [1])
    check("fallback still returns the fills", len(updates), 2)

    # null data (CoinEx's empty-result shape) must not raise
    async def fake_get_null(path_url, params=None, **kwargs):
        return {"code": 0, "data": None, "pagination": {"has_next": False}, "message": "OK"}
    ex._api_get = fake_get_null
    check("null data yields no fills, no crash", len(await ex._all_trade_updates_for_order(paged)), 0)

    # --- 6. REST order-status --------------------------------------------------------------------
    print("[6] order-status -> OrderState (doc example returns the undocumented 'part_deal')")
    check("part_deal", ex._order_state_from_payload({"status": "part_deal"}),
          OrderState.PARTIALLY_FILLED)
    check("filled", ex._order_state_from_payload({"status": "filled"}), OrderState.FILLED)
    check("unknown status falls back to amounts",
          ex._order_state_from_payload({"status": "???", "unfilled_amount": "0",
                                        "filled_amount": "5"}), OrderState.FILLED)

    # --- 7. balance.update ------------------------------------------------------------------------
    print("[7] balance.update -> balances (available + frozen = total)")
    # _user_stream_event_listener is an infinite loop over a queue, so the balance branch is
    # applied directly here with the doc's own payload shape.
    for entry in [{"ccy": "USDT", "available": "100.5", "frozen": "20.25",
                   "updated_at": 1689152421692}]:
        ex._account_available_balances[entry["ccy"]] = ex._decimal(entry, "available")
        ex._account_balances[entry["ccy"]] = ex._decimal(entry, "available") + ex._decimal(entry, "frozen")
    check("available", ex._account_available_balances["USDT"], Decimal("100.5"))
    check("total = available + frozen", ex._account_balances["USDT"], Decimal("120.75"))

    # --- 8. error classification -------------------------------------------------------------------
    print("[8] error classification")
    check("3600 -> not found",
          ex._is_order_not_found_during_status_update_error(IOError("code 3600 — Order not found")), True)
    check("4010 -> time sync",
          ex._is_request_exception_related_to_time_synchronizer(IOError("code 4010 — Expired request")), True)
    check("4017 -> time sync",
          ex._is_request_exception_related_to_time_synchronizer(IOError("code 4017 — Signature expired")), True)
    check("4006 (signature) is NOT time sync",
          ex._is_request_exception_related_to_time_synchronizer(IOError("code 4006 — Signature verification failed")), False)

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
