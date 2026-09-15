#!/usr/bin/env python3
"""
Phase-2 END-TO-END validation: the authenticated half of the CoinEx connector, driven by a real
Clock exactly the way the orchestrator drives it.

Why the Clock matters: ExchangePyBase._status_polling_loop blocks on `self._poll_notifier.wait()`
(:801), and that event is only set by tick(), which the Clock drives. Without a Clock registered,
balances are NEVER polled and the connector can never become ready — the same reason the
orchestrator "must register connectors with clock" (see the wiki's orchestrator notes).

Exercises, with no order placement:
  1. Clock-driven startup -> connector reaches ready
  2. Balance polling through the real loop (not a hand-called method)
  3. Balance parse: available + frozen -> total
  4. Private user stream stays connected and authenticated
  5. Order quantisation against the live trading rule
  6. A status lookup for a NON-EXISTENT order is classified as not-found rather than raising
     (the canceled-orders-are-not-retained quirk + CoinEx's null-payload behaviour)

Credentials come from the environment; nothing is written to disk:
    export COINEX_API_KEY=...  COINEX_API_SECRET=...
    PYTHONPATH=. python3 scripts/debug_coinex_private.py [SECONDS]
"""
import asyncio
import os
import sys
from decimal import Decimal
from typing import List

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.coinex.coinex_exchange import CoinexExchange
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.common import OrderType, TradeType

TRADING_PAIR = "BTC-USDT"


async def main() -> int:
    # Imported INSIDE the function on purpose: HB's autocomplete builder
    # (client/ui/completer.py get_strategies_v2_with_config) imports every scripts/*.py at
    # launch and treats any module-level BaseClientModel subclass as a strategy config, which
    # would list this debug script under `start --script`. Keeping it local stays out of that.
    from hummingbot.client.config.client_config_map import ClientConfigMap
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    failures: List[str] = []

    api_key = os.environ.get("COINEX_API_KEY")
    secret_key = os.environ.get("COINEX_SECRET_KEY") or os.environ.get("COINEX_API_SECRET")
    if not (api_key and secret_key):
        print("FATAL: set COINEX_API_KEY and COINEX_API_SECRET (or COINEX_SECRET_KEY)")
        return 1

    connector = CoinexExchange(
        client_config_map=ClientConfigAdapter(ClientConfigMap()),
        coinex_api_key=api_key,
        coinex_secret_key=secret_key,
        trading_pairs=[TRADING_PAIR],
        trading_required=True,
    )

    # --- 1. Clock-driven startup, mirroring the orchestrator ------------------------------------
    # Clock.run() must be entered as a context manager, and adding the connector as an iterator is
    # what calls start() -> start_network() on it.
    clock = Clock(ClockMode.REALTIME, tick_size=1.0)
    clock.add_iterator(connector)
    print(f"[1] clock started; waiting up to {duration}s for the connector to become ready ...")

    with clock:
        clock_task = asyncio.create_task(clock.run())
        for _ in range(duration):
            if connector.ready:
                break
            await asyncio.sleep(1)

        print(f"    status: {connector.status_dict}")
        print(f"    READY : {connector.ready}")
        if not connector.ready:
            failures.append(f"connector never became ready: {connector.status_dict}")

        # --- 2/3. balances via the real polling loop -------------------------------------------------
        balances = connector.get_all_balances()
        print(f"[2] balances polled through _status_polling_loop: {balances}")
        if not balances:
            failures.append("balances were never populated by the polling loop")
        for asset, total in balances.items():
            available = connector._account_available_balances.get(asset)
            print(f"[3] {asset:<6} available={available} total={total} (total = available + frozen)")
            if available is None or available > total:
                failures.append(f"{asset}: available {available} exceeds total {total}")

        # --- 4. private stream ------------------------------------------------------------------------
        us_ok = connector._is_user_stream_initialized()
        print(f"[4] user stream initialized: {us_ok}")
        if not us_ok:
            failures.append("user stream never initialized")

        # --- 5. quantisation against the live rule ----------------------------------------------------
        rule = connector.trading_rules.get(TRADING_PAIR)
        price = connector.get_price(TRADING_PAIR, True)
        q_amount = connector.quantize_order_amount(TRADING_PAIR, Decimal("0.000123456789"))
        q_price = connector.quantize_order_price(TRADING_PAIR, Decimal(str(price)) * Decimal("0.995"))
        print(f"[5] rule min_size={rule.min_order_size} price_inc={rule.min_price_increment} "
              f"amount_inc={rule.min_base_amount_increment}")
        print(f"    quantize amount 0.000123456789 -> {q_amount}")
        print(f"    quantize price  {price}*0.995 -> {q_price}")
        if q_price % rule.min_price_increment != 0:
            failures.append(f"quantized price {q_price} is not a multiple of {rule.min_price_increment}")

        # --- 6. the not-retained-order path -----------------------------------------------------------
        # CoinEx does not keep orders that were canceled without executing, and returns code 0 with a
        # null payload for an empty result. A status poll for such an order must be reported as
        # not-found so the tracker can finalise it, never raise repeatedly.
        print("[6] querying a non-existent order id to exercise the not-found path ...")
        ghost = InFlightOrder(
            client_order_id="HBOTGHOSTORDERDOESNOTEXIST00", exchange_order_id="1",
            trading_pair=TRADING_PAIR, order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            amount=Decimal("1"), creation_timestamp=connector.current_timestamp, price=Decimal("1"),
            initial_state=OrderState.OPEN,
        )
        try:
            update = await connector._request_order_status(ghost)
            print(f"    unexpectedly returned an update: {update.new_state}")
            failures.append("a non-existent order returned a status update instead of an error")
        except Exception as exception:
            classified = connector._is_order_not_found_during_status_update_error(exception)
            print(f"    raised {type(exception).__name__}; classified as order-not-found: {classified}")
            print(f"    message: {str(exception)[:130]}")
            if not classified:
                failures.append(f"non-existent order NOT classified as not-found: {exception}")

        clock_task.cancel()
        await asyncio.gather(clock_task, return_exceptions=True)
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
