#!/usr/bin/env python3
"""
Phase-2 validation for the CoinEx spot connector: authentication and the private stream.

Exercises the SHIPPED CoinexAuth / CoinexAPIUserStreamDataSource, not a reimplementation.

Offline checks run with no keys at all. The live checks need a CoinEx API key with READ
permission; nothing here places, modifies or cancels an order.

Credentials are read from the environment so they never touch the repo or the shell history:

    export COINEX_API_KEY=...        # the access_id
    export COINEX_SECRET_KEY=...
    python3 scripts/debug_coinex_auth.py

Run without those set to execute the offline checks only.
"""
import asyncio
import os
import re
import sys
from typing import List

from hummingbot.connector.exchange.coinex import coinex_constants as CONSTANTS, coinex_web_utils as web_utils
from hummingbot.connector.exchange.coinex.coinex_auth import CoinexAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class _FixedTime:
    """Pins the clock so the documented example can be reproduced exactly."""

    def time(self) -> float:
        return 1700490703.564


def offline_checks() -> List[str]:
    """Verify the signing payload against the worked example in authorization.md."""
    failures: List[str] = []
    auth = CoinexAuth("KEY", "SECRET", _FixedTime())

    request = RESTRequest(
        method=RESTMethod.GET,
        url="https://api.coinex.com/v2/spot/pending-order",
        params={"market": "BTCUSDT", "market_type": "SPOT", "side": "buy", "page": 1, "limit": 10},
        is_auth_required=True,
    )
    expected_path = "/v2/spot/pending-order?market=BTCUSDT&market_type=SPOT&side=buy&page=1&limit=10"
    built_path = auth._request_path(request)
    print(f"[offline] signed path: {built_path}")
    if built_path != expected_path:
        failures.append(f"signed path differs from the documented example:\n  got {built_path}\n  want {expected_path}")
    else:
        print("[offline] matches authorization.md worked example (incl. the /v2 prefix)")

    signature = auth._sign("GET" + expected_path + "1700490703564")
    if not re.fullmatch(r"[0-9a-f]{64}", signature):
        failures.append(f"signature is not 64-char lowercase hex: {signature}")
    else:
        print(f"[offline] signature is 64-char lowercase hex: {signature[:16]}...")

    payload = auth.get_ws_auth_payload()
    if payload["signed_str"] != auth._sign(str(payload["timestamp"])):
        failures.append("WS payload must sign the timestamp ALONE (a different rule from REST)")
    else:
        print("[offline] WS server.sign payload signs the bare timestamp, as documented")

    return failures


async def live_checks(api_key: str, secret_key: str) -> List[str]:
    """Read-only private calls. Never places or cancels an order."""
    failures: List[str] = []

    # CoinEx rejects a signature whose timestamp has drifted, so sync to server time first.
    from hummingbot.connector.time_synchronizer import TimeSynchronizer
    synchronizer = TimeSynchronizer()
    await synchronizer.update_server_time_offset_with_time_provider(
        time_provider=web_utils.get_current_server_time()
    )
    auth = CoinexAuth(api_key, secret_key, time_provider=synchronizer)
    api_factory = web_utils.build_api_factory(time_synchronizer=synchronizer, auth=auth)

    # --- signed REST: GET /assets/spot/balance -------------------------------------------------
    rest = await api_factory.get_rest_assistant()
    response = await rest.execute_request(
        url=web_utils.private_rest_url(CONSTANTS.ACCOUNT_BALANCE_ENDPOINT),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.ACCOUNT_BALANCE_ENDPOINT,
        is_auth_required=True,
    )
    code = response.get("code")
    print(f"[live] GET /assets/spot/balance -> code={code} message={response.get('message')}")
    if code != CONSTANTS.RET_CODE_OK:
        failures.append(f"signed balance request rejected: {response}")
        return failures
    holdings = [e for e in (response.get("data") or []) if float(e.get("available") or 0) or float(e.get("frozen") or 0)]
    print(f"[live] authenticated OK — {len(response.get('data') or [])} balance rows, {len(holdings)} non-zero")

    # --- private WS: server.sign + subscriptions ------------------------------------------------
    from hummingbot.connector.exchange.coinex.coinex_api_user_stream_data_source import (
        CoinexAPIUserStreamDataSource,
    )
    stream = CoinexAPIUserStreamDataSource(auth=auth, connector=None, api_factory=api_factory)
    try:
        ws = await stream._connected_websocket_assistant()   # performs server.sign
        print("[live] private WS authenticated (server.sign accepted)")
        await stream._subscribe_channels(ws)
        print("[live] subscribed to order.subscribe + balance.subscribe (all markets / all assets)")
        queue: asyncio.Queue = asyncio.Queue()
        try:
            await asyncio.wait_for(stream._process_websocket_messages(ws, queue), timeout=8)
        except asyncio.TimeoutError:
            pass
        print(f"[live] private stream healthy; {queue.qsize()} update(s) seen in an 8s idle window "
              f"(0 is expected with no open orders)")
        await ws.disconnect()
    except Exception as exception:
        failures.append(f"private WS failed: {type(exception).__name__}: {exception}")

    return failures


async def main() -> int:
    failures = offline_checks()

    # COINEX_API_SECRET is the name P1 uses in Tracker_cex_cex/configs/main_config.py, so keys can
    # be exported straight from there without renaming.
    api_key = os.environ.get("COINEX_API_KEY")
    secret_key = os.environ.get("COINEX_SECRET_KEY") or os.environ.get("COINEX_API_SECRET")
    if api_key and secret_key:
        print()
        failures += await live_checks(api_key, secret_key)
    else:
        print("\n[skip] COINEX_API_KEY / COINEX_SECRET_KEY not set — offline checks only.")

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
