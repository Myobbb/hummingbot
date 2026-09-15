#!/usr/bin/env python3
"""
Post-pull smoke test — run AFTER `git pull` and BEFORE restarting the orchestrator.

This branch removes AscendEX (connector + rate-oracle source + candles feed + their registrations)
and adds CoinEx. Both touch shared core modules — rate_oracle.py, client_config_map.py,
candles_factory.py, create_command.py — so a broken registration would only surface when the live
process restarts. This catches that while the running process is still safely on old code.

    cd ~/hummingbot && PYTHONPATH=. python3 scripts/debug_coinex_preflight.py

Expected after the pull: 12 == 12 registries and PREFLIGHT OK. A FAIL on "ascend_ex fully gone"
means the pull did not land; any other FAIL means do NOT restart.

⚠️ EVERYTHING MUST STAY INSIDE main() — nothing at module level but defs.
`Completer.get_strategies_v2_with_config` (client/ui/completer.py) IMPORTS every .py in scripts/
at launch to inspect it for a config class. Module-level work therefore runs during `bin/hummingbot.py`
startup, and its `except Exception` does NOT catch SystemExit (a BaseException) — so a bare
`sys.exit()` out here killed HB on launch instead of starting it. Import must be a no-op.
"""
import sys
from decimal import Decimal


def main() -> int:
    ok = True

    def chk(label, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'PASS' if cond else 'FAIL'}  {label} {detail}")

    from hummingbot.client.config.client_config_map import RATE_SOURCE_MODES
    from hummingbot.client.settings import AllConnectorSettings
    from hummingbot.core.rate_oracle.rate_oracle import RATE_ORACLE_SOURCES
    from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory  # noqa: F401
    import hummingbot.client.command.create_command  # noqa: F401

    s = AllConnectorSettings.get_connector_settings()
    chk("core modules import (rate_oracle / config_map / candles / settings)", True)
    chk("rate-source registries in sync", len(RATE_ORACLE_SOURCES) == len(RATE_SOURCE_MODES),
        f"({len(RATE_ORACLE_SOURCES)} == {len(RATE_SOURCE_MODES)})")
    chk("configured rate source 'gate_io' still available", "gate_io" in RATE_ORACLE_SOURCES)
    chk("ascend_ex fully gone", "ascend_ex" not in s and "ascend_ex" not in RATE_ORACLE_SOURCES)
    chk("coinex registered", "coinex" in s)
    chk("live venues intact", all(x in s for x in
        ["binance", "bybit", "kucoin", "gate_io", "mexc", "htx", "bing_x", "okx", "bitget", "bitmart"]))
    # The check that was missing: HB does NOT construct a connector the way a test script does.
    # UserBalances.connect_market (user/user_balances.py:37) builds kwargs via
    # ConnectorSetting.conn_init_parameters -- which ALWAYS injects `balance_asset_limit` -- and
    # calls connector_class(**params). A connector whose __init__ omits that argument imports and
    # registers perfectly, then dies with TypeError the first time `balance` touches it.
    # Exercise the real construction path, not a hand-written kwargs list.
    from hummingbot.client.config.config_helpers import get_connector_class
    for name in ("coinex", "gate_io", "bybit"):
        setting = s[name]
        keys = {k: "x" for k in (setting.config_keys.__class__.model_fields if setting.config_keys else {})
                if k != "connector"}
        params = setting.conn_init_parameters(
            trading_pairs=["BTC-USDT"], trading_required=False, api_keys=keys,
            balance_asset_limit={}, rate_limits_share_pct=Decimal("100"),
        )
        try:
            get_connector_class(name)(**params)
            chk(f"{name}: constructs through the real conn_init_parameters path", True)
        except Exception as exception:
            chk(f"{name}: constructs through the real conn_init_parameters path", False,
                f"-> {type(exception).__name__}: {exception}")

    print("\nPREFLIGHT OK" if ok else "\nPREFLIGHT FAILED — do not restart")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
