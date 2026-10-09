# GroveX spot connector: DISABLED, kept as reference (Pavel, 2026-10-09: GroveX scrapped).
# Built and verified offline (S5 167/167, 73/73 faults, S6 staged) but never connected or traded. myserver's AWS address
# gets Cloudflare's challenge on every private route and the book socket, so every request went through grovex-relay on
# brr_ws (CONSTANTS.PROXY_URL), which is stopped and disabled. `grovex` is in `disabled_exchanges` (conf/scripts/
# test_multi.yml); a runtime `control create ... gx:...` does not check that list, so don't create GroveX strategies.
# Revival: start grovex-relay on brr_ws, fund the account, run hmb_local_tools/grovex/grovex_private_probe.py --trade
# --fill (the §0.1 proof), drop `grovex` from disabled_exchanges, S6. Facts: wiki trading/exchanges/grovex-api.
