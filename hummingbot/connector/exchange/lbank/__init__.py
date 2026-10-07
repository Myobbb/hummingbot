# LBank spot connector: DISABLED, kept as reference (Pavel, 2026-10-07).
# LBank's API trading is available only to institutional partners and project teams (LBank support, 2026-10-07).
# A regular account's keys (spot trading enabled) read balances, fee rates and the private stream, but every trading
# route answers 10008 "currency pair nonsupport" for every pair: 3 keys, from Tokyo and Europe, in every request format
# (hmb_local_tools/lbank/lbank_order_path_probe.py --diag). `lbank` is in `disabled_exchanges` (conf/scripts/
# test_multi.yml); a runtime `control create ... lb:...` does not check that list, so don't create LBank strategies.
# Revival needs institutional API access: re-run the probe's --diag first. Facts: wiki trading/exchanges/lbank-api.
