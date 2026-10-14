"""P2-DEX `bsc` connector (build step 3, 2026-10-14). The DEX knobs live with the code that uses them: bsc_quoter (cadence,
rungs, switch, re-election) and bsc_pool_list (the payload files the watcher writes)."""

EXCHANGE_NAME = "bsc"
DEFAULT_DOMAIN = "bsc"
HBOT_ORDER_ID_PREFIX = "BSC-"
MAX_ORDER_ID_LEN = 32

QUOTE = "USDT"                    # every book is priced along its pools' paths to USDT (Q5)
NATIVE_ASSET = "BNB"              # gas
USDT = "0x55d398326f99059ff775485246999027b3197955"         # BSC-USD, 18 decimals
USDT_DECIMALS = 18
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
SEL_ETH_BALANCE = "4d2301cc"      # Multicall3.getEthBalance(address)
SEL_BALANCE_OF = "70a08231"       # ERC-20 balanceOf(address)

RPC_TIMEOUT_S = 10.0
BALANCE_POLL_S = 15.0             # WIP (Pavel 2026-10-14: relaxed for now, tuned later)
STALE_S = 5.0                     # no good poll, or a block older than this: every book goes empty (P1's B7: never a kept price)
FIRST_POLL_WAIT_S = 10.0          # how long a pair's first book waits for the first poll before it starts empty
SUMMARY_EVERY_S = 300.0           # the data source's summary line (DEBUG; a state change is INFO)

# Step 3 ships books and balances only. An order needs the transaction path (step 4) and the send method (its own
# session, Pavel 2026-10-14). Until both exist the connector refuses every order AND reports not ready (status_dict
# "trading_enabled"), so the orchestrator never ticks a strategy with a bsc leg: arb_l fires both legs at once (decision 1)
# and must never fire a CEX leg against a DEX leg that cannot execute. The orchestrator's all-connectors-ready gate (its
# one-time init) skips a connector whose `trading_enabled` is False, so the never-ready connector holds back only its own
# strategies.
TRADING_ENABLED = False
