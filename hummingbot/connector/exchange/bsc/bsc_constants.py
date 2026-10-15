"""P2-DEX `bsc` connector (build step 3, 2026-10-14). The DEX knobs live with the code that uses them: bsc_quoter (cadence,
rungs, switch, re-election) and bsc_pool_list (the payload files the watcher writes)."""
from decimal import Decimal

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
SEL_BLOCK_NUMBER = "42cbb15c"     # Multicall3.getBlockNumber(): the block a balance read saw
SEL_BALANCE_OF = "70a08231"       # ERC-20 balanceOf(address)

# Every request has a timeout (audit 2026-10-15: aiohttp takes timeout=None as no timeout at all, and none was ever set,
# so one stalled connection could hold the book poll, or the wallet's turn, until TCP gave up)
RPC_TIMEOUT_S = 2.0               # a node request (BlockPI: the book poll, the balances, check_network): p50 0.2 s from myserver
READ_TIMEOUT_S = 1.0              # a read of the swap lifecycle on SWAP_READ_URL (receipts, the head, the nonce): 3 ms away
BALANCE_POLL_S = 15.0             # WIP (Pavel 2026-10-14: relaxed for now, tuned later)
STALE_S = 5.0                     # a book whose block is older than this goes empty, however its poll ended (P1's B7: never a
                                  # kept price); a poll is never waited for past that moment
FIRST_POLL_WAIT_S = 10.0          # how long a pair's first book waits for the first poll before it starts empty
SUMMARY_EVERY_S = 300.0           # the data source's summary line (DEBUG; a state change is INFO)

# Trading ON since the first live swaps proved the send path end to end (2026-10-14, Pavel's first_swap_live.py on CAKE: the
# connector's own 8 approvals, then a $2 buy and its sell, both landing at head +2, filled 0.71 / 0.60 s after the send).
# False is the kill switch: the connector then refuses every order and every approval AND reports not ready (status_dict
# "trading_enabled"), so the orchestrator never ticks a strategy with a bsc leg: arb_l fires both legs at once (decision 1)
# and must never fire a CEX leg against a DEX leg that cannot execute. The orchestrator's all-connectors-ready gate (its
# one-time init) skips a connector whose `trading_enabled` is False, so a never-ready connector holds back only its own
# strategies.
TRADING_ENABLED = True

# Step 4: one swap transaction per order (bsc_swap builds it, bsc_tx signs it and reads its receipt).
SWAP_GAS_LIMIT = 400_000          # the most a swap may use; unused gas is not paid. Measured 2026-10-14 (eth_estimateGas, the
                                  # 16 test books): 124-208k without a hop, 244-285k with one
APPROVE_GAS_LIMIT = 100_000       # an ERC20 approve or a Permit2.approve (~46-60k)
GAS_PRICE_WEI = 50_000_000        # 0.05 gwei: BSC's floor. All 35 of the Sending session's live sends landed at it
SWAP_GAS_MAX_BNB = Decimal(SWAP_GAS_LIMIT * GAS_PRICE_WEI).scaleb(-18)   # the most one swap can cost: below it, nothing trades
APPROVED_AT_LEAST = 1 << 128      # an allowance this large counts as approved (the approvals are for the maximum amount)
# an approval that did not land (reverted, not included, or refused by every builder) is tried again only after a back-off
# that starts here and doubles each time, up to the cap: one that can never land (a token whose approve always reverts)
# would otherwise hold the wallet's one turn for up to BUNDLE_BLOCKS x BLOCK_S at every balance read (Pavel 2026-10-15)
APPROVE_RETRY_S = 300.0
APPROVE_RETRY_MAX_S = 3600.0

# Sending (the Sending session, Pavel 2026-10-14; measured from myserver -> wiki P2-DEX Overview §The send paths). Every
# transaction (a swap, an approval) goes as a one-transaction BUNDLE to both builders at once: 48 Club and BlockRazor build
# ~96% of BSC's blocks. It is valid up to its last block: it lands by then or never, and then nothing is paid and its nonce
# stays free (nothing can get stuck holding the wallet); a transaction that would revert is not included. Live: 5/5 within
# 2 blocks, receipt p50 0.53 s (the public paths were a block slower).
BUILDERS = ("https://puissant-builder.48.club", "https://rpc.blockrazor.builders")   # 2.6 / 7.5 ms from myserver
BLOCK_S = 0.45                    # BSC's block time since the Fermi hard fork (2026-01-14)
BUNDLE_BLOCKS = 12                # valid for the 12 blocks after the head at the send (~5.4 s): just past the swap's 5 s deadline
                                  # (bsc_swap.DEADLINE_S), so the deadline stays the swap's expiry
# The order lifecycle's reads (receipts, the head, the nonce): BNB's public seed, 3 ms from myserver (BlockPI is ~88 ms),
# free, and asked only while a transaction is out. The book feed stays on BlockPI (decision 4).
SWAP_READ_URL = "https://bsc-dataseed1.bnbchain.org"

# The order lifecycle (2026-10-14): one transaction at a time per wallet, followed on chain until it ends.
RECEIPT_POLL_S = 0.1              # a transaction out has its receipt asked for this often: it ends ~0.1 s after it is mined
SEND_TIMEOUT_S = 2.0              # a builder that hasn't answered a bundle by then counts as silent (the other may have taken it)
WARN_EVERY_S = 30.0               # a transaction's trouble (an unreadable receipt) is said at most this often
PRICE_SLACK = Decimal("1e-6")     # the limit check's relative slack: a strategy's limit and the book's levels are the same
                                  # prices read through float and Decimal, which differ in the last digits
# The room a swap's minimum output leaves past its limit price (Pavel 2026-10-15: "build it with 0.3%"). arb_l's limit is the
# deepest level its scan touched, so an order inside one level had a minimum equal to the book's own output, and any adverse
# tick between the book's block and the swap's (the WBNB/USDT hop moves in about half of all blocks) made it revert, leaving
# its CEX leg alone: 21-26% of such legs never landed, even with the bundle's retries (audit 2026-10-15, finding 2). Ticks
# go both ways, so on average the fill is still the book's; this is only how far one may slip instead of failing.
SWAP_SLIPPAGE = Decimal("0.003")
