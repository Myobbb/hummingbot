"""P2-DEX `bsc` connector (build step 3, 2026-10-14). The DEX knobs live with the code that uses them: bsc_quoter (cadence,
rungs, switch, re-election) and bsc_pool_list (the payload files the watcher writes)."""
import os
from decimal import Decimal

EXCHANGE_NAME = "bsc"
DEFAULT_DOMAIN = "bsc"
HBOT_ORDER_ID_PREFIX = "BSC-"
MAX_ORDER_ID_LEN = 32

QUOTE = "USDT"                    # every book is priced along its pools' paths to USDT (Q5)
# arb_l keeps 25 USDT of a buy venue's balance aside (arbitrage.pyx `buy_quote_balance = available - 25.0`, a legacy CEX rule
# kept by Pavel 2026-10-15). The buy ladder ends at the wallet's USDT less this: the most arb_l can spend there
ARB_QUOTE_RESERVE = Decimal(25)
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
READ_TIMEOUT_S = 1.0              # a read of the swap lifecycle (SWAP_READ_URL, SWAP_STATUS_URL): each a few ms away
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
BUNDLE_BLOCKS = 4                 # valid for the 4 blocks after the head at the send (~1.8 s; Pavel 2026-10-17, was 12). Each of
                                  # the 42 swaps landed by then landed within 3 blocks of the head at its send (+1: 6, +2: 30,
                                  # +3: 6), so blocks +4..+12 only kept a swap that lost its race holding the wallet's one turn
                                  # (~6.5 s from the order to "dropped"; ~2 s now: it ends as soon as SWAP_STATUS_URL has its
                                  # last block, bsc_feed._landed). The bundle's last block, not the swap's 5 s deadline
                                  # (bsc_swap.DEADLINE_S), is the swap's expiry
# The wallet's nonce (the lease's too: the asset manager sends on this node, so its pending transactions show here) and an
# approval's head: BNB's public seed, 3 ms from myserver (BlockPI is ~88 ms), free. The book feed stays on BlockPI (decision 4).
SWAP_READ_URL = "https://bsc-dataseed1.bnbchain.org"
# A transaction out is followed here: the head, its receipt, the wallet's nonce at its last block (Pavel 2026-10-17, DEX first:
# "minimize the time it takes for us to confirm that our DEX order got placed, and if it did not we need to minimize the wait
# time"). BlockRazor's public RPC, ~8 ms from myserver: a block's receipts readable p50 0.149 s after its timestamp (p90 0.185),
# each one there as soon as the head showed it (802/802); the seed shows a head p50 0.41 s after it, and refuses ~14% of reads
# at 10/s (measured 2026-10-17: hmb_local_tools p2_dex/receipt_sources_probe.py, block_sources_probe.py)
SWAP_STATUS_URL = "https://bsc.blockrazor.xyz"

# The order lifecycle (2026-10-14): one transaction at a time per wallet, followed on chain until it ends.
RECEIPT_POLL_S = 0.025            # a transaction out is looked at this often (the head; on a new block, its receipt): it ends
                                  # ~0.03 s after its block shows on SWAP_STATUS_URL (0.1 s on the seed before 2026-10-17)
SEND_TIMEOUT_S = 2.0              # a builder that hasn't answered a bundle by then counts as silent (the other may have taken it)
# The swap ledger (P3-DEX decision 3, Pavel 2026-10-16: "if AM can re-use it it's great"): one JSON line per transaction signed
# for the wallet, appended before it goes out. The asset manager books the wallet's fills from the chain and tags them by it
# (the chain can't tell HMB's swaps from manual ones, and the public node can't give a transaction's hash from its nonce).
# Contract v1 -> wiki P3-DEX Overview §The swap ledger. Beside P1's records; `.jsonl`, so no `*.json` glob takes it.
SWAP_LEDGER_PATH = os.path.expanduser("~/p2_dex/swaps.jsonl")
WARN_EVERY_S = 30.0               # a transaction's trouble (an unreadable receipt) is said at most this often
PRICE_SLACK = Decimal("1e-6")     # the limit check's relative slack: a strategy's limit and the book's levels are the same
                                  # prices read through float and Decimal, which differ in the last digits
# The room a swap's minimum output leaves past its limit price (Pavel 2026-10-15: "build it with 0.3%"). arb_l's limit is the
# deepest level its scan touched, so an order inside one level had a minimum equal to the book's own output, and any adverse
# tick between the book's block and the swap's (the WBNB/USDT hop moves in about half of all blocks) made it revert, leaving
# its CEX leg alone: 21-26% of such legs never landed, even with the bundle's retries (audit 2026-10-15, finding 2). Ticks
# go both ways, so on average the fill is still the book's; this is only how far one may slip instead of failing.
# 1% since Pavel 2026-10-17 (was 0.3%): the minimum is only a floor (a swap fills at the pool's own price at inclusion), and
# the DEX legs that lost their races on 2026-10-16 were 0.5-4.4% past the 0.3% (ALEO's 20:02 buy, +0.74%, fills at 1%)
SWAP_SLIPPAGE = Decimal("0.01")
