"""P2-DEX `bsc` connector (build step 3, 2026-10-14): BSC's SPEC for the EVM core (hummingbot/connector/utilities/evm_dex,
2026-10-22): every chain fact and knob the core reads (as `C`), for this chain. The core's own generic knobs (the rungs, the
switch, re-election) live with its code (evm_dex_quoter)."""
import os
from decimal import Decimal

EXCHANGE_NAME = "bsc"
DEFAULT_DOMAIN = "bsc"
HBOT_ORDER_ID_PREFIX = "BSC-"
MAX_ORDER_ID_LEN = 32
ERROR_PREFIX = "Bsc"              # an order's error type: BscSwapDropped, BscSwapReverted, BscSwapNoFill
P1_CHAIN = "BSC"                  # P1's name for the chain: its payloads' `chain`, its row of P1's quoter CHAINS
CHAIN_ID = 56                     # legacy (type 0) EIP-155 transactions (evm_dex_tx.build_tx)
# The payload files the watcher writes (P1's records of each traded CA, `<ca>.json`): evm_dex_pool_list.PoolList
DATA_DIR = os.path.expanduser("~/p2_dex")

QUOTE = "USDT"                    # every book is priced along its pools' paths to USDT (Q5)
# arb_l keeps 25 USDT of a buy venue's balance aside (arbitrage.pyx `buy_quote_balance = available - 25.0`, a legacy CEX rule
# kept by Pavel 2026-10-15). The buy ladder ends at the wallet's USDT less this: the most arb_l can spend there
ARB_QUOTE_RESERVE = Decimal(25)
NATIVE_ASSET = "BNB"              # gas
USDT = "0x55d398326f99059ff775485246999027b3197955"         # BSC-USD, 18 decimals
USDT_DECIMALS = 18
WRAPPED = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"      # WBNB: native BNB and WBNB are one token for a route
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
SEL_ETH_BALANCE = "4d2301cc"      # Multicall3.getEthBalance(address)
SEL_BLOCK_NUMBER = "42cbb15c"     # Multicall3.getBlockNumber(): the block a balance read saw
SEL_BALANCE_OF = "70a08231"       # ERC-20 balanceOf(address)

# Every request has a timeout (audit 2026-10-15: aiohttp takes timeout=None as no timeout at all, and none was ever set,
# so one stalled connection could hold the book poll, or the wallet's turn, until TCP gave up)
RPC_TIMEOUT_S = 2.0               # a request to the book's node (the book poll, the balances, check_network): BlockRazor runs
                                  # 20 CAs' call in ~0.06 s from myserver (BlockPI ~0.2 s)
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
SWAP_GAS_MAX_NATIVE = Decimal(SWAP_GAS_LIMIT * GAS_PRICE_WEI).scaleb(-18)   # the most one swap can cost: below it, nothing trades
APPROVED_AT_LEAST = 1 << 95       # an allowance this large counts as approved (the approvals are for the maximum amount;
                                  # 2^95: a token that caps an allowance at 96 bits, as RAD / COMP / UNI store a max approve
                                  # as 2^96 - 1, reads as approved; P2-DEX #rad-blacklisted-pool, 2026-10-30)
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
# WIP values (Pavel 2026-10-14: "cadence 1 second is fine for starters ... the rest relaxed, we will tune it later").
# POLL_S: the cadence without the block clock, and the data source's idle wait. One poll a block since 2026-10-19, sent when
# the status node's head shows the block (evm_dex_feed._poll_loop, _head_wait); BLOCK_S apart when it has to go unaligned
POLL_S = 0.45

# The DEX book: one poll a block (Pavel 2026-10-19: "change our DEX side polling to be every block instead of every 1s ... we
# should always have current block locally before calculation and broadcast"), sent as soon as SWAP_STATUS_URL's head shows
# the block (Pavel 2026-10-19: "Move the book to BlockRazor if it's the fastest way we can get quotes, use (B) trigger if it's
# best and robust, clock fix if needed"): the head read every HEAD_POLL_S from HEAD_START_S after the block's slot on the
# block clock. The book is quoted on the node bsc_rpc_url names: BlockRazor's free RPC, the status node, so the head and the
# call see the same node. Measured from myserver (2026-10-19, block_trigger_probe.py, 15 min, 11,982 calls; wiki
# p2-dex-book-sources): the book in hand p50 0.183 s / p90 0.21-0.22 s after its block for 20 CAs (0.147 / 0.178 for today's
# 2), the first answer at the block 100%; BlockPI at +240 ms 0.437 / 0.494, at its best offset 0.325 / 0.490
HEAD_START_S = 0.06               # BlockRazor's head shows a block p10 0.069 s after its timestamp (p50 0.129, p99 0.19-0.21)
HEAD_POLL_S = 0.02
HEAD_LATE_S = 0.35                # a block the head shows later than this after its slot: the grid moved, the clock is read
                                  # again. A missed slot (~59 a day, 2026-10-19) brings one block 1.0 s after its parent and
                                  # every later one 0.55 s off the grid; its block shows ~0.68 s after the old slot
HEAD_GIVE_UP_S = 0.9              # no block by then: the poll goes anyway, and the clock is read again
HEAD_READ_TIMEOUT_S = 0.25        # one head read (BlockRazor ~8 ms away); a slower or failed one counts as not there yet
POLL_RETRIES = 1                  # a poll answered from the block before (the book node behind the head) is asked again at
                                  # once, this many times at most for a block
# The blocks' times: one block's millisecond timestamp (its header's timestamp + mixHash) on SWAP_STATUS_URL places every
# block, BLOCK_S apart. Read again every ANCHOR_S, at once when a poll's block disagrees with it, and when the head shows a
# block late (HEAD_LATE_S) or not at all (HEAD_GIVE_UP_S). Without it the feed polls every POLL_S, unaligned
ANCHOR_S = 60.0
BUNDLE_BLOCKS = 4                 # valid for the 4 blocks after the head at the send (~1.8 s; Pavel 2026-10-17, was 12). Each of
                                  # the 42 swaps landed by then landed within 3 blocks of the head at its send (+1: 6, +2: 30,
                                  # +3: 6), so blocks +4..+12 only kept a swap that lost its race holding the wallet's one turn
                                  # (~6.5 s from the order to "dropped"; ~2 s now: it ends as soon as SWAP_STATUS_URL has its
                                  # last block, bsc_feed._landed). The bundle's last block, not the swap's 5 s deadline
                                  # (bsc_swap.DEADLINE_S), is the swap's expiry
# The wallet's nonce (the lease's too: the asset manager sends on this node, so its pending transactions show here) and an
# approval's head: BNB's public seed, 3 ms from myserver (BlockPI is ~88 ms), free. The book is quoted on BlockRazor
# (bsc_rpc_url, since 2026-10-19; it was BlockPI).
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

# The chain's DEX families and the contracts the wallet may pay (the core: evm_dex_pool_list, evm_dex_swap)
# The quote routes an official router can execute. P1's own-model pools (routerless pairs, unlisted deployments) have no
# router, so they stay out for now (decision 2).
ROUTED_KINDS = frozenset(("v2 router", "QuoterV2", "official quoter", "Algebra quoter"))

# A pool's DEX family, from its quote route. One router call chains pools of one family only, so a counter's hop has to be
# found inside its pool's family (Q5). PancakeSwap's and Uniswap's Universal Routers each chain their own v2, v3 and
# v4 / Infinity pools; any other route (a fork's v2 router, another v3 deployment's quoter, an Algebra quoter) is a family
# of its own. The routers themselves are pinned and checked on-chain in step 4.
FAMILY = {
    "0x10ed43c718714eb63d5aa57b78b54704e256024e": "pancakeswap",   # PancakeSwap v2 router
    "0xb048bbc1ee6b733fffcfb9e9cef7375518e25997": "pancakeswap",   # PancakeSwap v3 QuoterV2
    "0xd0737c9762912dd34c3271197e362aa736df0926": "pancakeswap",   # PancakeSwap Infinity CL quoter
    "0xc631f4b0fc2dd68ad45f74b2942628db117dd359": "pancakeswap",   # PancakeSwap Infinity Bin quoter
    "0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24": "uniswap",       # Uniswap v2 router
    "0x78d78e420da98ad378d7799be8f4af69033eb077": "uniswap",       # Uniswap v3 QuoterV2
    "0x9f75dd27d6664c475b90e105573e550ff69437b0": "uniswap",       # Uniswap v4 quoter
}
EXECUTORS = {
    "pancakeswap": {"router": "0xd9c500dff816a1da21a48a732d3498bf09dc9aeb",      # PancakeSwap Infinity Universal Router
                    "permit2": "0x31c2f6fcff4f8759b3bd5bf0e1084a055615c768",     # PancakeSwap's own Permit2 (BSC)
                    "types": ("v2", "v3", "inf-cl")},
    "uniswap": {"router": "0x1906c1d672b88cd1b9ac7593301ca990f94eae07",          # Uniswap Universal Router 2.0.0
                "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",         # Uniswap's Permit2
                "types": ("v2", "v3", "uni-v4")},
}
# The v2 routers of single-router forks the wallet may approve and swap through. Pinned by hand, once each one is checked
# (Pavel 2026-10-14: PancakeSwap + Uniswap only): none yet, so a `route:` pool is left out as "no executor". Every contract the
# wallet ever approves comes from EXECUTORS or this set, never from P1's data.
PINNED_V2_ROUTERS = ()
DEADLINE_S = 5                                # an arb leg not mined within ~5 s is stale (Pavel 2026-10-14). block.timestamp
                                             # has 1-s resolution (0.45 s blocks): a bundle's maxBlockNumber would be the
                                             # block-precise expiry, if the Sending session takes one
