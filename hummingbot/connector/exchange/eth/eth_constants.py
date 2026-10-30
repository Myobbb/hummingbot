"""P2-DEX `eth` connector (2026-10-22): Ethereum mainnet's SPEC for the EVM core (hummingbot/connector/utilities/evm_dex):
every chain fact and knob the core reads (as `C`), for this chain. Measured from myserver and researched on 2026-10-22, and
Pavel's decisions of that day ("go with your recommendations") -> wiki p2-dex-eth (§0.2 the facts, §decisions, §sending,
§executors). The core's own generic knobs (the rungs, the switch, re-election) live with its code (evm_dex_quoter)."""
import os
from decimal import Decimal

EXCHANGE_NAME = "eth"
DEFAULT_DOMAIN = "eth"
HBOT_ORDER_ID_PREFIX = "ETH-"     # the asset manager tags HMB's fills by it (decision 1)
MAX_ORDER_ID_LEN = 32
ERROR_PREFIX = "Eth"              # an order's error type: EthSwapDropped, EthSwapReverted, EthSwapNoFill
P1_CHAIN = "ETH"                  # P1's name for the chain: its payloads' `chain`, its row of P1's quoter CHAINS
CHAIN_ID = 1
TX_TYPE = 2                       # EIP-1559: a max fee and a priority tip (evm_dex_tx.build_tx, evm_dex_feed._gas_price)
CHECK_CHAIN_ID = True             # every node's eth_chainId before the first poll or signature (the runbook's trap)
# P3's contracts on ETH (2026-10-22, the per-chain layout, wiki p2-dex-eth §0.5): contract v1 in its own directory, since one
# address has its own nonce on each chain and 7 CAs share an address with BSC's list
DATA_DIR = os.path.expanduser("~/p2_dex/eth")

QUOTE = "USDT"                    # every book is priced along its pools' paths to USDT (decision 6)
ARB_QUOTE_RESERVE = Decimal(25)   # arb_l's buy-side reserve (arbitrage.pyx), as on BSC
NATIVE_ASSET = "ETH"              # gas
USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"         # Tether USD: 6 decimals; its approve reverts non-zero -> non-zero
USDT_DECIMALS = 6                                           # (the connector only ever approves 0 -> max, once)
WRAPPED = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"      # WETH: native ETH and WETH are one token for a route
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
SEL_ETH_BALANCE = "4d2301cc"      # Multicall3.getEthBalance(address)
SEL_BLOCK_NUMBER = "42cbb15c"     # Multicall3.getBlockNumber(): the block a balance read saw
SEL_BALANCE_OF = "70a08231"       # ERC-20 balanceOf(address)

RPC_TIMEOUT_S = 2.0               # a request to the book's node (BlockRazor ETH: P2's 22-CA call in ~0.05 s from myserver)
READ_TIMEOUT_S = 1.0              # a read of the swap lifecycle (SWAP_READ_URL, SWAP_STATUS_URL)
# A book of block N is replaced when N+1 shows: 12 s + its publishing delay (blocks show p10 1.5 / p50 1.9 / p90 3.5 / max
# 5.1 s into their slot, 10-22), ~12 +/- 2 s and at most ~17 s. So every on-time book stays on show; during a missed slot
# (0.4% of slots) the book goes empty until the next block shows (conservative)
STALE_S = 18.0
FIRST_POLL_WAIT_S = 20.0          # how long a pair's first book waits for the first poll before it starts empty
SUMMARY_EVERY_S = 300.0

# The kill switch (as BSC's): on since Pavel's first round trip proved the send path end to end (the runbook's phase 6:
# 2026-10-22, $5 AAVE bought and sold, 6/6 transactions in the next block)
TRADING_ENABLED = True

SWAP_GAS_LIMIT = 400_000          # the most a swap may use (unused gas is not paid): one hop ~108-134k, two ~223k on ETH
APPROVE_GAS_LIMIT = 100_000       # an ERC20 approve or a Permit2.approve (~46-60k)
# The fee (EIP-1559; Pavel 2026-10-22, decision 11: a fixed tip, no spike chasing): the tip about the blocks' per-block median
# (0.046 gwei on 10-22), the max fee twice the newest base fee plus the tip, held to the cap.
# The cap (Pavel 2026-10-30): a swap goes out in any market up to ~$5 of gas, with no outbidding. One constant, no USD math at
# the send: $5 / (ETH ~$2,200 x ~250k gas; the swaps used 218-276k) = ~9.09 gwei. Re-derive it when ETH's price moves a lot.
# On 10-29 the old 3 gwei cap refused 333 sends while the base fee went 0.16 -> 140 gwei; 32 of them were under this one.
GAS_CAP_USD = 5
NATIVE_USD_APPROX = 2_200
SWAP_GAS_TYPICAL = 250_000
PRIORITY_FEE_WEI = 50_000_000     # 0.05 gwei
MAX_FEE_CAP_WEI = GAS_CAP_USD * 10 ** 18 // (NATIVE_USD_APPROX * SWAP_GAS_TYPICAL)   # ~9.09 gwei: ~$5 a swap
SWAP_GAS_MAX_NATIVE = Decimal(SWAP_GAS_LIMIT * MAX_FEE_CAP_WEI).scaleb(-18)   # the most one swap can cost (~0.0036 ETH): below
                                                                              # it, nothing trades
APPROVED_AT_LEAST = 1 << 95       # an allowance this large counts as approved (the approvals are for the maximum amount;
                                  # 2^95: a token that caps an allowance at 96 bits, as RAD / COMP / UNI store a max approve
                                  # as 2^96 - 1, reads as approved; P2-DEX #rad-blacklisted-pool, 2026-10-30)
APPROVE_RETRY_S = 300.0
APPROVE_RETRY_MAX_S = 3600.0
# An order under this notional is refused (the connector's trading rule; the strategies' own floor, arb_l's min_order_usd,
# applies anyway). 0, as BSC (Pavel 2026-10-22: "tx fee on eth is currently around $0.08 so we don't have to go with $100 min
# ... keep same minimum as BSC side for now, later we might adjust"); decision 10 had it at $100
MIN_NOTIONAL_USD = Decimal(0)

# Sending (2026-10-22: the research and the no-funds probe, wiki p2-dex-eth §sending; phase 3 measures it with Pavel's live
# 0-value sends before it is final). No top builder takes a block range: a single-transaction bundle names its one block, and
# goes to every builder at once (Titan 52%, Quasar 21%, BuilderNet 15%, Eureka 7.5%, bombora 2% of MEV-Boost blocks: ~97%);
# the ones that want it get an X-Flashbots-Signature by the feed's identity (a key derived from the wallet key: decision 11).
# A builder takes a bundle before it simulates it, so the chain tells the outcome; a transaction that would revert is not
# included (nothing paid); no public copy, no RPC that shares with searchers
SENDER = "bundle_per_block"
BUILDERS = ("https://ap.rpc.titanbuilder.xyz", "https://direct-ap.buildernet.org", "https://rpc.eurekabuilder.xyz",
            "https://rpc.quasar.win", "https://ap-rpc.bombora.build")       # 3 / 4 / 2 / 181 / ~190 ms from myserver
SIGNED_BUILDERS = ("https://direct-ap.buildernet.org", "https://rpc.eurekabuilder.xyz", "https://rpc.quasar.win")
BLOCK_S = 12.0                    # ETH's slot: a block's timestamp is its slot's start
POLL_S = 12.0                     # the cadence without a block yet, and the data source's idle wait
# One poll a block, on the status node's head, on the slot clock (evm_dex_feed._slot_poll_loop): the head read every
# HEAD_POLL_S from HEAD_START_S after the next block's slot (the first blocks show ~1.1-1.5 s into their slot); no block by
# HEAD_GIVE_UP_S after its slot's start: the slot was missed, the next one is waited for
CLOCK = "slot"
HEAD_START_S = 1.0
HEAD_POLL_S = 0.05
HEAD_GIVE_UP_S = 11.0
HEAD_READ_TIMEOUT_S = 0.5
POLL_RETRIES = 1                  # a poll answered from the block before is asked again at once, this many times at most
LEASE_STEP_S = 3.0                # the lease step (its heartbeat) while a block is awaited: P3 takes 10 s of silence for no feed
# One try per block (Pavel 2026-10-22, decision 8): a bundle names the next block; one not in it is dropped when that block
# shows (its nonce stays free), and arb_l decides again on that block's book. The swap's deadline is that block's own
# timestamp (block-precise on ETH: DEADLINE_FROM_BLOCK)
BUNDLE_BLOCKS = 1
DEADLINE_FROM_BLOCK = True
DEADLINE_S = 12                   # only without DEADLINE_FROM_BLOCK
# That block's book is not held back (P2-DEX finding 7, Pavel 2026-10-23): a dropped bundle's last block came after the
# failure, so arb_l may fire again on its book at once, for the block after (evm_dex_exchange._after_failure). A refusal or a
# revert still waits for a newer block. bsc keeps the newer-block rule (0.45 s blocks, a 4-block window)
REFIRE_ON_DROP_BLOCK = True
# The book's node, the status node and the nonce's node: BlockRazor's ETH RPC (decision 2: first to show a block 19 / 25 and
# 8 / 15, receipts with its head 25 / 25, the call in ~0.05 s, 0 RU, eth_call cap 50M; it answers a nonce pinned to a block
# and refuses one it doesn't have). The nonce's node must be the one the asset manager sends ETH transfers on (P3, §0.5)
SWAP_READ_URL = "https://eth.blockrazor.xyz"
SWAP_STATUS_URL = "https://eth.blockrazor.xyz"
RECEIPT_POLL_S = 0.1              # a transaction out is looked at this often (12 s blocks: no need for BSC's 25 ms)
SEND_TIMEOUT_S = 2.0
SWAP_LEDGER_PATH = os.path.expanduser("~/p2_dex/eth/swaps.jsonl")     # P3-DEX's contract v1, ETH's own (§0.5)
WARN_EVERY_S = 30.0
PRICE_SLACK = Decimal("1e-6")
SWAP_SLIPPAGE = Decimal("0.01")   # 1%, as BSC (decision 8)

# The chain's DEX families and the contracts the wallet may pay (decision 4: Uniswap's Universal Router + Permit2 only, to
# start: 30 of the 33 pools the first 22 reported CAs trade on). Every address from Uniswap's own deployment lists, pinned
# 2026-10-22 (wiki p2-dex-eth §executors) and proven by simulation on live pools before it is used
ROUTED_KINDS = frozenset(("v2 router", "QuoterV2", "official quoter"))
FAMILY = {
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap",   # Uniswap v2 Router02
    "0x61ffe014ba17989e743c5f6cb21bf9697530b21e": "uniswap",   # Uniswap v3 QuoterV2
    "0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203": "uniswap",   # Uniswap V4Quoter (Jan 2025: the one P1 quotes on)
    "0x99bd42b2b22e531f283b7f8a86dff631eb16a9d1": "uniswap",   # Uniswap V4Quoter (Uniswap's current lists, if P1 moves)
}
EXECUTORS = {
    "uniswap": {"router": "0x23617e59a5925b2a4bf75d73ff6711cd0b29de85",          # Uniswap Universal Router 2.1.2 (802fe4c1)
                "permit2": "0x000000000022d473030f116ddee9f6b43ac78ba3",         # Uniswap's Permit2
                "types": ("v2", "v3", "uni-v4"),
                "encoding": "ur-2.1"},                                            # minHopPriceX36 in the swap inputs
}
PINNED_V2_ROUTERS = ()            # none yet (ShibaSwap v1, Sushi v2: when the data asks, Pavel's call)
