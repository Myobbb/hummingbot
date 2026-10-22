import asyncio
import json
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
from bidict import bidict
from eth_account import Account
from eth_account.signers.local import LocalAccount

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_wrapper
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.connector.utilities.evm_dex.evm_dex_feed import FeedClient, decode_wallet, mask, merged, wallet_plan  # noqa: F401
from hummingbot.connector.utilities.evm_dex.evm_dex_order_book_data_source import EvmDexAPIOrderBookDataSource
from hummingbot.connector.utilities.evm_dex.evm_dex_p1_quoter import word
from hummingbot.connector.utilities.evm_dex.evm_dex_pool_list import PoolList
from hummingbot.connector.utilities.evm_dex.evm_dex_quoter import EvmDexQuoter
from hummingbot.connector.utilities.evm_dex.evm_dex_tx import receipt_problem
from hummingbot.connector.utilities.evm_dex.evm_dex_user_stream_data_source import EvmDexAPIUserStreamDataSource


class EvmDexTradingDisabled(Exception):
    """an order while the chain's TRADING_ENABLED is False (the kill switch)"""


class EvmDexOrderRefused(Exception):
    """an order the connector can't turn into a swap: no fresh book, no route, no approval, no wallet key, another swap out"""


class EvmDexSendRefused(EvmDexOrderRefused):
    """every builder refused the swap's bundle: nothing went out, and its nonce is free again (read anew by the feed)"""


class EvmDexExchange(ExchangePyBase):
    """P2-DEX build step 3 (2026-10-14; the EVM core since 2026-10-22): a DEX connector, a DEX leg arb_l reads like any venue,
    on any EVM chain. One connector per chain (`bsc`, `eth`), each a thin subclass in exchange/<k>/ that names its spec (C: the
    chain's constants module, every chain fact), the feed's code for its child (SOURCES: evm_dex_feed.sources_for(C)), its data
    sources (their loggers named for the chain) and its error classes (TradingDisabled, OrderRefused, SendRefused: the names in
    its log lines); its constructor keeps the chain's config fields (<k>_rpc_url, <k>_wallet_address, <k>_private_key). The
    text below was written for `bsc`, the first chain; it holds for every chain by its spec.

    - The feed process (bsc_feed, Pavel 2026-10-15: "build the separate DEX feed process ... DEX related processes ideally
      should not affect CEX speed and recency"): everything done with the chain runs in a child process, never on HMB's
      event loop, which serves every CEX book and order. That is the book poll, the wallet's reads, a swap's or approval's
      build, signing and send, the nonce, and following each transaction to its end. This side keeps the framework's state
      (orders, balances, the wallet's one turn) and exchanges one small message per poll and per transaction with the child.
      It starts the child when a pair has a CA to quote, and stops it when none is left and nothing is out (idle: no
      process).
    - Books: the child polls one eth_call a block (evm_dex_quoter.EvmDexQuoter; per block since 2026-10-19,
      evm_dex_feed._poll_loop): per
      traded CA one book per side, from the
      depth-elected pool, with a $100 watch on its other pools, priced in USDT along each pool's path (Q5, Q7). A book side
      the wallet can't pay into its executor is not shown, so no strategy fires a CEX leg against a DEX leg that would be
      refused.
    - Pools: P1 hands over each candidate's pool records through the page -> watcher path; the watcher writes them to
      ~/p2_dex/<ca>.json (bsc_pool_list.PoolList). A trading pair is P1's ticker + "-USDT", bound to the CA its strategy's
      leg names (`bsc:MUBARAK-USDT:0x5c85…`, bind_ca by the orchestrator); an unbound pair has no book.
    - Balances: the wallet's BNB, USDT and traded tokens (and its approvals), read inside each book poll's one eth_call at
      the books' block, with no request of their own (Pavel 2026-10-15). Each swap's receipt is booked on them the moment
      it ends (a strategy sees a fill in the balances when its event fires).
    - Trading rules: from P1's records (the CA's decimals and reference price). No minimum notional: a DEX has none.
    - Orders (step 4): one exact-input swap per order along the route that priced its book (bsc_swap), signed by the wallet
      (bsc_tx); the transaction hash is the exchange order id; the receipt gives the fill (what really moved at the wallet,
      the gas in BNB) or a revert (a failed order). While self.C.TRADING_ENABLED is False every order is refused and the
      connector never reports ready, so the orchestrator never ticks a strategy with a bsc leg.
    - Sending (the Sending session, Pavel 2026-10-14): every transaction goes as a one-transaction bundle to both builders
      (self.C.BUILDERS), valid for BUNDLE_BLOCKS blocks: it lands by then or never (nothing paid, its nonce free again), and
      one that would revert is not included. A transaction out is followed on SWAP_STATUS_URL (the head, its receipt, the
      nonce at its last block); the wallet's nonce for a send is read on SWAP_READ_URL (the asset manager's node).
    - Placing a swap is ONE request, its bundle (Pavel 2026-10-15: an arb leg's placement must not wait on anything else):
      its last block comes from its book's block and age (no head read), and the wallet's nonce is known beforehand, read off
      the order path. Here an order is one message to the child and one back. It no longer yields to the CEX orders of its
      tick (2026-10-15 to 10-17): under DEX first its arb's CEX leg waits for its fill, so it is the leg the hedge waits on.
    - A swap venue (`is_swap_venue`, 2026-10-14): an order is one immediate swap, never resting, never cancelled; the limit
      price plus the SWAP_SLIPPAGE room is the worst average price accepted (no price grid). ONE swap at a time per
      wallet: an order takes the wallet's turn when it is placed and every available balance reads 0 until it ends, so
      the strategies (arb_l, the balancer), which size by it, place nothing here meanwhile. The child follows each swap out
      and says when it ended: FILLED with what it really got, or FAILED (reverted, or not included by its last block).
    - A swap's fill comes first to whoever waits on it (on_filled, Pavel 2026-10-17: "issue CEX order as fast as possible
      after we confirmed that DEX side filled ... all the dex housekeepings and recordings can happen after we sent CEX
      side"): arb_l's DEX first sends its CEX leg from there, before any of this side's work on the swap's end (_ended).
    - Approvals are read with the balances. A missing approval is sent by the connector itself (trading on), one at a time
      in the wallet's turn, to the pinned executors' contracts only (bsc_swap).
    - The wallet, like a CEX key pair: `bsc_wallet_address` names it, `bsc_private_key` (stored encrypted by `connect bsc`)
      must control it. HMB keeps the key in its signer and hands it to the child at its start, on its stdin (never in its
      arguments or environment). A key that is not valid, or controls another address, fails the balance read, so
      `connect bsc` reports it and keeps no bad key. No key: books and balances only, never ready.
    The node URL carries its key, and the wallet key is a key: neither reaches a log (_mask)."""

    web_utils = None
    is_swap_venue = True                               # the contract: ExchangePyBase.is_swap_venue
    C = None                                           # the chain's spec: set by the chain's subclass
    SOURCES = None                                     # the feed's code for the chain's child (evm_dex_feed.sources_for(C))
    ORDER_BOOK_DATA_SOURCE = EvmDexAPIOrderBookDataSource
    USER_STREAM_DATA_SOURCE = EvmDexAPIUserStreamDataSource
    TradingDisabled, OrderRefused, SendRefused = EvmDexTradingDisabled, EvmDexOrderRefused, EvmDexSendRefused

    def __init__(self,
                 rpc_url: str,
                 wallet_address: str,
                 private_key: str = "",
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: Optional[str] = None):
        if self.C is None or self.SOURCES is None:
            raise TypeError("an EVM DEX connector is a chain's subclass (its C and SOURCES)")
        domain = domain or self.C.DEFAULT_DOMAIN
        self._rpc_url = (rpc_url or "").strip()
        self._wallet = (wallet_address or "").strip().lower()
        self._account, self._key_problem = self._load_key(private_key)
        self._busy: Optional[str] = None               # the order holding the wallet's one turn (placed, not yet ended)
        self._swaps: Dict[str, dict] = {}              # client order id -> its swap out (from the moment it is signed)
        self._balances_block = 0                       # the block the balances are known at (a read's, or a booked receipt's)
        self._approvals: Dict[tuple, bool] = {}        # (token, router) -> the wallet can pay the token into the router
        self._missing_approvals: List[Tuple[str, bytes]] = []   # the approval calls the last balance read found missing
        self._approved_sent: set = set()               # approval calls included this run (a read behind can't resend them)
        self._approve_retry: Dict[tuple, Tuple[float, float]] = {}   # approval call -> (not before, its last back-off), after a failure
        self._approve_task: Optional[asyncio.Task] = None
        self._approval_id = 0
        self._domain = domain
        self._trading_pairs = list(trading_pairs or [])
        self._trading_required = trading_required
        self._pool_list = PoolList(self.C)
        self._pools_stale = True                       # the pool payloads are read again before the next poll (_reload_pools)
        self._pair_ca: Dict[str, str] = {}             # trading pair -> its bound CA with P1's records (_reload_pools)
        self._caps_dirty = True                        # a balance moved since the ladders' ends were last worked out
        self._cas: Dict[str, str] = {}                 # trading pair -> the CA its leg names (bind_ca: the orchestrator)
        self._feed = FeedClient(self._on_feed, self._log, self._mask, self.SOURCES, self.C.EXCHANGE_NAME)
        self._ledger_by = "strategy"                   # whose transactions the swap ledger records: the DEX manual's "manual"
        self._sent: Dict[str, Any] = {}                # what the feed was last told: "cas", "caps"
        self._feed_stats: Dict[str, int] = {}          # the feed's quoter counters, from its last poll
        self._reported: Dict[str, str] = {}           # what was last said about the payloads (a change is logged once)
        self._session: Optional[aiohttp.ClientSession] = None
        self._idle = False                             # the last poll found no DEX pair to quote (and made no call)
        self._fresh_after: Dict[str, int] = {}         # trading pair -> the newest block we had when its DEX order failed: its
                                                       # book shows again only from a newer block (DEX first, 2026-10-17/19)
        self._on_filled: Dict[str, Callable[[str, Decimal], None]] = {}   # order id -> who takes its fill first (on_filled)
        self._balances_read = False
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ---------------------------------------------------------------- identity
    @property
    def name(self) -> str:
        return self.C.EXCHANGE_NAME

    @property
    def authenticator(self):
        return None

    @property
    def rate_limits_rules(self):
        return []

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return self.C.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return self.C.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return ""

    @property
    def trading_pairs_request_path(self) -> str:
        return ""

    @property
    def check_network_request_path(self) -> str:
        return ""

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    @property
    def trading_enabled(self) -> bool:
        """False: the connector is never ready, so its strategies are never ticked; the orchestrator's all-connectors-ready
        gate (its one-time init) skips a connector that says so"""
        return self.C.TRADING_ENABLED

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "symbols_mapping_initialized": self.trading_pair_symbol_map_ready(),
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": not self.is_trading_required or self._balances_read,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self.is_trading_required else True,
            "wallet_key": self._account is not None,          # a key that controls the wallet: no key, no trading
            "trading_enabled": self.trading_enabled,          # the kill switch (the spec's TRADING_ENABLED)
        }

    # ---------------------------------------------------------------- the wallet
    def _load_key(self, private_key: str) -> Tuple[Optional[LocalAccount], str]:
        """-> (the wallet's signer, "") or (None, why). The key is never kept as text, printed or put in a message: only
        the signer holds it (and the feed process's own, from its start). No key: (None, "")."""
        private_key = (private_key or "").strip()
        if not private_key:
            return None, ""
        try:
            account = Account.from_key(private_key)
        except Exception:
            return None, f"{self.C.EXCHANGE_NAME}_private_key is not a valid private key"
        if account.address.lower() != self._wallet:
            return None, (f"{self.C.EXCHANGE_NAME}_private_key does not control "
                          f"{self._wallet or self.C.EXCHANGE_NAME + '_wallet_address (empty)'}")
        return account, ""

    # ---------------------------------------------------------------- the node (here only for `connect <k>` and check_network)
    def _mask(self, text: Any) -> str:
        return mask(text, self._rpc_url, self._account.key.hex().removeprefix("0x") if self._account is not None else None)

    def _log(self, level: str, text: str):
        """a line from the feed process (or about it), on HMB's logger"""
        getattr(self.logger(), level if level in ("debug", "info", "warning", "error") else "info")(text)

    async def _post(self, body: bytes) -> bytes:
        """one JSON-RPC POST to the book's node (<k>_rpc_url): `connect <k>`'s balance read and check_network's, the only node
        requests HMB's side makes (the feed process has its own session)"""
        if not self._rpc_url:
            raise RuntimeError(f"no {self.C.P1_CHAIN} RPC URL: connect {self.C.EXCHANGE_NAME}")
        return await self._post_to(self._rpc_url, body)

    async def _post_to(self, url: str, body: bytes, timeout: Optional[float] = None) -> bytes:
        """one JSON-RPC POST on the connector's kept-alive session. Every request has a timeout, RPC_TIMEOUT_S unless the
        caller gives its own (aiohttp takes timeout=None as no timeout at all)"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.C.RPC_TIMEOUT_S))
        try:
            async with self._session.post(url, data=body, headers={"Content-Type": "application/json"},
                                          timeout=aiohttp.ClientTimeout(total=timeout or self.C.RPC_TIMEOUT_S)) as r:
                return await r.read()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise RuntimeError(self._mask(f"{type(e).__name__}: {e}")) from None

    # ---------------------------------------------------------------- network
    async def start_network(self):
        self._throttler.label = self.name
        await self.stop_network()
        self._reload_pools()
        self._orderbook_ds.reset()
        self.order_book_tracker.start()            # its data source's loop starts the feed process (_poll_books)

    async def stop_network(self):
        await super().stop_network()
        if self._approve_task is not None:
            self._approve_task.cancel()
        self._approve_task = None
        await self._feed.stop()                    # a swap still out is followed again by the next start's feed
        self._sent = {}
        self._idle = False                         # a restart's first network check reads the node again
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def check_network(self) -> NetworkStatus:
        """a fresh poll answers by itself; so does an idle connector (no DEX pair to quote: nothing is polled, and the first
        poll when a pair comes checks the node itself); otherwise one eth_blockNumber (the poll only runs after
        start_network)"""
        if self._idle or time.time() - self._orderbook_ds.last_ok < self.C.STALE_S:
            return NetworkStatus.CONNECTED
        try:
            ans = json.loads(await self._post(b'{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'))
            return NetworkStatus.CONNECTED if "result" in ans else NetworkStatus.NOT_CONNECTED
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    # ---------------------------------------------------------------- P1's pools: the CAs quoted, symbol map, trading rules
    def bind_ca(self, trading_pair: str, ca: str):
        """the CA a strategy's leg names (`control create … bsc:MUBARAK-USDT:0x5c85…`, kept in test_multi.yml): the orchestrator
        binds each bsc leg before its strategy is built, at start and at create. A pair is P1's ticker + "-USDT"; its CA is the
        token, so two CAs sharing a ticker can't be confused. The orchestrator refuses a second strategy naming another CA
        for a bound pair"""
        self._cas[trading_pair] = ca.lower()           # the next poll reads the payloads and has the feed quote it
        self._pools_stale = True

    def unbind_ca(self, trading_pair: str):
        """the orchestrator, when no strategy trades the pair any more"""
        self._cas.pop(trading_pair, None)
        self._pools_stale = True

    def limit_for_worst_price(self, is_buy: bool, worst: Decimal) -> Decimal:
        """the limit to give an order so that its worst accepted AVERAGE price is exactly `worst`: the swap takes its limit and
        the SWAP_SLIPPAGE room on top (bsc_feed._swap_for), so the room is folded in here. arb_l's DEX first gives `worst` as
        the price at which the arb still makes the strategy's min_profitability against its CEX leg (Pavel 2026-10-17:
        "keep it at minimum profitability that is already set by the strategy"), in place of the book's limit and the room"""
        room = self.C.SWAP_SLIPPAGE
        return worst / (1 + room) if is_buy else worst / (1 - room)

    def on_filled(self, order_id: str, callback: Callable[[str, Decimal], None]):
        """`callback(order_id, the base filled)` runs the moment the feed says the swap filled, before any of this side's
        work on its end: its receipt on the balances, the fill and completion events (and the trade recorder on them), its
        line, the wallet's turn (Pavel 2026-10-17: "issue CEX order as fast as possible after we confirmed that DEX side
        filled"). arb_l's DEX first sends its CEX leg from there. Forgotten when the order ends any other way"""
        self._on_filled[order_id] = callback

    def _ca_for_pair(self, trading_pair: str) -> Tuple[Optional[str], str]:
        base, _, quote = trading_pair.partition("-")
        if quote.upper() != self.C.QUOTE:
            return None, f"quote {quote}: {self.C.EXCHANGE_NAME} books are priced in {self.C.QUOTE}"
        ca = self._cas.get(trading_pair)
        if not ca:
            return None, (f"no CA bound: a {self.C.EXCHANGE_NAME} leg is created as "
                          f"{self.C.EXCHANGE_NAME}:{trading_pair}:0x<CA>")
        if ca not in self._pool_list.cas:
            return None, f"no P1 records for {ca} in {self._pool_list.data_dir} (the watcher writes them from the page)"
        return ca, ""

    def _bound_cas(self) -> List[str]:
        """the CAs this connector's pairs are bound to that have P1's records: what the feed quotes"""
        return sorted(set(self._pair_ca.values()))

    def _reload_pools(self):
        """the payloads in the pool directory, read again: at a start, when a pair is bound, unbound or added, and when the
        feed process saw a file change. The trading rules and the symbol map follow them; the feed quotes the bound CAs
        (_poll_books)"""
        self._pools_stale = False
        try:
            self._pool_list.load()
        except Exception as e:
            self.logger().warning(f"{self.C.EXCHANGE_NAME}: the pool payloads could not be read: {e}")
            return
        self._pair_ca = {}
        for trading_pair in self._trading_pairs:
            ca, why = self._ca_for_pair(trading_pair)
            self._report(trading_pair, f"CA {ca}, {len(self._pool_list.pools(ca))} tradable pools" if ca else f"no book: {why}")
            if ca:
                self._pair_ca[trading_pair] = ca       # the per-poll work reads this map, not the payloads (HMB's loop)
        self._caps_dirty = True
        bound = set(self._bound_cas())
        for (pool, ca), why in self._pool_list.left_out.items():
            if ca in bound:
                self._report(f"{ca}/{pool}", f"left out: {why}")
        for path, why in self._pool_list.bad_files.items():
            self._report(path, f"payload not read: {why}")
        self._set_trading_pair_symbol_map(bidict({tp: tp for tp in self._trading_pairs}))
        self._trading_rules = {tp: rule for tp in self._trading_pairs if (rule := self._trading_rule(tp)) is not None}

    def _report(self, key: str, text: str):
        if self._reported.get(key) != text:
            self._reported[key] = text
            self.logger().info(f"{self.C.EXCHANGE_NAME}: {key}: {text}")

    def _trading_rule(self, trading_pair: str) -> Optional[TradingRule]:
        ca, _ = self._ca_for_pair(trading_pair)
        if not ca:
            return None
        m = self._pool_list.cas[ca]
        step = Decimal(1).scaleb(-min(int(m["decimals"]), 8))
        price_step = Decimal(1).scaleb(Decimal(str(m["reference_usd"])).adjusted() - 6)     # 7 significant digits
        # No minimum notional on BSC: a DEX has none (Pavel 2026-10-14). The strategies' own floor (min_order_usd) applies to
        # every order but a sell-to-zero sweep, which therefore sells any residue here down to the step. A chain whose gas is
        # worth counting sets one (the spec's MIN_NOTIONAL_USD; ETH $100, Pavel 2026-10-22: gas at most 0.24% at p90)
        return TradingRule(trading_pair=trading_pair, min_order_size=step, min_price_increment=price_step,
                           min_base_amount_increment=step, min_notional_size=getattr(self.C, "MIN_NOTIONAL_USD", Decimal(0)))

    # ---------------------------------------------------------------- the feed process
    async def _poll_books(self) -> Optional[dict]:
        """the next poll's books and wallet, from the feed process: HMB's loop only takes its message, puts the wallet's
        balances on HMB's and hands the books to the data source. The feed is started when a pair has a CA to quote (or a
        transaction is still out, to follow it), and stopped when neither is left: idle, no process, no call (Pavel
        2026-10-15: "obviously we should stop polling when no dex strategy is left"). None while there is nothing to quote"""
        if self._pools_stale:
            self._reload_pools()
        cas = self._bound_cas()
        if not cas and not self._swaps and self._busy is None:
            await self._feed.stop()
            self._sent = {}
            return self._idle_now()
        if self.is_trading_required and self._wallet and self._key_problem:
            self._report("wallet", f"not read: {self._key_problem}")
        if not self._feed.running:
            self._sent, self._caps_dirty = {"cas": cas, "caps": self._ladder_caps()}, False
            await self._feed.start(self._feed_init(cas, self._sent["caps"]))
        self._send_changed("cas", cas)
        if not cas:
            return self._idle_now()                    # the feed only follows what is out
        if self._idle:
            self._idle = False
            self._report("poll", f"polling again: {len(cas)} CA(s) to quote")
        message = await self._feed.next_poll()
        if message[0] == "poll_failed":
            raise RuntimeError(message[1])
        last = message[1]
        self._feed_stats = last.get("stats") or {}
        if last.get("wallet") is not None:
            self._apply_wallet(last["wallet"])
            if not last.get("lease"):                  # leased (another sender holds the wallet): no approval is due
                self._maybe_approve()
        self._send_caps()
        return last

    def _idle_now(self) -> None:
        self._idle = True
        self._report("poll", "idle: no DEX pair to quote, nothing is polled")
        return None

    def _feed_init(self, cas: List[str], caps: Dict[str, dict]) -> dict:
        """what the feed process starts with: the node, the wallet and its key (on its stdin, never in its arguments), what
        to quote, the swaps still out (followed again), the swap ledger with whose transactions it records, and whether it
        holds the wallet lease (P3-DEX: the asset manager's withdrawals; its files beside the ledger)"""
        return {"spec": self.C.__name__, "name": self.C.EXCHANGE_NAME, "rpc_url": self._rpc_url, "wallet": self._wallet,
                "key": bytes(self._account.key) if self._account is not None else None,
                "read_wallet": self.is_trading_required and bool(self._wallet) and not self._key_problem,
                "gate": self.is_trading_required, "data_dir": self._pool_list.data_dir, "poll_s": self.C.POLL_S,
                "builders": list(self.C.BUILDERS), "swap_read_url": self.C.SWAP_READ_URL,
                "swap_status_url": self.C.SWAP_STATUS_URL,
                "cas": cas, "caps": caps, "follow": dict(self._swaps),
                "ledger": self.C.SWAP_LEDGER_PATH, "ledger_by": self._ledger_by,
                "lease": self._ledger_by == "strategy"}   # HMB's feed holds the wallet lease; the DEX manual's never does

    def _send_changed(self, kind: str, value):
        """the feed told only what changed: the CAs to quote, the ladders' ends"""
        if self._feed.running and self._sent.get(kind) != value:
            self._sent[kind] = value
            self._feed.send((kind, value))

    def _send_caps(self):
        """the ladders' ends, worked out again only when a balance moved (a read, a receipt booked) or the CAs changed"""
        if self._caps_dirty and self._feed.running:
            self._caps_dirty = False
            self._send_changed("caps", self._ladder_caps())

    def _on_feed(self, message: tuple):
        """the feed process's messages other than polls and awaited answers"""
        kind = message[0]
        if kind == "log":
            self._log(message[1], message[2])
        elif kind == "signed":                     # it may be out from here: known before its send is answered
            self._swaps[message[1]] = message[2]
        elif kind == "ended":
            self._ended(message[1], message[2], message[3])
        elif kind == "pools_changed":
            self._pools_stale = True
        elif kind == "swap":                       # an answer no placement waits for any more (its task ended)
            if message[2] == "refused":
                self._swaps.pop(message[1], None)
            else:
                self._swaps[message[1]] = message[3]

    def _ladder_caps(self) -> Dict[str, dict]:
        """per traded CA what the wallet can put into one swap, where its book's ladders end (Pavel 2026-10-15: 350 USDT ->
        "100 / 250 / 325 quotes and executables"): {"buy": the USDT less arb_l's ARB_QUOTE_RESERVE (the most arb_l spends),
        "sell": the CA held, raw}. From the TOTAL balances: a swap out reads 0 available, and its book must not collapse
        meanwhile. {} until the first balance read (the quoter then quotes the $100 rung only)"""
        if not self._balances_read:
            return {}
        usdt = float(self._account_balances.get(self.C.QUOTE, Decimal(0)) - self.C.ARB_QUOTE_RESERVE)
        caps = {}
        for trading_pair, ca in self._pair_ca.items():
            held = self._account_balances.get(trading_pair.split("-")[0].upper(), Decimal(0))
            caps[ca] = {"buy": max(usdt, 0.0),
                        "sell": int(held.scaleb(int(self._pool_list.cas[ca]["decimals"])).to_integral_value(
                            rounding=ROUND_DOWN))}
        return caps

    def _books_by_pair(self, last: dict) -> Dict[str, tuple]:
        """trading pair -> (bids, asks) as [[price, qty], ...]: the asks are the CA bought on the DEX ("buy"), the bids sold;
        each side as the feed shows it (merged; empty while the wallet can't pay into its executor). A pair whose DEX order
        failed shows no book until a poll of a newer block than any we had at the failure (_after_failure)"""
        out = {}
        books = last.get("books") or {}
        for trading_pair in self._trading_pairs:
            failed_block = self._fresh_after.get(trading_pair)
            if failed_block is not None:
                if (last.get("block") or 0) <= failed_block:
                    out[trading_pair] = ([], [])           # no newer block yet: its price is the one that just lost
                    continue
                del self._fresh_after[trading_pair]
            book = books.get(self._pair_ca.get(trading_pair)) or {}
            out[trading_pair] = (book.get("sell") or [], book.get("buy") or [])
        return out

    async def _update_trading_rules(self):
        self._reload_pools()

    async def _fetch_trading_rule_for_pair(self, trading_pair: str):
        self._reload_pools()                       # a runtime-added pair: the next poll has the feed quote it

    async def _initialize_trading_pair_symbol_map(self):
        self._set_trading_pair_symbol_map(bidict({tp: tp for tp in self._trading_pairs}))

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        return

    async def _add_trading_pair_to_symbol_map(self, trading_pair: str):
        # set, not mutated: an empty map is answered with a fresh bidict (ExchangeBase.trading_pair_symbol_map)
        self._set_trading_pair_symbol_map(bidict({tp: tp for tp in set(self._trading_pairs) | {trading_pair}}))

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        return []

    # ---------------------------------------------------------------- balances
    def _wallet_tokens(self) -> Dict[str, Tuple[str, int]]:
        """asset -> (token, decimals): USDT, and each bound pair's CA under the pair's base"""
        tokens = {self.C.QUOTE: (self.C.USDT, self.C.USDT_DECIMALS)}
        for trading_pair, ca in self._pair_ca.items():
            tokens[trading_pair.split("-")[0].upper()] = (ca, int(self._pool_list.cas[ca]["decimals"]))
        return tokens

    async def _update_balances(self):
        """one wallet read of its own, the block with it: `connect <k>`'s key check (and HMB's `balance`), where no feed runs.
        A started connector never calls it: its wallet rides in every book poll, in the feed process"""
        if self._key_problem:
            raise ValueError(self._key_problem)       # `connect bsc` shows it and keeps no bad key
        if getattr(self.C, "CHECK_CHAIN_ID", False):
            await self._check_chain_id()               # `connect eth`: a URL of another chain is refused (the feed checks again)
        if not self._wallet:
            return
        plan = wallet_plan(self.C, self._wallet, self._bound_cas(), self._pool_list)
        res = await EvmDexQuoter(self._post, self.C.P1_CHAIN)._call(plan["calls"] + [(self.C.MULTICALL3,
                                                                                       bytes.fromhex(self.C.SEL_BLOCK_NUMBER))])
        ok, ret = res[-1]
        self._apply_wallet(decode_wallet(self.C, plan, res[:-1], word(ret, 0) if ok and len(ret) >= 32 else None,
                                         self._wallet))

    async def _check_chain_id(self):
        """`connect <k>`'s check of <k>_rpc_url: its eth_chainId must be the spec's CHAIN_ID. A pasted URL of another chain
        reads that chain with no error, since one wallet address exists on every EVM chain (the runbook's trap); the feed
        checks every node again before its first poll and signature"""
        ans = json.loads(await self._post(b'{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'))
        if "result" not in ans:
            raise ValueError(self._mask(f"{self.C.EXCHANGE_NAME}_rpc_url: eth_chainId refused: {str(ans.get('error'))[:120]}"))
        got = int(ans["result"], 16)
        if got != self.C.CHAIN_ID:
            raise ValueError(f"{self.C.EXCHANGE_NAME}_rpc_url serves chain {got}, not {self.C.CHAIN_ID} ({self.C.P1_CHAIN})")

    def _apply_wallet(self, w: dict):
        """one wallet read (evm_dex_feed.decode_wallet's) onto the balances and the approvals; an asset whose call failed keeps
        its last value (said once, not every poll). A read older than a receipt already booked on the balances (a node behind
        the one that served the receipt) is dropped, as it would undo the fill.
        A transaction this connector didn't send (a manual send or swap from the wallet, the DEX manual) used its next
        nonce. Every one pays gas, so it shows as the native coin below the booked amount: then the feed forgets the nonce and
        reads it
        anew, off the order path (Pavel 2026-10-15). Only while the wallet's turn is free, so our own swap, whose receipt is
        booked as it ends, never counts; an approval's unbooked gas counts, and costs one more nonce read"""
        block = w.get("block")
        if block is not None and block < self._balances_block:
            return
        booked_native = (self._account_balances.get(self.C.NATIVE_ASSET)
                         if self._balances_read and self._busy is None else None)
        read = [(self.C.NATIVE_ASSET, 18, w["native"])]
        read += [(asset, dec, w["balances"][token]) for asset, (token, dec) in self._wallet_tokens().items()
                 if token in w["balances"]]
        for asset, decimals, raw in read:
            if raw is None:
                self._report(f"balance {asset}", "not read: its last value kept")
                continue
            if f"balance {asset}" in self._reported:
                self._report(f"balance {asset}", "read again")
            amount = Decimal(raw).scaleb(-decimals)
            if self._account_balances.get(asset) != amount:
                self._caps_dirty = True
            self._account_balances[asset] = amount
            self._account_available_balances[asset] = amount
        if booked_native is not None and self._account_balances.get(self.C.NATIVE_ASSET, booked_native) < booked_native:
            if self._feed.running:
                self._feed.send(("nonce_lost",))       # an outside transaction: its nonce is spent, read anew by the feed
        for (token, router), approved in w["approvals"].items():
            if self._approvals.get((token, router)) != approved:      # said when it changes (the text is not built each poll)
                self._report(f"approval {token} -> {router}", "approved" if approved else
                             "MISSING: the wallet can't pay this token into this router (one-time approval transactions)")
        self._approvals.update(w["approvals"])
        self._missing_approvals = list(w["missing"])
        if block is not None:
            self._balances_block = block
        self._balances_read = True
        if not self._gas_ok() or "gas" in self._reported:          # said when short, and once when it is covered again
            self._report("gas", f"{self.C.NATIVE_ASSET} covers a swap's gas again" if self._gas_ok() else
                         f"{self.C.NATIVE_ASSET} {self._account_balances.get(self.C.NATIVE_ASSET, Decimal(0))} can't pay one "
                         f"swap's gas (up to {self.C.SWAP_GAS_MAX_NATIVE}): nothing is available to trade until the wallet is "
                         f"topped up")

    def _maybe_approve(self):
        """after a wallet read: the approvals it found missing go out (_approve), when trading is on, the wallet has a key
        and gas, and no earlier batch is still running. One that was included this run is not sent again, even if a read
        from a node behind still shows it missing; one that failed waits out its back-off (_approve_back_off)"""
        now = time.time()
        todo = [c for c in self._missing_approvals
                if c not in self._approved_sent and self._approve_retry.get(c, (0.0, 0.0))[0] <= now]
        if (todo and self.C.TRADING_ENABLED and self._account is not None and self._gas_ok()
                and (self._approve_task is None or self._approve_task.done())):
            self._approve_task = safe_ensure_future(self._approve(todo))

    def _approve_back_off(self, call: Tuple[str, bytes]) -> float:
        """an approval that failed (reverted, not included, or not sent) is tried again only after a back-off: APPROVE_RETRY_S
        the first time, doubled each time after, at most APPROVE_RETRY_MAX_S (Pavel 2026-10-15). One that can never land then
        can't hold the wallet's turn at every balance read. -> the back-off, in s"""
        last = self._approve_retry.get(call, (0.0, 0.0))[1]
        delay = min(last * 2, self.C.APPROVE_RETRY_MAX_S) if last else self.C.APPROVE_RETRY_S
        self._approve_retry[call] = (time.time() + delay, delay)
        return delay

    async def _approve(self, calls: List[Tuple[str, bytes]]):
        """the missing one-time approvals, sent by the connector itself (Pavel 2026-10-14), one at a time in the wallet's turn,
        each a bundle like a swap, built, sent and followed to its end by the feed process. Every spender is a pinned
        executor's contract (the spec's EXECUTORS, PINNED_V2_ROUTERS), never one from data. One that doesn't land is tried again
        after its back-off (_approve_back_off). The next poll's wallet read (within POLL_S) shows the approved sides"""
        for to, data in calls:
            while self._busy is not None:              # the wallet's one turn: after the swap (or approval) out
                await asyncio.sleep(self.C.RECEIPT_POLL_S)
            turn = f"approval:{to}:{data[:36].hex()}"
            self._busy = turn
            what = self._approval_name(to, data)
            try:
                self.logger().info(f"{self.C.EXCHANGE_NAME}: sending a one-time approval: {what}")
                self._approval_id += 1
                call_id = f"{turn}:{self._approval_id}"
                try:
                    outcome, detail = await self._feed.request(("approve", call_id, (to, data)), ("approval", call_id))
                except RuntimeError as e:              # the feed is not running, or ended meanwhile
                    outcome, detail = "refused", str(e)
                if outcome == "landed":
                    self._approved_sent.add((to, data))
                    self._approve_retry.pop((to, data), None)
                    self.logger().info(f"{self.C.EXCHANGE_NAME}: approved in block {detail}: {what}")
                elif outcome == "refused":
                    delay = self._approve_back_off((to, data))
                    self.logger().warning(f"{self.C.EXCHANGE_NAME}: the approval {what} was not sent: {self._mask(detail)}; it is "
                                          f"tried again in {delay / 60:.0f} min")
                else:
                    delay = self._approve_back_off((to, data))
                    self.logger().warning(f"{self.C.EXCHANGE_NAME}: the approval {what} did not land ("
                                          f"{'reverted' if outcome == 'reverted' else 'not included by its last block'}); "
                                          f"it is tried again in {delay / 60:.0f} min")
            finally:
                if self._busy == turn:
                    self._busy = None

    def _approval_name(self, to: str, data: bytes) -> str:
        """an approval call in words: '<token> -> <spender>' (the pinned contracts and the traded tokens by name)"""
        names = {self.C.USDT: self.C.QUOTE}
        for fam, ex in self.C.EXECUTORS.items():
            names[ex["router"]] = f"{fam} Universal Router"
            names[ex["permit2"]] = f"{fam} Permit2"
        for ca, meta in self._pool_list.cas.items():
            names.setdefault(ca, meta.get("ticker") or ca)
        name = (lambda a: names.get(a.lower(), a.lower()))
        first, second = "0x" + data[16:36].hex(), "0x" + data[48:68].hex()
        return f"{name(first)} -> {name(second)} (via {name(to)})" if to in names and "Permit2" in name(to) else \
            f"{name(to)} -> {name(first)}"

    # ---------------------------------------------------------------- orders: one swap each, one at a time
    def _get_fee(self, base_currency: str, quote_currency: str, order_type: OrderType, order_side: TradeType,
                 amount: Decimal, price: Decimal = s_decimal_NaN, is_maker: Optional[bool] = None) -> TradeFeeBase:
        """the pools' fees and the trade's tax are in the book's price; the gas is booked from the receipt"""
        return AddedToCostTradeFee(percent=Decimal("0"))

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT, price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        return self._take_turn(super().buy(trading_pair, amount, order_type, price, **kwargs))

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT, price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        return self._take_turn(super().sell(trading_pair, amount, order_type, price, **kwargs))

    def _take_turn(self, order_id: str) -> str:
        """ONE swap at a time per wallet (Pavel 2026-10-14): its nonces are sequential, so an order waits until the one before
        it has ended. The order placed while none is out takes the wallet's turn here, synchronously, before its creation
        runs, so a second order in the same tick already finds the turn taken (and _place_order refuses it). The turn is
        freed when the order ends: its receipt or drop (_settle), or a refusal (_update_order_after_failure)"""
        if self._busy is None:
            self._busy = order_id
        return order_id

    def _free_turn(self, order_id: str):
        if self._busy == order_id:
            self._busy = None

    def _gas_ok(self) -> bool:
        return self._account_balances.get(self.C.NATIVE_ASSET, Decimal(0)) >= self.C.SWAP_GAS_MAX_NATIVE

    def get_available_balance(self, currency: str) -> Decimal:
        """0 while a swap is out (the wallet's turn is taken) or while the native coin can't pay one swap's gas, so the strategies,
        which size by it, place nothing here meanwhile; otherwise the balance. The TOTAL (get_balance) is never held back:
        the hold band and the balancer's position read it"""
        if self._busy is not None or not self._gas_ok():
            return Decimal(0)
        return super().get_available_balance(currency)

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        """the limit as given: a swap has no price grid (its limit is the worst average price accepted), and flooring it to
        the trading rule's step would refuse an order priced exactly at its book's worst level"""
        return price

    async def _create_order(self, trade_type: TradeType, order_id: str, trading_pair: str, amount: Decimal,
                            order_type: OrderType, price: Optional[Decimal] = None, **kwargs):
        # No yield first any more (2026-10-15 to 10-17 it yielded, so the CEX leg fired in the same tick went out before it).
        # Under DEX first (arb_l, Pavel 2026-10-17) a DEX-CEX arb sends its DEX leg alone and its CEX leg on the fill, so
        # this is the leg the hedge waits on: it goes out at its turn (Pavel 2026-10-17: "might as well improve yield")
        try:
            await super()._create_order(trade_type, order_id, trading_pair, amount, order_type, price, **kwargs)
        finally:
            if self._order_tracker.fetch_order(client_order_id=order_id) is None:
                self._free_turn(order_id)            # an error before it was tracked: no event will ever end it
                self._on_filled.pop(order_id, None)

    def _update_order_after_failure(self, order_id: str, trading_pair: str, exception: Optional[Exception] = None):
        """an order refused before or at its send (the framework's checks, _place_order): it ends here"""
        self._after_failure(trading_pair)              # before the failure event reaches the strategy
        self._on_filled.pop(order_id, None)
        super()._update_order_after_failure(order_id, trading_pair, exception)
        self._free_turn(order_id)

    def _after_failure(self, trading_pair: str):
        """a DEX order that failed (refused, dropped, reverted): the pair's book is emptied at once, and shown again only from a
        poll of a block newer than any we had at the failure (Pavel 2026-10-17: "Refire only on a DEX price snapshot taken
        after the failure"; 10-19: a poll every block, so that is at most a block away, and no extra poll is asked for).
        Emptied in the tracker's own book too, the one the strategies read: the data source's empty snapshot reaches it only
        through the tracker's queues, two loop turns later, and a tick in between fired on the old book (ALEO 10-18 14:54:11,
        10-19 10:47:18: 3-4 ms after a refusal)"""
        self._fresh_after[trading_pair] = self._orderbook_ds.last_block or 0
        update_id = self._orderbook_ds.hide(trading_pair)
        book = self.order_book_tracker.order_books.get(trading_pair)
        if book is not None:
            book.apply_snapshot([], [], update_id)

    def _on_order_failure(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                          order_type: OrderType, price: Optional[Decimal], exception: Exception, **kwargs):
        """the connector's own refusals are one WARNING line, the reason in it; any other error is the framework's to log"""
        if not isinstance(exception, (self.OrderRefused, self.TradingDisabled)):
            return super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception,
                                             **kwargs)
        self.logger().warning(f"{self.C.EXCHANGE_NAME}: {trade_type.name.lower()} {amount} {trading_pair} at {price} refused: "
                              f"{self._mask(exception)}")
        self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair, exception=exception)

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        """the order as one swap, built, signed and sent by the feed process (evm_dex_feed.Feed._swap) -> (its transaction
        hash = the exchange order id, the time). Off while self.C.TRADING_ENABLED is False. Here it is one message out and one
        back: none of the swap's work is on HMB's loop (Pavel 2026-10-15). The feed follows it on chain and says when it
        ended (_settle). Refused (OrderRefused, why) when nothing went out"""
        name = self.C.EXCHANGE_NAME
        if not self.C.TRADING_ENABLED:
            raise self.TradingDisabled(f"{name}: orders are off ({name}_constants.TRADING_ENABLED)")
        if self._busy != order_id:
            raise self.OrderRefused("one swap at a time: it was placed while another transaction was out")
        if self._account is None:
            raise self.OrderRefused(self._key_problem or f"no wallet key: connect {name} with {name}_private_key")
        ca, why = self._ca_for_pair(trading_pair)
        if not ca:
            raise self.OrderRefused(why)
        params = {"trading_pair": trading_pair, "ca": ca, "side": "buy" if trade_type == TradeType.BUY else "sell",
                  "amount": amount, "price": price}
        try:
            outcome, detail, *kind = await self._feed.request(("swap", order_id, params), ("swap", order_id))
        except RuntimeError as e:                      # the feed is not running, or ended before it answered
            if order_id in self._swaps:                # signed before that: it may be out, so the next feed follows it
                return self._swaps[order_id]["tx_hash"], time.time()
            raise self.OrderRefused(f"not sent: {e}") from None
        if outcome == "refused":
            self._swaps.pop(order_id, None)
            raise (self.SendRefused if kind == ["send"] else self.OrderRefused)(detail)
        self._swaps[order_id] = detail
        return detail["tx_hash"], time.time()

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """False, quietly: a sent swap can't be called back. It ends on its own within seconds (its receipt, or its
        deadline), and the framework leaves the order as it is meanwhile"""
        return False

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """a swap can't be cancelled, but it ends on its own within seconds: wait for the orders out, up to the timeout. One
        that ended reads as done (True); one still out as not cancelled (False)"""
        waiting = [o.client_order_id for o in self.in_flight_orders.values() if not o.is_done]
        end = time.time() + timeout_seconds
        while time.time() < end and any(oid in self.in_flight_orders for oid in waiting):
            await asyncio.sleep(self.C.RECEIPT_POLL_S)
        return [CancellationResult(oid, oid not in self.in_flight_orders) for oid in waiting]

    # ---------------------------------------------------------------- a swap's end, as the feed process saw it
    def _ended(self, order_id: str, parsed: Optional[dict], info: Optional[dict]):
        """a swap's end, the moment its message is read (parsed None: dropped, its nonce already read anew by the feed).
        What it got is worked out first, and a fill goes straight to whoever waits on it (on_filled: arb_l's DEX first sends
        its CEX leg there), before anything else; a failure has its pair's book hidden and a fresh poll asked for. Then the
        rest (_settle), started eagerly: it runs to its end here, with no wait for the loop, so no tick sees the swap ended
        and its fill not yet on the balances"""
        swap = self._swaps.get(order_id)
        if swap is None:
            return
        read_at = time.perf_counter()
        first = self._on_filled.pop(order_id, None)          # taken here, whatever the end: never called twice, never left
        end = handed = None
        try:
            end = self._end_of(order_id, swap, parsed)
            if end[1] is not None and first is not None:
                try:
                    first(order_id, end[1].fill_base_amount)
                except Exception:
                    self.logger().error(f"{self.C.EXCHANGE_NAME}: {order_id} filled, but whoever waited on its fill failed",
                                        exc_info=True)
                handed = time.perf_counter() - read_at
            if end[0] == OrderState.FAILED:
                self._after_failure(swap["trading_pair"])    # before the failure event reaches the strategy
        except Exception:                                    # never the feed's reader: the settle works it out again
            self.logger().error(f"{self.C.EXCHANGE_NAME}: {order_id}: its end could not be worked out at once", exc_info=True)
        # (the loop given: Python 3.12's C Task starts eagerly on the loop it is GIVEN, and fails without one)
        asyncio.Task(safe_wrapper(self._settle(order_id, swap, parsed, info, end, handed)), loop=asyncio.get_running_loop(),
                     eager_start=True)

    def _end_of(self, order_id: str, swap: dict, parsed: Optional[dict]) -> Tuple[OrderState, Optional[TradeUpdate],
                                                                                    Optional[tuple]]:
        if parsed is None:
            return OrderState.FAILED, None, (
                f"{self.C.ERROR_PREFIX}SwapDropped", f"not included by its last block {swap['last_block']}: no builder took it "
                f"in time, or it "
                                  f"would have reverted (nothing paid)")
        return self._outcome(order_id, swap, parsed)

    async def _settle(self, order_id: str, swap: dict, parsed: Optional[dict], info: Optional[dict], end: Optional[tuple],
                      handed: Optional[float]):
        """a swap's end, after its fill was handed on (_ended): this side's own work. Its receipt's moves go on the balances
        first, so whoever reads them at the fill event sees the trade (the hold band, the balancer); then the fill and FILLED
        with what it really got, or FAILED, processed in place (no 5 s wait: what the swap got IS the order); then the
        wallet's turn is free"""
        if end is None:
            end = self._end_of(order_id, swap, parsed)
            if end[0] == OrderState.FAILED:
                self._after_failure(swap["trading_pair"])
        state, fill, problem = end
        if parsed is not None:
            self._book_receipt(swap, parsed)
        if fill is not None:
            self._order_tracker.process_trade_update(fill)
            order = self._order_tracker.fetch_order(client_order_id=order_id)
            if order is not None and order.executed_amount_base < order.amount:
                order.amount = order.executed_amount_base     # an exact-input buy may get less than its amount: no more comes
        try:
            line = self._swap_line(order_id, swap, info, parsed, problem, handed)
        except Exception as e:                        # the line is information only: it never holds the settle up
            line = (f"{self.C.EXCHANGE_NAME}: {order_id} {swap.get('tx_hash')}: {problem[1] if problem else 'filled'} "
                    f"({type(e).__name__} in its line)")
        (self.logger().warning if problem is not None else self.logger().info)(line)
        await self._order_tracker._process_order_update(self._order_update(order_id, swap, state, problem))
        self._swaps.pop(order_id, None)
        self._free_turn(order_id)
        self._send_caps()                                   # its moves are on the balances: the ladders end there now

    def _swap_line(self, order_id: str, swap: dict, info: Optional[dict], parsed: Optional[dict],
                   problem: Optional[tuple], handed: Optional[float] = None) -> str:
        """one line per swap's end (Pavel 2026-10-15; audit finding 7: the data the room, a gas bid and the cadence are tuned
        on): what it got against its book, where it landed against its book's block (+N, its index in the block), the
        seconds from the send, how old its book was at the send, the gas. The send reads no head, so the book's block is
        the line's reference: +N covers the book's age and the wait for inclusion. A fill someone waited on (on_filled)
        also says how long from its end read to that taker done ("handed on in": arb_l's CEX leg sent, Pavel 2026-10-17)"""
        book = swap["book_block"]
        where = f"its book (block {book}) {swap['sent'] - swap['book_at']:.2f} s old at the send"
        what = f"{self.C.EXCHANGE_NAME}: {swap['trading_pair']} {swap['side']} ({order_id})"
        if info is None:
            return f"{what} DROPPED: {problem[1]}; {where}; {swap['tx_hash']}"
        landed, index = info["block"], info["index"]
        at = (f"landed +{landed - book} blocks after its book (block {landed}, tx #{index if index is not None else '?'}), "
              f"{time.time() - swap['sent']:.2f} s after the send")
        if problem is not None:                                             # its text carries the gas
            return f"{what} {problem[0]}: {problem[1]}; {at}; {where}; {swap['tx_hash']}"
        got, expected = parsed["amount_out"], swap.get("expected_out")
        decimals, unit = ((swap["ca_decimals"], swap["trading_pair"].split("-")[0]) if swap["side"] == "buy" else
                          (self.C.USDT_DECIMALS, self.C.QUOTE))
        versus = f"{(got / expected - 1) * 1e4:+.1f} bps vs its book" if expected else "no book figure"
        minimum = swap.get("min_out")                                       # its own minimum: the room, or a DEX-first floor
        floor = (f"its minimum {(minimum / expected - 1) * 1e4:+.0f} bps" if expected and minimum else
                 f"room {self.C.SWAP_SLIPPAGE * 10000:.0f} bps")
        first = f"; its fill handed on in {handed * 1e3:.2f} ms" if handed is not None else ""
        return (f"{what} filled: got {got / 10 ** decimals:.8g} {unit} ({versus}, {floor}); {at}; {where}; gas "
                f"{Decimal(parsed['gas_fee_wei']).scaleb(-18)} {self.C.NATIVE_ASSET} ({info['gas_used']:,}){first}; "
                f"{swap['tx_hash']}")

    def _outcome(self, order_id: str, swap: dict, parsed: dict) -> Tuple[OrderState, Optional[TradeUpdate], Optional[tuple]]:
        """a mined swap -> (FILLED, its one fill, None) or (FAILED, None, (error type, why)): reverted (its gas still paid),
        or mined without moving both tokens at the wallet. The fill is what really left and reached the wallet (net of any
        tax); its fee the gas in the native coin"""
        gas = Decimal(parsed["gas_fee_wei"]).scaleb(-18)
        coin, prefix = self.C.NATIVE_ASSET, self.C.ERROR_PREFIX
        if parsed["status"] == "reverted":
            return OrderState.FAILED, None, (f"{prefix}SwapReverted", f"reverted on chain (block {parsed['block']}); gas {gas} {coin}")
        problem = receipt_problem(parsed)
        if problem:
            return OrderState.FAILED, None, (f"{prefix}SwapNoFill", f"mined without a fill: {problem}; gas {gas} {coin}")
        ca_raw, usd_raw = ((parsed["amount_out"], parsed["amount_in"]) if swap["side"] == "buy"
                           else (parsed["amount_in"], parsed["amount_out"]))
        base = Decimal(ca_raw).scaleb(-swap["ca_decimals"])
        quote = Decimal(usd_raw).scaleb(-self.C.USDT_DECIMALS)
        fee = AddedToCostTradeFee(flat_fees=[TokenAmount(self.C.NATIVE_ASSET, gas)])
        return OrderState.FILLED, TradeUpdate(trade_id=swap["tx_hash"], client_order_id=order_id,
                                              exchange_order_id=swap["tx_hash"], trading_pair=swap["trading_pair"],
                                              fill_timestamp=time.time(), fill_price=quote / base, fill_base_amount=base,
                                              fill_quote_amount=quote, fee=fee), None

    @staticmethod
    def _order_update(order_id: str, swap: dict, state: OrderState, problem: Optional[tuple] = None) -> OrderUpdate:
        return OrderUpdate(client_order_id=order_id, exchange_order_id=swap["tx_hash"], trading_pair=swap["trading_pair"],
                           update_timestamp=time.time(), new_state=state,
                           misc_updates={"error_type": problem[0], "error_message": problem[1]} if problem else None)

    def _book_receipt(self, swap: dict, parsed: dict):
        """a mined swap's moves on the balances: the gas off the native coin and, filled, its input off and its output on, as the
        receipt's transfers at the wallet say. Skipped when a balance read already saw its block (it holds them)"""
        if parsed["block"] <= self._balances_block:
            return
        asset = {self.C.USDT: (self.C.QUOTE, self.C.USDT_DECIMALS),
                 swap["ca"].lower(): (swap["trading_pair"].split("-")[0].upper(), swap["ca_decimals"])}
        moves = [(self.C.NATIVE_ASSET, -Decimal(parsed["gas_fee_wei"]).scaleb(-18))]
        if parsed["status"] == "filled":
            token_in, token_out = asset[swap["token_in"].lower()], asset[swap["token_out"].lower()]
            moves += [(token_in[0], -Decimal(parsed["amount_in"]).scaleb(-token_in[1])),
                      (token_out[0], Decimal(parsed["amount_out"]).scaleb(-token_out[1]))]
        for name, delta in moves:
            balance = self._account_balances.get(name, Decimal(0)) + delta
            self._account_balances[name] = balance
            self._account_available_balances[name] = balance
        self._balances_block = parsed["block"]
        self._caps_dirty = True

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """the framework's poll, unused: the feed process follows every swap, and _settle books its fill"""
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """the framework's poll, unused: OPEN while the swap is out (the feed process says when it ended: _settle)"""
        swap = self._swaps.get(tracked_order.client_order_id)
        if swap is None:
            raise self.OrderRefused(f"no swap out for {tracked_order.client_order_id}")
        return self._order_update(tracked_order.client_order_id, swap, OrderState.OPEN)

    async def _update_trading_fees(self):
        return

    async def _user_stream_event_listener(self):
        return

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    # ---------------------------------------------------------------- factories
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return WebAssistantsFactory(throttler=self._throttler)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return self.ORDER_BOOK_DATA_SOURCE(trading_pairs=self._trading_pairs, connector=self)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return self.USER_STREAM_DATA_SOURCE()
