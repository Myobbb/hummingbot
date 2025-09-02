"""HTX (Huobi) WebSocket v2 private adapter.

Implements auth, subscriptions (balances/orders/trades), ping/pong, and message routing
aligned with BaseWebSocketHandler conventions.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import time
import aiohttp
from typing import Dict, Optional, Set
from decimal import Decimal as D

from .base_websocket import BaseWebSocketHandler
from .htx_constants import (
    HTX_WS_PRIVATE_URL,
    HTX_CHANNEL_BALANCES_TEMPLATE,
    HTX_CHANNEL_BALANCES_MODE_DEFAULT,
    ALL_PRIVATE_CHANNELS,
    WS_PING_INTERVAL,
    get_htx_unified_event_mapping,
)
from ...config.settings import ExchangeConfig
from ...core.events import IEventBus
from ...core.portfolio_interface import IPortfolioService


class HtxWebSocketHandler(BaseWebSocketHandler):
    """HTX WebSocket handler for balances, orders, and trade executions."""

    def __init__(self, config: ExchangeConfig, portfolio_service: IPortfolioService, event_bus: IEventBus, trading_data_service):
        super().__init__(config, portfolio_service, event_bus, trading_data_service)
        self._ping_task = None
        # Active subscriptions are tracked by base via _subscription_status; no local set needed
        # Account id required for balances; provided by user for now (HTTP discovery later)
        self._account_id: Optional[str] = str(getattr(config, "htx_account_id", "") or getattr(config, "account_id_htx", "") or "")
        # Auth gate
        try:
            self._auth_event = asyncio.Event()
        except Exception:
            self._auth_event = None
        self._authenticated: bool = False
        self._subscriptions_sent: bool = False
        # Pending balances for assets not yet seeded; applied right after first MARKET trade
        self._pending_balances: Dict[str, Dict] = {}
        # Track last received frame timestamp for inactivity detection
        try:
            self._last_frame_ts = time.time()
        except Exception:
            self._last_frame_ts = 0
        self._last_inactivity_warn_ts = 0.0
        # Aggregate fills per order to emit a single consolidated summary
        self._order_fill_accumulators: Dict[str, Dict] = {}
        self._order_emit_tasks: Dict[str, asyncio.Task] = {}
        self._emit_debounce_seconds: float = 0.6

    @property
    def exchange_name(self) -> str:
        return "HTX"

    @property
    def websocket_url(self) -> str:
        return HTX_WS_PRIVATE_URL

    def _has_custom_ping(self) -> bool:
        return True

    def _get_event_mapping_function(self):
        return get_htx_unified_event_mapping

    async def _get_connection_headers(self) -> Dict[str, str]:
        # No special headers for WS v2
        return {}

    async def _subscribe_to_channels(self) -> None:
        if not self._websocket:
            self.logger.error("Cannot subscribe - WebSocket not connected")
            return

        # Reset per-connection subscription summary
        try:
            self._subscription_status.clear()
        except Exception:
            pass
        # Reset auth/subscription flags on every fresh connection so re-connects resubscribe
        try:
            self._authenticated = False
            self._subscriptions_sent = False
            if self._auth_event is not None:
                self._auth_event.clear()
        except Exception:
            pass
        # Reset last-frame time to now to avoid immediate inactivity warnings after reconnect
        try:
            self._last_frame_ts = time.time()
        except Exception:
            pass

        try:
            # Authenticate (non-blocking); subscriptions will be sent after auth ack inside handler
            await self._authenticate()

            # Start JSON ping loop
            if not self._ping_task or self._ping_task.done():
                self._ping_task = asyncio.create_task(self._ping_loop_json())
        except Exception as e:
            self.logger.error("Failed HTX subscription setup", error=str(e))
            raise

    async def _subscribe_after_auth(self) -> None:
        if self._subscriptions_sent:
            return
        self._subscriptions_sent = True
        # Subscribe to balances
        try:
            if self._account_id:
                balance_topic = HTX_CHANNEL_BALANCES_TEMPLATE.format(self._account_id)
            else:
                balance_topic = HTX_CHANNEL_BALANCES_MODE_DEFAULT
            await self._subscribe_channel(balance_topic)
            await asyncio.sleep(self.SUBSCRIPTION_WAIT_TIME)
            # Subscribe to private orders/trades
            for ch in ALL_PRIVATE_CHANNELS:
                await self._subscribe_channel(ch)
                await asyncio.sleep(self.SUBSCRIPTION_WAIT_TIME)
        except Exception as e:
            self.logger.error("HTX post-auth subscription failed", error=str(e))

    async def _authenticate(self) -> None:
        """Perform WS v2 auth: action=req, ch=auth, params with signature v2.1."""
        try:
            access_key = self.config.api_key
            secret_key = self.config.api_secret

            from urllib.parse import urlencode, urlparse

            parsed = urlparse(self.websocket_url)
            hostname = parsed.hostname or "api-aws.huobi.pro"

            def _build_params(ts: str):
                sp = {
                    "accessKey": access_key,
                    "signatureMethod": "HmacSHA256",
                    "signatureVersion": "2.1",
                    "timestamp": ts,
                }
                q = urlencode(sorted(sp.items()))
                string_to_sign = "\n".join(["GET", hostname, "/ws/v2", q])
                signature = base64.b64encode(
                    hmac.new(secret_key.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
                ).decode()
                send = dict(sp)
                send["authType"] = "api"
                send["signature"] = signature
                return string_to_sign, send

            # Compute timestamp (correct for ms vs s) and send a single primary auth (lowercase, no 'Z')
            ts_noz = None
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://api.huobi.pro/v1/common/timestamp", timeout=5) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            raw = int(data.get("data")) if isinstance(data, dict) else None
                            if raw:
                                if raw > 1_000_000_000_000:
                                    raw = raw // 1000
                                ts_noz = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(raw))
            except Exception:
                ts_noz = None
            if not ts_noz:
                ts_noz = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

            payload_str, params_to_send = _build_params(ts_noz)
            self.logger.debug("HTX stringToSign", host=hostname, s=payload_str)
            await self._send_message({"action": "req", "ch": "auth", "params": params_to_send})
        except Exception as e:
            self.logger.error("HTX auth failed", error=str(e))
            raise

    async def _subscribe_channel(self, channel: str) -> None:
        sub_msg = {"action": "sub", "ch": channel}
        self.logger.debug("Subscribing HTX channel", channel=channel)
        await self._send_message(sub_msg)

    async def _ping_loop_json(self) -> None:
        try:
            while True:
                await asyncio.sleep(WS_PING_INTERVAL)
                if not self._is_connected or not self._websocket:
                    break
                try:
                    # HTX expects action: "ping" with data: { ts }
                    ts = int(time.time() * 1000)
                    await self._send_message({"action": "ping", "data": {"ts": ts}})
                    # Health checks: if no pong recently or no frames, force reconnect
                    now = time.time()
                    # Treat missing/old pong as unhealthy (> 45s since last pong)
                    try:
                        last_pong = float(self._last_pong) if self._last_pong else 0.0
                    except Exception:
                        last_pong = 0.0
                    if last_pong and (now - last_pong) > 45:
                        self.logger.error("Ping timeout; closing connection to reconnect (JSON ping)")
                        try:
                            await self._websocket.close()
                        except Exception:
                            pass
                        break
                    # Inactivity warning if no frames for > 120s
                    try:
                        last_frame = float(self._last_frame_ts or 0.0)
                    except Exception:
                        last_frame = 0.0
                    if last_frame and (now - last_frame) > 120:
                        if (now - self._last_inactivity_warn_ts) > 60:
                            self._last_inactivity_warn_ts = now
                            self.logger.warning("No HTX WS frames received recently", idle_seconds=int(now - last_frame))
                        # If prolonged inactivity persists (> 180s), force reconnect per robustness policy
                        if (now - last_frame) > 180:
                            self.logger.error("Inactivity threshold exceeded; closing connection to reconnect", idle_seconds=int(now - last_frame))
                            try:
                                await self._websocket.close()
                            except Exception:
                                pass
                            break
                except Exception:
                    try:
                        if self._websocket:
                            await self._websocket.close()
                    except Exception:
                        pass
                    break
        except asyncio.CancelledError:
            return

    async def _handle_message(self, message) -> None:
        # HTX may send compressed frames; decode to text JSON
        text = None
        try:
            self._last_frame_ts = time.time()
        except Exception:
            pass
        if isinstance(message, (bytes, bytearray)):
            # Try gzip first, then zlib (deflate) with different wbits, then raw utf-8
            import binascii
            text = None
            # Try gzip
            try:
                import gzip
                text = gzip.decompress(message).decode("utf-8")
            except Exception:
                text = None
            # Try zlib wrapper
            if text is None:
                try:
                    import zlib
                    text = zlib.decompress(message, wbits=16 + zlib.MAX_WBITS).decode("utf-8")
                except Exception:
                    text = None
            # Try raw deflate
            if text is None:
                try:
                    import zlib
                    text = zlib.decompress(message, wbits=-zlib.MAX_WBITS).decode("utf-8")
                except Exception:
                    text = None
            # Fallback: plain utf-8
            if text is None:
                try:
                    text = message.decode("utf-8")
                except Exception:
                    text = None
            if text is None:
                # Log a small hex sample to aid troubleshooting and return
                try:
                    sample_hex = binascii.hexlify(message[:64]).decode("ascii")
                except Exception:
                    sample_hex = ""
                self.logger.debug("HTX undecoded binary frame", length=len(message), sample_hex=sample_hex)
                return
        else:
            text = message

        if isinstance(text, str):
            self._debug_log_raw_message(text, "received")
        else:
            # Un-decodable frame; skip
            return

        try:
            data = json.loads(text)
            # Robust heartbeat handling: keep only private v2 'action' ping/pong
            try:
                # Emit concise debug for control frames except ping/pong to avoid log clutter
                act = data.get("action")
                if act in ("req", "sub", "unsub"):
                    self.logger.debug("HTX control frame", action=act, code=data.get("code"), ch=data.get("ch"))
            except Exception:
                pass

            action = data.get("action")
            if action == "ping":
                # Respond with pong echoing ts (silently in logs)
                ts = None
                try:
                    ts = int((data.get("data") or {}).get("ts", 0))
                except Exception:
                    ts = None
                pong = {"action": "pong", "data": ({"ts": ts} if ts is not None else {})}
                await self._send_message(pong)
                try:
                    await self._handle_pong()
                except Exception:
                    pass
                return

            if action == "pong":
                await self._handle_pong()
                return

            if action in ("req", "sub"):  # acks
                code = data.get("code")
                ch = data.get("ch")
                ok = (code == 200) if code is not None else True
                try:
                    if ch:
                        self._mark_subscription_attempt(ch, bool(ok))
                except Exception:
                    pass
                # Handle auth ack explicitly
                if action == "req":
                    # Only treat as auth when channel is exactly 'auth'
                    if ok and (str(ch or "").lower() == "auth"):
                        self._authenticated = True
                        try:
                            if self._auth_event is not None:
                                self._auth_event.set()
                        except Exception:
                            pass
                        self.logger.info("HTX authentication successful")
                        try:
                            asyncio.create_task(self._subscribe_after_auth())
                        except Exception:
                            pass
                    else:
                        self._authenticated = False
                        try:
                            if self._auth_event is not None:
                                self._auth_event.set()
                        except Exception:
                            pass
                        self.logger.error("HTX authentication failed", code=code, err_message=data.get("message"), ch=ch)
                return

            if action == "push":
                ch = data.get("ch", "")
                payload = data.get("data") or {}

                if ch.startswith("accounts.update"):
                    await self._handle_balance_push(payload)
                    return
                if ch.startswith("orders#"):
                    await self._handle_order_push(payload)
                    return
                if ch.startswith("trade.clearing#"):
                    await self._handle_trade_push(payload)
                    return

            # Unhandled
            self.logger.debug("Unhandled HTX message", keys=list(data.keys()))
        except Exception as e:
            self.logger.error("HTX message handling error", error=str(e))

    async def _handle_balance_push(self, payload: Dict) -> None:
        try:
            # HTX payload example fields: currency, change, balance, accountId, eventType, seqNum, ts
            asset = (payload.get("currency") or "").upper()
            if not asset:
                return
            # available balance may be under 'available' or computed from 'balance' - 'frozen'. Prefer 'available' if present.
            available_raw = payload.get("available")
            if available_raw in (None, ""):
                # fallbacks
                available_raw = payload.get("balance")
            change_type = (payload.get("eventType") or payload.get("changeType") or "").lower()

            event_data = {
                "available": str(available_raw) if available_raw is not None else None,
                "total": str(payload.get("balance")) if payload.get("balance") is not None else None,
                "event_time": payload.get("ts") or payload.get("timestamp"),
                "change": str(payload.get("change")) if payload.get("change") is not None else None,
            }
            try:
                avail_dec = D(str(available_raw)) if available_raw not in (None, "") else D("0")
            except Exception:
                avail_dec = D("0")
            # If asset not yet in portfolio, cache latest balance to apply after MARKET trade seeding
            try:
                portfolio = await self.portfolio_service.get_portfolio()
                canonical_asset = self._resolve_asset_alias(asset)
                if canonical_asset not in getattr(portfolio, 'assets', {}):
                    self._pending_balances[canonical_asset] = {
                        "asset": asset,
                        "available": str(avail_dec),
                        "event_type": change_type or "unknown",
                        "event_data": event_data,
                    }
                    return
            except Exception:
                pass
            await self._process_balance_update_standard(asset, avail_dec, change_type or "unknown", event_data)
        except Exception as e:
            self.logger.error("Failed to handle HTX balance push", error=str(e))

    async def _handle_order_push(self, payload: Dict) -> None:
        try:
            # Order updates: we emit concise summaries for final statuses and record cancels/underfills in future increment
            # Keep minimal debug log for now; executions are handled in trade.clearing
            self.logger.debug("HTX order push", status=payload.get("orderStatus"), order_id=payload.get("orderId"))
            status = (payload.get("orderStatus") or "").lower()
            order_id = str(payload.get("orderId") or "")
            symbol = payload.get("symbol") or payload.get("currencyPair") or ""
            side = (payload.get("orderSide") or payload.get("side") or "").upper()
            client_order_id = payload.get("clientOrderId") or None
            # Normalize order type
            ot_raw = (payload.get("type") or payload.get("orderType") or "").strip().lower()
            if "market" in ot_raw:
                order_type = "MARKET"
            elif "limit" in ot_raw:
                order_type = "LIMIT"
            else:
                order_type = (payload.get("orderType") or payload.get("type") or "").upper() or None
            # Derive side from type when orderSide is absent on orders channel
            if not side:
                try:
                    if ot_raw.startswith("buy"):
                        side = "BUY"
                    elif ot_raw.startswith("sell"):
                        side = "SELL"
                except Exception:
                    pass

            # Accumulate fills also from orders channel trade events (dedup by tradeId)
            try:
                if (payload.get("eventType") or "").lower() == "trade":
                    trade_id = str(payload.get("tradeId") or "")
                    trade_qty = str(payload.get("tradeVolume") or payload.get("filledAmount") or "0")
                    trade_price = str(payload.get("tradePrice") or payload.get("price") or "0")
                    if trade_id:
                        self._accumulate_fill(
                            order_id=order_id,
                            trade_id=trade_id,
                            symbol=symbol,
                            side=side,
                            order_type=order_type,
                            client_order_id=client_order_id,
                            qty_str=trade_qty,
                            price_str=trade_price,
                        )
            except Exception:
                pass

            if status in ("canceled", "cancelled", "rejected", "expired"):
                try:
                    qty_str = str(payload.get("tradeVolume") or payload.get("filledAmount") or payload.get("amount") or "0")
                    price_str = str(payload.get("tradePrice") or payload.get("price") or "0")
                    exec_total = str(payload.get("totalTradeAmount") or payload.get("filledTotal") or "")
                    self._emit_cancelled_summary(
                        symbol=symbol,
                        side=side,
                        quantity=qty_str,
                        price=price_str,
                        quote_amount=(exec_total if exec_total not in (None, "", "0") else None),
                    )
                    # Clear any pending aggregation for this order
                    try:
                        self._clear_aggregation(order_id)
                    except Exception:
                        pass
                except Exception:
                    pass
            elif status == "filled":
                # Schedule a short debounce to allow trade.clearing fills to arrive
                try:
                    self._schedule_emit(order_id)
                except Exception:
                    pass
        except Exception as e:
            self.logger.error("Failed to handle HTX order push", error=str(e))

    async def _handle_trade_push(self, payload: Dict) -> None:
        try:
            # Fields often include: orderId, clientOrderId, symbol (lowercase), tradePrice, tradeVolume, tradeTime, role, fee, feeCurrency, orderType, orderSide
            symbol = payload.get("symbol") or ""
            side = (payload.get("orderSide") or payload.get("side") or "").upper()
            qty = str(payload.get("tradeVolume") or payload.get("filledAmount") or "0")
            price = str(payload.get("tradePrice") or payload.get("price") or "0")
            quote = None
            try:
                quote = str(D(str(qty)) * D(str(price)))
            except Exception:
                quote = None
            # Normalize HTX order type to MARKET/LIMIT for seeding logic
            order_type_raw = (payload.get("orderType") or payload.get("type") or "").strip().lower()
            if "market" in order_type_raw:
                order_type = "MARKET"
            elif "limit" in order_type_raw:
                order_type = "LIMIT"
            else:
                order_type = (payload.get("orderType") or "").upper()
            order_id = str(payload.get("orderId") or payload.get("tradeId") or "")
            client_order_id = payload.get("clientOrderId") or None
            # Prefer transactFee from HTX trade-clearing; fallback to fee
            fee_amount = payload.get("transactFee") or payload.get("fee")
            fee_currency = (payload.get("feeCurrency") or "").upper()

            # Normalize fees to USDT: if fee currency is base, convert via price; if quote (USDT), pass as-is
            fees_param = None
            try:
                if fee_amount not in (None, "", "0"):
                    # Determine base and quote from symbol
                    parsed_pair = self._parse_symbol(symbol)
                    base_asset = parsed_pair[0] if parsed_pair else None
                    quote_asset = parsed_pair[1] if parsed_pair else None
                    fee_dec = D(str(fee_amount))
                    if fee_currency == "USDT" or (quote_asset and fee_currency == quote_asset):
                        fees_param = str(fee_dec)
                    elif base_asset and fee_currency == base_asset:
                        fees_param = str(fee_dec * D(str(price)))
                    else:
                        fees_param = None
            except Exception:
                fees_param = None

            raw_ts = payload.get("tradeTime") or payload.get("ts")
            await self._record_trade_execution(
                order_id=order_id or client_order_id or "",
                symbol=symbol,
                side=side,
                trade_type=order_type or "UNKNOWN",
                quantity=qty,
                price=price,
                quote_quantity=quote,
                fees=fees_param,
                client_order_id=client_order_id,
                raw_timestamp=raw_ts,
            )
            # After seeding on MARKET trade, apply any cached pending balance for this base asset
            try:
                parsed_pair = self._parse_symbol(symbol)
                base_exchange_asset = parsed_pair[0] if parsed_pair else None
                if base_exchange_asset:
                    canonical_asset = self._resolve_asset_alias(base_exchange_asset)
                    pending = self._pending_balances.pop(canonical_asset, None)
                    if pending is not None:
                        try:
                            avail_dec = D(str(pending.get("available", "0")))
                        except Exception:
                            avail_dec = D("0")
                        await self._process_balance_update_standard(
                            pending.get("asset", base_exchange_asset),
                            avail_dec,
                            pending.get("event_type", "unknown"),
                            pending.get("event_data", {}),
                        )
            except Exception:
                pass
            # Accumulate per-order fills (dedup by tradeId) for a single consolidated summary
            try:
                trade_id = str(payload.get("tradeId") or "")
                if trade_id:
                    self._accumulate_fill(
                        order_id=order_id,
                        trade_id=trade_id,
                        symbol=symbol,
                        side=side,
                        order_type=order_type,
                        client_order_id=client_order_id,
                        qty_str=qty,
                        price_str=price,
                    )
            except Exception:
                pass
        except Exception as e:
            self.logger.error("Failed to handle HTX trade push", error=str(e))

    async def disconnect(self) -> None:
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None
        self._active_subscriptions.clear()
        # Ensure flags are reset so a subsequent connect performs fresh auth/subscriptions
        try:
            self._authenticated = False
            self._subscriptions_sent = False
            if self._auth_event is not None:
                self._auth_event.clear()
        except Exception:
            pass
        await super().disconnect()

    # ----------------------
    # Aggregation helpers
    # ----------------------

    def _accumulate_fill(
        self,
        *,
        order_id: str,
        trade_id: str,
        symbol: str,
        side: str,
        order_type: Optional[str],
        client_order_id: Optional[str],
        qty_str: str,
        price_str: str,
    ) -> None:
        if not order_id or not trade_id:
            return
        acc = self._order_fill_accumulators.get(order_id)
        if acc is None:
            acc = {
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "client_order_id": client_order_id,
                "fills_seen": set(),  # type: Set[str]
                "total_qty": D("0"),
                "total_quote": D("0"),
            }
            self._order_fill_accumulators[order_id] = acc
        fills_seen: Set[str] = acc["fills_seen"]
        if trade_id in fills_seen:
            return
        fills_seen.add(trade_id)
        try:
            qty_dec = D(str(qty_str))
            price_dec = D(str(price_str))
            acc["total_qty"] = acc["total_qty"] + qty_dec
            acc["total_quote"] = acc["total_quote"] + (qty_dec * price_dec)
        except Exception:
            # If parsing fails, skip but keep dedup marker to avoid infinite retries
            pass
        # Keep most recent metadata
        acc["symbol"] = symbol or acc["symbol"]
        acc["side"] = side or acc["side"]
        acc["order_type"] = order_type or acc["order_type"]
        acc["client_order_id"] = client_order_id or acc["client_order_id"]

    def _schedule_emit(self, order_id: str) -> None:
        if not order_id:
            return
        # If already scheduled, do nothing; existing task will include any fills accrued during debounce
        if order_id in self._order_emit_tasks and not self._order_emit_tasks[order_id].done():
            return
        async def _emit_later():
            try:
                await asyncio.sleep(self._emit_debounce_seconds)
                self._finalize_and_emit(order_id)
            finally:
                # Cleanup task ref after execution
                try:
                    self._order_emit_tasks.pop(order_id, None)
                except Exception:
                    pass
        task = asyncio.create_task(_emit_later())
        self._order_emit_tasks[order_id] = task

    def _finalize_and_emit(self, order_id: str) -> None:
        acc = self._order_fill_accumulators.get(order_id)
        if not acc:
            return
        try:
            total_qty: D = acc.get("total_qty", D("0"))
            total_quote: D = acc.get("total_quote", D("0"))
            if total_qty <= D("0"):
                # Nothing meaningful to emit
                self._clear_aggregation(order_id)
                return
            # Average price for display; executed_quote passed explicitly
            try:
                avg_price = (total_quote / total_qty) if total_qty > D("0") else D("0")
            except Exception:
                avg_price = D("0")
            self._emit_filled_summary_with_context(
                symbol=acc.get("symbol", ""),
                side=acc.get("side", ""),
                quantity=str(total_qty),
                price=str(avg_price),
                executed_quote=str(total_quote),
                order_type=acc.get("order_type"),
                client_order_id=acc.get("client_order_id"),
            )
        finally:
            self._clear_aggregation(order_id)

    def _clear_aggregation(self, order_id: str) -> None:
        try:
            task = self._order_emit_tasks.pop(order_id, None)
            if task and not task.done():
                task.cancel()
        except Exception:
            pass
        self._order_fill_accumulators.pop(order_id, None)


