"""MEXC WebSocket handler implementation."""

import asyncio
import hashlib
import hmac
import json
from typing import Any
from decimal import Decimal
import time
from typing import Dict
from urllib.parse import urlencode

import aiohttp

from .base_websocket import BaseWebSocketHandler
from .mexc_constants import (
    USER_TRADES_ENDPOINT, USER_ORDERS_ENDPOINT, USER_BALANCE_ENDPOINT,
    ORDER_STATUS_MAP, DOMAIN_STATUS_MAP, SIDE_MAP,
    MEXC_WS_PRIVATE_URL,
)

from .pb import PushDataV3ApiWrapper_pb2 as PBPush
from ...config.settings import ExchangeConfig
from ...core.events import IEventBus
from ...core.portfolio_interface import IPortfolioService
from ...core.events.types.trading_events import OrderCancelledEvent
from ...core.trading_data import TradeSide as DomainTradeSide, CancelledOrder
from ...utils.timestamp_utils import ensure_milliseconds

class MEXCWebSocketHandler(BaseWebSocketHandler):
    """MEXC WebSocket handler for balance updates, order status, and trade executions."""
    
    def __init__(self, config: ExchangeConfig, portfolio_service: IPortfolioService, event_bus: IEventBus, trading_data_service):
        super().__init__(config, portfolio_service, event_bus, trading_data_service)
        self._listen_key = None
        self._keep_alive_task = None
        self._active_subscriptions = set()   # Track confirmed subscriptions
        self._ws_ping_task = None
        # Log PB availability once for diagnostics
        self.logger.debug("MEXC Protobuf decoder loaded", pb_module="PushDataV3ApiWrapper_pb2")
        # TTL cache: client_order_id -> order_type (LIMIT/MARKET)
        from ..data_management.memory_manager import TimedCache
        self._order_type_by_client_id = TimedCache(default_ttl_ms=300000)
    
    @property
    def exchange_name(self) -> str:
        return "Mexc"
    
    @property
    def websocket_url(self) -> str:
        """WebSocket URL with listen key (force PB host)."""
        base_url = MEXC_WS_PRIVATE_URL
        if self._listen_key:
            return f"{base_url}?listenKey={self._listen_key}"
        return base_url
    
    def _has_custom_ping(self) -> bool:
        """Use MEXC-specific JSON ping/pong; do not use base TCP ping loop."""
        return True

    async def _send_ping(self) -> None:
        """Silent protocol-level ping with bounded pong wait handled in base."""
        await super()._send_ping()
    
    def _get_event_mapping_function(self):
        """Return MEXC's event mapping function."""
        from .mexc_constants import get_mexc_unified_event_mapping
        return get_mexc_unified_event_mapping
    
    async def connect(self) -> None:
        """Establish WebSocket connection with MEXC listen key."""
        if self._is_connected:
            self.logger.info("Already connected")
            return
        
        try:
            # Create initial listen key
            await self._create_listen_key()
            if not self._listen_key:
                self.logger.error("Failed to create MEXC listen key")
                return
            
            # Start keep-alive task  
            self._keep_alive_task = asyncio.create_task(self._keep_alive_loop())
            
            # Use base class connection logic
            await super().connect()
            # Start JSON ping loop (MEXC expects JSON {"method":"ping"})
            if self._ws_ping_task is None or self._ws_ping_task.done():
                self._ws_ping_task = asyncio.create_task(self._ws_ping_loop())
            
        except Exception as e:
            self.logger.error("Failed to establish MEXC WebSocket connection", error=str(e))
            raise
    
    async def disconnect(self) -> None:
        """Close WebSocket connection and cleanup MEXC-specific tasks."""
        # Cancel keep-alive task
        if self._keep_alive_task:
            self._keep_alive_task.cancel()
            try:
                await self._keep_alive_task
            except asyncio.CancelledError:
                pass
            self._keep_alive_task = None
        
        # Delete listen key to clean up server resources
        if self._listen_key:
            try:
                await self._delete_listen_key()
            except Exception as e:
                self.logger.warning("Failed to delete listen key on disconnect", error=str(e))
        
        # Reset subscription tracking
        self._active_subscriptions.clear()
        self._listen_key = None
        
        # Call parent disconnect
        await super().disconnect()
        # Cancel ping loop
        if self._ws_ping_task:
            self._ws_ping_task.cancel()
            try:
                await self._ws_ping_task
            except asyncio.CancelledError:
                pass
            self._ws_ping_task = None
    
    async def _create_listen_key(self) -> None:
        """Create initial MEXC listen key for private data stream."""
        await self._manage_listen_key("POST", "Creating")
    
    async def _extend_listen_key(self) -> None:
        """Extend existing MEXC listen key to keep it alive."""
        await self._manage_listen_key("PUT", "Extending")
    
    async def _delete_listen_key(self) -> None:
        """Delete MEXC listen key to clean up server resources."""
        await self._manage_listen_key("DELETE", "Deleting")
    
    async def _manage_listen_key(self, method: str, action: str) -> None:
        """Unified listen key management for MEXC."""
        url = "https://api.mexc.com/api/v3/userDataStream"
        timestamp = int(time.time() * 1000)
        
        params = {'timestamp': timestamp}
        
        # For PUT/DELETE, include listen key in params
        if method in ["PUT", "DELETE"] and self._listen_key:
            params['listenKey'] = self._listen_key
        
        # Create signature
        query_string = urlencode(params)
        signature = hmac.new(
            self.config.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        params['signature'] = signature
        headers = {
            "X-MEXC-APIKEY": self.config.api_key,
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                request_method = getattr(session, method.lower())
                async with request_method(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        if method == "POST":
                            data = await response.json()
                            self._listen_key = data.get('listenKey')
                            self.logger.info("MEXC listen key created successfully", 
                                           listen_key_length=len(self._listen_key) if self._listen_key else 0)
                        elif method == "PUT":
                            self.logger.debug("MEXC listen key extended successfully")
                        elif method == "DELETE":
                            self.logger.debug("MEXC listen key deleted successfully")
                            self._listen_key = None
                    else:
                        error_text = await response.text()
                        self.logger.error(f"Failed to {action.lower()} MEXC listen key", 
                                        method=method,
                                        status=response.status, 
                                        error=error_text)
        except Exception as e:
            self.logger.error(f"Error {action.lower()} MEXC listen key", 
                            method=method,
                            error=str(e),
                            error_type=type(e).__name__)
    
    async def _keep_alive_loop(self) -> None:
        """Keep listen key alive by extending it periodically (correct approach)."""
        keep_alive_counter = 1
        
        while True:
            try:
                await asyncio.sleep(1800)  # 30 minutes - MEXC recommended interval
                
                if not self._listen_key:
                    self.logger.warning("No listen key available for extension")
                    break
                
                # Use PUT to extend existing key (correct method)
                await self._extend_listen_key()
                self.logger.debug("MEXC listen key keep-alive", counter=keep_alive_counter)
                keep_alive_counter += 1
                
            except asyncio.CancelledError:
                self.logger.debug("MEXC keep-alive loop cancelled")
                break
            except Exception as e:
                self.logger.error("MEXC keep-alive error", error=str(e))
                # Continue the loop to attempt recovery
    
    async def _get_connection_headers(self) -> Dict[str, str]:
        """Get headers for MEXC WebSocket connection."""
        return {}
    
    async def _subscribe_to_channels(self) -> None:
        """Subscribe to MEXC balance updates and order execution reports after connection."""
        if not self._listen_key or not self._websocket:
            self.logger.error("Cannot subscribe - missing listen key or WebSocket connection")
            return
        
        # Always resubscribe on a fresh connection. Server does not persist subs across connections.
        # Clear any carry-over state from previous socket instances to avoid false "already subscribed".
        self._active_subscriptions.clear()
        try:
            self._subscription_status.clear()
        except Exception:
            pass
        required_channels = [USER_BALANCE_ENDPOINT, USER_ORDERS_ENDPOINT, USER_TRADES_ENDPOINT]
        channels_to_subscribe = required_channels
        
        try:
            # Give the gateway a brief moment before subscribing (avoids early Blocked acks)
            await asyncio.sleep(self.SUBSCRIPTION_WAIT_TIME)
            # Batch subscribe in one request (as in working examples)
            subscribe_message = {"method": "SUBSCRIPTION", "params": channels_to_subscribe}
            self.logger.debug("Subscribing channels (batch)", channels=channels_to_subscribe)
            await self._send_message(subscribe_message)
            self._active_subscriptions.update(channels_to_subscribe)
            # Mark subscription attempts for unified summary line
            for ch in channels_to_subscribe:
                try:
                    self._mark_subscription_attempt(ch, True)
                except Exception:
                    pass
            self.logger.debug(
                "MEXC WebSocket setup complete",
                subscribed_channels=list(self._active_subscriptions),
                listen_key_length=len(self._listen_key) if self._listen_key else 0,
            )
            # Ensure JSON ping loop is running for this socket instance (restarts on reconnects)
            try:
                if self._ws_ping_task is None or self._ws_ping_task.done():
                    self._ws_ping_task = asyncio.create_task(self._ws_ping_loop())
            except Exception:
                pass

        except Exception as e:
            self.logger.error("Failed to complete MEXC WebSocket setup", error=str(e))
            raise

    async def _ws_ping_loop(self) -> None:
        """Send silent JSON pings periodically; close on failure to trigger reconnect.
        Reference: MEXC WS examples send {"method":"ping"} ~30s.
        """
        try:
            while True:
                await asyncio.sleep(30)
                if not self._is_connected or not self._websocket:
                    break
                try:
                    await self._send_message({"method": "ping"})
                except Exception:
                    # Silent ping; on failure, close to trigger reconnect
                    try:
                        if self._websocket:
                            await self._websocket.close()
                    except Exception:
                        pass
                    break
        except asyncio.CancelledError:
            return
    
    async def _handle_message(self, message: Any) -> None:
        """Handle MEXC WebSocket messages (supports PB binary and JSON)."""
        # For JSON frames, log the full payload directly (uniform with other connectors).
        # For binary PB frames, defer logging until after successful decode so we can log the decoded JSON,
        # and only fall back to a binary sample if decoding fails.
        if isinstance(message, str):
            # No truncation for JSON frames
            self._debug_log_raw_message(message, "received")

        # Decode PB to JSON-like dict if needed
        data = None
        if isinstance(message, (bytes, bytearray)):
            # Prepare frame metadata for consolidated logging
            frame_len = len(message)
            try:
                frame_sample_hex = message[:32].hex()
            except Exception:
                frame_sample_hex = ""
            if PBPush is None:
                # No decoder available
                self.logger.error("Skipping PB decode; decoder not loaded (PBPush is None)",
                                  length=frame_len, sample_hex=frame_sample_hex)
            else:
                try:
                    wrapper = PBPush.PushDataV3ApiWrapper()
                    wrapper.ParseFromString(message)
                    which_body = wrapper.WhichOneof('body')
                    channel = wrapper.channel
                    symbol = getattr(wrapper, 'symbol', '') or ''
                    # Single concise line combining binary frame meta and parse info
                    self.logger.debug("PB frame decoded",
                                      length=frame_len,
                                      sample_hex=frame_sample_hex,
                                      channel=channel,
                                      which_body=str(which_body),
                                      symbol=symbol)
                    # Map to existing downstream JSON format using which_body to avoid brittle HasField checks
                    if which_body == 'privateAccount':
                        body = wrapper.privateAccount
                        # Map protobuf fields to our standardized JSON-like payload
                        # Include operation type and deltas so we can classify DEPOSIT/WITHDRAW events
                        data = {
                            'c': USER_BALANCE_ENDPOINT,
                            'd': {
                                # Asset symbol
                                'a': str(getattr(body, 'vcoinName', '')),
                                # Free and locked balances
                                'f': str(getattr(body, 'balanceAmount', '')),
                                'l': str(getattr(body, 'frozenAmount', '')),
                                # Deltas (free/locked)
                                'fd': str(getattr(body, 'balanceAmountChange', '')),
                                'ld': str(getattr(body, 'frozenAmountChange', '')),
                                # Operation type (e.g., DEPOSIT, WITHDRAW, ENTRUST, ...)
                                'o': (str(getattr(body, 'type', '')) or '').upper(),
                                # Event time (ms per PB spec)
                                'c': int(getattr(body, 'time', 0) or 0),
                            }
                        }
                    elif which_body == 'privateOrders':
                        body = wrapper.privateOrders
                        data = {
                            'c': USER_ORDERS_ENDPOINT,
                            's': symbol,
                            'd': {
                                'i': str(getattr(body, 'id', '')),
                                'c': str(getattr(body, 'clientId', '')),
                                'S': int(getattr(body, 'tradeType', 0)),
                                's': int(getattr(body, 'status', 0)),
                                'ot': int(getattr(body, 'orderType', 0)),
                                'ap': str(getattr(body, 'avgPrice', '')),
                                'cv': str(getattr(body, 'cumulativeQuantity', '')),
                                'ca': str(getattr(body, 'cumulativeAmount', '')),
                                'p': str(getattr(body, 'price', '')),
                                # Original order amounts for proper cancellation rendering
                                'A': str(getattr(body, 'amount', '')),
                                'a': str(getattr(body, 'amount', '')),
                                'V': str(getattr(body, 'quantity', '')),
                                'v': str(getattr(body, 'quantity', '')),
                                # Order creation time for timestamping cancelled orders
                                'O': int(getattr(body, 'createTime', 0) or 0),
                            },
                        }
                    elif which_body == 'privateDeals':
                        body = wrapper.privateDeals
                        data = {
                            'c': USER_TRADES_ENDPOINT,
                            's': symbol,
                            'd': {
                                't': str(getattr(body, 'tradeId', '')),
                                'S': int(getattr(body, 'tradeType', 0)),
                                'v': str(getattr(body, 'quantity', '')),
                                'a': str(getattr(body, 'amount', '')),
                                'p': str(getattr(body, 'price', '')),
                                'c': str(getattr(body, 'clientOrderId', '')),
                                'T': int(getattr(body, 'time', 0) or 0),
                                # Optional fee info
                                'fe': str(getattr(body, 'feeAmount', '')),
                                'fc': str(getattr(body, 'feeCurrency', '')),
                            }
                        }
                    else:
                        # Parsed PB, but no expected body present
                        self.logger.debug("Parsed PB but unexpected body", channel=channel, which_body=str(which_body))
                except Exception as e:
                    # Log first bytes to help diagnose framing issues
                    try:
                        sample = (message[:64].hex())
                    except Exception:
                        sample = ""
                    self.logger.error("Failed to decode PB frame", error=str(e), sample_hex=sample)
                    data = None

        if data is None:
            # Try JSON
            try:
                data = json.loads(message)
            except Exception:
                data = None

        # After successful decode, emit a uniform raw_data line with the decoded JSON for PB frames
        if isinstance(data, dict):
            try:
                self._debug_log_raw_message(json.dumps(data), "received")
            except Exception:
                pass
        else:
            if isinstance(message, (bytes, bytearray)):
                self.logger.debug("Unhandled binary frame (no PB decode)")
            return

        # Handle subscription confirmations (skip PONG noise)
        if 'id' in data and 'code' in data:
            # Some servers echo PONG as msg; mark pong and suppress periodic noise
            if str(data.get('msg', '')).upper() == 'PONG':
                try:
                    await self._handle_pong()
                except Exception:
                    pass
                return
            if data.get('code') == 0:
                msg = str(data.get('msg', ''))
                self.logger.debug("MEXC subscription confirmed", msg=msg)
                # Mark all requested channels as successful for unified summary
                try:
                    for ch in list(self._active_subscriptions):
                        self._mark_subscription_attempt(ch, True)
                except Exception:
                    pass
                # If server reports Blocked, keep PB-only strategy and log for diagnostics
                if 'Not Subscribed' in msg and 'Blocked' in msg:
                    self.logger.error("MEXC reported blocked subscription for PB channel(s). Verify websocket_url host is 'wbs-api.mexc.com' and listenKey is valid.")
            else:
                err_msg = str(data.get('msg', '') or data.get('message', ''))
                self.logger.error(
                    "MEXC subscription failed",
                    code=data.get('code'),
                    msg=err_msg,
                    expected_channels=list(self._active_subscriptions),
                )
                try:
                    for ch in list(self._active_subscriptions):
                        self._mark_subscription_attempt(ch, False)
                except Exception:
                    pass
                # Auto-recover on listen key issues: recreate key and force reconnect
                try:
                    lower_msg = err_msg.lower()
                    if ('listen' in lower_msg and 'key' in lower_msg) and any(k in lower_msg for k in [
                        'invalid', 'expire', 'expired', 'not found', 'invalid key', 'invalid listen', 'invalid listen key'
                    ]):
                        self.logger.warning("MEXC listen key invalid/expired; recreating and reconnecting")
                        await self._create_listen_key()
                        try:
                            if self._websocket:
                                await self._websocket.close()
                        except Exception:
                            pass
                        return
                except Exception:
                    pass
            return
        # Handle incidental keepalive chatter
        msg_upper = str(data.get('msg', '')).upper()
        if msg_upper == 'PING':
            # Respond with JSON pong and mark pong silently
            try:
                await self._send_message({"method": "pong"})
            except Exception:
                pass
            try:
                await self._handle_pong()
            except Exception:
                pass
            return
        if msg_upper == 'PONG':
            try:
                await self._handle_pong()
            except Exception:
                pass
            return

        # Route messages by channel (PB only)
        channel = data.get('c')
        if channel == USER_BALANCE_ENDPOINT:
            await self._handle_balance_update(data)
        elif channel == USER_ORDERS_ENDPOINT:
            await self._handle_order_status_update(data)
        elif channel == USER_TRADES_ENDPOINT:
            await self._handle_trade_execution(data)
        elif channel == 'executionReport':  # Legacy support
            await self._handle_order_status_update(data)
        elif channel:
            self.logger.debug("Unhandled MEXC message type", channel=channel)

    # No fallback/retry loops for Blocked; keep PB-only and surface diagnostics
    
    async def _handle_balance_update(self, data: Dict) -> None:
        """Handle balance update from MEXC using standardized approach."""
        try:
            balance_data = data.get('d', {})
            asset = balance_data.get('a', "").upper()  # asset symbol
            
            if not asset:
                self.logger.debug("Empty asset symbol in MEXC balance update")
                return
            
            # Parse balances with validation
            try:
                free_balance = Decimal(str(balance_data.get('f', '0')))
                locked_balance = Decimal(str(balance_data.get('l', '0')))
            except Exception as e:
                raise ValueError(f"Invalid balance data for {asset}: {e}")
            
            # Skip MX token updates unless balance is 0 (legacy special case)
            if asset == 'MX' and free_balance > Decimal("0"):
                self.logger.debug("Skipping MX token update", free=free_balance)
                return
            
            # Zero-balance skip centralized in base handler
            
            # Only log notable changes to reduce spam
            if free_balance > Decimal("1000") or locked_balance > Decimal("0"):
                self.logger.debug(
                    "MEXC significant balance update",
                    asset=asset,
                    free=str(free_balance),
                    locked=(str(locked_balance) if locked_balance > Decimal("0") else None),
                )
            
            # Prepare event data context (preserve deltas and event time when present)
            balance_payload = data.get('d', {})
            event_time = data.get('E') or balance_payload.get('c') or 0
            event_data = {
                "free": str(free_balance),
                "locked": str(locked_balance),
                "event_time": event_time,
                # Provide a generic 'change' field so upstream handlers can render delta
                "change": balance_payload.get('fd') if isinstance(balance_payload.get('fd'), (str, float, int)) else None,
            }
            
            # Extract MEXC operation type from the data
            operation_type = balance_payload.get('o', 'balance-update')
            
            # Use standardized balance update processing with actual MEXC event type
            await self._process_balance_update_standard(asset, free_balance, operation_type, event_data)
            
        except Exception as e:
            self.logger.error("Failed to handle MEXC balance update", 
                            error=str(e),
                            error_type=type(e).__name__)
    
    async def _handle_order_status_update(self, data: Dict) -> None:
        """Handle MEXC order status updates."""
        try:
            order_data = data.get('d', {})
            order_id = order_data.get('i')
            client_order_id = order_data.get('c')
            symbol = data.get('s')
            side_num = order_data.get('S')
            status_num = order_data.get('s')
            order_type_num = order_data.get('ot')
            
            if not all([order_id, symbol, side_num is not None, status_num is not None]):
                return
            
            # Convert to readable formats using constants
            side = SIDE_MAP.get(side_num, "UNKNOWN")
            status = ORDER_STATUS_MAP.get(status_num, f"UNKNOWN_{status_num}")
            # Map order type numeric to string
            # MEXC emits: 1=LIMIT, 2=MARKET; additional MARKET variants (e.g., 5) observed in practice
            if isinstance(order_type_num, int):
                if order_type_num in (2, 5):
                    trade_type_str = "MARKET"
                elif order_type_num == 1:
                    trade_type_str = "LIMIT"
                else:
                    trade_type_str = "UNKNOWN"
            else:
                trade_type_str = "UNKNOWN"
            
            # Keep concise DEBUG for non-final transitions
            self.logger.debug(
                "MEXC order status update",
                order_id=order_id,
                symbol=symbol,
                side=side,
                status=status,
                client_order_id=client_order_id,
            )

            # Cache order type by client id to enrich trade frames
            try:
                if client_order_id and trade_type_str:
                    await self._order_type_by_client_id.set(client_order_id, trade_type_str)
            except Exception:
                pass
            
            # For FILLED orders: do not record trades here; privateDeals channel handles executions
            if status_num == 2:  # FILLED
                # Emit unified emoji summary with executed quote when available
                avg_price_raw = order_data.get('ap') or order_data.get('p') or '0'
                filled_base_raw = order_data.get('cv') or '0'  # cumulative executed base
                executed_quote_raw = order_data.get('ca') or None  # cumulative executed quote (USDT)

                self._emit_filled_summary_with_context(
                    symbol=symbol,
                    side=side,
                    quantity=str(filled_base_raw),
                    price=str(avg_price_raw),
                    executed_quote=(str(executed_quote_raw) if executed_quote_raw not in (None, "", "0") else None),
                    order_type=trade_type_str,
                    client_order_id=client_order_id,
                )
                return
                
            # Record canceled orders for display (status 4 = CANCELED)
            elif status_num == 4:
                # Skip manual (non-Hummingbot) orders
                try:
                    is_hbot = self.trading_data_service.config.hummingbot_filter.is_hummingbot_trade(self.exchange_name, client_order_id)
                except Exception:
                    is_hbot = True
                if is_hbot:
                    await self._record_canceled_order_for_display(
                    order_data=order_data,
                    order_id=order_id, 
                    symbol=symbol, 
                    side=side, 
                    client_order_id=client_order_id
                    )

            # Record underfilled finalized orders via state (status 5 = PARTIALLY_CANCELED)
            elif status_num == 5:
                try:
                    raw_timestamp = order_data.get('O')  # creation time
                    timestamp_ms = ensure_milliseconds(raw_timestamp) if raw_timestamp else ensure_milliseconds(0)
                    avg_price = order_data.get('ap') or order_data.get('p') or '0'

                    exec_base = order_data.get('cv') or '0'
                    exec_quote = order_data.get('ca') or '0'
                    orig_base = order_data.get('V') or order_data.get('v') or '0'
                    orig_quote = order_data.get('A') or order_data.get('a') or None

                    # Normalize by side semantics
                    if side == "BUY":
                        original_quote_amount = (Decimal(str(orig_quote)) if orig_quote not in (None, '') else None)
                        executed_quote_amount = (Decimal(str(exec_quote)) if exec_quote not in (None, '') else None)
                        executed_quantity = (Decimal(str(exec_base)) if exec_base not in (None, '') else None)
                        original_quantity = None
                        if original_quote_amount is not None and avg_price and avg_price != '0':
                            try:
                                original_quantity = (original_quote_amount / Decimal(str(avg_price)))
                            except Exception:
                                original_quantity = None
                        await self._maybe_record_underfilled(
                            symbol=symbol,
                            side=side,
                            is_market_buy_quote=True,
                            order_id=order_id,
                            client_order_id=client_order_id,
                            avg_price_str=str(avg_price),
                            timestamp_ms=timestamp_ms,
                            original_quantity_dec=(original_quantity if original_quantity is not None else executed_quantity),
                            executed_quantity_dec=(executed_quantity if executed_quantity is not None else Decimal('0')),
                            original_quote_dec=original_quote_amount,
                            executed_quote_dec=executed_quote_amount,
                        )
                    else:
                        original_quantity = (Decimal(str(orig_base)) if orig_base not in (None, '') else None)
                        executed_quantity = (Decimal(str(exec_base)) if exec_base not in (None, '') else None)
                        executed_quote_amount = (Decimal(str(exec_quote)) if exec_quote not in (None, '') else None)
                        await self._maybe_record_underfilled(
                            symbol=symbol,
                            side=side,
                            is_market_buy_quote=False,
                            order_id=order_id,
                            client_order_id=client_order_id,
                            avg_price_str=str(avg_price),
                            timestamp_ms=timestamp_ms,
                            original_quantity_dec=(original_quantity if original_quantity is not None else executed_quantity),
                            executed_quantity_dec=(executed_quantity if executed_quantity is not None else Decimal('0')),
                            original_quote_dec=None,
                            executed_quote_dec=executed_quote_amount,
                        )
                except Exception as e:
                    self.logger.error("Failed to record MEXC underfilled order", order_id=order_id, error=str(e))
            
        except Exception as e:
            self.logger.error("Failed to process MEXC order status", error=str(e))

    
    async def _handle_trade_execution(self, data: Dict) -> None:
        """Handle MEXC trade execution messages."""
        try:
            trade_data = data.get('d', {})
            
            # Extract trade fields (following Hummingbot mappings)
            trade_id = trade_data.get('t')
            symbol = data.get('s')
            side_num = trade_data.get('S')
            fill_base_amount = trade_data.get('v', '0')
            fill_quote_amount = trade_data.get('a', '0')
            fill_price = trade_data.get('p', '0')
            client_order_id = trade_data.get('c')
            # Trade frames do not carry order type; attempt to resolve via last known order update cached in-memory
            trade_type = "UNKNOWN"
            try:
                if client_order_id:
                    cached_type = await self._order_type_by_client_id.get(client_order_id)
                    if isinstance(cached_type, str) and cached_type:
                        trade_type = cached_type
            except Exception:
                trade_type = "UNKNOWN"
            # Optional fee info (fee amount and currency). For MEXC, fees are paid in USDT for both buys and sells.
            # We normalize at the WebSocket level to USDT so downstream only consumes USDT fees.
            fee_amount = trade_data.get('fe')  # may be None/""
            fee_currency = trade_data.get('fc')  # e.g., "USDT"
            
            if not all([trade_id, symbol, side_num is not None]):
                return
                
            side = SIDE_MAP.get(side_num, "UNKNOWN")
            
            self.logger.debug("MEXC trade execution", 
                           trade_id=trade_id, symbol=symbol, side=side,
                           fill_base_amount=fill_base_amount, fill_quote_amount=fill_quote_amount,
                           fill_price=fill_price, client_order_id=client_order_id)
            
            # Record trade execution with raw timestamp from trade data
            from ...core.trading_data import OrderStatus as DomainOrderStatus
            raw_timestamp = trade_data.get('T') or trade_data.get('t')  # Trade timestamp
            
            # Normalize fee params for recording
            fees_param = None
            try:
                if fee_amount not in (None, "", "0"):
                    # Normalize to USDT (native quote), keep numeric as-is.
                    fees_param = fee_amount
            except Exception:
                # If anything goes wrong, omit fee to avoid bad data
                fees_param = None
                fee_asset_param = None

            await self._record_trade_execution(
                order_id=trade_id,
                symbol=symbol,
                side=side,
                trade_type=trade_type,
                quantity=fill_base_amount,
                price=fill_price,
                quote_quantity=fill_quote_amount,
                fees=fees_param,
                client_order_id=client_order_id,
                order_status=DomainOrderStatus.FILLED,
                raw_timestamp=raw_timestamp
            )
            
        except Exception as e:
            self.logger.error("Failed to handle MEXC trade execution", error=str(e))
    
    async def _record_canceled_order_for_display(self, order_data: dict, order_id: str, 
                                                symbol: str, side: str, client_order_id: str = None) -> None:
        """Record canceled orders for live display."""
        try:
            # Skip manual (non-Hummingbot) orders
            try:
                is_hbot = self.trading_data_service.config.hummingbot_filter.is_hummingbot_trade(self.exchange_name, client_order_id)
            except Exception:
                is_hbot = True
            if not is_hbot:
                return
            price_str = order_data.get('p', '0') or order_data.get('ap', '0')
            
            # MEXC field meanings depend on order side:
            # BUY orders (quote -> base): 'a'/'A' = original quote amount (USDT), 'v' = executed base (usually 0)
            # SELL orders (base -> quote): 'v'/'V' = original base amount (tokens), 'a' = executed quote (usually 0)
            
            if side == "BUY":
                # For BUY orders: use original quote amount ('a'/'A') to calculate base quantity
                original_quote_amount = order_data.get('A', '0') or order_data.get('a', '0') or order_data.get('ca', '0')
                quote_quantity = original_quote_amount if original_quote_amount and original_quote_amount != "0" else "0"
                
                # Calculate base quantity from quote amount and price
                quantity = "0"
                if quote_quantity and quote_quantity != "0" and price_str and price_str != "0":
                    try:
                        quantity_dec = (Decimal(quote_quantity) / Decimal(price_str))
                        quantity = str(quantity_dec)
                        self.logger.debug("MEXC canceled BUY order", 
                                        quote_amount=quote_quantity, price=price_str, calculated_base_quantity=quantity)
                    except (Exception):
                        self.logger.warning("MEXC canceled BUY order - failed to calculate base quantity",
                                          quote_amount=quote_quantity, price=price_str)
                        
            else:  # SELL orders
                # For SELL orders: use original base amount ('v'/'V') directly
                quantity = order_data.get('V', '0') or order_data.get('v', '0') or order_data.get('cv', '0')
                
                # Calculate quote quantity from base amount and price
                quote_quantity = "0"
                if quantity and quantity != "0" and price_str and price_str != "0":
                    try:
                        quote_quantity_dec = (Decimal(quantity) * Decimal(price_str))
                        quote_quantity = str(quote_quantity_dec)
                        self.logger.debug("MEXC canceled SELL order", 
                                        base_quantity=quantity, price=price_str, calculated_quote_amount=quote_quantity)
                    except Exception:
                        self.logger.warning("MEXC canceled SELL order - failed to calculate quote amount",
                                          base_quantity=quantity, price=price_str)
            
            # Fallback: try cumulative amounts if primary calculation fails
            if quantity == "0":
                direct_usdt_amount = order_data.get('ca', '0') or order_data.get('cv', '0')
                if direct_usdt_amount and direct_usdt_amount != "0":
                    quote_quantity = direct_usdt_amount
                    if price_str and price_str != "0":
                        try:
                            quantity_dec = (Decimal(quote_quantity) / Decimal(price_str))
                            quantity = str(quantity_dec)
                            self.logger.debug("MEXC canceled order - using fallback cumulative amount", 
                                            quote_amount=quote_quantity, source="ca/cv")
                        except Exception:
                            pass
            
            # Extract raw timestamp from order data
            raw_timestamp = order_data.get('O')  # Order creation timestamp
            timestamp_ms = ensure_milliseconds(raw_timestamp) if raw_timestamp else ensure_milliseconds(0)
            
            from ...core.trading_data import OrderStatus as DomainOrderStatus
            
            # Emit unified inline summary for cancellations
            self._emit_cancelled_summary(
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price_str,
                quote_amount=(quote_quantity if quote_quantity not in (None, "0") else None),
            )

            # Persist cancelled order (bounded store) and publish event
            try:
                cancelled_obj = CancelledOrder(
                    order_id=order_id,
                    client_order_id=client_order_id,
                    exchange=self.exchange_name,
                    symbol=symbol,
                    side=DomainTradeSide.BUY if side == "BUY" else DomainTradeSide.SELL,
                    quantity=Decimal(quantity) if quantity else Decimal('0'),
                    price=Decimal(price_str) if price_str else Decimal('0'),
                    quote_amount=Decimal(quote_quantity) if quote_quantity and quote_quantity != "0" else None,
                    timestamp=timestamp_ms
                )
                await self.trading_data_service.record_cancelled_order(cancelled_obj)
            except Exception as e:
                self.logger.error("Failed to persist cancelled order", order_id=order_id, error=str(e))

            try:
                event = OrderCancelledEvent(
                    timestamp=timestamp_ms,
                    source=f"websocket.{self.exchange_name}",
                    exchange=self.exchange_name,
                    symbol=symbol,
                    side=side,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    quantity=quantity,
                    price=price_str
                )
                await self.event_bus.publish(event)
            except Exception as e:
                self.logger.error("Failed to publish OrderCancelledEvent", order_id=order_id, error=str(e))
            
        except Exception as e:
            self.logger.error("Failed to record canceled order", order_id=order_id, error=str(e))
    
    def get_subscription_status(self) -> dict:
        """Get current subscription status for monitoring."""
        return {
            "active_subscriptions": list(self._active_subscriptions),
            "total_active": len(self._active_subscriptions),
            "listen_key_status": "active" if self._listen_key else "inactive",
            "is_keep_alive_active": self._keep_alive_task is not None and not self._keep_alive_task.done()
        }
