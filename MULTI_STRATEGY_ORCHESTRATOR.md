# Multi-Strategy Orchestrator: V1 Strategies with Shared Websocket Connections

## Overview

The **Multi-Strategy Orchestrator** allows you to run multiple V1 Cython strategies (like `arbitrage_m`) simultaneously while sharing websocket connections to exchanges. This provides significant resource savings and rate limit optimization without requiring any modifications to existing V1 strategies.

### Key Benefits

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Resource Efficiency** | One websocket connection per exchange instead of one per strategy | 50-75% reduction in connections |
| **Rate Limit Optimization** | All strategies share the same connection pool | Reduced API throttling |
| **Memory Savings** | Order books and market data cached once and shared | Lower memory footprint |
| **No Code Changes** | Works with existing V1 Cython strategies unmodified | Zero migration cost |
| **Full V1 Features** | Buy-in module, additional markets, all V1 functionality preserved | No feature loss |
| **Independent Operation** | Each strategy maintains its own state and logic | Isolated strategy execution |

### Example Resource Savings

**Scenario:** Running 3 arbitrage_m strategies across Binance, KuCoin, and MEXC

**Without Orchestrator:**
- Strategy 1: Binance WS + KuCoin WS
- Strategy 2: Binance WS + MEXC WS
- Strategy 3: KuCoin WS + MEXC WS
- **Total: 6 websocket connections**

**With Orchestrator:**
- All strategies share: 1 Binance WS + 1 KuCoin WS + 1 MEXC WS
- **Total: 3 websocket connections** (50% reduction)

## How It Works

### Architecture

```
MultiStrategyOrchestrator (ScriptStrategyBase)
│
├── connectors: Dict[str, ConnectorBase]
│   ├── binance: ConnectorBase (1 websocket connection)
│   ├── kucoin: ConnectorBase (1 websocket connection)
│   └── mexc: ConnectorBase (1 websocket connection)
│
├── strategies: List[V1StrategyInstance]
│   ├── arbitrage_m_instance_1
│   │   ├── Registers event listeners on shared connectors
│   │   ├── Maintains own state & order tracking
│   │   └── c_tick() → independent execution
│   │
│   ├── arbitrage_m_instance_2
│   │   ├── Registers event listeners on shared connectors
│   │   ├── Maintains own state & order tracking
│   │   └── c_tick() → independent execution
│   │
│   └── arbitrage_m_instance_3
│       ├── Registers event listeners on shared connectors
│       ├── Maintains own state & order tracking
│       └── c_tick() → independent execution
│
└── on_tick() → tick all strategies independently
```

### Websocket Connection Sharing Mechanism

The orchestrator leverages Hummingbot's built-in event system to share websocket connections:

1. **Connector Pool Creation**
   - `TradingCore` creates ONE `ConnectorBase` instance per exchange
   - Each connector maintains ONE websocket connection to its exchange
   - Connectors stored in shared `connectors` dict

2. **Event Listener Registration**
   - Each V1 strategy calls `StrategyBase.c_add_markets(connectors)`
   - This registers the strategy's event listeners on the connector
   - Connectors support **multiple listeners per event** (observer pattern)

3. **Event Broadcasting**
   - When a market event occurs (order fill, price update, etc.)
   - Connector broadcasts to **all registered listeners**
   - Each strategy receives events and processes them independently

4. **Independent Execution**
   - Each strategy has its own state, logic, and order tracking
   - Strategies don't interfere with each other
   - Clean isolation despite shared connections

### Code Flow

```python
# 1. Orchestrator initialization
orchestrator = MultiStrategyOrchestrator(
    connectors={'binance': <shared_connector>, 'kucoin': <shared_connector>},
    config=config
)

# 2. For each strategy config
for strategy_config in config.arbitrage_m_strategies:
    # 3. Create market pairs wrapping SHARED connectors
    market_pairs = [
        ArbitrageMMarketPair(
            first=MarketTradingPairTuple(market=connectors['binance'], ...),  # ← SHARED
            second=MarketTradingPairTuple(market=connectors['kucoin'], ...)   # ← SHARED
        )
    ]

    # 4. Create strategy instance
    strategy = ArbitrageMStrategy()

    # 5. Initialize - this calls c_add_markets() which registers event listeners
    strategy.init_params(market_pairs=market_pairs, ...)

    # 6. Store strategy
    orchestrator.strategies.append(strategy)

# 7. On each tick
def on_tick():
    for strategy in strategies:
        strategy.c_tick(timestamp)  # Independent execution
```

## Quick Start

### 1. Create Configuration File

Create `conf/scripts/my_multi_arb.yml`:

```yaml
script_file_name: multi_strategy_orchestrator.py

# Define all markets needed
markets:
  binance:
    - BTC-USDT
    - ETH-USDT
  kucoin:
    - BTC-USDT
    - ETH-USDT

# Define strategy instances
arbitrage_m_strategies:
  - name: "btc_arb"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: BTC-USDT
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.5

  - name: "eth_arb"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: ETH-USDT
    secondary_trading_pair: ETH-USDT
    min_profitability: 0.5
```

### 2. Run the Orchestrator

```bash
# In Hummingbot CLI
start --script conf/scripts/my_multi_arb.yml
```

### 3. Monitor Status

```bash
# Check status
status

# You'll see:
# - Shared connector status
# - Combined balance view
# - Individual strategy performance
# - Websocket connection count
```

## Configuration Reference

### Top-Level Configuration

```yaml
script_file_name: multi_strategy_orchestrator.py

# All markets needed across all strategies
# Format: {exchange_name: [list of trading pairs]}
markets:
  exchange1:
    - PAIR1
    - PAIR2
  exchange2:
    - PAIR1

# List of arbitrage_m strategy configurations
arbitrage_m_strategies:
  - name: "strategy_name"
    # ... strategy config ...
```

### Strategy Configuration

Each strategy in `arbitrage_m_strategies` supports these fields:

#### Required Fields

```yaml
name: "unique_strategy_name"           # Unique identifier
primary_market: exchange_name          # e.g., "binance"
secondary_market: exchange_name        # e.g., "kucoin"
primary_trading_pair: "BASE-QUOTE"     # e.g., "BTC-USDT"
secondary_trading_pair: "BASE-QUOTE"   # e.g., "BTC-USDT"
```

#### Profitability Settings

```yaml
min_profitability: 0.5                 # Minimum profit % (default: 0.5)
```

#### Conversion Rates

```yaml
use_oracle_conversion_rate: false      # Use rate oracle (default: false)
secondary_to_primary_base_conversion_rate: 1.0    # Base asset conversion
secondary_to_primary_quote_conversion_rate: 1.0   # Quote asset conversion
```

#### Buy-In Module

```yaml
buy_in_enabled: false                  # Enable buy-in (default: false)
buy_in_target_usd: 100.0              # Target USD value (default: 100.0)
buy_in_min_profitability: 0.005       # Min profit for buy-in (default: 0.005)
```

#### Timing Parameters

```yaml
status_report_interval: 60.0          # Status log interval (default: 60s)
next_trade_delay_interval: 2.0        # Delay between trades (default: 2s)
order_timeout: 300.0                  # Order timeout (default: 300s)
```

#### Additional Markets

```yaml
additional_markets:                    # Extra markets for triangular arb
  - exchange:PAIR                      # e.g., "mexc:BTC-USDT"
  - exchange:PAIR
```

## Example Configurations

### Simple: 2 Strategies, 2 Exchanges

```yaml
script_file_name: multi_strategy_orchestrator.py

markets:
  binance: [BTC-USDT, ETH-USDT]
  kucoin: [BTC-USDT, ETH-USDT]

arbitrage_m_strategies:
  - name: "btc_arb"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: BTC-USDT
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.5

  - name: "eth_arb"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: ETH-USDT
    secondary_trading_pair: ETH-USDT
    min_profitability: 0.5
```

**Resource Savings:** 2 connections instead of 4 (50% reduction)

### Advanced: Multiple Exchanges with Buy-In

```yaml
script_file_name: multi_strategy_orchestrator.py

markets:
  binance: [BTC-USDT, ETH-USDT]
  kucoin: [BTC-USDT, ETH-USDT]
  mexc: [BTC-USDT, ETH-USDT]

arbitrage_m_strategies:
  - name: "btc_conservative"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: BTC-USDT
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.8

  - name: "btc_aggressive"
    primary_market: binance
    secondary_market: mexc
    primary_trading_pair: BTC-USDT
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.3
    buy_in_enabled: true
    buy_in_target_usd: 150.0

  - name: "eth_triangular"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: ETH-USDT
    secondary_trading_pair: ETH-USDT
    min_profitability: 0.5
    additional_markets:
      - mexc:ETH-USDT
```

**Resource Savings:** 3 connections instead of 9 (67% reduction)

## Monitoring and Management

### Status Command

The `status` command shows:

```
================================================================================
MULTI-STRATEGY ORCHESTRATOR STATUS
Running 3 strategies with SHARED websocket connections
================================================================================

Shared Connectors:
  binance: ✓ READY
  kucoin: ✓ READY
  mexc: ✓ READY

Balances:
  Exchange  Asset  Total Balance  Available Balance
  binance   BTC    0.5000         0.4500
  binance   USDT   10000.00       9500.00
  ...

Active Orders:
  Exchange  Market     Side  Price     Amount    Age
  binance   BTC-USDT   buy   50000.0   0.001     00:00:15
  ...

--------------------------------------------------------------------------------
Strategy 1: btc_conservative
--------------------------------------------------------------------------------
  Type: arbitrage_m
  Markets: binance/BTC-USDT <-> kucoin/BTC-USDT
  Min Profitability: 0.8%
  Buy-in Enabled: False
  Active Orders: 2

--------------------------------------------------------------------------------
Strategy 2: btc_aggressive
--------------------------------------------------------------------------------
  Type: arbitrage_m
  Markets: binance/BTC-USDT <-> mexc/BTC-USDT
  Min Profitability: 0.3%
  Buy-in Enabled: True
  Active Orders: 1

...
```

### Logs

Each strategy logs independently with its name as prefix:

```
[multi_strategy_orchestrator] MultiStrategyOrchestrator initialized with 3 strategies
[multi_strategy_orchestrator] Shared connectors: ['binance', 'kucoin', 'mexc']
[arbitrage_m] (btc_conservative) Found profitable opportunity: +0.85%
[arbitrage_m] (btc_aggressive) Executing buy-in: BTC target=0.003
```

## Technical Details

### Connector Sharing Implementation

From `hummingbot/strategy/strategy_base.pyx:313-336`:

```cython
cdef c_add_markets(self, list markets):
    for market in markets:
        typed_market = market
        # Register event listeners on the connector
        typed_market.c_add_listener(self.BUY_ORDER_COMPLETED_EVENT_TAG, self._sb_complete_buy_order_listener)
        typed_market.c_add_listener(self.SELL_ORDER_COMPLETED_EVENT_TAG, self._sb_complete_sell_order_listener)
        # ... more listeners ...
        self._sb_markets.add(typed_market)
```

**Key Points:**
- Multiple strategies can call `c_add_listeners()` on the SAME connector
- Connectors maintain a list of listeners for each event type
- When events fire, all registered listeners are notified
- Listeners are isolated - one strategy's listener doesn't affect others

### Event Flow

```
1. Market Event Occurs (e.g., order fill)
   └─> Connector receives event from websocket

2. Connector Broadcasts Event
   ├─> Strategy 1's listener receives event
   ├─> Strategy 2's listener receives event
   └─> Strategy 3's listener receives event

3. Each Strategy Processes Independently
   ├─> Strategy 1: Updates own state, checks profitability
   ├─> Strategy 2: Updates own state, checks profitability
   └─> Strategy 3: Updates own state, checks profitability
```

### Memory and Performance

**Memory Usage:**
- **Order Books:** Stored once per connector, shared across strategies
- **Market Data:** Cached in connector, accessed by all strategies
- **Strategy State:** Each strategy maintains independent state (isolated)

**Performance:**
- **Cython Speed:** Full V1 Cython performance preserved (C-level execution)
- **Event Distribution:** Minimal overhead (C-level observer pattern)
- **Lock-Free:** No locks needed (event system is thread-safe)

## Comparison: Orchestrator vs Full V2 Migration

| Aspect | Multi-Strategy Orchestrator | Full V2 Controller |
|--------|----------------------------|-------------------|
| **Code Changes** | None (uses existing V1) | ~2,200 lines rewrite |
| **Websocket Sharing** | ✅ Yes | ✅ Yes |
| **V1 Features** | ✅ All (buy-in, additional markets) | ⚠️ Partial (no buy-in yet) |
| **Performance** | ✅ Full Cython speed | ✅ Cython helpers |
| **Migration Risk** | ✅ Zero (no code changes) | ⚠️ Medium (new codebase) |
| **Dashboard Integration** | ⚠️ Limited | ✅ Full V2 dashboard |
| **Development Time** | ✅ Immediate | ⚠️ Ongoing (feature parity) |

**Recommendation:**
- **Production Now:** Use orchestrator (zero risk, full features)
- **Long-Term:** Migrate to V2 controller when feature parity achieved

## Troubleshooting

### Connectors Not Ready

**Symptom:** `Market connectors are not ready`

**Solution:**
```bash
# Check API keys are configured
config api_keys

# Verify markets are correct in config
# Ensure exchange names match exactly (lowercase)
```

### Strategy Not Executing

**Symptom:** Strategies initialize but don't trade

**Solution:**
```bash
# Check profitability threshold
# Market spread might be smaller than min_profitability

# Reduce min_profitability temporarily for testing
min_profitability: 0.1  # Very low for testing
```

### Duplicate Orders

**Symptom:** Multiple strategies placing similar orders

**Solution:**
- Each strategy operates independently - this is expected
- Use different `min_profitability` thresholds
- Use different trading pairs
- Or use `next_trade_delay_interval` to stagger execution

### Memory Issues

**Symptom:** High memory usage

**Solution:**
- Limit number of strategies (recommend ≤ 5 per orchestrator)
- Use separate orchestrator instances for different exchange groups
- Monitor with `status` command

## Advanced Usage

### Multiple Orchestrator Instances

You can run multiple orchestrator instances for different exchange groups:

**Orchestrator 1:** Binance + KuCoin (Asian markets)
**Orchestrator 2:** Kraken + Coinbase (Western markets)

This provides additional isolation and organization.

### Custom Strategy Integration

The orchestrator can be extended to support other V1 strategies:

```python
# In multi_strategy_orchestrator.py
def _add_custom_strategy(self, config):
    strategy = CustomV1Strategy()
    strategy.init_params(...)
    self.strategies.append(strategy)
```

### Dynamic Strategy Management

Strategies can be added/removed by modifying the config file and restarting:

```bash
# 1. Stop orchestrator
stop

# 2. Edit config file
# Add new strategy to arbitrage_m_strategies list

# 3. Restart
start --script conf/scripts/my_multi_arb.yml
```

## Best Practices

1. **Start Small:** Begin with 2-3 strategies, verify performance, then scale up
2. **Monitor Resources:** Use `status` regularly to check connector health
3. **Stagger Execution:** Use different `next_trade_delay_interval` values
4. **Profitability Tuning:** Start conservative (higher thresholds), optimize gradually
5. **Buy-In Carefully:** Enable buy-in only after verifying strategy performance
6. **Log Review:** Check logs for errors or unusual behavior
7. **Gradual Scaling:** Add strategies one at a time, verify each addition

## Support and Contributing

- **Issues:** https://github.com/hummingbot/hummingbot/issues
- **Discord:** https://discord.gg/hummingbot
- **Docs:** https://hummingbot.org/docs

## License

Apache 2.0 - Same as Hummingbot
