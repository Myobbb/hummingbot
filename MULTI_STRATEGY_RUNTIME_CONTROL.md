# Runtime Control for Multi-Strategy Orchestrator

This document describes how to control individual arbitrage_m strategies at runtime when using the `multi_strategy_orchestrator.py` script.

## Overview

The Multi-Strategy Orchestrator allows you to run multiple arbitrage_m strategies simultaneously while sharing websocket connections. With runtime control, you can now **pause and resume individual strategies** without stopping the entire bot.

## Key Features

✅ **Pause/Resume Individual Strategies** - Control specific arbitrage_m instances
✅ **Zero Downtime** - Other strategies continue running while one is paused
✅ **Automatic Order Cancellation** - Open orders are cancelled when pausing
✅ **Real-Time Status Display** - See which strategies are running or paused
✅ **Simple Python Console Interface** - Easy control from Hummingbot CLI

## Quick Start

### 1. Configure Multiple Arbitrage_m Strategies

Create a configuration file (e.g., `conf/scripts/my_multi_arb.yml`):

```yaml
script_file_name: multi_strategy_orchestrator.py

markets:
  gate_io:
    - BSX-USDT
  bitmart:
    - BSX-USDT
    - PHL-USDT
  mexc:
    - PHL-USDT

arbitrage_m_strategies:
  - name: arb_bsx_gate_bitmart
    primary_market: gate_io
    secondary_market: bitmart
    primary_trading_pair: BSX-USDT
    secondary_trading_pair: BSX-USDT
    min_profitability: 1.9
    buy_in_enabled: false

  - name: arb_phl_mexc_bitmart
    primary_market: mexc
    secondary_market: bitmart
    primary_trading_pair: PHL-USDT
    secondary_trading_pair: PHL-USDT
    min_profitability: 2.2
    buy_in_enabled: true
    buy_in_target_usd: 1100.0
    buy_in_min_profitability: 2.5
```

### 2. Start the Orchestrator

```bash
# In Hummingbot CLI
>>> start --script multi_strategy_orchestrator.py --conf my_multi_arb.yml
```

### 3. Control Strategies at Runtime

Once running, use the Python console (`>>>` prompt in Hummingbot):

```python
# View status
>>> status

# Pause a specific strategy
>>> self.strategy.pause_strategy("arb_bsx_gate_bitmart")

# Resume it
>>> self.strategy.resume_strategy("arb_bsx_gate_bitmart")

# List all strategies
>>> self.strategy.list_strategies()
```

## Available Methods

### `pause_strategy(strategy_name: str) -> bool`

Pause a specific arbitrage_m strategy by name.

```python
>>> self.strategy.pause_strategy("arb_bsx_gate_bitmart")
INFO - Pausing strategy: arb_bsx_gate_bitmart
INFO - Cancelled all open orders for 'arb_bsx_gate_bitmart'
INFO - Strategy 'arb_bsx_gate_bitmart' paused successfully
```

**Parameters:**
- `strategy_name`: The name from your config (e.g., "arb_bsx_gate_bitmart")

**Returns:**
- `True` if successful, `False` if strategy not found or already paused

**Behavior:**
- Stops ticking the strategy (no new trades)
- Cancels all open orders
- Preserves strategy state
- Other strategies continue unaffected

### `resume_strategy(strategy_name: str) -> bool`

Resume a paused strategy.

```python
>>> self.strategy.resume_strategy("arb_bsx_gate_bitmart")
INFO - Resuming strategy: arb_bsx_gate_bitmart
INFO - Strategy 'arb_bsx_gate_bitmart' resumed successfully
```

**Parameters:**
- `strategy_name`: The name of the strategy to resume

**Returns:**
- `True` if successful, `False` if strategy not found or already running

**Behavior:**
- Resumes ticking the strategy
- Strategy begins evaluating arbitrage opportunities again
- Websocket connections remain active throughout

### `pause_all_strategies() -> None`

Pause all running strategies.

```python
>>> self.strategy.pause_all_strategies()
INFO - Pausing all strategies...
INFO - Pausing strategy: arb_bsx_gate_bitmart
INFO - Strategy 'arb_bsx_gate_bitmart' paused successfully
INFO - Pausing strategy: arb_phl_mexc_bitmart
INFO - Strategy 'arb_phl_mexc_bitmart' paused successfully
```

### `resume_all_strategies() -> None`

Resume all paused strategies.

```python
>>> self.strategy.resume_all_strategies()
INFO - Resuming all strategies...
INFO - Resuming strategy: arb_bsx_gate_bitmart
INFO - Strategy 'arb_bsx_gate_bitmart' resumed successfully
INFO - Resuming strategy: arb_phl_mexc_bitmart
INFO - Strategy 'arb_phl_mexc_bitmart' resumed successfully
```

### `list_strategies() -> Dict[str, Dict]`

Get detailed information about all strategies.

```python
>>> self.strategy.list_strategies()
{
  'arb_bsx_gate_bitmart': {
    'status': 'RUNNING',
    'paused': False,
    'primary_market': 'gate_io',
    'secondary_market': 'bitmart',
    'primary_pair': 'BSX-USDT',
    'secondary_pair': 'BSX-USDT',
    'min_profitability': Decimal('1.9'),
    'best_profitability': '2.1%'
  },
  'arb_phl_mexc_bitmart': {
    'status': 'PAUSED',
    'paused': True,
    'primary_market': 'mexc',
    'secondary_market': 'bitmart',
    'primary_pair': 'PHL-USDT',
    'secondary_pair': 'PHL-USDT',
    'min_profitability': Decimal('2.2'),
    'best_profitability': 'PAUSED'
  }
}
```

## Status Display

The `status` command shows runtime control information:

```
================================================================================
STRATEGY CONTROL (Python Console)
================================================================================
Available commands (use Python console '>>>'):
  self.strategy.pause_strategy('strategy_name')     # Pause specific arbitrage_m
  self.strategy.resume_strategy('strategy_name')    # Resume specific arbitrage_m
  self.strategy.pause_all_strategies()              # Pause all strategies
  self.strategy.resume_all_strategies()             # Resume all strategies
  self.strategy.list_strategies()                   # Show strategy summary

Strategies: 1 running, 1 paused
  ▶ arb_bsx_gate_bitmart: RUNNING
  ⏸ arb_phl_mexc_bitmart: PAUSED
================================================================================

Balances:
  Exchange    Asset    Total   Available
  --------    -----    -----   ---------
  gate_io     BSX      1000    1000
  ...

BSX-USDT gate_bitmart | min 1.9% | best 2.1%
PHL-USDT mexc_bitmart | min 2.2% | best PAUSED
```

## Use Cases

### 1. Pause Underperforming Strategy

```python
# Check which strategy is losing
>>> self.strategy.list_strategies()

# Pause the one that's underperforming
>>> self.strategy.pause_strategy("arb_token_losing_money")
```

### 2. Pause During High Volatility

```python
# Pause risky arbitrage during market events
>>> self.strategy.pause_strategy("arb_volatile_pair")

# Resume when markets stabilize
>>> self.strategy.resume_strategy("arb_volatile_pair")
```

### 3. Rotate Strategies Based on Opportunities

```python
# Get current status
>>> summary = self.strategy.list_strategies()

# Pause strategies with low profitability
>>> for name, info in summary.items():
...     if info['best_profitability'] == 'n/a':
...         self.strategy.pause_strategy(name)
```

### 4. Pause for Maintenance

```python
# Pause one strategy to check logs/balances
>>> self.strategy.pause_strategy("arb_bsx_gate_bitmart")

# Verify balances, check order history, etc.

# Resume when ready
>>> self.strategy.resume_strategy("arb_bsx_gate_bitmart")
```

### 5. Emergency Stop Individual Strategy

```python
# Immediately pause strategy if something goes wrong
>>> self.strategy.pause_strategy("arb_problem_strategy")

# All orders cancelled, strategy stops trading
# Other strategies continue unaffected
```

## How It Works

### Internal Mechanism

1. **Paused State Tracking**: Each `V1StrategyInstance` has a `paused` boolean flag
2. **Tick Skipping**: Paused strategies are skipped in the `on_tick()` loop
3. **Order Cancellation**: When pausing, calls `cancel_all_orders()` on the strategy
4. **State Preservation**: Strategy state is preserved while paused
5. **Instant Resume**: Resuming simply sets `paused=False` and strategy begins ticking again

### Websocket Connections

- **Shared Connections Remain Active**: Websockets stay connected even when strategies are paused
- **Efficient Resource Usage**: Pausing doesn't disconnect from exchanges
- **Instant Resume**: No reconnection delay when resuming
- **Other Strategies Unaffected**: Shared websockets continue serving active strategies

### Thread Safety

The orchestrator runs in a single thread with Hummingbot's event loop, so pause/resume operations are thread-safe.

## Configuration Reference

### Strategy Configuration

```yaml
arbitrage_m_strategies:
  - name: unique_strategy_name  # Used for runtime control
    primary_market: exchange_name
    secondary_market: exchange_name
    primary_trading_pair: BASE-QUOTE
    secondary_trading_pair: BASE-QUOTE
    min_profitability: 1.5  # Percentage

    # Optional advanced settings
    use_oracle_conversion_rate: false
    buy_in_enabled: false
    buy_in_target_usd: 1000.0
    buy_in_min_profitability: 1.5
    additional_markets:
      - mexc:BASE-QUOTE  # Additional exchanges for opportunities
```

### Important: Strategy Naming

- Each strategy **must have a unique `name`**
- Use this name for runtime control commands
- Names are case-sensitive
- Use descriptive names (e.g., "arb_bsx_gate_bitmart" not "strategy1")

## Troubleshooting

### Strategy Not Found

```python
>>> self.strategy.pause_strategy("wrong_name")
ERROR - Strategy 'wrong_name' not found. Available strategies: ['arb_bsx_gate_bitmart', 'arb_phl_mexc_bitmart']
```

**Solution**: Check available strategy names with `list_strategies()` or `status`

### Strategy Already Paused

```python
>>> self.strategy.pause_strategy("arb_bsx_gate_bitmart")
WARNING - Strategy 'arb_bsx_gate_bitmart' is already paused
```

**Solution**: Check status before pausing

### Strategy Already Running

```python
>>> self.strategy.resume_strategy("arb_bsx_gate_bitmart")
WARNING - Strategy 'arb_bsx_gate_bitmart' is already running
```

**Solution**: Check status before resuming

## Comparison: V1 vs V2 Control

This runtime control is for **V1 strategies** (arbitrage_m). If you're using **V2 controllers**, see `CONTROLLER_RUNTIME_MANAGEMENT.md` instead.

| Feature | V1 (multi_strategy_orchestrator) | V2 (StrategyV2Base) |
|---------|----------------------------------|---------------------|
| Strategy Type | Cython V1 (arbitrage_m) | V2 Controllers |
| Control Method | `pause_strategy()` | `pause_controller()` |
| Paused State | `strategy_instance.paused` | `controller.status` |
| Tick Behavior | Skip if paused | Controller stops |

## Best Practices

1. **Use Descriptive Names**: Makes runtime management easier
   ```yaml
   name: arb_bsx_gate_bitmart  # ✅ Clear and descriptive
   name: strategy1             # ❌ Not descriptive
   ```

2. **Check Status Before Control**: Avoid warnings
   ```python
   >>> summary = self.strategy.list_strategies()
   >>> if not summary['arb_name']['paused']:
   ...     self.strategy.pause_strategy('arb_name')
   ```

3. **Monitor After Pausing**: Verify orders were cancelled
   ```python
   >>> self.strategy.pause_strategy("arb_name")
   >>> # Check that no open orders remain
   ```

4. **Test in Paper Trading First**: Verify behavior before live trading

5. **Log Important Actions**: Commands and results are logged automatically

## Examples

See example configurations in:
- `scripts/examples/conf_multi_arbitrage_m_simple.yml`
- `scripts/examples/conf_multi_arbitrage_m_advanced.yml`
- `scripts/examples/conf_multi_arbitrage_m_cross_asset.yml`

## API Reference Summary

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `pause_strategy()` | `strategy_name: str` | `bool` | Pause specific strategy |
| `resume_strategy()` | `strategy_name: str` | `bool` | Resume specific strategy |
| `pause_all_strategies()` | - | `None` | Pause all strategies |
| `resume_all_strategies()` | - | `None` | Resume all strategies |
| `list_strategies()` | - | `Dict[str, Dict]` | Get all strategies summary |

## Contributing

Found an issue or have a suggestion? Open an issue or PR on the Hummingbot repository.

## See Also

- [Multi-Strategy Orchestrator Documentation](scripts/multi_strategy_orchestrator.py)
- [Arbitrage_m Strategy Guide](hummingbot/strategy/arbitrage_m/)
- [V2 Controller Runtime Management](CONTROLLER_RUNTIME_MANAGEMENT.md)
