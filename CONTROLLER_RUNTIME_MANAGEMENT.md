# Runtime Controller Management for Multi-Strategy Orchestration

This document describes how to control individual strategies (controllers) at runtime in Hummingbot's V2 framework.

## Overview

The V2 framework now supports **pausing and resuming individual controllers** without stopping the entire bot. This is particularly useful when running multiple arbitrage strategies simultaneously and you want to:

- Pause underperforming strategies
- Resume strategies when market conditions improve
- Manage risk on a per-strategy basis
- Control strategies based on custom conditions

## Quick Start

### 1. Setup Multiple Arbitrage Strategies

**Create controller configs** in `conf/controllers/`:

```bash
# Copy examples and customize
cp conf/controllers/arb_example_1.yml.example conf/controllers/arb_bsx_gate.yml
cp conf/controllers/arb_example_2.yml.example conf/controllers/arb_phl_mexc.yml
```

**Create main strategy config** in `conf/scripts/`:

```yaml
# conf/scripts/my_multi_arb.yml
script_file_name: multi_arbitrage_orchestrator.py
markets: {}
candles_config: []
controllers_config:
  - arb_bsx_gate.yml
  - arb_phl_mexc.yml
max_controller_drawdown_quote: 100.0
max_global_drawdown_quote: 500.0
```

### 2. Start the Strategy

```bash
# In Hummingbot CLI
>>> start --script multi_arbitrage_orchestrator.py --conf my_multi_arb.yml
```

### 3. Control Strategies at Runtime

**View controller status:**
```bash
>>> status
```

The status display now shows controller management commands and current status.

**Control via Python console** (type `>>>` in Hummingbot):

```python
# List all controllers
>>> self.strategy.list_controllers()

# Pause a specific controller
>>> self.strategy.pause_controller("arb_bsx_gate")

# Resume a controller
>>> self.strategy.resume_controller("arb_bsx_gate")

# Pause all controllers
>>> self.strategy.pause_all_controllers()

# Resume all controllers
>>> self.strategy.resume_all_controllers()

# Get controller status
>>> self.strategy.get_controller_status("arb_bsx_gate")
```

## Available Methods

### Core Methods

#### `pause_controller(controller_id: str) -> bool`
Pause a specific controller and stop its non-trading executors.

```python
success = self.strategy.pause_controller("arb_bsx_gate")
```

**Parameters:**
- `controller_id`: The ID of the controller (from config file)

**Returns:**
- `True` if successful, `False` otherwise

**Behavior:**
- Stops the controller's control loop
- Stops all active executors that are not currently trading
- Trading executors are allowed to complete
- Controller state is preserved

#### `resume_controller(controller_id: str) -> bool`
Resume a paused controller.

```python
success = self.strategy.resume_controller("arb_bsx_gate")
```

**Parameters:**
- `controller_id`: The ID of the controller

**Returns:**
- `True` if successful, `False` otherwise

**Behavior:**
- Restarts the controller's control loop
- Controller begins creating new executors based on its strategy logic

#### `pause_all_controllers() -> None`
Pause all running controllers.

```python
self.strategy.pause_all_controllers()
```

#### `resume_all_controllers() -> None`
Resume all paused controllers.

```python
self.strategy.resume_all_controllers()
```

#### `get_controller_status(controller_id: str) -> Optional[RunnableStatus]`
Get the current status of a controller.

```python
status = self.strategy.get_controller_status("arb_bsx_gate")
# Returns: RunnableStatus.RUNNING or RunnableStatus.TERMINATED
```

#### `list_controllers() -> Dict[str, Dict]`
Get a summary of all controllers.

```python
summary = self.strategy.list_controllers()
# Returns:
# {
#     "arb_bsx_gate": {
#         "status": RunnableStatus.RUNNING,
#         "type": "generic",
#         "name": "arbitrage_controller",
#         "active_executors": 2,
#         "total_executors": 15,
#         "global_pnl": Decimal("45.67"),
#         "volume_traded": Decimal("10000.00")
#     },
#     ...
# }
```

## Use Cases

### 1. Manual Control During Market Events

```python
# Pause risky arbitrage during high volatility
>>> self.strategy.pause_controller("arb_volatile_pair")

# Resume when markets stabilize
>>> self.strategy.resume_controller("arb_volatile_pair")
```

### 2. Conditional Auto-Pause

Add custom logic in your strategy:

```python
def on_tick(self):
    super().on_tick()

    # Auto-pause if controller loses too much
    for controller_id in self.controllers.keys():
        perf = self.get_performance_report(controller_id)
        if perf.global_pnl_quote < Decimal("-50"):
            if self.get_controller_status(controller_id) == RunnableStatus.RUNNING:
                self.logger().warning(f"Auto-pausing {controller_id} due to losses")
                self.pause_controller(controller_id)
```

### 3. Time-Based Control

```python
def on_tick(self):
    super().on_tick()

    import datetime
    hour = datetime.datetime.now().hour

    # Pause arbitrage during low liquidity hours
    if 2 <= hour <= 6:  # 2am-6am
        if self.get_controller_status("arb_low_liquidity") == RunnableStatus.RUNNING:
            self.pause_controller("arb_low_liquidity")
    else:
        if self.get_controller_status("arb_low_liquidity") == RunnableStatus.TERMINATED:
            self.resume_controller("arb_low_liquidity")
```

### 4. Performance-Based Rotation

```python
def on_tick(self):
    super().on_tick()

    # Pause worst performer every hour
    if self.current_timestamp % 3600 == 0:  # Every hour
        controller_perfs = {
            ctrl_id: self.get_performance_report(ctrl_id).global_pnl_quote
            for ctrl_id in self.controllers.keys()
        }
        worst_controller = min(controller_perfs, key=controller_perfs.get)
        self.pause_controller(worst_controller)
```

## Configuration Options

### Controller Config (`conf/controllers/arb_*.yml`)

```yaml
controller_type: generic
controller_name: arbitrage_controller
id: arb_bsx_gate  # Unique ID used for runtime control

# Manual kill switch - can also pause via config file edit
manual_kill_switch: false

# Other controller-specific settings...
```

### Main Strategy Config (`conf/scripts/*.yml`)

```yaml
script_file_name: multi_arbitrage_orchestrator.py

# List of controller configs to load
controllers_config:
  - arb_bsx_gate.yml
  - arb_phl_mexc.yml

# Auto-pause controller if drawdown exceeds this amount
max_controller_drawdown_quote: 100.0

# Stop entire bot if total drawdown exceeds this amount
max_global_drawdown_quote: 500.0

# How often to send performance reports (seconds)
performance_report_interval: 60

# Enable status change notifications
enable_controller_status_notifications: true
```

## Alternative Control Methods

### 1. Via Config File Hot-Reload

Edit the controller config file:

```yaml
# conf/controllers/arb_bsx_gate.yml
manual_kill_switch: true  # Set to true to pause
```

The framework reloads configs every 10 seconds, so the controller will pause automatically.

### 2. Via Drawdown Limits

Set automatic pause thresholds:

```yaml
# In main strategy config
max_controller_drawdown_quote: 100.0
```

The controller will auto-pause if it loses more than $100 from its peak PnL.

## Monitoring

### Status Display

The enhanced `status` command shows:

```
================================================================================
CONTROLLER MANAGEMENT (Python Console)
================================================================================
Available commands (use Python console):
  self.strategy.pause_controller('controller_id')    # Pause specific strategy
  self.strategy.resume_controller('controller_id')   # Resume specific strategy
  ...

Controller Status Summary:
  ▶ arb_bsx_gate: RUNNING | Active: 2/15 | PnL: $45.67 | Volume: $10000.00
  ⏸ arb_phl_mexc: TERMINATED | Active: 0/8 | PnL: $-12.34 | Volume: $5000.00

================================================================================
DETAILED STATUS
================================================================================
[Full performance details for each controller...]
```

### Logs

Controller status changes are logged:

```
2025-11-08 10:15:23 - INFO - Pausing controller: arb_bsx_gate
2025-11-08 10:15:23 - INFO - Controller 'arb_bsx_gate' paused successfully
2025-11-08 10:20:15 - INFO - Resuming controller: arb_bsx_gate
2025-11-08 10:20:15 - INFO - Controller 'arb_bsx_gate' resumed successfully
```

## Best Practices

1. **Use Descriptive Controller IDs**: Makes runtime management easier
   ```yaml
   id: arb_bsx_gate_bitmart  # Clear and descriptive
   ```

2. **Set Reasonable Drawdown Limits**: Protect against runaway losses
   ```yaml
   max_controller_drawdown_quote: 100.0
   ```

3. **Monitor Before Manual Control**: Check status before pausing/resuming
   ```python
   >>> self.strategy.list_controllers()  # Check current state
   >>> self.strategy.pause_controller("arb_id")  # Then control
   ```

4. **Allow Trading Executors to Complete**: The framework automatically handles this
   - Non-trading executors are stopped immediately
   - Trading executors are allowed to complete their trades

5. **Test in Paper Trading First**: Verify behavior with paper trading before live

## Troubleshooting

### Controller Not Found
```python
>>> self.strategy.pause_controller("wrong_id")
ERROR - Controller 'wrong_id' not found. Available controllers: ['arb_bsx_gate', 'arb_phl_mexc']
```
**Solution**: Use `list_controllers()` to see available IDs

### Controller Already Paused
```python
>>> self.strategy.pause_controller("arb_bsx_gate")
WARNING - Controller 'arb_bsx_gate' is already paused/stopped (status: TERMINATED)
```
**Solution**: Check status first with `get_controller_status()`

### Controller Not Resuming
If a controller was paused due to drawdown limits, it may be in `drawdown_exited_controllers` list and won't auto-resume. Check your strategy logic.

## Examples

See the complete example script at:
- `scripts/multi_arbitrage_orchestrator.py`

See example configurations at:
- `conf/controllers/arb_example_*.yml.example`
- `conf/scripts/multi_arbitrage_orchestrator_example.yml`

## API Reference

All methods are available on `StrategyV2Base` and its subclasses:

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `pause_controller()` | `controller_id: str` | `bool` | Pause specific controller |
| `resume_controller()` | `controller_id: str` | `bool` | Resume specific controller |
| `pause_all_controllers()` | - | `None` | Pause all controllers |
| `resume_all_controllers()` | - | `None` | Resume all controllers |
| `get_controller_status()` | `controller_id: str` | `Optional[RunnableStatus]` | Get controller status |
| `list_controllers()` | - | `Dict[str, Dict]` | Get all controllers summary |

## Contributing

If you find issues or have suggestions for improvements, please open an issue or PR on the Hummingbot repository.
