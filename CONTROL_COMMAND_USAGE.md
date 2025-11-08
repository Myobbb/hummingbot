# Control Command Usage Guide

## Overview
The `control` command allows you to manage individual strategies when running multi-strategy orchestrators like `multi_strategy_orchestrator.py`.

## Implementation Status
✅ **COMPLETE** - All components are implemented and working:
- ✅ Parser integration (`hummingbot/client/ui/parser.py`)
- ✅ Autocomplete support (`hummingbot/client/ui/completer.py`)
- ✅ Command implementation (`hummingbot/client/command/strategy_control_command.py`)
- ✅ Command registration (`hummingbot/client/command/__init__.py`)
- ✅ Multi-strategy orchestrator support (`scripts/multi_strategy_orchestrator.py`)

## Fixed Issues
1. **Return Type Fix**: Updated `pause_all_strategies()` and `resume_all_strategies()` to return `int` (count of affected strategies) instead of `None`.

## Usage

### Available Commands

#### 1. List all strategies
```bash
control list
```
Shows all strategies with their current status, trading pairs, and performance stats.

#### 2. Pause a strategy
```bash
control pause <strategy_name_or_token>
```
Examples:
- `control pause arb_bsx_gate_bitmart` (by full name)
- `control pause BSX` (by token symbol)

#### 3. Resume a strategy
```bash
control resume <strategy_name_or_token>
```
Examples:
- `control resume arb_bsx_gate_bitmart`
- `control resume BSX`

#### 4. Pause all strategies
```bash
control pause_all
```
Pauses all currently running strategies.

#### 5. Resume all strategies
```bash
control resume_all
```
Resumes all paused strategies.

## Autocomplete Support
The control command has full autocomplete support:
- Type `control ` and press TAB to see available actions
- Actions: `list`, `pause`, `resume`, `pause_all`, `resume_all`

## Requirements
- The running strategy must be a multi-strategy orchestrator
- The strategy must implement these methods:
  - `pause_strategy_by_identifier(identifier: str) -> bool`
  - `resume_strategy_by_identifier(identifier: str) -> bool`
  - `list_strategies() -> Dict[str, Dict[str, Any]]`
  - `pause_all_strategies() -> int`
  - `resume_all_strategies() -> int`

## Compatibility
Currently compatible with:
- `scripts/multi_strategy_orchestrator.py`

## Troubleshooting

### Command not recognized
If the `control` command is not recognized:
1. **Restart Hummingbot** - Changes to parser/commands require a restart
2. **Verify branch** - Ensure you're on the `dev_bb28` branch
3. **Check imports** - Verify no import errors at startup

### Command recognized but does nothing
If the command runs but shows "does not support runtime control":
1. Verify you're running `multi_strategy_orchestrator.py`
2. Check that the strategy is properly started
3. Look for any error messages in the logs

### How to restart Hummingbot
1. Type `stop` to stop the current strategy
2. Type `exit` to exit Hummingbot
3. Restart Hummingbot
4. Type `start --script multi_strategy_orchestrator.py --conf your_config.yml`

## Alternative: Python Console Commands
You can also control strategies directly from the Python console (>>>):

```python
# Import all control functions
from scripts.multi_strategy_orchestrator import *

# Use the functions
pause("BSX")
resume("BSX")
list_arb()
pause_all()
resume_all()
help_arb()
```

## Examples

### Example Session
```bash
# Start the multi-strategy orchestrator
>>> start --script multi_strategy_orchestrator.py --conf my_arb_config.yml

# List all strategies
>>> control list
================================================================================
STRATEGY STATUS
================================================================================

▶ arb_bsx_gate_bitmart
   Status: ACTIVE
   Tokens: BSX

⏸ arb_phl_kucoin_mexc
   Status: PAUSED
   Tokens: PHL

================================================================================

# Pause a strategy by token
>>> control pause BSX
✓ Strategy paused successfully

# Resume by full name
>>> control resume arb_phl_kucoin_mexc
✓ Strategy resumed successfully

# Pause everything
>>> control pause_all
✓ Paused 2 strategies
```

## Notes
- Pausing a strategy cancels all its open orders
- Paused strategies don't execute trades but remain loaded
- Strategy state is preserved when paused
- Resume restores normal operation immediately
