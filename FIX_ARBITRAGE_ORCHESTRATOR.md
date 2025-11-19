# Fix for ArbitrageL Orchestrator AttributeError

## Problem

When running `scripts/multi_strategy_orchestrator.py` with `buy_in_enabled: true`, you encounter:

```
AttributeError: 'hummingbot.strategy.arbitrage_l.position_balancer_handler.PositionBalancerHandler' object has no attribute 'c_handle_position_balancing'
```

## Root Cause

The Cython extensions were not recompiled after the latest code changes to `position_balancer_handler.pyx` were merged. The method `c_handle_position_balancing` exists in the source code but is not present in the compiled `.so` files.

## Solution

Rebuild the Cython extensions using one of these methods:

### Option 1: Quick rebuild (recommended)
```bash
./compile
```

### Option 2: Manual rebuild
```bash
python setup.py build_ext --inplace
```

### Option 3: Full clean rebuild
```bash
./clean && ./compile
```

## Verification

After rebuilding, verify the fix by:

1. Starting the orchestrator:
   ```bash
   python scripts/multi_strategy_orchestrator.py
   ```

2. Check that the position balancer handler works without the AttributeError

## Code Status

✅ The code is correct:
- `hummingbot/strategy/arbitrage_l/position_balancer_handler.pyx` - Method defined at line 473
- `hummingbot/strategy/arbitrage_l/position_balancer_handler.pxd` - Method declared at line 48
- `hummingbot/strategy/arbitrage_l/arbitrage.pyx` - Method imported and called at lines 575, 579, 584

The issue was simply that the compiled extensions were out of date.

## Branch

This fix is on branch: `claude/fix-arbitrage-orchestrator-01D57PVM3nS6TeppSyqxgTW3`
Based on: `dev_bb28`
