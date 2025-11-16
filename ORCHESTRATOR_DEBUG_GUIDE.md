# Multi-Strategy Orchestrator Debug Guide

## Issue: "No strategies loaded" despite valid config file

### Summary
The orchestrator shows `0 active 0 paused` in status even though the config file contains 43 properly formatted strategies.

### Investigation Steps

#### 1. Verify Config File Loading

When you start the orchestrator, you should now see comprehensive DEBUG logs showing:

```
DEBUG: Config type: <class 'scripts.multi_strategy_orchestrator.MultiStrategyOrchestratorConfig'>
DEBUG: Config markets: {...}
DEBUG: Config arbitrage_m_strategies type: <class 'list'>
DEBUG: Number of strategies in config: 43
DEBUG: Config dict keys: ['script_file_name', 'markets', 'arbitrage_m_strategies', 'config_file_path']
DEBUG: arbitrage_m_strategies in dict: True
DEBUG: arbitrage_m_strategies value type: <class 'list'>
DEBUG: arbitrage_m_strategies length: 43
DEBUG: First strategy: {'name': 'arb_phl_mexc_bitmart', ...}
```

**If you see `Number of strategies in config: 0`:**
- The config file is loading but the `arbitrage_m_strategies` list is empty
- Check that your YAML file has correct indentation (2 spaces, no tabs)
- Verify the field name is exactly `arbitrage_m_strategies` (not `strategies` or `arbitrage_strategies`)

**If you see `NO ATTR` or errors:**
- The config object is not being created correctly
- The config file path might be wrong
- Pydantic validation might be failing

#### 2. Check Connector Availability

The debug logs will show:

```
DEBUG: Available connectors: ['binance', 'kucoin', 'gate_io', ...]
DEBUG: Processing strategy 1/43: arb_phl_mexc_bitmart
DEBUG:   Requires connectors: mexc, bitmart
✓ Successfully added strategy 1: arb_phl_mexc_bitmart
```

**If you see `✗ Failed to initialize strategy` errors:**
- Check the error message - it will show which connector is missing
- Verify your `markets:` section in the YAML includes ALL exchanges used by ALL strategies
- Verify exchange names match exactly (e.g., `gate_io` not `gate-io`, `bing_x` not `bingx`)

#### 3. Common Issues and Solutions

##### Issue: Config file not found
**Symptom:** No DEBUG logs appear, or errors about missing config file
**Solution:**
- Start with explicit config path: `start --script multi_strategy_orchestrator --conf test_multi`
- Verify file exists at `/home/ubuntu/hummingbot/conf/scripts/test_multi.yml`
- Check file permissions (should be readable)

##### Issue: Missing connector
**Symptom:** Errors like `ValueError: Primary market 'mexc' not in connector pool`
**Solution:**
Add the missing exchange to your `markets:` section:
```yaml
markets:
  mexc:  # Add this if missing
    - PHL-USDT
    - F-USDT
```

##### Issue: YAML formatting error
**Symptom:** Pydantic validation errors or `arbitrage_m_strategies` is empty
**Solution:**
- Use 2 spaces for indentation (not tabs)
- Ensure list items start with `- ` (dash + space)
- Verify all string values are properly quoted if they contain special characters
- Check that market dict values are lists: `mexc: [PHL-USDT]` or:
  ```yaml
  mexc:
    - PHL-USDT
  ```

##### Issue: Connector name mismatch
**Symptom:** Some strategies fail with "not in connector pool"
**Solution:**
Verify connector names match exactly:
- ✓ `gate_io` (underscore)
- ✗ `gate-io` (hyphen)
- ✓ `bing_x` (underscore)
- ✗ `bingx` (no underscore)

#### 4. Testing with Minimal Config

If you're still having issues, test with the minimal example config:

```bash
start --script multi_strategy_orchestrator --conf multi_strategy_orchestrator_example
```

This config has only 2 strategies using common exchanges (Binance, KuCoin). If this works, gradually add your strategies one by one to identify which one is causing issues.

#### 5. Examining Startup Logs

Look for these key log messages in order:

1. `DEBUG: Config type:` - Confirms config object was created
2. `DEBUG: Number of strategies in config:` - Shows how many strategies loaded from YAML
3. `DEBUG: Available connectors:` - Shows which exchanges are initialized
4. `DEBUG: Processing strategy X/Y:` - Shows each strategy being initialized
5. `MultiStrategyOrchestrator initialized with X strategies` - Final count

**If count goes from 43 → 0:**
All strategies failed to initialize. Check for connector name mismatches.

**If count goes from 43 → 20:**
Some strategies failed. Look for the specific error messages above each failure.

### Expected Behavior with Valid Config

```
DEBUG: Config type: <class 'scripts.multi_strategy_orchestrator.MultiStrategyOrchestratorConfig'>
DEBUG: Number of strategies in config: 43
DEBUG: Available connectors: ['mexc', 'bitmart', 'gate_io', 'kucoin', 'bing_x', 'bybit', 'htx', 'bitget', 'okx']
DEBUG: Processing strategy 1/43: arb_phl_mexc_bitmart
DEBUG:   Requires connectors: mexc, bitmart
Adding arbitrage_m strategy: arb_phl_mexc_bitmart
Strategy 'arb_phl_mexc_bitmart' initialized: mexc/PHL-USDT <-> bitmart/PHL-USDT, 2 arbitrage pairs, min_profit=2.2%
DEBUG: ✓ Successfully added strategy 1: arb_phl_mexc_bitmart
[... 42 more strategies ...]
MultiStrategyOrchestrator initialized with 43 strategies
Shared connectors: ['mexc', 'bitmart', 'gate_io', 'kucoin', 'bing_x', 'bybit', 'htx', 'bitget', 'okx']
```

### Still Having Issues?

If the debug logs show strategies are loading (`Number of strategies in config: 43`) but then all fail to initialize, the most likely cause is:

1. **Connector name mismatch** - Exchange name in strategy doesn't match exactly
2. **Missing markets section entry** - Strategy references exchange not in `markets:` section
3. **Missing trading pair** - Strategy references pair not listed under the exchange in `markets:` section

The debug logs will pinpoint exactly which strategy is failing and why.
