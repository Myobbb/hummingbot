# Multi-Strategy Orchestrator - Quick Start Guide

Get multiple arbitrage_m strategies running with shared websockets in 5 minutes!

## Prerequisites

- Hummingbot installed and configured
- API keys configured for at least 2 exchanges
- Basic familiarity with Hummingbot CLI

## Step 1: Choose a Configuration Template (30 seconds)

We provide 3 ready-to-use templates:

### Option A: Simple (Recommended for first-time users)
**File:** `conf/scripts/conf_multi_arbitrage_m_simple.yml`
- 2 strategies: BTC and ETH arbitrage
- 2 exchanges: Binance and KuCoin
- Conservative settings, no buy-in

### Option B: Advanced
**File:** `conf/scripts/conf_multi_arbitrage_m_advanced.yml`
- 3 strategies across 3 exchanges
- Buy-in enabled for some strategies
- Different profitability thresholds

### Option C: Cross-Asset
**File:** `conf/scripts/conf_multi_arbitrage_m_cross_asset.yml`
- Demonstrates oracle and fixed conversion rates
- Advanced cross-asset arbitrage scenarios

## Step 2: Customize Configuration (2 minutes)

Copy and edit your chosen template:

```bash
# Copy template
cp conf/scripts/conf_multi_arbitrage_m_simple.yml conf/scripts/my_multi_arb.yml

# Edit with your settings
nano conf/scripts/my_multi_arb.yml
```

**Key Settings to Customize:**

```yaml
markets:
  binance: [BTC-USDT, ETH-USDT]     # ← Your exchanges and pairs
  kucoin: [BTC-USDT, ETH-USDT]

arbitrage_m_strategies:
  - name: "btc_arb"                  # ← Unique name
    primary_market: binance          # ← Your primary exchange
    secondary_market: kucoin         # ← Your secondary exchange
    primary_trading_pair: BTC-USDT   # ← Your trading pair
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.5           # ← Your profit threshold (%)
```

## Step 3: Verify API Keys (1 minute)

Ensure API keys are configured for your exchanges:

```bash
# In Hummingbot CLI
config api_keys

# You should see:
# binance: configured ✓
# kucoin: configured ✓
```

If not configured:

```bash
# Configure API keys
connect binance
# Enter API key and secret when prompted

connect kucoin
# Enter API key and secret when prompted
```

## Step 4: Start the Orchestrator (30 seconds)

```bash
# In Hummingbot CLI
start --script conf/scripts/my_multi_arb.yml
```

**Expected Output:**

```
[multi_strategy_orchestrator] MultiStrategyOrchestrator initialized with 2 strategies
[multi_strategy_orchestrator] Shared connectors: ['binance', 'kucoin']
[multi_strategy_orchestrator] Strategy 'btc_arb' initialized: binance/BTC-USDT <-> kucoin/BTC-USDT
[multi_strategy_orchestrator] Strategy 'eth_arb' initialized: binance/ETH-USDT <-> kucoin/ETH-USDT
```

## Step 5: Monitor Performance (ongoing)

### Check Status

```bash
status
```

You'll see:
- ✅ Shared connector status
- 💰 Combined balances
- 📊 Active orders
- 📈 Individual strategy performance

### View Logs

```bash
# Real-time logs
(logs scroll automatically)

# Search logs
history

# Filter by strategy
history --keyword btc_arb
```

### Key Metrics to Watch

1. **Connector Health:** All should show `✓ READY`
2. **Profitability:** Look for "Found profitable opportunity" messages
3. **Order Execution:** Watch for order fills and completions
4. **Balance Changes:** Monitor asset balances over time

## Verification: Is Websocket Sharing Working?

### Check 1: Connector Count

In the status output, verify you see:

```
Shared Connectors:
  binance: ✓ READY    ← Only ONE binance connector
  kucoin: ✓ READY     ← Only ONE kucoin connector
```

**Not** multiple connectors per exchange.

### Check 2: Log Messages

Look for initialization messages showing shared connectors:

```
[multi_strategy_orchestrator] Shared connectors: ['binance', 'kucoin']
```

This confirms all strategies use the same connector instances.

### Check 3: Resource Usage

Compare memory usage:

**Before Orchestrator** (2 separate strategies):
- Memory: ~500MB
- Connections: 4 (2 per strategy)

**With Orchestrator** (2 strategies sharing):
- Memory: ~350MB (30% less)
- Connections: 2 (shared)

## Common First-Time Issues

### Issue 1: "Market connectors are not ready"

**Cause:** Exchange not connected or API keys invalid

**Fix:**
```bash
# Stop strategy
stop

# Check connection
connect binance
connect kucoin

# Restart
start --script conf/scripts/my_multi_arb.yml
```

### Issue 2: No trades executing

**Cause:** Profitability threshold too high for current market

**Fix:** Lower `min_profitability` temporarily for testing:

```yaml
min_profitability: 0.1  # Very low for testing only
```

### Issue 3: Strategies interfering with each other

**Cause:** Too similar configurations

**Fix:** Differentiate strategies:
- Use different pairs (BTC vs ETH)
- Use different profitability thresholds
- Use different `next_trade_delay_interval`

## Next Steps

Once you're comfortable with basic usage:

1. **Optimize Profitability:** Fine-tune `min_profitability` thresholds
2. **Add More Strategies:** Scale up to 3-5 strategies
3. **Enable Buy-In:** Activate buy-in module for better capital efficiency
4. **Add More Exchanges:** Expand to 3+ exchanges for more opportunities
5. **Monitor Performance:** Track PnL and adjust parameters

## Example: Scaling from 2 to 4 Strategies

```yaml
arbitrage_m_strategies:
  # Original 2 strategies
  - name: "btc_binance_kucoin"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: BTC-USDT
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.5

  - name: "eth_binance_kucoin"
    primary_market: binance
    secondary_market: kucoin
    primary_trading_pair: ETH-USDT
    secondary_trading_pair: ETH-USDT
    min_profitability: 0.5

  # New strategies (add MEXC to markets section!)
  - name: "btc_binance_mexc"
    primary_market: binance
    secondary_market: mexc
    primary_trading_pair: BTC-USDT
    secondary_trading_pair: BTC-USDT
    min_profitability: 0.4

  - name: "eth_kucoin_mexc"
    primary_market: kucoin
    secondary_market: mexc
    primary_trading_pair: ETH-USDT
    secondary_trading_pair: ETH-USDT
    min_profitability: 0.4
```

**Update markets section:**

```yaml
markets:
  binance: [BTC-USDT, ETH-USDT]
  kucoin: [BTC-USDT, ETH-USDT]
  mexc: [BTC-USDT, ETH-USDT]    # ← Add this
```

**Resource Impact:**
- Connections: Still only 3 (one per exchange)
- Memory: ~450MB (vs 1GB for 4 separate strategies)

## Testing Checklist

Before running in production:

- [ ] API keys configured for all exchanges
- [ ] Test with small `order_amount` first
- [ ] Monitor for 1 hour in test mode
- [ ] Verify profitability calculations are accurate
- [ ] Check that orders execute correctly
- [ ] Confirm balances update properly
- [ ] Review logs for errors or warnings
- [ ] Increase `min_profitability` for production

## Getting Help

**Discord:** https://discord.gg/hummingbot
- #support channel for troubleshooting
- #strategies channel for configuration help

**Documentation:** See `MULTI_STRATEGY_ORCHESTRATOR.md` for full details

**Logs:** Always include logs when asking for help:
```bash
# Export recent logs
history --export logs.txt
```

## Quick Reference

### Start Orchestrator
```bash
start --script conf/scripts/my_multi_arb.yml
```

### Stop Orchestrator
```bash
stop
```

### Check Status
```bash
status
```

### View Configuration
```bash
config
```

### Emergency Stop All
```bash
exit
```

## Success Indicators

You'll know it's working when you see:

✅ "MultiStrategyOrchestrator initialized with N strategies"
✅ "Shared connectors: [list of exchanges]"
✅ All connectors show "✓ READY"
✅ "Found profitable opportunity" messages in logs
✅ Order fills showing in status
✅ Balances updating over time

Happy arbitraging! 🚀
