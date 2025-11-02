# Arbitrage M V2 - Quick Start Guide

Get started with arbitrage_m V2 controller in 5 minutes!

## Prerequisites

1. Hummingbot installed and configured
2. API keys for at least 2 exchanges
3. Python 3.8+ with Cython support

## Step 1: Compile Cython Helpers (Optional but Recommended)

For best performance, compile the Cython helpers:

```bash
# Option A: From hummingbot root (compiles everything)
./compile

# Option B: Compile just arbitrage_m helpers
cd controllers/arbitrage_m
python setup.py build_ext --inplace
cd ../..
```

**Note**: The controller works without Cython compilation but will be slower.

## Step 2: Create Configuration

Create a config file for your arbitrage pair:

```bash
cd conf/controllers/arbitrage_m
cp conf_arbitrage_m_binance_kucoin_btc.yml conf_arbitrage_m_my_strategy.yml
```

Edit `conf_arbitrage_m_my_strategy.yml`:

```yaml
id: arbitrage_m_my_strategy
controller_name: arbitrage_m

# Your exchanges and trading pairs
primary_market:
  connector_name: binance       # Change to your exchange
  trading_pair: BTC-USDT        # Change to your pair

secondary_market:
  connector_name: kucoin        # Change to your exchange
  trading_pair: BTC-USDT        # Change to your pair

# Trading settings
min_profitability: 0.5          # 0.5% minimum profit
order_amount: 100               # 100 USDT per trade (adjust to your capital)
min_order_usd: 15               # Minimum order size
max_concurrent_arbitrages: 1    # How many simultaneous arbitrages

# Timing
next_trade_delay_interval: 2.0  # Wait 2 seconds between trades
```

## Step 3: Configure Exchanges

Make sure both exchanges are configured in Hummingbot:

```bash
./start
hummingbot >>> connect binance
hummingbot >>> connect kucoin
```

## Step 4: Create v2_with_controllers Config

```bash
hummingbot >>> create --script-config v2_with_controllers
```

**Enter the following:**

- **Markets**: `binance.BTC-USDT,kucoin.BTC-USDT`  (your pairs)
- **Controllers**: `conf_arbitrage_m_my_strategy.yml`
- **Candles**: (leave empty, press Enter)
- **Config Update Interval**: `60`
- **Script Name**: `conf_v2_arbitrage_m`

## Step 5: Start Trading!

```bash
hummingbot >>> start --script v2_with_controllers --conf conf_v2_arbitrage_m.yml
```

You should see:

```
Markets ready. Trading started.
Arbitrage M Controller: arbitrage_m_my_strategy
Primary:   binance:BTC-USDT
Secondary: kucoin:BTC-USDT
```

## Step 6: Monitor Performance

```bash
# Check status
hummingbot >>> status

# Check profitability snapshot
# This shows current arbitrage opportunities

# View active executors
# Shows running arbitrage operations
```

## Common Issues & Solutions

### Issue: "Cython helpers not available"

**Solution**: Compile the helpers (see Step 1) or continue with Python fallback.

### Issue: "No arbitrage opportunities found"

**Possible causes**:
- `min_profitability` is too high → Lower it (try 0.3%)
- Insufficient liquidity → Choose more liquid pairs
- Markets not ready → Wait a few seconds for orderbook data

### Issue: "Insufficient balance"

**Solution**: Ensure you have enough balance on BOTH exchanges:
- Buy side: Need quote currency (e.g., USDT)
- Sell side: Need base currency (e.g., BTC)

## Pro Tips

### 1. Start Small

Begin with small `order_amount` to test:
```yaml
order_amount: 20  # $20 per trade
```

### 2. Monitor Profitability

Watch the status display to see current spreads:
```
Profitability Snapshot:
  Buy-binance Sell-kucoin: +0.35%  ✓ Above threshold
  Buy-kucoin Sell-binance: -0.12%  ✗ Below threshold
```

### 3. Adjust Parameters Live

Update `min_profitability` without restarting:
```bash
hummingbot >>> config min_profitability 0.8
```

Changes take effect within 60 seconds (your `config_update_interval`).

### 4. Run Multiple Pairs

Create multiple configs and run them together:

```yaml
# In v2_with_controllers config:
markets: binance.BTC-USDT,kucoin.BTC-USDT,binance.ETH-USDT,kucoin.ETH-USDT
controllers: conf_arbitrage_m_btc.yml,conf_arbitrage_m_eth.yml
```

One bot, multiple arbitrage controllers! 🚀

## Next Steps

- Review the [full README](README.md) for advanced features
- Experiment with cross-asset arbitrage
- Optimize `min_profitability` based on your observations
- Track performance over 24 hours

## Need Help?

- Check logs: `logs/logs_ctrl_c.log`
- Review config: Make sure trading pairs and exchanges are correct
- Join Hummingbot Discord: https://discord.hummingbot.io

Happy arbitraging! 💰
