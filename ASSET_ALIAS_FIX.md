# Fix: Position Balancer Asset Alias Support

## Problem

When running `arbitrage_l` strategies with cross-exchange pairs that have different token names (e.g., NODE on kucoin, NODEOPS on htx), the position balancer was not managing orders for both assets properly:

**Symptoms:**
- Orders placed for both NODE and NODEOPS
- Only one asset's orders refreshed regularly (~10 min interval)
- Other asset's orders timed out after ~15 minutes from main strategy timeout check
- 20-minute effective cycle instead of 10-minute refresh interval

**Root Cause:**
Position balancer called once per tick with a single asset (either NODE or NODEOPS), leaving the other asset's orders unmanaged until main strategy timeout kicked in.

## Solution

Implemented **smart asset alias detection** for cases where assets have 1:1 conversion:

### Detection Logic

Asset aliases are automatically detected when:
1. `use_oracle_conversion_rate: false`
2. `secondary_to_primary_base_conversion_rate: 1.0`

This identifies cases like NODE/NODEOPS as the same asset with different names on different exchanges.

### Implementation

**Position Balancer Handler (`position_balancer_handler.pyx`):**

1. **Asset Alias Mapping:**
   ```python
   # Build aliases on init
   _asset_aliases = {canonical: [alias1, alias2, ...]}
   _canonical_asset = {alias: canonical}

   # Example: NODE/NODEOPS → {NODE: [NODE, NODEOPS]}
   ```

2. **Balance Aggregation:**
   - `c_get_aggregated_base_balance()`: Sums NODE + NODEOPS total
   - `c_get_pending_buy_base()`: Sums pending across aliases
   - `c_get_pending_sell_base()`: Sums pending across aliases

3. **Order Management:**
   - `c_handle_position_balancing()`: Cancels stale orders for ALL aliases
   - Checks for active orders across ALL aliases before placing new orders
   - Prevents duplicate orders when one alias already has active order

4. **Completion Tracking:**
   - Uses canonical asset name for unified tracking
   - Aggregates balances across all aliases for target checking

### Benefits

1. **Unified Position Management:**
   - Treats NODE + NODEOPS as a single position with combined balance
   - Single target (e.g., $1100) applies to total across both exchanges

2. **Proper Order Refresh:**
   - All aliases' orders refreshed together every 10 minutes
   - No more timeout issues from unmanaged "other" asset

3. **Backward Compatible:**
   - No changes needed for normal single-asset strategies
   - Works transparently when assets have same name on both exchanges

4. **Smart Detection:**
   - Only activates for true 1:1 conversions
   - Oracle mode and non-1:1 rates keep assets independent

## Configuration Requirements

No configuration changes needed! Alias detection is automatic based on existing settings:

```yaml
# Example config that triggers alias detection
use_oracle_conversion_rate: false
secondary_to_primary_base_conversion_rate: 1.0

# These will be treated as aliases:
primary_trading_pair: NODE-USDT
secondary_trading_pair: NODEOPS-USDT
```

## Testing

Test with NODE/NODEOPS pair:
1. Enable buy-in with 10-minute refresh interval
2. Observe both NODE and NODEOPS orders refresh together
3. Check logs for: "Position balancer: Detected asset aliases [NODE, NODEOPS]"
4. Verify no timeout cancellations from main strategy

## Technical Details

**Canonical Asset Selection:**
- Uses alphabetically first asset as canonical name (NODE in NODE/NODEOPS)
- All tracking uses canonical name internally
- Order placement uses actual market's asset name

**Order Refresh Logic:**
```python
# In c_handle_position_balancing():
for alias in asset_aliases:  # [NODE, NODEOPS]
    self.c_cancel_stale_orders(alias)  # Refresh both
```

**Balance Aggregation:**
```python
# Example for NODE/NODEOPS:
# kucoin NODE: 100 tokens
# htx NODEOPS: 150 tokens
# Total: 250 tokens (treated as unified position)
```

## Files Modified

- `hummingbot/strategy/arbitrage_l/position_balancer_handler.pyx`
  - Added `_build_asset_aliases()` for detection
  - Added `_get_canonical_asset()` and `_get_all_asset_aliases()` helpers
  - Updated all balance aggregation methods
  - Updated `c_handle_position_balancing()` to handle all aliases
  - Updated `c_scan_and_mark_completion()` for unified tracking

## Future Enhancements

Potential improvements:
- Support for N:M conversion rates (e.g., wrapped tokens)
- Configurable asset alias mappings (override auto-detection)
- Status display showing alias groupings
