# Multi-Strategy Orchestrator - Audit Report

## Executive Summary

**Status:** ⚠️ **CRITICAL ISSUES FOUND** - Implementation requires fixes before production use

I've identified 4 critical issues that will prevent the orchestrator from working correctly with V1 arbitrage_m strategies. These issues relate to lifecycle management, event listener registration, and clock coordination.

## Issues Found

### ❌ CRITICAL #1: Double Event Listener Registration

**Severity:** HIGH
**File:** `scripts/multi_strategy_orchestrator.py:163`
**Line:** `super().__init__(connectors, config)`

**Problem:**
```python
def __init__(self, connectors: Dict[str, ConnectorBase], config: MultiStrategyOrchestratorConfig):
    super().__init__(connectors, config)  # ← Calls ScriptStrategyBase.__init__()
    # ...
```

`ScriptStrategyBase.__init__()` at line 58 calls:
```python
self.add_markets(list(connectors.values()))  # Registers event listeners for orchestrator
```

Then each V1 strategy in `_add_arbitrage_m_strategy()` calls:
```python
strategy.init_params(...)  # → calls c_add_markets() internally
```

**Result:**
- Orchestrator has event listeners registered on all connectors
- Each V1 strategy ALSO has event listeners on the same connectors
- Orchestrator receives order events it shouldn't process
- Potential conflicts and confusion in order tracking

**Impact:**
- Orchestrator's `order_tracker` receives events meant for V1 strategies
- May interfere with V1 strategy order tracking
- Memory waste from unnecessary listeners

**Fix Required:** Override `__init__` to skip parent's `add_markets()` call.

---

### ❌ CRITICAL #2: Missing Strategy start() Call

**Severity:** CRITICAL
**File:** `scripts/multi_strategy_orchestrator.py:265-283`
**Missing:** Call to `strategy.start(clock, timestamp)`

**Problem:**
V1 strategies expect lifecycle management:
1. `init_params()` - Configure strategy ✅ (done)
2. `start(clock, timestamp)` - Initialize with clock ❌ (MISSING)
3. `tick(timestamp)` - Execute strategy logic ✅ (called manually)
4. `stop(clock)` - Clean up ❌ (uses wrong clock)

From `strategy_py_base.pyx:28-33`:
```cython
cdef c_start(self, Clock clock, double timestamp):
    StrategyBase.c_start(self, clock, timestamp)  # Parent initialization
    self.start(clock, timestamp)  # Strategy-specific initialization
```

**Current Implementation:**
```python
strategy = ArbitrageMStrategy()
strategy.init_params(...)  # ✅ Called
# strategy.c_start(clock, timestamp)  # ❌ MISSING!
```

**Impact:**
- `StrategyBase.c_start()` never called - missing initialization
- `_current_timestamp` may not be set correctly
- Strategy state machine not properly initialized
- Potential crashes or incorrect behavior

**Fix Required:** Call `strategy.c_start(clock, timestamp)` when clock becomes available.

---

### ❌ CRITICAL #3: Incorrect Clock Reference in stop()

**Severity:** CRITICAL
**File:** `scripts/multi_strategy_orchestrator.py:339`
**Line:** `strategy_instance.strategy.stop(self._clock)`

**Problem:**
```python
async def on_stop(self):
    for strategy_instance in self.strategies:
        strategy_instance.strategy.stop(self._clock)  # ← Wrong clock!
```

**Issues:**
1. `self._clock` is the orchestrator's clock from `ScriptStrategyBase`
2. V1 strategies were NEVER registered with `self._clock`
3. According to lifecycle contract, `stop(clock)` must receive the SAME clock used in `start(clock, timestamp)`
4. Currently, start() is never called, so NO clock was associated with strategies

**From `strategy_base.pyx:308-311`:**
```cython
cdef c_stop(self, Clock clock):
    TimeIterator.c_stop(self, clock)  # Expects clock it was started with
    self._sb_order_tracker.c_stop(clock)
    self.c_remove_markets(list(self._sb_markets))  # Remove event listeners
```

**Impact:**
- Event listeners may not be removed correctly
- Order tracker not stopped properly
- Resources not cleaned up
- Potential memory leaks
- Crash if `self._clock` is None

**Fix Required:** Store clock reference and pass correct clock to stop().

---

### ❌ CRITICAL #4: Lifecycle Timing Issue

**Severity:** HIGH
**File:** `scripts/multi_strategy_orchestrator.py:170`
**Line:** `self._initialize_arbitrage_m_strategies()`

**Problem:**
```python
def __init__(self, connectors: Dict[str, ConnectorBase], config):
    super().__init__(connectors, config)
    self.strategies: List[V1StrategyInstance] = []
    self._initialize_arbitrage_m_strategies()  # ← Called in __init__
    # Clock not available yet!
```

The clock is only available when `start(clock, timestamp)` is called on the orchestrator (by TradingCore/Clock system). But strategies are initialized in `__init__` before the clock exists.

**Timeline:**
```
1. TradingCore creates orchestrator
   └─> MultiStrategyOrchestrator.__init__()
       └─> _initialize_arbitrage_m_strategies()
           └─> strategy.init_params()  ✅
           └─> strategy.c_start(???)   ❌ No clock available!

2. TradingCore calls clock.add_iterator(orchestrator)

3. Clock calls orchestrator.c_start(clock, timestamp)
   └─> Now clock is available, but strategies already initialized!
```

**Impact:**
- Can't call strategy.c_start() at initialization time
- Need to defer start() calls until orchestrator's start() is called

**Fix Required:** Override orchestrator's `start()` to call strategy start() methods.

---

## Trace: Execution Flow Analysis

### Current (Broken) Flow:

```
1. INITIALIZATION (TradingCore.start_script)
   └─> MultiStrategyOrchestrator.__init__(connectors, config)
       ├─> ScriptStrategyBase.__init__(connectors, config)
       │   ├─> self.connectors = connectors
       │   └─> self.add_markets(list(connectors.values()))  ← PROBLEM: Adds listeners
       │
       └─> self._initialize_arbitrage_m_strategies()
           └─> For each strategy config:
               └─> _add_arbitrage_m_strategy(config)
                   ├─> Creates MarketTradingPairTuples
                   ├─> strategy = ArbitrageMStrategy()
                   ├─> strategy.init_params(market_pairs=...)
                   │   └─> strategy.c_add_markets(...)  ← Adds MORE listeners
                   └─> self.strategies.append(strategy_instance)

                   ❌ MISSING: strategy.c_start(clock, timestamp)

2. CLOCK REGISTRATION (TradingCore)
   └─> clock.add_iterator(orchestrator)  ← Only orchestrator added, not strategies

3. START (Clock system)
   └─> clock.c_start(orchestrator, timestamp)
       └─> orchestrator.c_start(clock, timestamp)  ← Clock now available!
           └─> ScriptStrategyBase has no start() override

           ❌ MISSING: Strategies not started with clock

4. TICK (Every second)
   └─> clock.c_tick(orchestrator, timestamp)
       └─> ScriptStrategyBase.tick(timestamp)
           └─> MultiStrategyOrchestrator.on_tick()
               └─> For each strategy:
                   └─> strategy.c_tick(timestamp)  ← Direct call (OK)
                       └─> ArbitrageMStrategy.c_tick(timestamp)
                           ├─> StrategyBase.c_tick(timestamp)  ← Updates _current_timestamp
                           └─> Main arbitrage logic

5. STOP (User command)
   └─> clock.c_stop(orchestrator)
       └─> orchestrator.on_stop()
           └─> For each strategy:
               └─> strategy.stop(self._clock)  ← PROBLEM: Wrong clock!
```

### Correct (Fixed) Flow:

```
1. INITIALIZATION
   └─> MultiStrategyOrchestrator.__init__(connectors, config)
       ├─> super().__init__() WITHOUT add_markets  ✅ FIX #1
       ├─> self.connectors = connectors
       ├─> self.strategies = []
       ├─> self._strategies_started = False  ✅ NEW: Track start state
       └─> self._initialize_arbitrage_m_strategies()
           └─> For each config:
               ├─> strategy = ArbitrageMStrategy()
               ├─> strategy.init_params(...)  ← Only init, no start yet
               └─> self.strategies.append(strategy)

2. CLOCK REGISTRATION
   └─> clock.add_iterator(orchestrator)

3. START
   └─> clock.c_start(orchestrator, timestamp)
       └─> orchestrator.start(clock, timestamp)  ✅ FIX #2 - Override start()
           ├─> self._clock = clock  ✅ Store clock reference
           ├─> For each strategy:
           │   └─> strategy.c_start(clock, timestamp)  ✅ Start strategies!
           └─> self._strategies_started = True

4. TICK
   └─> clock.c_tick(orchestrator, timestamp)
       └─> orchestrator.on_tick()
           └─> For each strategy:
               └─> strategy.c_tick(timestamp)  ← Works correctly now

5. STOP
   └─> clock.c_stop(orchestrator)
       └─> orchestrator.on_stop()
           └─> For each strategy:
               └─> strategy.c_stop(self._clock)  ✅ FIX #3 - Correct clock!
```

---

## Why These Issues Exist

The orchestrator was designed assuming V1 strategies could be used as "library code" without full lifecycle management. However, V1 strategies (StrategyBase) are designed to be **top-level TimeIterator objects** managed by the Clock system, not sub-components.

**V1 Strategy Expectations:**
1. Created
2. Registered with Clock via `clock.add_iterator(strategy)`
3. Clock calls `strategy.c_start(clock, timestamp)`
4. Clock calls `strategy.c_tick(timestamp)` every tick
5. Clock calls `strategy.c_stop(clock)` on shutdown

**Orchestrator Pattern:**
- Strategies are NOT registered with clock directly
- Orchestrator acts as proxy/wrapper
- Must manually replicate clock lifecycle management

---

## Additional Observations

### ✅ OK: Manual c_tick() Calling

**Finding:** The orchestrator manually calls `strategy.c_tick(timestamp)` instead of letting the clock manage it.

**Analysis:** This is actually FINE. From `strategy_py_base.pyx:42-44`:
```cython
cdef c_tick(self, double timestamp):
    StrategyBase.c_tick(self, timestamp)  # Updates _current_timestamp
    self.tick(timestamp)  # Calls strategy logic
```

Since we're calling `c_tick()` directly with the correct timestamp, the strategy executes normally. The clock management is just a convenience - the actual logic works with direct calls.

**Status:** ✅ NOT AN ISSUE

### ✅ OK: Event Listener Pattern

**Finding:** Multiple strategies register listeners on the same connector.

**Analysis:** This is the CORRECT pattern. From `strategy_base.pyx:313-336`:
```cython
cdef c_add_markets(self, list markets):
    for market in markets:
        typed_market = market
        # Each strategy adds its OWN listeners
        typed_market.c_add_listener(self.BUY_ORDER_COMPLETED_EVENT_TAG, self._sb_complete_buy_order_listener)
        # ...
```

Connectors maintain a list of listeners per event type. When an event fires, ALL listeners are notified. This is standard observer pattern and works correctly.

**Status:** ✅ NOT AN ISSUE (once we fix the double registration)

---

## Testing Recommendations

After fixes are applied:

### Unit Test: Event Listener Registration
```python
def test_no_double_listeners():
    orchestrator = MultiStrategyOrchestrator(connectors, config)
    binance_connector = connectors['binance']

    # Count listeners for OrderFilled event
    listeners = binance_connector._listeners[MarketEvent.OrderFilled.value]

    # Should have N listeners (one per strategy using binance)
    # Should NOT have N+1 (orchestrator + strategies)
    assert len(listeners) == num_strategies_using_binance
```

### Integration Test: Strategy Lifecycle
```python
async def test_strategy_lifecycle():
    orchestrator = MultiStrategyOrchestrator(connectors, config)
    clock = Clock(ClockMode.REALTIME)

    # Start
    orchestrator.start(clock, time.time())

    # Verify strategies started
    for strategy in orchestrator.strategies:
        assert hasattr(strategy.strategy, '_current_timestamp')
        assert strategy.strategy._current_timestamp > 0

    # Tick
    orchestrator.on_tick()

    # Stop
    await orchestrator.on_stop()

    # Verify strategies stopped and listeners removed
    binance_connector = connectors['binance']
    listeners = binance_connector._listeners.get(MarketEvent.OrderFilled.value, [])
    assert len(listeners) == 0  # All removed
```

### Runtime Test: Websocket Sharing
```python
def test_websocket_sharing():
    # Monitor network connections before/after
    before = count_websocket_connections(['binance', 'kucoin'])

    orchestrator = MultiStrategyOrchestrator(connectors, config_with_3_strategies)
    orchestrator.start(clock, time.time())

    after = count_websocket_connections(['binance', 'kucoin'])

    # Should have same number of connections (shared)
    assert before == after
    assert after == 2  # One per exchange, not 6 (3 strategies × 2 exchanges)
```

---

## Conclusion

The multi-strategy orchestrator concept is **sound and well-designed**, but the implementation has **4 critical lifecycle management bugs** that must be fixed before it can work with V1 strategies.

**Good News:**
- The architecture is correct (websocket sharing will work)
- The issues are well-understood and fixable
- Fixes are straightforward (see next section)

**Action Required:**
1. Apply the 4 fixes detailed in the next document
2. Test with real V1 arbitrage_m strategies
3. Verify event listeners are correctly registered/removed
4. Confirm websocket sharing is working

**Estimated Fix Time:** 30-60 minutes
**Risk Level After Fixes:** LOW (architecture is sound)
