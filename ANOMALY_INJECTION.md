# Anomaly Injection System - 3-Day Experiment

## Overview
The anomaly injection system implements a **3-day sequential experiment** with date filtering and validation to prevent overlaps. This ensures clear identification of which anomaly types are detected during analysis.

## 3-Day Experiment Timeline

### Day 08-11-2021 (Monday) - Collective Anomalies
```
09:30                      14:00    14:30              16:00
|────────Phase 1────────────|       |─────Phase 3─────|
 Tick Rate Decline                   Feed Silence
```

### Day 09-11-2021 (Tuesday) - Contextual Anomalies
```
09:30                                            15:00
|──────────────Phase 2──────────────────────────|
 Contextual Price Anomalies
```
### Day 10-11-2021 (Wednesday) - Point Anomalies
```
09:30                                                15:30
|──────────────────Phase 4──────────────────────────|
 Sudden Point Failures
```

### Days 11-11-2021 & 12-11-2021 (Thu-Fri) - Baseline
No anomaly injection. Clean data for comparison.

## Date Filtering

Each phase can be configured with a `date_filter` to only activate on specific dates:

```yaml
phase1_tick_rate_decline:
  enabled: true
  date_filter: ["08-11-2021"]  # Only active on Monday
  window:
    start: "09:30:00"
    end:   "14:00:00"
```

### Date Filter Behavior
- **Empty date filter** (`date_filter: []` or omitted): Phase runs on **ALL days**
- **Specified dates**: Phase only runs on those dates (format: `"DD-MM-YYYY"`)
- Multiple phases can run on the same day if time windows don't overlap

### Examples

**Valid: Same day, non-overlapping times**
```yaml
phase1:
  date_filter: ["08-11-2021"]
  window: {start: "09:30:00", end: "14:00:00"}

phase3:
  date_filter: ["08-11-2021"]
  window: {start: "14:30:00", end: "16:00:00"}  # ✅ Sequential
```

**Valid: Different days, any times**
```yaml
phase1:
  date_filter: ["08-11-2021"]
  window: {start: "09:30:00", end: "14:00:00"}

phase2:
  date_filter: ["09-11-2021"]
  window: {start: "09:30:00", end: "15:00:00"}  # ✅ Different date
```

**Invalid: Same date, overlapping times**
```yaml
phase1:
  date_filter: ["08-11-2021"]
  window: {start: "09:30:00", end: "14:00:00"}

phase2:
  date_filter: ["08-11-2021"]
  window: {start: "13:00:00", end: "15:00:00"}  # ❌ Overlaps Phase 1
```

## Configuration

### Full 3-Day Experiment
```yaml
anomaly:
  enabled: true
  seed: 42
  log_file: anomaly_log.csv

  phase1_tick_rate_decline:
    enabled: true
    date_filter: ["08-11-2021"]
    window: {start: "09:30:00", end: "14:00:00"}
    decline_pattern: linear
    initial_rate: 1.0      # 1.0 = 100%
    final_rate: 0.3        # 0.3 = 30%
    instrument_ratio: 0.4  # 0.4 = 40% of instruments

  phase2_contextual_anomalies:
    enabled: true
    date_filter: ["09-11-2021"]
    window: {start: "09:30:00", end: "15:00:00"}
    context_window_hours: 1.0
    strategies:
      - {type: price_spike, probability: 0.03, deviation_range: [2.0, 5.0]}
      - {type: stale_price, probability: 0.05, repeat_count: [3, 10]}
      - {type: price_deviation, probability: 0.01, deviation_range: [4.0, 8.0]}

  phase3_feed_silence:
    enabled: true
    date_filter: ["08-11-2021"]
    window: {start: "14:30:00", end: "16:00:00"}
    blackout_seconds: 30
    instrument_ratio: 0.7
    exchange_filter: ["ETR"]   # ID suffix (ETR, FR, NL), not the venue name

  phase4_point_failures:
    enabled: true
    date_filter: ["10-11-2021"]
    window: {start: "09:30:00", end: "15:30:00"}
    strategies:
      - {type: null_price, probability: 0.02, field: ["Bid", "Ask", "both"]}
      - {type: malformed_isin, probability: 0.01, corruption: ["truncate", "random_chars"]}
      - {type: timestamp_inversion, probability: 0.015, rewind_seconds: [1, 300]}
```

### Test Single Phase on Specific Date
```yaml
anomaly:
  enabled: true
  
  phase1_tick_rate_decline:
    enabled: false
    
  phase2_contextual_anomalies:
    enabled: true
    date_filter: ["09-11-2021"]  # ← Only Tuesday
    window: {start: "09:30:00", end: "15:00:00"}
    
  phase3_feed_silence:
    enabled: false
    
  phase4_point_failures:
    enabled: false
```

## Anomaly Types by Phase

### Phase 1: Gradual Tick Rate Decline (Collective)
- **Window**: 09:30-14:00 on Monday
- **Affects**: 40% of instruments
- **Behavior**: Linear decline from 100% → 30% tick rate
- **Manifestation**: Ticks dropped silently

### Phase 2: Contextual Price Anomalies (Contextual)
- **Window**: 09:30-15:00 on Tuesday
- **Context**: Last 1 hour of price history
- **Strategies**:
  - **Price Spike**: 3% probability, 2-5x deviation
  - **Stale Price**: 5% probability, repeated 3-10 times
  - **Price Deviation**: 1% probability, last price moved by 4-8 std-devs of recent
    log returns (stays plausible; only anomalous in context)
  - **Stale Price**: a run of 3-10 consecutive ticks of an instrument frozen at the
    previous price (probability is the chance a run *starts*)

### Phase 3: Feed Silence (Collective)
- **Window**: 14:30-16:00 on Monday (same day as Phase 1)
- **Affects**: 70% of instruments on ETR (Xetra)
- **Behavior**: 30-second blackout per instrument
- **Manifestation**: All ticks dropped during blackout

### Phase 4: Sudden Point Failures (Point)
- **Window**: 09:30-15:30 on Wednesday
- **Strategies**:
  - **Null Price**: 2% probability, Bid/Ask/both
  - **Malformed ISIN**: 1% probability, truncate/random
  - **Timestamp Inversion**: 1.5% probability, rewind 1-300s

## Anomaly Logging

### CSV Log File
Each injected anomaly is logged to `anomaly_log.csv`:

```csv
timestamp,instrument,phase,anomaly_type,detail
2021-11-08 09:45:12,SAP.ETR,phase1,tick_dropped,
2021-11-09 10:23:45,BMW.ETR,phase2,price_spike,multiplier=3.2x
2021-11-08 14:35:01,VOW3.ETR,phase3,feed_silence,blackout_start
2021-11-10 11:12:33,BAYN.ETR,phase4,null_price,field=Bid
```

### Episode ground truth (`anomaly_log_episodes.csv`)
Written next to the tick log (`<log_file>_episodes.csv`, or `anomaly.episode_file`). One row
per anomaly *episode*, in event time as epoch milliseconds. This is the file to evaluate
against; the tick log above additionally records every dropped tick.

```csv
EpisodeID,Phase,AnomalyType,Exchange,InstrumentID,StartMs,EndMs,ObservedMs,ResumeMs,LastDeliveredMs,Detail,SecType
```

| Phase | Row | StartMs / EndMs | Other columns |
|-------|-----|-----------------|---------------|
| phase1 | one per instrument and day | window start / end | Detail: `ticks_seen`, `ticks_dropped`, rates |
| phase2 spike, deviation | one per injected tick | tick time (equal) | ObservedMs = tick time |
| phase2 stale_price | one per run | first / last tick of the run | Detail: `changed_ticks` = ticks whose true price differed from the frozen one |
| phase3 | one per blackout | first dropped tick / scheduled end | `LastDeliveredMs` = last tick delivered before, `ResumeMs` = first tick delivered after (empty if none); Detail: `delivered_before` = ticks the instrument had delivered so far (a detector needs history to be warm) |
| phase4 | one per injected tick | original tick time (equal) | `ObservedMs` = timestamp the downstream sees (differs for `timestamp_inversion`) |

`SecType` is `E` (equity) or `I` (index). The injector does not filter on it, but the
feed-handler only forwards equities, so evaluate against `SecType == "E"` rows. For
`timestamp_inversion`, `Detail` includes `prev_ms`, the previous tick of the instrument:
a rewind is only detectable by a per-instrument monotonicity check when
`ObservedMs < prev_ms` (minus the validator's tolerance).

Only equities (`SecType == "E"`) are injected and appear in the ground truth and in the
instrument-day summary: index rows never reach the detector and are almost all "trades",
so they would swallow the injection budget.

Phase 1 and 3 rows and unfinished stale runs are written when the injector closes, so the
file is not in chronological order; sort by `StartMs`. `EpisodeID` is a write-order counter.

The tick log `anomaly_log.csv` now has millisecond timestamps and an extra
`ObservedTimestamp` column (last column).

On shutdown the injector warns if an enabled phase injected nothing (usually a
`date_filter`/`window` that misses the data or an `exchange_filter` that matches no tick).

### Kafka Message Metadata
Modified ticks include anomaly metadata:

```json
{
  "ID": "SAP.ETR",
  "Bid": 100.5,
  "Ask": 101.0,
  "TradingTime": "2021-11-09T10:23:45Z",
  "anomaly_injected": true,
  "anomaly_type": "price_spike"
}
```

Dropped ticks are **not sent** to Kafka (simulating feed silence).

## Statistics Output

After simulation, anomaly statistics are printed:

```
Anomaly Injection Statistics:
  Ticks Modified: 12,847
  Ticks Dropped:  8,234
  
  Phase 1 (Tick Rate Decline):
    Dropped: 6,123
  
  Phase 2 (Contextual Anomalies):
    price_spike:         412
    stale_price:         389
    price_deviation:     104
  
  Phase 3 (Feed Silence):
    Dropped: 2,111
  
  Phase 4 (Point Failures):
    null_price:           234
    malformed_isin:       98
    timestamp_inversion:  187
```

## Reproducibility

### Random Seeding
```yaml
anomaly:
  seed: 42  # Same seed = identical anomaly pattern
```

- Same seed → identical instrument selection and anomaly placement
- Different seed → different experiment run
- Critical for A/B testing detection algorithms

### Acceleration-Safe
Uses **simulated market time**, not real time:
```yaml
simulator:
  mode: accelerated
  acceleration_factor: 100  # Works with any acceleration
```

Time windows (e.g., "09:30:00-10:00:00") refer to the data's market time, not wall-clock time.

## Analysis Tips

### Detecting Phase Transitions
Look for changes in anomaly patterns at phase boundaries:
- **08-11-2021 14:00**: Phase 1 ends (tick rate recovers)
- **08-11-2021 14:30**: Phase 3 starts (sudden blackouts)
- **09-11-2021 09:30**: Phase 2 starts (contextual anomalies)
- **10-11-2021 09:30**: Phase 4 starts (point failures)

### Baseline Comparison
Use **11-11-2021 and 12-11-2021** as baseline:
- No anomalies injected
- Normal feed behavior
- Compare detection metrics (false positive rate)

### Coupling Same-Day Phases
**08-11-2021** demonstrates how multiple collective anomalies can occur on the same trading day:
- Morning: Degrading tick rate (Phase 1)
- Afternoon: Complete blackouts (Phase 3)

This tests whether detectors can distinguish between different types of collective anomalies.

## Example Scenarios

### Full 3-Day Experiment
Use `config/simulator-with-anomalies.yaml` as-is. Processes all dates with appropriate phases.

### Single Day Testing
Modify `date_filter` to test one day:
```yaml
phase2_contextual_anomalies:
  enabled: true
  date_filter: ["09-11-2021"]  # Only Tuesday
```

### Baseline Only
Disable all phases, run on days 11 and 12:
```yaml
anomaly:
  enabled: false  # Or set all phases to enabled: false
```

### Extended Experiment
Add more dates to date_filter:
```yaml
phase1_tick_rate_decline:
  date_filter: ["08-11-2021", "15-11-2021", "22-11-2021"]
```

## Validation on Startup

The system validates configuration on startup:
-  Time windows are valid (start < end)
-  No overlapping phases on same date
-  All probability values are valid (0-1)
-  All time strings parse correctly

If validation fails, the simulator exits with an error message pointing to the issue.
