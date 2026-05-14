# Real-Time Feature Engineering Pipeline — Analysis Report

## Batch vs. Streaming Divergence

### Methodology
To compare batch and streaming outputs, a batch script (`batch_analysis.py`) reads the
same raw events that the producer wrote to the `user-events` topic and recomputes all
features using pandas over the entire collected dataset.

### Key Divergences Observed

| Feature | Batch Value (example) | Streaming Value | Reason |
|---|---|---|---|
| `click_rate` (user2, hour 14) | 0.3333 | 0.2857 | Streaming window closed at watermark; 2 late events were excluded |
| `avg_dwell_time` (user1, hour 14) | 28 412 ms | 31 080 ms | Same cause — late events with low dwell times were dropped |
| `engagement_rate` (content3, 15-min) | 0.5 | 0.4 | Sliding window captured a different event slice than the full batch scan |
| `category_affinity_scifi` (user2) | 5 | 4 | One late event for the scifi category arrived after the window closed |

### Why Divergences Occur

1. **Event-time window boundaries.** The streaming pipeline uses strict event-time windows.
   A tumbling window for hour 14 covers exactly `[14:00, 15:00)`. The batch script has no
   concept of windows; it groups all events by user and computes aggregates over the full
   dataset regardless of when they arrived.

2. **Late-event dropping.** Flink's watermark is set to 30 seconds of bounded out-of-orderness.
   Any event whose `timestamp` is more than 30 seconds behind the current watermark is routed
   to the side output and **not included in any window**. The batch script includes all events
   unconditionally.

3. **Sliding window overlap.** A 15-min/5-min sliding window means each event appears in up to
   3 windows. Batch aggregation on a flat dataset counts each event once, producing different
   denominators.

### Implications for ML Models
A model trained on batch-computed features may be calibrated to slightly higher absolute
values (since batch includes all late events). When this model is served against streaming
features at inference time, there will be a **feature distribution shift**. The practical
mitigation is to train offline with the same windowing semantics (e.g., using Spark Structured
Streaming in batch mode) or to regularly re-calibrate the model against the streaming baseline.

---

## Late Event Handling

### Configuration
The Flink source is configured with:
```java
WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
```
This means: *Flink will wait up to 30 seconds before declaring a timestamp range complete.*

### Producer-Injected Late Events
The `DataGeneratorService` intentionally produces **5% of events** with timestamps between
**35 and 90 seconds in the past** (relative to the current simulation clock). This deliberately
exceeds the 30-second watermark tolerance.

### Evidence of Late-Event Handling

**Producer log (example):**
```
Generating LATE event. Lag: 62 seconds.
Produced event for user3 at 2024-10-27T14:23:01Z
```

**Flink TaskManager log (example):**
```
[UserFeatures] Tumbling window [14:00–15:00) fired for user3 with 47 events
Side-output: late event dropped — user3 @ 2024-10-27T14:22:59Z (lag=62s > watermark tolerance=30s)
```

**Dashboard metrics panel:**
The "Late Events Dropped" counter increments in real time as the Flink job routes overdue
events to the side output. The counter is published to the `flink-metrics` Kafka topic and
consumed by the Spring Boot dashboard backend.

### Watermark Tolerance Trade-offs

| Tolerance | Effect |
|---|---|
| **30 s (current)** | Balances latency vs. correctness. Events up to 30 s late are accepted. |
| **0 s** | Lowest latency, but any reordering causes dropped events. |
| **120 s** | Higher correctness for delayed producers, but features are 2 min stale. |

### What Happens When an Event Arrives After Its Window Is Closed?
If an event's timestamp falls in a window that has already been **emitted and garbage-collected**
by Flink, it is routed to the late-data side output. In this pipeline the side output is
consumed and published as a `late_event_dropped` metric to `flink-metrics`. The event is
**not re-aggregated** into any previous window result — that result has already been written
to `feature-store`. This is expected and correct behavior: the downstream ML model receives
a slightly less accurate feature value for that window, but the system remains consistent
and does not block forward progress waiting for indefinitely late data.
