# Real-Time Feature Engineering Pipeline Analysis

## Batch vs. Streaming Divergence
Batch computations calculate feature values identically across all historical data at a fixed point in time. Our Streaming pipeline computes these continuously using Watermarks and Windowing.
**Divergence Reason**: Flink event-time windows discard late-arriving metrics (those falling outside the 30-second watermark). While batch systems would count these clicks strictly by log time, the streaming system accepts minor deviations but hard-drops severe ones for real-time model relevancy.

## Late Event Handling
The pipeline producer purposely injected timestamps 35-90 seconds in the past for 5% of all traffic. 
Because Flink was configured with `forBoundedOutOfOrderness(Duration.ofSeconds(30))`, events arriving with 30s delays were buffered perfectly. Events greater than 30s were correctly dropped as late data, establishing exact deterministic behaviour for the output of `feature-store` features.
