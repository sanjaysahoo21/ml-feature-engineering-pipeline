package com.pipeline.dashboard.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks operational Flink pipeline metrics from the flink-metrics Kafka topic.
 * Exposes late-event count and watermark lag for the dashboard UI.
 */
@Service
public class MetricsService {

    private final ObjectMapper mapper          = new ObjectMapper();
    private final AtomicLong lateEventsDropped = new AtomicLong(0);
    private volatile String lastWatermarkTime  = Instant.now().toString();

    public void ingest(String rawJson) {
        try {
            JsonNode node = mapper.readTree(rawJson);
            switch (node.get("metric_type").asText()) {
                case "late_event_dropped" -> {
                    if (node.has("count")) lateEventsDropped.set(node.get("count").asLong());
                    else                   lateEventsDropped.incrementAndGet();
                }
                case "watermark_heartbeat" -> {
                    lastWatermarkTime = node.get("feature_computed_at").asText();
                    if (node.has("late_events_total"))
                        lateEventsDropped.set(node.get("late_events_total").asLong());
                }
            }
        } catch (Exception ignored) {}
    }

    public long   getLateEventsDropped() { return lateEventsDropped.get(); }
    public String getLastWatermarkTime() { return lastWatermarkTime; }

    public long getWatermarkLagMs() {
        try {
            return Instant.now().toEpochMilli() - Instant.parse(lastWatermarkTime).toEpochMilli();
        } catch (Exception e) { return -1; }
    }
}
