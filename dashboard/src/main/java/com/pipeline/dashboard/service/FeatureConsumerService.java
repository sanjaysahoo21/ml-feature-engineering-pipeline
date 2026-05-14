package com.pipeline.dashboard.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class FeatureConsumerService {

    @Autowired private SimpMessagingTemplate template;
    @Autowired private FeatureStoreService   featureStore;
    @Autowired private MetricsService        metricsService;

    @KafkaListener(topics = "feature-store", groupId = "dashboard-feature-group")
    public void consumeFeature(String message) {
        featureStore.ingest(message);
        template.convertAndSend("/topic/features", message);
    }

    @KafkaListener(topics = "flink-metrics", groupId = "dashboard-metrics-group")
    public void consumeMetric(String message) {
        metricsService.ingest(message);
        template.convertAndSend("/topic/metrics", buildMetricsSnapshot());
    }

    private String buildMetricsSnapshot() {
        return String.format(
            "{\"lateEventsDropped\":%d,\"watermarkLagMs\":%d,\"lastWatermarkTime\":\"%s\"}",
            metricsService.getLateEventsDropped(),
            metricsService.getWatermarkLagMs(),
            metricsService.getLastWatermarkTime()
        );
    }
}
