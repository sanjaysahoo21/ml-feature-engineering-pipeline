package com.pipeline.dashboard.controller;

import com.pipeline.dashboard.service.FeatureStoreService;
import com.pipeline.dashboard.service.MetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class DashboardController {

    @Autowired private FeatureStoreService featureStore;
    @Autowired private MetricsService      metricsService;

    /** Returns all latest features for a given user_id or content_id. */
    @GetMapping("/features/{entityId}")
    public ResponseEntity<List<FeatureStoreService.FeatureRecord>> getFeatures(
            @PathVariable String entityId) {
        return ResponseEntity.ok(featureStore.getFeaturesForEntity(entityId));
    }

    /** Pipeline health snapshot: late events dropped, watermark lag, last heartbeat. */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> m = new HashMap<>();
        m.put("lateEventsDropped", metricsService.getLateEventsDropped());
        m.put("watermarkLagMs",    metricsService.getWatermarkLagMs());
        m.put("lastWatermarkTime", metricsService.getLastWatermarkTime());
        return ResponseEntity.ok(m);
    }

    /** Freshness (ms since last update) for click_rate and engagement_rate. */
    @GetMapping("/freshness")
    public ResponseEntity<Map<String, Object>> getFreshness() {
        Map<String, Object> result = new HashMap<>();

        featureStore.allEntityIds().stream()
            .filter(id -> featureStore.getFeaturesForEntity(id).stream()
                .anyMatch(f -> "click_rate".equals(f.featureName())))
            .findFirst()
            .ifPresent(uid -> result.put("click_rate_freshness_ms",
                    featureStore.freshnessMs(uid, "click_rate")));

        featureStore.allEntityIds().stream()
            .filter(id -> featureStore.getFeaturesForEntity(id).stream()
                .anyMatch(f -> "engagement_rate".equals(f.featureName())))
            .findFirst()
            .ifPresent(cid -> result.put("engagement_rate_freshness_ms",
                    featureStore.freshnessMs(cid, "engagement_rate")));

        return ResponseEntity.ok(result);
    }
}
