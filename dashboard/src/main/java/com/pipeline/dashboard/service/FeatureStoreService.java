package com.pipeline.dashboard.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache of the latest computed feature values.
 * Structure: entityId → { featureName → FeatureRecord }
 */
@Service
public class FeatureStoreService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Map<String, FeatureRecord>> store = new ConcurrentHashMap<>();

    public void ingest(String rawJson) {
        try {
            JsonNode node       = mapper.readTree(rawJson);
            String entityId     = node.get("entity_id").asText();
            String featureName  = node.get("feature_name").asText();
            String featureValue = node.get("feature_value").asText();
            String computedAt   = node.get("computed_at").asText();

            store.computeIfAbsent(entityId, k -> new ConcurrentHashMap<>())
                 .put(featureName, new FeatureRecord(entityId, featureName, featureValue, computedAt));
        } catch (Exception ignored) {}
    }

    public List<FeatureRecord> getFeaturesForEntity(String entityId) {
        Map<String, FeatureRecord> features = store.get(entityId);
        return features == null ? new ArrayList<>() : new ArrayList<>(features.values());
    }

    public Set<String> allEntityIds() {
        return store.keySet();
    }

    /** Returns how stale (ms) a feature is; -1 if not yet seen. */
    public long freshnessMs(String entityId, String featureName) {
        Map<String, FeatureRecord> features = store.get(entityId);
        if (features == null) return -1;
        FeatureRecord rec = features.get(featureName);
        if (rec == null) return -1;
        try {
            return Instant.now().toEpochMilli() - Instant.parse(rec.computedAt()).toEpochMilli();
        } catch (Exception e) { return -1; }
    }

    public record FeatureRecord(String entityId, String featureName,
                                String featureValue, String computedAt) {}
}
