package com.pipeline.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Apache Flink Feature Engineering Pipeline.
 *
 * Features computed:
 *  1. click_rate, avg_dwell_time  — per user, 1-hour tumbling window
 *  2. engagement_rate             — per content, 15-min sliding / 5-min slide
 *  3. category_affinity_*         — per user×category, 1-hour tumbling window (broadcast join)
 *
 * Late events and watermark heartbeats are published to the flink-metrics topic.
 */
public class FlinkFeatureJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Routes late events (timestamp > watermark + 30s) to a side output
    static final OutputTag<JsonNode> LATE_EVENTS_TAG =
            new OutputTag<JsonNode>("late-events") {};

    // Broadcast state: content_id -> metadata JSON string
    static final MapStateDescriptor<String, String> METADATA_STATE =
            new MapStateDescriptor<>("content-metadata",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

    static final AtomicLong lateEventCounter = new AtomicLong(0L);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60_000);

        String brokers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        KafkaSource<String> userEventsSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("user-events")
                .setGroupId("flink-feature-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> metadataSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("content-metadata")
                .setGroupId("flink-metadata-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Tolerate up to 30 s of out-of-orderness; events beyond this are routed to LATE_EVENTS_TAG
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((json, ts) -> {
                    try {
                        return Instant.parse(
                                MAPPER.readTree(json).get("timestamp").asText()
                        ).toEpochMilli();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });

        DataStream<JsonNode> userEventsStream = env
                .fromSource(userEventsSource, watermarkStrategy, "User Events Source")
                .map((MapFunction<String, JsonNode>) MAPPER::readTree)
                .name("Parse User Events");

        // Metadata has no event-time semantics — it is reference data broadcast to all workers
        DataStream<JsonNode> metadataStream = env
                .fromSource(metadataSource, WatermarkStrategy.noWatermarks(),
                        "Content Metadata Source")
                .map((MapFunction<String, JsonNode>) MAPPER::readTree)
                .name("Parse Metadata");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");

        KafkaSink<String> featureStoreSink = buildKafkaSink(brokers, "feature-store", sinkProps);
        KafkaSink<String> metricsSink      = buildKafkaSink(brokers, "flink-metrics", new Properties());

        // Per-user features — 1-hour tumbling window (REQ-6)
        SingleOutputStreamOperator<String> userFeaturesStream = userEventsStream
                .keyBy(e -> e.get("user_id").asText())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(LATE_EVENTS_TAG)
                .process(new UserFeatureWindowFunction())
                .name("User Features (Tumbling 1h)");

        // Per-content features — 15-min sliding window, slides every 5 min (REQ-7)
        DataStream<String> contentFeaturesStream = userEventsStream
                .keyBy(e -> e.get("content_id").asText())
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .process(new ContentFeatureWindowFunction())
                .name("Content Features (Sliding 15m/5m)");

        // Stream-table join: broadcast content-metadata, enrich each event with category (REQ-8)
        BroadcastStream<JsonNode> broadcastMetadata =
                metadataStream.broadcast(METADATA_STATE);

        DataStream<JsonNode> enrichedStream = userEventsStream
                .connect(broadcastMetadata)
                .process(new MetadataEnrichmentFunction())
                .name("Enrich Events with Metadata");

        DataStream<String> categoryAffinityStream = enrichedStream
                .filter(e -> e.has("category"))
                .keyBy(e -> e.get("user_id").asText())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new CategoryAffinityWindowFunction())
                .name("Category Affinity (Tumbling 1h)");

        DataStream<String> lateMetricsStream = userFeaturesStream
                .getSideOutput(LATE_EVENTS_TAG)
                .map(lateEvent -> {
                    lateEventCounter.incrementAndGet();
                    ObjectNode m = MAPPER.createObjectNode();
                    m.put("metric_type",     "late_event_dropped");
                    m.put("count",           lateEventCounter.get());
                    m.put("dropped_user_id", lateEvent.path("user_id").asText("unknown"));
                    m.put("dropped_event_ts",lateEvent.path("timestamp").asText("unknown"));
                    m.put("recorded_at",     Instant.now().toString());
                    return MAPPER.writeValueAsString(m);
                })
                .name("Late Events Metrics");

        // Emit a heartbeat after each window fires so the dashboard can track watermark lag
        DataStream<String> watermarkMetricsStream = userFeaturesStream
                .map(feature -> {
                    ObjectNode m = MAPPER.createObjectNode();
                    m.put("metric_type",        "watermark_heartbeat");
                    m.put("feature_computed_at", Instant.now().toString());
                    m.put("late_events_total",   lateEventCounter.get());
                    return MAPPER.writeValueAsString(m);
                })
                .name("Watermark Heartbeat");

        userFeaturesStream.sinkTo(featureStoreSink);
        contentFeaturesStream.sinkTo(featureStoreSink);
        categoryAffinityStream.sinkTo(featureStoreSink);
        lateMetricsStream.sinkTo(metricsSink);
        watermarkMetricsStream.sinkTo(metricsSink);

        System.out.println("[FlinkFeatureJob] Starting Real-Time ML Feature Engineering Pipeline...");
        env.execute("Real-Time ML Feature Engineering Pipeline");
    }

    /**
     * Computes click_rate and avg_dwell_time per user over a 1-hour tumbling window.
     * click_rate = clicks / total_events
     */
    static class UserFeatureWindowFunction
            extends ProcessWindowFunction<JsonNode, String, String, TimeWindow> {
        @Override
        public void process(String userId, Context ctx,
                            Iterable<JsonNode> elements, Collector<String> out) {
            int total = 0, clicks = 0;
            long dwellSum = 0;

            for (JsonNode e : elements) {
                total++;
                if ("click".equalsIgnoreCase(e.get("event_type").asText())) clicks++;
                dwellSum += e.get("dwell_time_ms").asLong(0);
            }

            double clickRate    = total > 0 ? (double) clicks / total : 0.0;
            double avgDwellTime = total > 0 ? (double) dwellSum / total : 0.0;

            out.collect(featureJson(userId, "click_rate",     clickRate));
            out.collect(featureJson(userId, "avg_dwell_time", avgDwellTime));

            System.out.printf("[UserFeatures] user=%s  events=%d  click_rate=%.3f  avg_dwell=%.1f%n",
                    userId, total, clickRate, avgDwellTime);
        }
    }

    /**
     * Computes engagement_rate per content over a 15-min sliding window.
     * engagement_rate = (likes + shares) / views  — returns 0 when there are no views.
     */
    static class ContentFeatureWindowFunction
            extends ProcessWindowFunction<JsonNode, String, String, TimeWindow> {
        @Override
        public void process(String contentId, Context ctx,
                            Iterable<JsonNode> elements, Collector<String> out) {
            int views = 0, engagements = 0;

            for (JsonNode e : elements) {
                String type = e.get("event_type").asText();
                if ("view".equalsIgnoreCase(type))  views++;
                if ("like".equalsIgnoreCase(type) || "share".equalsIgnoreCase(type)) engagements++;
            }

            double engagementRate = views > 0 ? (double) engagements / views : 0.0;
            out.collect(featureJson(contentId, "engagement_rate", engagementRate));

            System.out.printf("[ContentFeatures] content=%s  views=%d  engagements=%d  rate=%.3f%n",
                    contentId, views, engagements, engagementRate);
        }
    }

    /**
     * Attaches category and creator_id from the broadcast metadata state to each event.
     * Events with no matching metadata are forwarded without enrichment (not dropped).
     */
    static class MetadataEnrichmentFunction
            extends BroadcastProcessFunction<JsonNode, JsonNode, JsonNode> {

        @Override
        public void processElement(JsonNode event, ReadOnlyContext ctx,
                                   Collector<JsonNode> out) throws Exception {
            String contentId = event.get("content_id").asText();
            String metaJson  = ctx.getBroadcastState(METADATA_STATE).get(contentId);

            if (metaJson != null) {
                ObjectNode enriched = (ObjectNode) MAPPER.readTree(event.toString());
                JsonNode meta = MAPPER.readTree(metaJson);
                enriched.put("category",   meta.get("category").asText());
                enriched.put("creator_id", meta.get("creator_id").asText());
                out.collect(enriched);
            } else {
                out.collect(event);
            }
        }

        @Override
        public void processBroadcastElement(JsonNode metadata, Context ctx,
                                            Collector<JsonNode> out) throws Exception {
            String contentId = metadata.get("content_id").asText();
            ctx.getBroadcastState(METADATA_STATE).put(contentId,
                    MAPPER.writeValueAsString(metadata));
            System.out.println("[Metadata] Registered content_id=" + contentId);
        }
    }

    /**
     * Counts events per category per user in a 1-hour tumbling window.
     * Outputs one feature per category: category_affinity_<category>.
     */
    static class CategoryAffinityWindowFunction
            extends ProcessWindowFunction<JsonNode, String, String, TimeWindow> {
        @Override
        public void process(String userId, Context ctx,
                            Iterable<JsonNode> elements, Collector<String> out) {
            Map<String, Integer> counts = new HashMap<>();
            for (JsonNode e : elements) {
                String category = e.has("category") ? e.get("category").asText() : "unknown";
                counts.merge(category, 1, Integer::sum);
            }
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                String featureName = "category_affinity_" + entry.getKey();
                out.collect(featureJson(userId, featureName, entry.getValue()));
                System.out.printf("[CategoryAffinity] user=%s  %s=%d%n",
                        userId, featureName, entry.getValue());
            }
        }
    }

    private static KafkaSink<String> buildKafkaSink(
            String brokers, String topic, Properties extra) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(extra)
                .build();
    }

    static String featureJson(String entityId, String featureName, Object featureValue) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("entity_id",    entityId);
            node.put("feature_name", featureName);
            if (featureValue instanceof Double)        node.put("feature_value", (Double)  featureValue);
            else if (featureValue instanceof Integer)  node.put("feature_value", (Integer) featureValue);
            else                                       node.put("feature_value", featureValue.toString());
            node.put("computed_at", Instant.now().toString());
            return MAPPER.writeValueAsString(node);
        } catch (Exception e) {
            return "{}";
        }
    }
}
