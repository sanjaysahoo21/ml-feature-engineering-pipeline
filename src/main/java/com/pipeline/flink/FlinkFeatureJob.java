package com.pipeline.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class FlinkFeatureJob {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("user-events")
                .setGroupId("flink-feature-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((eventJson, timestamp) -> {
                    try {
                        JsonNode node = mapper.readTree(eventJson);
                        String timestampStr = node.get("timestamp").asText();
                        return Instant.parse(timestampStr).toEpochMilli();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                });

        DataStream<JsonNode> userEventsStream = env
                .fromSource(source, watermarkStrategy, "User Events Source")
                .map((MapFunction<String, JsonNode>) mapper::readTree);

        DataStream<String> userFeatures = userEventsStream
                .keyBy(json -> json.get("user_id").asText())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<JsonNode, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<JsonNode> elements, Collector<String> out) {
                        out.collect(createFeatureJson(key, "user_activity", "active"));
                    }
                });

        DataStream<String> contentFeatures = userEventsStream
                .keyBy(json -> json.get("content_id").asText())
                .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5)))
                .process(new ProcessWindowFunction<JsonNode, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<JsonNode> elements, Collector<String> out) {
                        out.collect(createFeatureJson(key, "content_activity", "active"));
                    }
                });

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("feature-store")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

        userFeatures.sinkTo(sink);
        contentFeatures.sinkTo(sink);

        System.out.println("Starting Flink Feature Engineering Job...");
        env.execute("Real-Time ML Feature Engineering Pipeline");
    }

    private static String createFeatureJson(String entityId, String featureName, String featureValue) {
        try {
            return mapper.writeValueAsString(mapper.createObjectNode()
                    .put("entity_id", entityId)
                    .put("feature_name", featureName)
                    .put("feature_value", featureValue)
                    .put("computed_at", Instant.now().toString()));
        } catch (Exception e) {
            return "{}";
        }
    }
}
