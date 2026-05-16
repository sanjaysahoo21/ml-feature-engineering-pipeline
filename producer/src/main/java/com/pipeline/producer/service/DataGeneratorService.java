package com.pipeline.producer.service;

import com.pipeline.producer.model.ContentMetadata;
import com.pipeline.producer.model.UserEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Service
public class DataGeneratorService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC_USER_EVENTS      = "user-events";
    private static final String TOPIC_CONTENT_METADATA = "content-metadata";

    private final Random            random    = new Random();
    private final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    private final List<String> users      = Arrays.asList("user1", "user2", "user3", "user4", "user5");
    private final List<String> contents   = Arrays.asList("content1", "content2", "content3", "content4");
    private final List<String> eventTypes = Arrays.asList("click", "view", "like", "share");
    private final List<String> categories = Arrays.asList("scifi", "news", "sports", "comedy");

    /** Publishes one metadata record per content item on startup so Flink's broadcast state is populated. */
    @PostConstruct
    public void initMetadata() {
        System.out.println("[Producer] Initialising content-metadata topic...");
        for (int i = 0; i < contents.size(); i++) {
            ContentMetadata meta = new ContentMetadata(
                    contents.get(i),
                    categories.get(i % categories.size()),
                    "creator-" + (char) ('A' + i),
                    formatter.format(Instant.now().minusSeconds(86_400 * 7L))
            );
            kafkaTemplate.send(TOPIC_CONTENT_METADATA, meta.getContent_id(), meta);
            System.out.printf("[Producer] Metadata: %s → %s%n", meta.getContent_id(), meta.getCategory());
        }
    }

    /** Emits one user event per second. 5% are deliberately late (35–90 s) to test the watermark. */
    @Scheduled(fixedRate = 1000)
    public void generateUserEvent() {
        boolean isLate = random.nextDouble() < 0.05;
        Instant eventTime = Instant.now();

        if (isLate) {
            int lagSeconds = 35 + random.nextInt(56);
            eventTime = eventTime.minusSeconds(lagSeconds);
            System.out.printf("[Producer] LATE event injected — lag=%d s%n", lagSeconds);
        }

        UserEvent event = new UserEvent(
                users.get(random.nextInt(users.size())),
                contents.get(random.nextInt(contents.size())),
                eventTypes.get(random.nextInt(eventTypes.size())),
                random.nextInt(60_000),
                formatter.format(eventTime)
        );

        kafkaTemplate.send(TOPIC_USER_EVENTS, event.getUser_id(), event);
        System.out.printf("[Producer] Event: user=%s  type=%s  ts=%s%n",
                event.getUser_id(), event.getEvent_type(), event.getTimestamp());
    }
}