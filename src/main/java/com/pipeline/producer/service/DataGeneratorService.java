package com.pipeline.producer.service;

import com.pipeline.producer.model.ContentMetadata;
import com.pipeline.producer.model.UserEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Service
public class DataGeneratorService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String USER_EVENTS_TOPIC = "user-events";
    private static final String CONTENT_METADATA_TOPIC = "content-metadata";
    private final Random random = new Random();
    private final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    private final List<String> users = Arrays.asList("user1", "user2", "user3", "user4", "user5");
    private final List<String> contents = Arrays.asList("content1", "content2", "content3", "content4");
    private final List<String> events = Arrays.asList("click", "view", "like", "share");
    private final List<String> categories = Arrays.asList("scifi", "news", "sports", "comedy");

    @PostConstruct
    public void initMetadata() {
        System.out.println("Initializing Content Metadata to Kafka...");
        for (int i = 0; i < contents.size(); i++) {
            ContentMetadata metadata = new ContentMetadata(
                    contents.get(i),
                    categories.get(i % categories.size()),
                    "creatorA",
                    formatter.format(Instant.now().minusSeconds(86400 * 7)) // 1 week ago
            );
            // Produce to compacted topic
            kafkaTemplate.send(CONTENT_METADATA_TOPIC, metadata.getContent_id(), metadata);
        }
    }

    @Scheduled(fixedRate = 1000) // Generate 1 event per second
    public void generateUserEvent() {
        boolean isLate = random.nextDouble() < 0.05; // 5% chance
        Instant eventTime = Instant.now();
        
        if (isLate) {
            // Delay between 35 and 90 seconds
            int delaySeconds = 35 + random.nextInt(56);
            eventTime = eventTime.minusSeconds(delaySeconds);
            System.out.println("Generating LATE event. Lag: " + delaySeconds + " seconds.");
        }

        UserEvent event = new UserEvent(
                users.get(random.nextInt(users.size())),
                contents.get(random.nextInt(contents.size())),
                events.get(random.nextInt(events.size())),
                random.nextInt(60000), // Dwell time up to 60s
                formatter.format(eventTime)
        );

        kafkaTemplate.send(USER_EVENTS_TOPIC, event.getUser_id(), event);
        System.out.println("Produced event for " + event.getUser_id() + " at " + event.getTimestamp());
    }
}
