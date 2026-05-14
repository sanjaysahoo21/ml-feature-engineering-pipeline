package com.pipeline.producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class FeatureConsumerService {

    @Autowired
    private SimpMessagingTemplate template;

    @KafkaListener(topics = "feature-store", groupId = "dashboard-group")
    public void consumeFeature(String message) {
        // Forward the Kafka message to the WebSocket frontend
        System.out.println("Pushing feature to dashboard: " + message);
        template.convertAndSend("/topic/features", message);
    }
}
