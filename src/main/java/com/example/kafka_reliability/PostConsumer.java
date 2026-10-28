
package com.example.kafka_reliability;

import tools.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PostConsumer {
    private final ObjectMapper objectMapper;
    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    public PostConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(attempts = "4", dltTopicSuffix = "-dlt")
    @KafkaListener(topics = "scheduled-posts")
    public void consume(String message) throws Exception {
        PostEvent event = objectMapper.readValue(message, PostEvent.class);

        if (processedEvents.contains(event.id())) {
            System.out.println("Duplicate event ignored: " + event.id());
            return;
        }

        System.out.println("Processing event: " + event.id());

        if (event.shouldFail()) {
            throw new RuntimeException("Simulated processing failure");
        }

        processedEvents.add(event.id());
        System.out.println("Post processed successfully: " + event.content());
    }

    @KafkaListener(topics = "scheduled-posts-dlt")
    public void consumeDeadLetter(String message) {
        System.out.println("DLQ received failed event: " + message);
    }
}
