
package com.example.kafka_reliability;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

@RestController
public class PostController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PostController(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/posts")
    public String createPost(@RequestBody PostEvent event) throws Exception {
        String message = objectMapper.writeValueAsString(event);
        kafkaTemplate.send("scheduled-posts", message);
        return "Post event sent successfully: " + event.id();
    }
}
