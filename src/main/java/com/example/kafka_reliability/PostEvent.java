package com.example.kafka_reliability;

public record PostEvent(
        String id,
        String content,
        boolean shouldFail
) {}