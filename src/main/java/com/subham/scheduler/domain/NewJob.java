package com.subham.scheduler.domain;

import java.time.Instant;
import java.util.UUID;

public record NewJob(
        UUID id,
        String type,
        String payloadJson,
        int priority,
        Instant runAt,
        String cronExpression,
        int maxAttempts,
        int timeoutSeconds,
        String idempotencyKey) {
}
