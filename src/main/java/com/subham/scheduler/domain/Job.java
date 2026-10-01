package com.subham.scheduler.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * A row of the jobs table. {@code attempts} is incremented when a node claims the job and
 * doubles as a fencing token: a node may only record the outcome of the attempt it claimed.
 */
public record Job(
        UUID id,
        String type,
        String payload,
        JobStatus status,
        int priority,
        Instant runAt,
        String cronExpression,
        int attempts,
        int maxAttempts,
        int timeoutSeconds,
        String idempotencyKey,
        String lockedBy,
        Instant leaseUntil,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {

    public boolean isRecurring() {
        return cronExpression != null && !cronExpression.isBlank();
    }
}
