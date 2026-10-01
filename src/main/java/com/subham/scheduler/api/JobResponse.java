package com.subham.scheduler.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.subham.scheduler.domain.Job;
import com.subham.scheduler.domain.JobStatus;

import java.time.Instant;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String type,
        JsonNode payload,
        JobStatus status,
        int priority,
        Instant runAt,
        String cron,
        int attempts,
        int maxAttempts,
        int timeoutSeconds,
        String idempotencyKey,
        String lockedBy,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {

    public static JobResponse from(Job job, ObjectMapper mapper) {
        JsonNode payload;
        try {
            payload = mapper.readTree(job.payload());
        } catch (JsonProcessingException e) {
            payload = mapper.getNodeFactory().textNode(job.payload());
        }
        return new JobResponse(job.id(), job.type(), payload, job.status(), job.priority(), job.runAt(),
                job.cronExpression(), job.attempts(), job.maxAttempts(), job.timeoutSeconds(), job.idempotencyKey(),
                job.lockedBy(), job.lastError(), job.createdAt(), job.updatedAt(), job.completedAt());
    }
}
