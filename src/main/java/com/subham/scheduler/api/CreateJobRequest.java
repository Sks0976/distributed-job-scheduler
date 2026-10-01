package com.subham.scheduler.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record CreateJobRequest(
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        @Min(-100) @Max(100) Integer priority,
        Instant runAt,
        @Size(max = 100) String cron,
        @Min(1) @Max(20) Integer maxAttempts,
        @Min(1) @Max(3600) Integer timeoutSeconds,
        @Size(max = 200) String idempotencyKey) {

    public static CreateJobRequest of(String type, JsonNode payload) {
        return new CreateJobRequest(type, payload, null, null, null, null, null, null);
    }
}
