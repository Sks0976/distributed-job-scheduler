package com.subham.scheduler.domain;

import java.time.Instant;
import java.util.UUID;

public record JobExecution(
        long id,
        UUID jobId,
        int attempt,
        String nodeId,
        Instant startedAt,
        Instant finishedAt,
        String outcome,
        String error) {
}
