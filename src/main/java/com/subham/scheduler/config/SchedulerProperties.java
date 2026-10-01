package com.subham.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(String nodeId, Worker worker, Retry retry, Lease lease) {

    public record Worker(boolean enabled, int threads, int batchSize, Duration pollInterval, Duration shutdownTimeout) {
    }

    public record Retry(Duration baseDelay, Duration maxDelay, double jitter) {
    }

    public record Lease(Duration grace, Duration reaperInterval) {
    }
}
