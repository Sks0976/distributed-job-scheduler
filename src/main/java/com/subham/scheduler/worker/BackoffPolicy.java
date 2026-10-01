package com.subham.scheduler.worker;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with jitter: base * 2^(attempt - 1), capped at max, then randomized by
 * +/- jitter so that many jobs failing together (e.g. a downstream outage) do not retry in lockstep.
 */
public final class BackoffPolicy {

    private final Duration baseDelay;
    private final Duration maxDelay;
    private final double jitter;
    private final DoubleSupplier random;

    public BackoffPolicy(Duration baseDelay, Duration maxDelay, double jitter) {
        this(baseDelay, maxDelay, jitter, () -> ThreadLocalRandom.current().nextDouble());
    }

    BackoffPolicy(Duration baseDelay, Duration maxDelay, double jitter, DoubleSupplier random) {
        if (baseDelay.isNegative() || maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("Require 0 <= baseDelay <= maxDelay");
        }
        if (jitter < 0 || jitter > 1) {
            throw new IllegalArgumentException("jitter must be between 0 and 1");
        }
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.jitter = jitter;
        this.random = random;
    }

    /** @param failedAttempt the attempt number that just failed, starting at 1 */
    public Duration delayFor(int failedAttempt) {
        int exponent = Math.min(Math.max(failedAttempt - 1, 0), 30);
        double exponential = baseDelay.toMillis() * Math.pow(2, exponent);
        double capped = Math.min(exponential, maxDelay.toMillis());
        double factor = 1 + jitter * (2 * random.getAsDouble() - 1);
        long millis = Math.round(Math.min(capped * factor, maxDelay.toMillis()));
        return Duration.ofMillis(Math.max(millis, 0));
    }
}
