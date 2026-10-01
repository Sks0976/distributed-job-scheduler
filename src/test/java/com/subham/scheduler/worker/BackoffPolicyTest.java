package com.subham.scheduler.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffPolicyTest {

    private static final Duration BASE = Duration.ofSeconds(2);
    private static final Duration MAX = Duration.ofMinutes(1);

    @Test
    void doublesDelayForEachFailedAttempt() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX, 0.2, () -> 0.5); // 0.5 means no jitter

        assertThat(policy.delayFor(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayFor(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayFor(3)).isEqualTo(Duration.ofSeconds(8));
    }

    @Test
    void neverExceedsMaxDelay() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX, 0.2, () -> 1.0); // maximum upward jitter

        assertThat(policy.delayFor(10)).isEqualTo(MAX);
        assertThat(policy.delayFor(1_000)).isEqualTo(MAX);
    }

    @Test
    void jitterStaysWithinConfiguredBounds() {
        BackoffPolicy low = new BackoffPolicy(BASE, MAX, 0.2, () -> 0.0);
        BackoffPolicy high = new BackoffPolicy(BASE, MAX, 0.2, () -> 1.0);

        assertThat(low.delayFor(2)).isEqualTo(Duration.ofMillis(3200));   // 4s - 20%
        assertThat(high.delayFor(2)).isEqualTo(Duration.ofMillis(4800));  // 4s + 20%
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new BackoffPolicy(MAX, BASE, 0.2)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(BASE, MAX, 1.5)).isInstanceOf(IllegalArgumentException.class);
    }
}
