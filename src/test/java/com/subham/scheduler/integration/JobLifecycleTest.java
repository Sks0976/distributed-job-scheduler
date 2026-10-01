package com.subham.scheduler.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.subham.scheduler.api.CreateJobRequest;
import com.subham.scheduler.domain.Job;
import com.subham.scheduler.domain.JobExecution;
import com.subham.scheduler.domain.JobStatus;
import com.subham.scheduler.service.JobService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "scheduler.node-id=test-node",
        "scheduler.worker.poll-interval=50ms",
        "scheduler.retry.base-delay=50ms",
        "scheduler.retry.max-delay=200ms",
        "scheduler.lease.reaper-interval=500ms"
})
// Stop this context's worker afterwards so it cannot claim jobs seeded by other test classes.
@DirtiesContext
class JobLifecycleTest extends PostgresTestBase {

    @Autowired
    private JobService service;

    @Autowired
    private ObjectMapper mapper;

    @Test
    void successfulJobCompletes() {
        Job job = service.create(request("sleep", payload("millis", 20), 3, 10, null)).job();

        Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);

        assertThat(done.attempts()).isEqualTo(1);
        assertThat(done.completedAt()).isNotNull();
        awaitExecutions(job.id(), 1);
    }

    @Test
    void failingJobIsRetriedWithBackoffThenMarkedFailed() {
        Job job = service.create(request("flaky", payload("failureRate", 1.0), 3, 10, null)).job();

        Job failed = awaitStatus(job.id(), JobStatus.FAILED);

        assertThat(failed.attempts()).isEqualTo(3);
        assertThat(failed.lastError()).contains("Simulated transient failure");
        awaitExecutions(job.id(), 3);
        assertThat(service.executions(job.id())).extracting(JobExecution::outcome)
                .containsExactly("retry_scheduled", "retry_scheduled", "failed");
    }

    @Test
    void jobExceedingTimeoutIsInterrupted() {
        Job job = service.create(request("sleep", payload("millis", 10_000), 1, 1, null)).job();

        Job failed = awaitStatus(job.id(), JobStatus.FAILED);

        assertThat(failed.lastError()).isEqualTo("Timed out after 1s");
    }

    @Test
    void duplicateSubmissionWithSameIdempotencyKeyReturnsOriginalJob() {
        String key = "order-" + UUID.randomUUID();
        JobService.CreateResult first = service.create(request("sleep", payload("millis", 1), 3, 10, key));
        JobService.CreateResult second = service.create(request("sleep", payload("millis", 1), 3, 10, key));

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.job().id()).isEqualTo(first.job().id());
    }

    @Test
    void futureJobCanBeCancelledBeforeItRuns() {
        CreateJobRequest request = new CreateJobRequest("sleep", payload("millis", 1), null,
                Instant.now().plus(Duration.ofHours(1)), null, null, null, null);
        Job job = service.create(request).job();

        assertThat(service.cancel(job.id()).status()).isEqualTo(JobStatus.CANCELLED);
        assertThatThrownBy(() -> service.cancel(job.id())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unknownJobTypeIsRejected() {
        assertThatThrownBy(() -> service.create(request("does-not-exist", null, 3, 10, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown job type");
    }

    private Job awaitStatus(UUID id, JobStatus status) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                .until(() -> service.get(id).status() == status);
        return service.get(id);
    }

    private void awaitExecutions(UUID id, int count) {
        await().atMost(Duration.ofSeconds(5)).until(() -> service.executions(id).size() == count);
    }

    private ObjectNode payload(String field, Object value) {
        ObjectNode node = mapper.createObjectNode();
        if (value instanceof Double d) {
            node.put(field, d);
        } else {
            node.put(field, ((Number) value).longValue());
        }
        return node;
    }

    private static CreateJobRequest request(String type, ObjectNode payload, int maxAttempts, int timeout, String key) {
        return new CreateJobRequest(type, payload, null, null, null, maxAttempts, timeout, key);
    }
}
