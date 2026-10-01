package com.subham.scheduler.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.subham.scheduler.api.CreateJobRequest;
import com.subham.scheduler.domain.Job;
import com.subham.scheduler.domain.JobExecution;
import com.subham.scheduler.domain.JobStatus;
import com.subham.scheduler.domain.NewJob;
import com.subham.scheduler.handler.JobHandlerRegistry;
import com.subham.scheduler.repository.JobRepository;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class JobService {

    static final int DEFAULT_MAX_ATTEMPTS = 3;
    static final int DEFAULT_TIMEOUT_SECONDS = 60;

    private final JobRepository repository;
    private final JobHandlerRegistry handlers;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public JobService(JobRepository repository, JobHandlerRegistry handlers, ObjectMapper objectMapper, Clock clock) {
        this.repository = repository;
        this.handlers = handlers;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public record CreateResult(Job job, boolean created) {
    }

    /** Idempotent when an idempotency key is supplied: resubmitting returns the original job. */
    public CreateResult create(CreateJobRequest request) {
        NewJob newJob = toNewJob(request);
        return repository.insert(newJob)
                .map(job -> new CreateResult(job, true))
                .or(() -> repository.findByIdempotencyKey(newJob.idempotencyKey()).map(job -> new CreateResult(job, false)))
                .orElseThrow(() -> new IllegalStateException("Job was neither inserted nor found"));
    }

    /** Returns the number of jobs inserted; jobs whose idempotency key already exists are skipped. */
    public int createBatch(List<CreateJobRequest> requests) {
        return repository.insertBatch(requests.stream().map(this::toNewJob).toList());
    }

    public Job get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    public List<Job> list(JobStatus status, int limit) {
        return repository.findRecent(status, Math.min(Math.max(limit, 1), 500));
    }

    public List<JobExecution> executions(UUID id) {
        get(id);
        return repository.findExecutions(id);
    }

    public Map<JobStatus, Long> stats() {
        return repository.countByStatus();
    }

    public Job cancel(UUID id) {
        if (!repository.cancel(id)) {
            Job job = get(id);
            throw new IllegalStateException("Cannot cancel job in status " + job.status());
        }
        return get(id);
    }

    public Job retry(UUID id) {
        if (!repository.retry(id)) {
            Job job = get(id);
            throw new IllegalStateException("Only FAILED or CANCELLED jobs can be retried; job is " + job.status());
        }
        return get(id);
    }

    public Set<String> jobTypes() {
        return handlers.types();
    }

    private NewJob toNewJob(CreateJobRequest request) {
        if (handlers.find(request.type()).isEmpty()) {
            throw new IllegalArgumentException("Unknown job type '" + request.type() + "'. Known types: " + handlers.types());
        }
        String cron = blankToNull(request.cron());
        Instant runAt = request.runAt();
        if (cron != null) {
            if (!CronExpression.isValidExpression(cron)) {
                throw new IllegalArgumentException("Invalid cron expression '" + cron
                        + "'. Use 6 fields: second minute hour day-of-month month day-of-week (UTC)");
            }
            if (runAt == null) {
                ZonedDateTime next = CronExpression.parse(cron).next(ZonedDateTime.ofInstant(clock.instant(), ZoneOffset.UTC));
                if (next == null) {
                    throw new IllegalArgumentException("Cron expression never fires: " + cron);
                }
                runAt = next.toInstant();
            }
        }
        return new NewJob(
                UUID.randomUUID(),
                request.type(),
                toJson(request),
                request.priority() == null ? 0 : request.priority(),
                runAt == null ? clock.instant() : runAt,
                cron,
                request.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : request.maxAttempts(),
                request.timeoutSeconds() == null ? DEFAULT_TIMEOUT_SECONDS : request.timeoutSeconds(),
                blankToNull(request.idempotencyKey()));
    }

    private String toJson(CreateJobRequest request) {
        if (request.payload() == null || request.payload().isNull()) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(request.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Payload is not valid JSON", e);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
