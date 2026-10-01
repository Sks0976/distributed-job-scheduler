package com.subham.scheduler.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.subham.scheduler.config.SchedulerProperties;
import com.subham.scheduler.domain.Job;
import com.subham.scheduler.handler.JobContext;
import com.subham.scheduler.handler.JobHandler;
import com.subham.scheduler.handler.JobHandlerRegistry;
import com.subham.scheduler.handler.NonRetryableJobException;
import com.subham.scheduler.repository.JobRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * One worker runs on every node. It has three moving parts:
 * <ul>
 *   <li>a poller thread that claims due jobs, but only as many as there are free execution slots
 *       (a semaphore gives backpressure, so a busy node never hoards work other nodes could run);</li>
 *   <li>a fixed thread pool that runs handlers, with a watchdog that interrupts jobs past their timeout;</li>
 *   <li>a reaper that returns jobs with expired leases (crashed or hung nodes) to the queue.</li>
 * </ul>
 */
@Component
public class JobWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);
    private static final int MAX_ERROR_LENGTH = 2000;
    private static final int REAP_BATCH = 100;

    private final SchedulerProperties properties;
    private final JobRepository repository;
    private final JobHandlerRegistry handlers;
    private final BackoffPolicy backoff;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meters;
    private final Clock clock;
    private final String nodeId;
    private final int threads;
    private final Semaphore permits;

    private volatile boolean running;
    private Thread poller;
    private ExecutorService executor;
    private ScheduledThreadPoolExecutor maintenance;

    private final Counter claimed;
    private final Counter succeeded;
    private final Counter retried;
    private final Counter failed;
    private final Counter timedOut;
    private final Counter reaped;
    private final Counter staleCompletions;

    public JobWorker(SchedulerProperties properties, JobRepository repository, JobHandlerRegistry handlers,
                     BackoffPolicy backoff, ObjectMapper objectMapper, MeterRegistry meters, Clock clock) {
        this.properties = properties;
        this.repository = repository;
        this.handlers = handlers;
        this.backoff = backoff;
        this.objectMapper = objectMapper;
        this.meters = meters;
        this.clock = clock;
        this.nodeId = properties.nodeId();
        this.threads = properties.worker().threads();
        this.permits = new Semaphore(threads);

        this.claimed = meters.counter("scheduler.jobs.claimed");
        this.succeeded = meters.counter("scheduler.jobs.completed", "outcome", "succeeded");
        this.retried = meters.counter("scheduler.jobs.completed", "outcome", "retry_scheduled");
        this.failed = meters.counter("scheduler.jobs.completed", "outcome", "failed");
        this.timedOut = meters.counter("scheduler.jobs.timeouts");
        this.reaped = meters.counter("scheduler.jobs.reaped");
        this.staleCompletions = meters.counter("scheduler.jobs.stale_completions");
        Gauge.builder("scheduler.jobs.active", permits, p -> threads - p.availablePermits()).register(meters);
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (!properties.worker().enabled()) {
            log.info("Worker disabled on node {}; this node serves the API only", nodeId);
            return;
        }
        running = true;
        executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                Thread.ofPlatform().name("job-exec-", 1).factory());
        maintenance = new ScheduledThreadPoolExecutor(2, Thread.ofPlatform().name("job-maint-", 1).factory());
        maintenance.setRemoveOnCancelPolicy(true);

        long reapMillis = properties.lease().reaperInterval().toMillis();
        maintenance.scheduleWithFixedDelay(this::reapExpiredLeases, reapMillis, reapMillis, TimeUnit.MILLISECONDS);

        poller = Thread.ofPlatform().name("job-poller").start(this::pollLoop);
        log.info("Worker started on node {} with {} threads", nodeId, threads);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        log.info("Stopping worker on node {}: no new claims, waiting for running jobs", nodeId);
        running = false;
        poller.interrupt();
        try {
            poller.join(5_000);
            executor.shutdown();
            if (!executor.awaitTermination(properties.worker().shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Jobs still running after shutdown timeout; interrupting them");
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        } finally {
            maintenance.shutdownNow();
            int released = repository.releaseLocksHeldBy(nodeId);
            if (released > 0) {
                log.info("Released {} unfinished job(s) back to the queue", released);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // ---------------------------------------------------------------- polling

    private void pollLoop() {
        long idleMillis = properties.worker().pollInterval().toMillis();
        long graceSeconds = properties.lease().grace().toSeconds();
        int batchSize = properties.worker().batchSize();

        while (running) {
            try {
                int wanted = Math.min(permits.availablePermits(), batchSize);
                if (wanted == 0 || !permits.tryAcquire(wanted)) {
                    Thread.sleep(idleMillis);   // all slots busy: backpressure, leave work for other nodes
                    continue;
                }
                List<Job> jobs;
                try {
                    jobs = repository.claimBatch(nodeId, wanted, graceSeconds);
                } catch (RuntimeException e) {
                    permits.release(wanted);
                    throw e;
                }
                permits.release(wanted - jobs.size());
                claimed.increment(jobs.size());
                jobs.forEach(this::dispatch);
                if (jobs.isEmpty()) {
                    Thread.sleep(idleMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.error("Polling failed; backing off", e);
                sleepQuietly(idleMillis * 4);
            }
        }
    }

    private void dispatch(Job job) {
        Execution execution = new Execution();
        try {
            executor.execute(() -> execute(job, execution));
        } catch (RuntimeException rejected) {
            // Only happens during shutdown; the lock is released in stop().
            permits.release();
            return;
        }
        maintenance.schedule(execution::timeout, job.timeoutSeconds(), TimeUnit.SECONDS);
    }

    // ---------------------------------------------------------------- execution

    private void execute(Job job, Execution execution) {
        execution.start();
        Instant startedAt = clock.instant();
        Timer.Sample sample = Timer.start(meters);
        String outcome = "succeeded";
        String error = null;
        try {
            JobHandler handler = handlers.find(job.type()).orElseThrow(
                    () -> new NonRetryableJobException("No handler registered for type '" + job.type() + "'"));
            handler.execute(new JobContext(job.id(), job.attempts(), objectMapper.readTree(job.payload())));
            execution.finish();
            Thread.interrupted();   // clear a late timeout interrupt before touching the database
            onSuccess(job);
        } catch (Exception e) {
            boolean wasTimeout = execution.finish();
            Thread.interrupted();
            if (wasTimeout) {
                timedOut.increment();
                error = "Timed out after " + job.timeoutSeconds() + "s";
            } else {
                error = describe(e);
            }
            outcome = onFailure(job, error, !(e instanceof NonRetryableJobException));
        } finally {
            execution.finish();
            Thread.interrupted();
            permits.release();
            sample.stop(meters.timer("scheduler.jobs.duration", "type", job.type(), "outcome", outcome));
            recordExecution(job, startedAt, outcome, error);
        }
    }

    private void onSuccess(Job job) {
        boolean updated = job.isRecurring()
                ? repository.rescheduleRecurring(job, nodeId, nextCronRun(job), null)
                : repository.markSucceeded(job, nodeId);
        if (updated) {
            succeeded.increment();
        } else {
            reportStale(job);
        }
    }

    private String onFailure(Job job, String error, boolean retryable) {
        if (retryable && job.attempts() < job.maxAttempts()) {
            Instant retryAt = clock.instant().plus(backoff.delayFor(job.attempts()));
            if (repository.scheduleRetry(job, nodeId, retryAt, error)) {
                retried.increment();
                log.info("Job {} ({}) failed attempt {}/{}; retrying at {}: {}",
                        job.id(), job.type(), job.attempts(), job.maxAttempts(), retryAt, error);
            } else {
                reportStale(job);
            }
            return "retry_scheduled";
        }
        boolean updated = job.isRecurring()
                ? repository.rescheduleRecurring(job, nodeId, nextCronRun(job), error)
                : repository.markFailed(job, nodeId, error);
        if (updated) {
            failed.increment();
            log.warn("Job {} ({}) failed permanently after {} attempt(s): {}",
                    job.id(), job.type(), job.attempts(), error);
        } else {
            reportStale(job);
        }
        return "failed";
    }

    /** The job was cancelled, or our lease expired and another node took it over. Our result is discarded. */
    private void reportStale(Job job) {
        staleCompletions.increment();
        log.warn("Discarding result of job {} attempt {}: no longer owned by node {}", job.id(), job.attempts(), nodeId);
    }

    private Instant nextCronRun(Job job) {
        ZonedDateTime next = CronExpression.parse(job.cronExpression())
                .next(ZonedDateTime.ofInstant(clock.instant(), ZoneOffset.UTC));
        if (next == null) {
            throw new IllegalStateException("Cron expression has no future run: " + job.cronExpression());
        }
        return next.toInstant();
    }

    private void recordExecution(Job job, Instant startedAt, String outcome, String error) {
        try {
            repository.recordExecution(job, nodeId, startedAt, clock.instant(), outcome, error);
        } catch (RuntimeException e) {
            log.warn("Could not record execution history for job {}", job.id(), e);
        }
    }

    private void reapExpiredLeases() {
        try {
            int count = repository.reapExpiredLeases(REAP_BATCH);
            if (count > 0) {
                reaped.increment(count);
                log.warn("Recovered {} job(s) with expired leases", count);
            }
        } catch (RuntimeException e) {
            log.error("Lease reaper failed", e);
        }
    }

    private static String describe(Exception e) {
        String message = e.getClass().getSimpleName() + ": " + e.getMessage();
        return message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Guards the timeout interrupt so the watchdog can never interrupt a pool thread
     * after it has moved on to a different job.
     */
    private static final class Execution {
        private Thread runner;
        private boolean finished;
        private boolean timedOut;

        synchronized void start() {
            runner = Thread.currentThread();
        }

        /** @return true if the job was interrupted because it timed out */
        synchronized boolean finish() {
            finished = true;
            runner = null;
            return timedOut;
        }

        synchronized void timeout() {
            if (!finished && runner != null) {
                timedOut = true;
                runner.interrupt();
            }
        }
    }
}
