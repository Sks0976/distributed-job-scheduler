package com.subham.scheduler.integration;

import com.subham.scheduler.domain.Job;
import com.subham.scheduler.domain.NewJob;
import com.subham.scheduler.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulates many nodes polling the same table at once and proves no job is handed out twice.
 */
@SpringBootTest(properties = "scheduler.worker.enabled=false")
class JobClaimConcurrencyTest extends PostgresTestBase {

    private static final int JOBS = 2_000;
    private static final int POLLERS = 12;

    @Autowired
    private JobRepository repository;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void seed() {
        jdbc.update("DELETE FROM jobs");
        List<NewJob> jobs = new ArrayList<>();
        for (int i = 0; i < JOBS; i++) {
            jobs.add(new NewJob(UUID.randomUUID(), "sleep", "{}", i % 5, Instant.now().minusSeconds(1),
                    null, 3, 30, null));
        }
        repository.insertBatch(jobs);
    }

    @Test
    void everyJobIsClaimedExactlyOnceAcrossConcurrentPollers() throws Exception {
        Set<UUID> claimedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger totalClaims = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(POLLERS);

        List<Callable<Void>> pollers = new ArrayList<>();
        for (int p = 0; p < POLLERS; p++) {
            String node = "node-" + p;
            pollers.add(() -> {
                List<Job> batch;
                do {
                    batch = repository.claimBatch(node, 25, 30);
                    batch.forEach(job -> claimedIds.add(job.id()));
                    totalClaims.addAndGet(batch.size());
                } while (!batch.isEmpty());
                return null;
            });
        }
        for (Future<Void> f : pool.invokeAll(pollers)) {
            f.get();
        }
        pool.shutdown();

        assertThat(totalClaims.get()).isEqualTo(JOBS);
        assertThat(claimedIds).hasSize(JOBS);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE status = 'RUNNING' AND attempts = 1",
                Integer.class)).isEqualTo(JOBS);
    }

    @Test
    void expiredLeasesAreReturnedToTheQueue() {
        List<Job> claimed = repository.claimBatch("crashed-node", 10, 30);
        jdbc.update("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE locked_by = 'crashed-node'");

        int reaped = repository.reapExpiredLeases(100);

        assertThat(reaped).isEqualTo(claimed.size());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE locked_by = 'crashed-node'", Integer.class))
                .isZero();
    }

    @Test
    void staleNodeCannotOverwriteResultAfterLosingItsLease() {
        Job original = repository.claimBatch("slow-node", 1, 30).get(0);
        jdbc.update("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = ?", original.id());
        repository.reapExpiredLeases(100);
        jdbc.update("UPDATE jobs SET priority = 1000 WHERE id = ?", original.id()); // make it the next one claimed
        Job takeover = repository.claimBatch("healthy-node", 1, 30).get(0);

        assertThat(takeover.id()).isEqualTo(original.id());
        assertThat(repository.markSucceeded(original, "slow-node")).isFalse();
        assertThat(repository.markSucceeded(takeover, "healthy-node")).isTrue();
    }
}
