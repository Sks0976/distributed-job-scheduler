package com.subham.scheduler.repository;

import com.subham.scheduler.domain.Job;
import com.subham.scheduler.domain.JobExecution;
import com.subham.scheduler.domain.JobStatus;
import com.subham.scheduler.domain.NewJob;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * All state lives in PostgreSQL. Nodes coordinate only through row locks and conditional updates,
 * so there is no leader and no single point of failure apart from the database itself.
 */
@Repository
public class JobRepository {

    private static final RowMapper<Job> JOB_MAPPER = (rs, rowNum) -> new Job(
            rs.getObject("id", UUID.class),
            rs.getString("type"),
            rs.getString("payload"),
            JobStatus.valueOf(rs.getString("status")),
            rs.getInt("priority"),
            instant(rs, "run_at"),
            rs.getString("cron_expression"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            rs.getInt("timeout_seconds"),
            rs.getString("idempotency_key"),
            rs.getString("locked_by"),
            instant(rs, "lease_until"),
            rs.getString("last_error"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"),
            instant(rs, "completed_at"));

    private static final RowMapper<JobExecution> EXECUTION_MAPPER = (rs, rowNum) -> new JobExecution(
            rs.getLong("id"),
            rs.getObject("job_id", UUID.class),
            rs.getInt("attempt"),
            rs.getString("node_id"),
            instant(rs, "started_at"),
            instant(rs, "finished_at"),
            rs.getString("outcome"),
            rs.getString("error"));

    private static final String INSERT_SQL = """
            INSERT INTO jobs (id, type, payload, status, priority, run_at, cron_expression,
                              max_attempts, timeout_seconds, idempotency_key)
            VALUES (:id, :type, CAST(:payload AS jsonb), 'PENDING', :priority, :runAt, :cron,
                    :maxAttempts, :timeoutSeconds, :idempotencyKey)
            ON CONFLICT (idempotency_key) DO NOTHING
            """;

    /**
     * Atomically claims up to :limit due jobs. FOR UPDATE SKIP LOCKED makes concurrent pollers on
     * different nodes skip rows another transaction is already claiming, so each job is handed to
     * exactly one node without blocking. The lease tells other nodes when the claim is abandoned.
     */
    private static final String CLAIM_SQL = """
            UPDATE jobs
               SET status      = 'RUNNING',
                   locked_by   = :nodeId,
                   attempts    = attempts + 1,
                   lease_until = now() + (timeout_seconds + :graceSeconds) * interval '1 second',
                   updated_at  = now()
             WHERE id IN (SELECT id
                            FROM jobs
                           WHERE status = 'PENDING'
                             AND run_at <= now()
                           ORDER BY priority DESC, run_at
                           LIMIT :limit
                             FOR UPDATE SKIP LOCKED)
            RETURNING *
            """;

    /** Fencing: only the node holding the claim for this exact attempt may record its outcome. */
    private static final String FENCE = " WHERE id = :id AND status = 'RUNNING' AND locked_by = :nodeId AND attempts = :attempts";

    private static final String REAP_SQL = """
            UPDATE jobs
               SET status       = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
                   completed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE completed_at END,
                   locked_by    = NULL,
                   lease_until  = NULL,
                   run_at       = now(),
                   last_error   = 'Lease expired: worker node stopped responding or job overran its timeout',
                   updated_at   = now()
             WHERE id IN (SELECT id
                            FROM jobs
                           WHERE status = 'RUNNING'
                             AND lease_until < now()
                           ORDER BY lease_until
                           LIMIT :limit
                             FOR UPDATE SKIP LOCKED)
            RETURNING id
            """;

    private final NamedParameterJdbcTemplate jdbc;

    public JobRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Returns the inserted job, or empty if a job with the same idempotency key already exists. */
    public Optional<Job> insert(NewJob job) {
        List<Job> rows = jdbc.query(INSERT_SQL + " RETURNING *", insertParams(job), JOB_MAPPER);
        return rows.stream().findFirst();
    }

    public int insertBatch(List<NewJob> jobs) {
        SqlParameterSource[] params = jobs.stream().map(JobRepository::insertParams).toArray(SqlParameterSource[]::new);
        int inserted = 0;
        for (int count : jdbc.batchUpdate(INSERT_SQL, params)) {
            // Some drivers report SUCCESS_NO_INFO (-2) for batched statements; treat it as one row.
            inserted += count == -2 ? 1 : count;
        }
        return inserted;
    }

    public List<Job> claimBatch(String nodeId, int limit, long graceSeconds) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("nodeId", nodeId)
                .addValue("limit", limit)
                .addValue("graceSeconds", graceSeconds);
        return jdbc.query(CLAIM_SQL, params, JOB_MAPPER);
    }

    public boolean markSucceeded(Job job, String nodeId) {
        String sql = """
                UPDATE jobs
                   SET status = 'SUCCEEDED', locked_by = NULL, lease_until = NULL, last_error = NULL,
                       completed_at = now(), updated_at = now()
                """ + FENCE;
        return jdbc.update(sql, fenceParams(job, nodeId)) == 1;
    }

    public boolean scheduleRetry(Job job, String nodeId, Instant retryAt, String error) {
        String sql = """
                UPDATE jobs
                   SET status = 'PENDING', locked_by = NULL, lease_until = NULL, run_at = :runAt,
                       last_error = :error, updated_at = now()
                """ + FENCE;
        MapSqlParameterSource params = fenceParams(job, nodeId)
                .addValue("runAt", timestamp(retryAt))
                .addValue("error", error, Types.VARCHAR);
        return jdbc.update(sql, params) == 1;
    }

    /** Recurring jobs go back to PENDING at their next cron slot with a fresh attempt budget. */
    public boolean rescheduleRecurring(Job job, String nodeId, Instant nextRunAt, String error) {
        String sql = """
                UPDATE jobs
                   SET status = 'PENDING', locked_by = NULL, lease_until = NULL, run_at = :runAt,
                       attempts = 0, last_error = :error, completed_at = now(), updated_at = now()
                """ + FENCE;
        MapSqlParameterSource params = fenceParams(job, nodeId)
                .addValue("runAt", timestamp(nextRunAt))
                .addValue("error", error, Types.VARCHAR);
        return jdbc.update(sql, params) == 1;
    }

    public boolean markFailed(Job job, String nodeId, String error) {
        String sql = """
                UPDATE jobs
                   SET status = 'FAILED', locked_by = NULL, lease_until = NULL, last_error = :error,
                       completed_at = now(), updated_at = now()
                """ + FENCE;
        return jdbc.update(sql, fenceParams(job, nodeId).addValue("error", error, Types.VARCHAR)) == 1;
    }

    /** Recovers jobs whose owner crashed or hung. Safe to run on every node concurrently. */
    public int reapExpiredLeases(int limit) {
        return jdbc.queryForList(REAP_SQL, new MapSqlParameterSource("limit", limit), UUID.class).size();
    }

    /** Called on graceful shutdown: hands unfinished work back without consuming an attempt. */
    public int releaseLocksHeldBy(String nodeId) {
        String sql = """
                UPDATE jobs
                   SET status = 'PENDING', locked_by = NULL, lease_until = NULL,
                       attempts = GREATEST(attempts - 1, 0), updated_at = now()
                 WHERE status = 'RUNNING' AND locked_by = :nodeId
                """;
        return jdbc.update(sql, new MapSqlParameterSource("nodeId", nodeId));
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = :id", new MapSqlParameterSource("id", id), JOB_MAPPER)
                .stream().findFirst();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT * FROM jobs WHERE idempotency_key = :key",
                new MapSqlParameterSource("key", key), JOB_MAPPER).stream().findFirst();
    }

    public List<Job> findRecent(JobStatus status, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource("limit", limit);
        if (status == null) {
            return jdbc.query("SELECT * FROM jobs ORDER BY updated_at DESC LIMIT :limit", params, JOB_MAPPER);
        }
        params.addValue("status", status.name());
        return jdbc.query("SELECT * FROM jobs WHERE status = :status ORDER BY updated_at DESC LIMIT :limit",
                params, JOB_MAPPER);
    }

    public Map<JobStatus, Long> countByStatus() {
        Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
        for (JobStatus status : JobStatus.values()) {
            counts.put(status, 0L);
        }
        jdbc.query("SELECT status, count(*) AS total FROM jobs GROUP BY status", rs -> {
            counts.put(JobStatus.valueOf(rs.getString("status")), rs.getLong("total"));
        });
        return counts;
    }

    public boolean cancel(UUID id) {
        String sql = """
                UPDATE jobs
                   SET status = 'CANCELLED', locked_by = NULL, lease_until = NULL,
                       completed_at = now(), updated_at = now()
                 WHERE id = :id AND status IN ('PENDING', 'RUNNING')
                """;
        return jdbc.update(sql, new MapSqlParameterSource("id", id)) == 1;
    }

    public boolean retry(UUID id) {
        String sql = """
                UPDATE jobs
                   SET status = 'PENDING', attempts = 0, run_at = now(), last_error = NULL,
                       completed_at = NULL, updated_at = now()
                 WHERE id = :id AND status IN ('FAILED', 'CANCELLED')
                """;
        return jdbc.update(sql, new MapSqlParameterSource("id", id)) == 1;
    }

    public void recordExecution(Job job, String nodeId, Instant startedAt, Instant finishedAt,
                                String outcome, String error) {
        String sql = """
                INSERT INTO job_executions (job_id, attempt, node_id, started_at, finished_at, outcome, error)
                VALUES (:jobId, :attempt, :nodeId, :startedAt, :finishedAt, :outcome, :error)
                """;
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("jobId", job.id())
                .addValue("attempt", job.attempts())
                .addValue("nodeId", nodeId)
                .addValue("startedAt", timestamp(startedAt))
                .addValue("finishedAt", timestamp(finishedAt))
                .addValue("outcome", outcome)
                .addValue("error", error, Types.VARCHAR);
        jdbc.update(sql, params);
    }

    public List<JobExecution> findExecutions(UUID jobId) {
        return jdbc.query("SELECT * FROM job_executions WHERE job_id = :jobId ORDER BY attempt, id",
                new MapSqlParameterSource("jobId", jobId), EXECUTION_MAPPER);
    }

    private static MapSqlParameterSource insertParams(NewJob job) {
        return new MapSqlParameterSource()
                .addValue("id", job.id())
                .addValue("type", job.type())
                .addValue("payload", job.payloadJson())
                .addValue("priority", job.priority())
                .addValue("runAt", timestamp(job.runAt()))
                .addValue("cron", job.cronExpression(), Types.VARCHAR)
                .addValue("maxAttempts", job.maxAttempts())
                .addValue("timeoutSeconds", job.timeoutSeconds())
                .addValue("idempotencyKey", job.idempotencyKey(), Types.VARCHAR);
    }

    private static MapSqlParameterSource fenceParams(Job job, String nodeId) {
        return new MapSqlParameterSource()
                .addValue("id", job.id())
                .addValue("nodeId", nodeId)
                .addValue("attempts", job.attempts());
    }

    private static OffsetDateTime timestamp(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }
}
