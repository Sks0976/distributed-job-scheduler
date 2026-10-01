CREATE TABLE jobs (
    id               UUID PRIMARY KEY,
    type             VARCHAR(100)  NOT NULL,
    payload          JSONB         NOT NULL DEFAULT '{}'::jsonb,
    status           VARCHAR(20)   NOT NULL,
    priority         INT           NOT NULL DEFAULT 0,
    run_at           TIMESTAMPTZ   NOT NULL,
    cron_expression  VARCHAR(100),
    attempts         INT           NOT NULL DEFAULT 0,
    max_attempts     INT           NOT NULL DEFAULT 3,
    timeout_seconds  INT           NOT NULL DEFAULT 60,
    idempotency_key  VARCHAR(200)  UNIQUE,
    locked_by        VARCHAR(100),
    lease_until      TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
);

-- Partial indexes keep the hot paths (claiming due jobs, reaping expired leases) small and fast.
CREATE INDEX idx_jobs_due     ON jobs (priority DESC, run_at) WHERE status = 'PENDING';
CREATE INDEX idx_jobs_leases  ON jobs (lease_until)           WHERE status = 'RUNNING';
CREATE INDEX idx_jobs_status  ON jobs (status, updated_at DESC);

CREATE TABLE job_executions (
    id           BIGSERIAL PRIMARY KEY,
    job_id       UUID         NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt      INT          NOT NULL,
    node_id      VARCHAR(100) NOT NULL,
    started_at   TIMESTAMPTZ  NOT NULL,
    finished_at  TIMESTAMPTZ  NOT NULL,
    outcome      VARCHAR(20)  NOT NULL,
    error        TEXT
);

CREATE INDEX idx_job_executions_job ON job_executions (job_id, attempt);
