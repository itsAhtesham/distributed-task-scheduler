-- Create jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id              VARCHAR(26) PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    type            VARCHAR(20) NOT NULL CHECK (type IN ('cron', 'once', 'delayed')),
    cron_expression VARCHAR(100),
    scheduled_at    TIMESTAMPTZ,
    payload         JSONB NOT NULL DEFAULT '{}',
    handler         VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'paused', 'completed', 'failed', 'cancelled')),
    max_retries     INT NOT NULL DEFAULT 3,
    retry_count     INT NOT NULL DEFAULT 0,
    timeout_ms      INT NOT NULL DEFAULT 30000,
    priority        INT NOT NULL DEFAULT 0,
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    next_run_at     TIMESTAMPTZ,
    last_run_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_next_run ON jobs(next_run_at) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);

-- Create job_runs table
CREATE TABLE IF NOT EXISTS job_runs (
    id              VARCHAR(26) PRIMARY KEY,
    job_id          VARCHAR(26) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id       VARCHAR(50) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'timed_out', 'retrying')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    duration_ms     INT,
    attempt         INT NOT NULL DEFAULT 1,
    error_message   TEXT,
    result          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_worker ON job_runs(worker_id);

-- Create webhooks table
CREATE TABLE IF NOT EXISTS webhooks (
    id              VARCHAR(26) PRIMARY KEY,
    job_id          VARCHAR(26) REFERENCES jobs(id) ON DELETE CASCADE,
    url             VARCHAR(2048) NOT NULL,
    events          VARCHAR(50)[] NOT NULL DEFAULT '{}',
    secret          VARCHAR(255),
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
