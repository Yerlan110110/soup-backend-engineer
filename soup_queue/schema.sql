CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK(state IN ('queued','running','succeeded','failed','cancelled')),
    generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    owner TEXT,
    lease_until_ms INTEGER,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    finished_ms INTEGER,
    error TEXT,
    CHECK ((state = 'running' AND owner IS NOT NULL AND lease_until_ms IS NOT NULL)
        OR (state != 'running' AND owner IS NULL AND lease_until_ms IS NULL)),
    CHECK ((state IN ('succeeded','failed','cancelled') AND finished_ms IS NOT NULL)
        OR (state IN ('queued','running') AND finished_ms IS NULL)),
    CHECK ((state = 'failed' AND error IS NOT NULL) OR (state != 'failed' AND error IS NULL))
);
CREATE INDEX IF NOT EXISTS jobs_queued ON jobs(id) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS jobs_expired ON jobs(lease_until_ms, id) WHERE state = 'running';
CREATE TABLE IF NOT EXISTS attempts (
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    generation INTEGER NOT NULL,
    owner TEXT NOT NULL,
    claimed_ms INTEGER NOT NULL,
    PRIMARY KEY(job_id, generation)
);
CREATE TABLE IF NOT EXISTS effects (
    job_id INTEGER PRIMARY KEY REFERENCES jobs(id),
    generation INTEGER NOT NULL,
    owner TEXT NOT NULL,
    value TEXT NOT NULL,
    committed_ms INTEGER NOT NULL,
    FOREIGN KEY(job_id, generation) REFERENCES attempts(job_id, generation)
);
PRAGMA user_version = 1;
