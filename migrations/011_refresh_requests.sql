CREATE TABLE IF NOT EXISTS refresh_requests (
    id BIGSERIAL PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_type TEXT NOT NULL,
    source_view TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    requested_by TEXT NOT NULL,
    note TEXT,
    handled_at TIMESTAMPTZ,
    handler TEXT,
    result_message TEXT,
    ingestion_run_id TEXT,
    CONSTRAINT refresh_requests_status_check CHECK (
        status IN ('pending', 'accepted', 'running', 'completed', 'failed', 'dismissed')
    )
);

CREATE INDEX IF NOT EXISTS refresh_requests_pending_idx
    ON refresh_requests (status, requested_at);

CREATE INDEX IF NOT EXISTS refresh_requests_dedupe_idx
    ON refresh_requests (request_type, requested_by, requested_at DESC)
    WHERE status IN ('pending', 'accepted', 'running');
