CREATE TABLE events (
    id bigserial PRIMARY KEY,
    -- mission-qualified for disambiguation only;
    -- use notices.mission for mission metadata.
    external_id text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notices (
    id bigserial PRIMARY KEY,
    event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    ivorn text NOT NULL UNIQUE,
    topic text NOT NULL,
    mission text NOT NULL,
    instrument text NOT NULL,
    published_at timestamptz NOT NULL,
    burst_datetime timestamptz NOT NULL,
    ra double precision,
    dec double precision,
    err_radius double precision,
    raw_uri text NOT NULL,
    is_retraction boolean NOT NULL DEFAULT false,
    -- retraction notice that locally invalidated this notice; app code ensures is_retraction = true.
    -- ON DELETE SET NULL makes deleting the retraction undo local retraction state.
    retracted_by bigint REFERENCES notices(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    -- all or nothing completeness
    CHECK (num_nonnulls(ra, dec, err_radius) IN (0, 3)),
    CHECK (ra IS NULL OR (ra >= 0 AND ra <= 360)),
    CHECK (dec IS NULL OR (dec >= -90 AND dec <= 90)),
    CHECK (err_radius IS NULL OR err_radius > 0),
    -- a notice cannot retract itself.
    CHECK (retracted_by IS NULL OR retracted_by <> id),
    -- retraction notices cannot be retracted until real examples require it.
    CHECK (NOT is_retraction OR retracted_by IS NULL)
);

CREATE TABLE jobs (
    id bigserial PRIMARY KEY,
    event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    job_type text NOT NULL,
    subject_time_start timestamptz NOT NULL,
    subject_time_end timestamptz NOT NULL,
    scheduled_at timestamptz NOT NULL,
    -- mutable, and intended to be edited with retries
    run_after timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    worker_id text,
    lease_until timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),

    CHECK (subject_time_end > subject_time_start),
    CHECK (attempt_count >= 0 AND max_attempts > 0),
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'dead')),
    UNIQUE (event_id, job_type, subject_time_start, subject_time_end)
);

CREATE TABLE conesearches (
    id bigserial PRIMARY KEY,
    event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    job_id bigint NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
    broker text NOT NULL,
    survey text NOT NULL,
    subject_time_start timestamptz NOT NULL,
    subject_time_end timestamptz NOT NULL,
    queried_at timestamptz NOT NULL,
    ra double precision NOT NULL,
    dec double precision NOT NULL,
    radius_arcsec double precision NOT NULL,
    alert_count integer NOT NULL,
    result_uri text,
    created_at timestamptz NOT NULL DEFAULT now(),

    CHECK (subject_time_end > subject_time_start),
    CHECK (ra >= 0 AND ra <= 360),
    CHECK (dec >= -90 AND dec <= 90),
    CHECK (radius_arcsec > 0),
    CHECK (alert_count >= 0),
    CHECK ((alert_count = 0 AND result_uri IS NULL) OR (alert_count > 0 AND result_uri IS NOT NULL))
);

CREATE INDEX notices_event_published_at_idx
ON notices (event_id, published_at);

CREATE INDEX conesearches_event_queried_at_idx
ON conesearches (event_id, queried_at);

CREATE INDEX jobs_runnable_run_after_idx
ON jobs (run_after)
WHERE status IN ('pending', 'failed');
