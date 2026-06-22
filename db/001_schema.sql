CREATE TABLE events (
    id bigserial PRIMARY KEY,
    external_id text NOT NULL UNIQUE,
    mission text NOT NULL,
    instrument text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE milestones (
    id bigserial PRIMARY KEY,
    event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    external_id text NOT NULL UNIQUE,
    milestone_type text NOT NULL, -- e.g., `notice`, `conesearch`
    milestone_subtype text NOT NULL,  -- e.g., a kafka notice topic, `ztf-fink-query`
    published_at timestamptz NOT NULL,
    subject_time_start timestamptz NOT NULL,
    subject_time_end timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    CHECK (subject_time_end >= subject_time_start)
);

CREATE TABLE artifacts (
    id bigserial PRIMARY KEY,
    milestone_id bigint NOT NULL REFERENCES milestones(id) ON DELETE CASCADE,
    artifact_type text NOT NULL,
    uri text NOT NULL,

    UNIQUE (milestone_id, artifact_type, uri)
);

CREATE INDEX milestones_event_published_at_idx
ON milestones (event_id, published_at);

CREATE INDEX artifacts_milestone_idx
ON artifacts (milestone_id);


CREATE TABLE jobs (
    id bigserial PRIMARY KEY,
    event_id bigint NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    job_type text NOT NULL,
    subject_time_start timestamptz NOT NULL,
    subject_time_end timestamptz NOT NULL,
    scheduled_at timestamptz NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 2,
    worker_id text,
    lease_until timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    result_artifact_id bigint REFERENCES artifacts(id) ON DELETE SET NULL,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),

    CHECK (subject_time_end > subject_time_start),
    CHECK (attempt_count >= 0 AND max_attempts > 0),
    CHECK (status IN ('pending', 'leased', 'succeeded', 'failed', 'dead')),
    UNIQUE (event_id, job_type, subject_time_start, subject_time_end)
);

CREATE INDEX jobs_pending_scheduled_at_idx
ON jobs (scheduled_at)
WHERE status = 'pending';
