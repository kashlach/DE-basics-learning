CREATE TABLE IF NOT EXISTS github_events (
    event_id VARCHAR(30) PRIMARY KEY,
    type VARCHAR(30) NOT NULL,
    repo VARCHAR(200) NOT NULL,
    actor VARCHAR(100),
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS github_events_raw (
    event_id VARCHAR(30) PRIMARY KEY,
    raw_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_type ON github_events(type);
CREATE INDEX IF NOT EXISTS idx_events_repo ON github_events(repo);
CREATE INDEX IF NOT EXISTS idx_raw_event_id ON github_events_raw(event_id);
CREATE INDEX IF NOT EXISTS idx_raw_json ON github_events_raw USING GIN (raw_json);
