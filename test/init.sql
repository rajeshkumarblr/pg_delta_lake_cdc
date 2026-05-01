-- Initial Schema for HN Ingest
CREATE TABLE IF NOT EXISTS stories (
    id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT,
    score INTEGER,
    by TEXT,
    descendants INTEGER,
    posted_at TIMESTAMP WITH TIME ZONE,
    hn_rank INTEGER,
    iframe_blocked BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS comments (
    id BIGINT PRIMARY KEY,
    story_id BIGINT REFERENCES stories(id),
    parent_id BIGINT,
    text TEXT,
    by TEXT,
    posted_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    created INTEGER,
    karma INTEGER,
    about TEXT,
    submitted INTEGER[],
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS integration_test (
    id SERIAL PRIMARY KEY,
    name TEXT,
    score DOUBLE PRECISION,
    is_active BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Seed Historical Data (will be captured via SNAPSHOT)
INSERT INTO integration_test (name, score, is_active)
SELECT 'Historical_' || i, i * 1.5, true
FROM generate_series(1, 100) s(i);

-- CDC Setup
-- Create publication for all tables
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'hn_stories_pub') THEN
        CREATE PUBLICATION hn_stories_pub FOR ALL TABLES;
    ELSE
        ALTER PUBLICATION hn_stories_pub ADD TABLE stories, comments, users, integration_test;
    END IF;
END $$;

-- Replication slot will be created by cdc-daemon using EXPORT_SNAPSHOT
