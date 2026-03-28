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

-- CDC Setup
-- Create publication for all tables (or just stories for now as per daemon config)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'hn_stories_pub') THEN
        CREATE PUBLICATION hn_stories_pub FOR TABLE stories, comments;
    END IF;
END $$;

-- Create replication slot (Logical)
-- Note: In PG16, we can use pg_create_logical_replication_slot
SELECT pg_create_logical_replication_slot('hn_stories_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'hn_stories_slot'
);
