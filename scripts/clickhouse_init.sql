-- ClickHouse table definitions for nostrarchives-api analytics.
-- These tables are also created programmatically via ClickHouseAnalytics::init_tables().
-- This file is provided for reference and manual setup.

-- Core events (aggregation columns only, no content/tags/raw)
CREATE TABLE IF NOT EXISTS events (
    id          String,
    pubkey      String,
    created_at  Int64,
    kind        Int32,
    client_name LowCardinality(String),
    created_date Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (kind, created_at, pubkey, id)
SETTINGS index_granularity = 8192;

-- Engagement events: reactions (kind=7), reposts (kind=6/16), replies (kind=1)
-- Replaces Postgres counter columns with individual fact rows for aggregation.
CREATE TABLE IF NOT EXISTS engagement (
    event_id      String,
    kind          Int16,
    target_id     String,
    target_pubkey String,
    source_pubkey String,
    created_at    Int64,
    created_date  Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (target_id, kind, created_at)
SETTINGS index_granularity = 8192;

-- Zap metadata (parsed from kind-9735 zap receipts)
CREATE TABLE IF NOT EXISTS zap_metadata (
    event_id         String,
    sender_pubkey    String,
    recipient_pubkey String,
    amount_msats     Int64,
    zapped_event_id  String,
    created_at       Int64,
    created_date     Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_at, sender_pubkey)
SETTINGS index_granularity = 8192;

-- Note hashtags (extracted from "t" tags on kind-1 events)
CREATE TABLE IF NOT EXISTS note_hashtags (
    event_id   String,
    hashtag    LowCardinality(String),
    created_at Int64,
    created_date Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (hashtag, created_at)
SETTINGS index_granularity = 8192;

-- Social graph follows (from kind-3 contact list events)
CREATE TABLE IF NOT EXISTS follows (
    follower_pubkey String,
    followed_pubkey String,
    created_at      Int64,
    created_date    Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = ReplacingMergeTree()
ORDER BY (follower_pubkey, followed_pubkey)
SETTINGS index_granularity = 8192;

-- Relay lists (from kind-10002 NIP-65 events)
CREATE TABLE IF NOT EXISTS relay_lists (
    pubkey    String,
    relay_url LowCardinality(String),
    created_at Int64
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (pubkey, relay_url)
SETTINGS index_granularity = 8192;
