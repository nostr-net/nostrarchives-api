use serde::Serialize;

/// A Nostr event row for ClickHouse (aggregation columns only, no content/tags/raw).
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct ChEvent {
    pub id: String,
    pub pubkey: String,
    pub created_at: i64,
    pub kind: i32,
    pub client_name: String, // empty string = no client tag
}

/// Engagement event (reaction, repost, or reply) for ClickHouse.
/// Replaces Postgres counter columns with individual fact rows.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct ChEngagement {
    pub event_id: String,
    pub kind: i16, // 7=reaction, 6/16=repost, 1=reply
    pub target_id: String,
    pub target_pubkey: String, // denormalized for leaderboard queries
    pub source_pubkey: String,
    pub created_at: i64,
}

/// Zap metadata row for ClickHouse.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct ChZapMetadata {
    pub event_id: String,
    pub sender_pubkey: String,
    pub recipient_pubkey: String,
    pub amount_msats: i64,
    pub zapped_event_id: String, // empty string if no target
    pub created_at: i64,
}

/// Note hashtag row for ClickHouse.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct ChNoteHashtag {
    pub event_id: String,
    pub hashtag: String,
    pub created_at: i64,
}

/// Follow edge row for ClickHouse.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct ChFollow {
    pub follower_pubkey: String,
    pub followed_pubkey: String,
    pub created_at: i64,
}

/// Relay list entry for ClickHouse.
#[derive(Debug, Clone, clickhouse::Row, Serialize)]
pub struct ChRelayList {
    pub pubkey: String,
    pub relay_url: String,
    pub created_at: i64,
}
