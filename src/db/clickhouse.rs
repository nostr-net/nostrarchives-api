use super::clickhouse_models::*;
use crate::db::models::{
    ClientEntry, ClientUserEntry, DailyAnalyticsRow, DailyStats, MostLikedAuthor,
    MostSharedAuthor, NewUser, RelayLeaderboardEntry, TopPoster, TopZapper, TrendingHashtag,
    TrendingUser,
};
use crate::error::AppError;

/// ClickHouse analytics client for dual-write ingestion and aggregation queries.
///
/// All insert methods are fire-and-forget: errors are logged but never propagated
/// to callers, keeping ClickHouse out of the critical ingestion path.
#[derive(Clone)]
pub struct ClickHouseAnalytics {
    client: clickhouse::Client,
}

// ── Table DDL ────────────────────────────────────────────────────────────

const CREATE_EVENTS: &str = "
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
SETTINGS index_granularity = 8192
";

const CREATE_ENGAGEMENT: &str = "
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
SETTINGS index_granularity = 8192
";

const CREATE_ZAP_METADATA: &str = "
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
SETTINGS index_granularity = 8192
";

const CREATE_NOTE_HASHTAGS: &str = "
CREATE TABLE IF NOT EXISTS note_hashtags (
    event_id   String,
    hashtag    LowCardinality(String),
    created_at Int64,
    created_date Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (hashtag, created_at)
SETTINGS index_granularity = 8192
";

const CREATE_FOLLOWS: &str = "
CREATE TABLE IF NOT EXISTS follows (
    follower_pubkey String,
    followed_pubkey String,
    created_at      Int64,
    created_date    Date MATERIALIZED toDate(toDateTime(created_at))
) ENGINE = ReplacingMergeTree()
ORDER BY (follower_pubkey, followed_pubkey)
SETTINGS index_granularity = 8192
";

const CREATE_RELAY_LISTS: &str = "
CREATE TABLE IF NOT EXISTS relay_lists (
    pubkey    String,
    relay_url LowCardinality(String),
    created_at Int64
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (pubkey, relay_url)
SETTINGS index_granularity = 8192
";

impl ClickHouseAnalytics {
    pub fn new(url: &str) -> Self {
        let client = clickhouse::Client::default().with_url(url);
        Self { client }
    }

    /// Create all ClickHouse tables (idempotent).
    pub async fn init_tables(&self) -> Result<(), clickhouse::error::Error> {
        for ddl in [
            CREATE_EVENTS,
            CREATE_ENGAGEMENT,
            CREATE_ZAP_METADATA,
            CREATE_NOTE_HASHTAGS,
            CREATE_FOLLOWS,
            CREATE_RELAY_LISTS,
        ] {
            self.client.query(ddl).execute().await?;
        }
        Ok(())
    }

    // ── Insert Methods ──────────────────────────────────────────────────

    pub async fn insert_event(&self, row: &ChEvent) {
        if let Err(e) = self.try_insert_event(row).await {
            tracing::warn!(error = %e, "clickhouse: failed to insert event");
        }
    }

    async fn try_insert_event(&self, row: &ChEvent) -> Result<(), clickhouse::error::Error> {
        let mut insert = self.client.insert::<ChEvent>("events").await?;
        insert.write(row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_engagement(&self, row: &ChEngagement) {
        if let Err(e) = self.try_insert_engagement(row).await {
            tracing::warn!(error = %e, "clickhouse: failed to insert engagement");
        }
    }

    async fn try_insert_engagement(
        &self,
        row: &ChEngagement,
    ) -> Result<(), clickhouse::error::Error> {
        let mut insert = self.client.insert::<ChEngagement>("engagement").await?;
        insert.write(row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_zap(&self, row: &ChZapMetadata) {
        if let Err(e) = self.try_insert_zap(row).await {
            tracing::warn!(error = %e, "clickhouse: failed to insert zap");
        }
    }

    async fn try_insert_zap(&self, row: &ChZapMetadata) -> Result<(), clickhouse::error::Error> {
        let mut insert = self.client.insert::<ChZapMetadata>("zap_metadata").await?;
        insert.write(row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_hashtags(&self, event_id: &str, hashtags: &[String], created_at: i64) {
        if let Err(e) = self.try_insert_hashtags(event_id, hashtags, created_at).await {
            tracing::warn!(error = %e, "clickhouse: failed to insert hashtags");
        }
    }

    async fn try_insert_hashtags(
        &self,
        event_id: &str,
        hashtags: &[String],
        created_at: i64,
    ) -> Result<(), clickhouse::error::Error> {
        if hashtags.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<ChNoteHashtag>("note_hashtags").await?;
        for tag in hashtags {
            insert
                .write(&ChNoteHashtag {
                    event_id: event_id.to_string(),
                    hashtag: tag.to_lowercase(),
                    created_at,
                })
                .await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_follows(&self, rows: &[ChFollow]) {
        if let Err(e) = self.try_insert_follows(rows).await {
            tracing::warn!(error = %e, "clickhouse: failed to insert follows");
        }
    }

    async fn try_insert_follows(&self, rows: &[ChFollow]) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<ChFollow>("follows").await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_relay_list(&self, rows: &[ChRelayList]) {
        if let Err(e) = self.try_insert_relay_list(rows).await {
            tracing::warn!(error = %e, "clickhouse: failed to insert relay list");
        }
    }

    async fn try_insert_relay_list(
        &self,
        rows: &[ChRelayList],
    ) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<ChRelayList>("relay_lists").await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    // ── Query Methods (replace Postgres materialized views) ─────────────

    /// Convert a range string to a ClickHouse time filter (epoch seconds).
    fn range_to_since(range: &str) -> Option<i64> {
        let now = chrono::Utc::now().timestamp();
        match range {
            "today" => Some(now - 86_400),
            "7d" => Some(now - 7 * 86_400),
            "30d" => Some(now - 30 * 86_400),
            "1y" => Some(now - 365 * 86_400),
            _ => None, // "all" and any other value
        }
    }

    /// Top notes by a single engagement metric. Returns (event_id, metric_count, reactions, reposts, replies, zap_sats).
    pub async fn top_notes_by_metric(
        &self,
        metric: &str,
        since: Option<i64>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TopNoteRow>, AppError> {
        // Build the metric expression
        let metric_expr = match metric {
            "reaction" => "countIf(kind = 7)",
            "repost" => "countIf(kind IN (6, 16))",
            "reply" => "countIf(kind = 1)",
            "zap" => "0", // zaps come from zap_metadata, handled separately
            _ => "countIf(kind = 7)",
        };

        if metric == "zap" {
            // Zap-based ranking uses the zap_metadata table
            let since_clause = if let Some(s) = since {
                format!("AND created_at >= {s}")
            } else {
                String::new()
            };

            let sql = format!(
                "SELECT zapped_event_id AS event_id,
                        0 AS reactions, 0 AS reposts, 0 AS replies,
                        intDiv(sum(amount_msats), 1000) AS zap_sats,
                        intDiv(sum(amount_msats), 1000) AS metric_count
                 FROM zap_metadata
                 WHERE zapped_event_id != ''
                   {since_clause}
                 GROUP BY zapped_event_id
                 HAVING zap_sats > 0
                 ORDER BY zap_sats DESC
                 LIMIT {limit} OFFSET {offset}"
            );

            let rows: Vec<TopNoteRow> = self.client.query(&sql)
                .fetch_all()
                .await
                .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;
            return Ok(rows);
        }

        // Engagement-based ranking
        let since_clause = if let Some(s) = since {
            format!("AND created_at >= {s}")
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT target_id AS event_id,
                    countIf(kind = 7) AS reactions,
                    countIf(kind IN (6, 16)) AS reposts,
                    countIf(kind = 1) AS replies,
                    0 AS zap_sats,
                    {metric_expr} AS metric_count
             FROM engagement
             WHERE target_id != ''
               {since_clause}
             GROUP BY target_id
             HAVING metric_count > 0
             ORDER BY metric_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        let rows: Vec<TopNoteRow> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;
        Ok(rows)
    }

    /// Trending note IDs with composite scoring (24h window).
    /// Score = zap_sats + reposts*1000 + replies*500 + reactions*100.
    pub async fn trending_note_ids(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TrendingNoteRow>, AppError> {
        let since = chrono::Utc::now().timestamp() - 86400;

        // Two-source query: engagement + zap_metadata, then merge
        let sql = format!(
            "WITH eng AS (
                SELECT target_id AS event_id,
                       countIf(kind = 7) AS reactions,
                       countIf(kind IN (6, 16)) AS reposts,
                       countIf(kind = 1) AS replies
                FROM engagement
                WHERE created_at >= {since}
                  AND target_id != ''
                GROUP BY target_id
            ),
            zaps AS (
                SELECT zapped_event_id AS event_id,
                       intDiv(sum(amount_msats), 1000) AS zap_sats
                FROM zap_metadata
                WHERE created_at >= {since}
                  AND zapped_event_id != ''
                GROUP BY zapped_event_id
            ),
            combined AS (
                SELECT
                    coalesce(eng.event_id, zaps.event_id) AS event_id,
                    coalesce(eng.reactions, 0) AS reactions,
                    coalesce(eng.reposts, 0) AS reposts,
                    coalesce(eng.replies, 0) AS replies,
                    coalesce(zaps.zap_sats, 0) AS zap_sats
                FROM eng
                FULL OUTER JOIN zaps ON eng.event_id = zaps.event_id
            )
            SELECT event_id,
                   reactions,
                   reposts,
                   replies,
                   zap_sats,
                   (zap_sats + reposts * 1000 + replies * 500 + reactions * 100) AS score
            FROM combined
            WHERE (reactions + reposts + replies + zap_sats) > 0
            ORDER BY score DESC
            LIMIT {limit} OFFSET {offset}"
        );

        let rows: Vec<TrendingNoteRow> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;
        Ok(rows)
    }

    /// Trending hashtags in the last 24h.
    pub async fn trending_hashtags(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TrendingHashtag>, AppError> {
        let since = chrono::Utc::now().timestamp() - 86400;

        let sql = format!(
            "SELECT hashtag,
                   uniq(event_id) AS count
            FROM note_hashtags
            WHERE created_at >= {since}
            GROUP BY hashtag
            HAVING count >= 3
            ORDER BY count DESC
            LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            hashtag: String,
            count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| TrendingHashtag {
                hashtag: r.hashtag,
                count: r.count as i64,
            })
            .collect())
    }

    /// Trending users: pubkeys that gained the most new followers in the last 24h.
    pub async fn trending_user_ids(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TrendingUser>, AppError> {
        let since = chrono::Utc::now().timestamp() - 86400;

        let sql = format!(
            "SELECT followed_pubkey AS pubkey,
                   uniq(follower_pubkey) AS new_followers
            FROM follows
            WHERE created_at >= {since}
            GROUP BY followed_pubkey
            ORDER BY new_followers DESC
            LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            new_followers: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| TrendingUser {
                pubkey: r.pubkey,
                new_followers: r.new_followers as i64,
            })
            .collect())
    }

    /// New users: pubkeys whose earliest event is within the last 24h.
    pub async fn new_user_ids(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<NewUser>, AppError> {
        let since = chrono::Utc::now().timestamp() - 86400;

        let sql = format!(
            "SELECT pubkey,
                   min(created_at) AS first_seen,
                   count() AS event_count
            FROM events
            GROUP BY pubkey
            HAVING first_seen >= {since}
            ORDER BY first_seen DESC
            LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            first_seen: i64,
            event_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| NewUser {
                pubkey: r.pubkey,
                first_seen: r.first_seen,
                event_count: r.event_count as i64,
            })
            .collect())
    }

    /// Top posters by note count in the given time range.
    pub async fn top_posters(
        &self,
        range: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TopPoster>, AppError> {
        let since = Self::range_to_since(range);
        let since_clause = if let Some(s) = since {
            format!("AND created_at >= {s}")
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT pubkey, count() AS note_count
             FROM events
             WHERE kind = 1
               {since_clause}
             GROUP BY pubkey
             HAVING note_count > 0
             ORDER BY note_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            note_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| TopPoster {
                pubkey: r.pubkey,
                note_count: r.note_count as i64,
            })
            .collect())
    }

    /// Most liked authors: authors whose notes received the most reactions.
    pub async fn most_liked_authors(
        &self,
        range: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MostLikedAuthor>, AppError> {
        let since = Self::range_to_since(range);
        let since_clause = if let Some(s) = since {
            format!("AND created_at >= {s}")
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT target_pubkey AS pubkey,
                    count() AS like_count
             FROM engagement
             WHERE kind = 7
               AND target_pubkey != ''
               {since_clause}
             GROUP BY target_pubkey
             HAVING like_count > 0
             ORDER BY like_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            like_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| MostLikedAuthor {
                pubkey: r.pubkey,
                like_count: r.like_count as i64,
            })
            .collect())
    }

    /// Most shared authors: authors whose notes received the most reposts.
    pub async fn most_shared_authors(
        &self,
        range: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MostSharedAuthor>, AppError> {
        let since = Self::range_to_since(range);
        let since_clause = if let Some(s) = since {
            format!("AND created_at >= {s}")
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT target_pubkey AS pubkey,
                    count() AS repost_count
             FROM engagement
             WHERE kind IN (6, 16)
               AND target_pubkey != ''
               {since_clause}
             GROUP BY target_pubkey
             HAVING repost_count > 0
             ORDER BY repost_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            repost_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| MostSharedAuthor {
                pubkey: r.pubkey,
                repost_count: r.repost_count as i64,
            })
            .collect())
    }

    /// Top zappers: users ranked by total sats sent or received.
    pub async fn top_zappers(
        &self,
        direction: &str,
        range: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TopZapper>, AppError> {
        let since = Self::range_to_since(range);
        let since_clause = if let Some(s) = since {
            format!("AND created_at >= {s}")
        } else {
            String::new()
        };

        let pubkey_col = if direction == "sent" {
            "sender_pubkey"
        } else {
            "recipient_pubkey"
        };

        let sql = format!(
            "SELECT {pubkey_col} AS pubkey,
                    toInt64(sum(amount_msats)) AS total_msats,
                    toUInt64(count()) AS zap_count
             FROM zap_metadata
             WHERE {pubkey_col} != ''
               {since_clause}
             GROUP BY {pubkey_col}
             ORDER BY total_msats DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            total_msats: i64,
            zap_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| TopZapper {
                pubkey: r.pubkey,
                total_sats: r.total_msats / 1000,
                zap_count: r.zap_count as i64,
            })
            .collect())
    }

    /// Client leaderboard: top clients by note count and user count.
    pub async fn client_leaderboard(
        &self,
        range: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ClientEntry>, AppError> {
        let since = Self::range_to_since(range);
        let since_clause = if let Some(s) = since {
            format!("AND created_at >= {s}")
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT client_name,
                    count() AS note_count,
                    uniq(pubkey) AS user_count
             FROM events
             WHERE kind = 1
               AND client_name IS NOT NULL
               AND client_name != ''
               {since_clause}
             GROUP BY client_name
             HAVING note_count > 0
             ORDER BY note_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            client_name: String,
            note_count: u64,
            user_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| ClientEntry {
                client_name: r.client_name,
                note_count: r.note_count as i64,
                user_count: r.user_count as i64,
            })
            .collect())
    }

    /// Top users for a specific client.
    pub async fn client_users(
        &self,
        client_name: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ClientUserEntry>, AppError> {
        let escaped = client_name.to_lowercase().replace('\'', "''");

        let sql = format!(
            "SELECT pubkey,
                    count() AS note_count,
                    min(created_at) AS first_seen,
                    max(created_at) AS last_seen
             FROM events
             WHERE kind = 1
               AND lower(client_name) = '{escaped}'
             GROUP BY pubkey
             ORDER BY note_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            pubkey: String,
            note_count: u64,
            first_seen: i64,
            last_seen: i64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| ClientUserEntry {
                pubkey: r.pubkey,
                note_count: r.note_count as i64,
                first_seen: r.first_seen,
                last_seen: r.last_seen,
            })
            .collect())
    }

    /// Relay leaderboard: top relays by user count.
    pub async fn relay_leaderboard(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<RelayLeaderboardEntry>, AppError> {
        let sql = format!(
            "SELECT relay_url,
                    uniq(pubkey) AS user_count
             FROM relay_lists FINAL
             GROUP BY relay_url
             ORDER BY user_count DESC
             LIMIT {limit} OFFSET {offset}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct Row {
            relay_url: String,
            user_count: u64,
        }

        let rows: Vec<Row> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| RelayLeaderboardEntry {
                relay_url: r.relay_url,
                user_count: r.user_count as i64,
            })
            .collect())
    }

    /// Daily stats: DAU, total sats, daily posts (last 24h).
    pub async fn daily_stats(&self) -> Result<DailyStats, AppError> {
        let since = chrono::Utc::now().timestamp() - 86400;

        // Events stats
        let sql = format!(
            "SELECT uniq(pubkey) AS dau,
                   countIf(kind = 1) AS posts
            FROM events
            WHERE created_at >= {since}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct EventRow {
            dau: u64,
            posts: u64,
        }

        let event_row: EventRow = self.client.query(&sql)
            .fetch_one()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        // Zap sats
        let zap_sql = format!(
            "SELECT intDiv(sum(amount_msats), 1000) AS total_sats
            FROM zap_metadata
            WHERE created_at >= {since}"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ZapRow {
            total_sats: i64,
        }

        let zap_row: ZapRow = self.client.query(&zap_sql)
            .fetch_one()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        Ok(DailyStats {
            daily_active_users: event_row.dau as i64,
            total_sats_sent: zap_row.total_sats,
            daily_posts: event_row.posts as i64,
        })
    }

    /// Daily analytics: per-day breakdown between two dates.
    pub async fn daily_analytics(
        &self,
        since: chrono::NaiveDate,
        until: chrono::NaiveDate,
    ) -> Result<Vec<DailyAnalyticsRow>, AppError> {
        let since_ts = since.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
        let until_ts = (until + chrono::Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();

        // Events by day
        let sql = format!(
            "SELECT toDate(toDateTime(created_at)) AS date,
                   uniq(pubkey) AS active_users,
                   countIf(kind = 1) AS notes_posted
            FROM events
            WHERE created_at >= {since_ts}
              AND created_at < {until_ts}
            GROUP BY date
            ORDER BY date ASC"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct EventDayRow {
            date: u16, // ClickHouse Date is days since epoch
            active_users: u64,
            notes_posted: u64,
        }

        let event_rows: Vec<EventDayRow> = self.client.query(&sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        // Zaps by day
        let zap_sql = format!(
            "SELECT toDate(toDateTime(created_at)) AS date,
                   intDiv(sum(amount_msats), 1000) AS zaps_sent
            FROM zap_metadata
            WHERE created_at >= {since_ts}
              AND created_at < {until_ts}
            GROUP BY date
            ORDER BY date ASC"
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ZapDayRow {
            date: u16,
            zaps_sent: i64,
        }

        let zap_rows: Vec<ZapDayRow> = self.client.query(&zap_sql)
            .fetch_all()
            .await
            .map_err(|e| AppError::Internal(format!("clickhouse: {e}")))?;

        // Merge event and zap data by date
        let mut zap_map: std::collections::HashMap<u16, i64> = std::collections::HashMap::new();
        for zr in &zap_rows {
            zap_map.insert(zr.date, zr.zaps_sent);
        }

        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let now = chrono::Utc::now();

        Ok(event_rows
            .into_iter()
            .map(|r| {
                let date = epoch + chrono::Duration::days(r.date as i64);
                let zaps = zap_map.get(&r.date).copied().unwrap_or(0);
                DailyAnalyticsRow {
                    date,
                    active_users: r.active_users as i64,
                    zaps_sent: zaps,
                    notes_posted: r.notes_posted as i64,
                    computed_at: now,
                }
            })
            .collect())
    }
}

// ── Result row types for two-phase queries ──────────────────────────────

/// Result from top_notes_by_metric: event ID + engagement counts.
/// The caller fetches full StoredEvent data from Postgres.
#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
pub struct TopNoteRow {
    pub event_id: String,
    pub reactions: i64,
    pub reposts: i64,
    pub replies: i64,
    pub zap_sats: i64,
    pub metric_count: i64,
}

/// Result from trending_note_ids: event ID + composite score.
#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
pub struct TrendingNoteRow {
    pub event_id: String,
    pub reactions: i64,
    pub reposts: i64,
    pub replies: i64,
    pub zap_sats: i64,
    pub score: i64,
}
