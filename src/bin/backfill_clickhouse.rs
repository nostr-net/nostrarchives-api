//! One-time backfill: reads existing data from Postgres and bulk-inserts into ClickHouse.
//!
//! Usage:
//!   cargo run --bin backfill_clickhouse
//!
//! Requires DATABASE_URL and CLICKHOUSE_URL to be set.

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter("backfill_clickhouse=info")
        .init();

    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL must be set");

    let pg = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await?;
    tracing::info!("connected to postgres");

    let ch = clickhouse::Client::default().with_url(&clickhouse_url);
    tracing::info!("connected to clickhouse");

    // ── 1. Events ───────────────────────────────────────────────────
    tracing::info!("backfilling events...");
    let event_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events")
        .fetch_one(&pg)
        .await?;
    tracing::info!(total = event_count.0, "events to backfill");

    let batch_size = 50_000i64;
    let mut offset = 0i64;
    let mut total_inserted = 0u64;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT id, pubkey, created_at, kind, tags
            FROM events
            ORDER BY created_at ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&pg)
        .await?;

        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        let mut insert = ch.insert::<ChEvent>("events").await?;
        for row in &rows {
            let tags: serde_json::Value = row.get("tags");
            let client_name = extract_client_from_tags(&tags);
            insert
                .write(&ChEvent {
                    id: row.get("id"),
                    pubkey: row.get("pubkey"),
                    created_at: row.get("created_at"),
                    kind: row.get("kind"),
                    client_name,
                })
                .await?;
        }
        insert.end().await?;

        total_inserted += count as u64;
        offset += batch_size;
        tracing::info!(
            inserted = total_inserted,
            total = event_count.0,
            "events progress"
        );
    }
    tracing::info!(total = total_inserted, "events backfill complete");

    // ── 2. Engagement (seen_events) ─────────────────────────────────
    tracing::info!("backfilling engagement (seen_events)...");
    let seen_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM seen_events")
        .fetch_one(&pg)
        .await?;
    tracing::info!(total = seen_count.0, "seen_events to backfill");

    offset = 0;
    total_inserted = 0;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT s.event_id, s.kind, s.target_id, s.created_at,
                   COALESCE(e.pubkey, '') AS target_pubkey
            FROM seen_events s
            LEFT JOIN events e ON e.id = s.target_id
            ORDER BY s.created_at ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&pg)
        .await?;

        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        let mut insert = ch.insert::<ChEngagement>("engagement").await?;
        for row in &rows {
            insert
                .write(&ChEngagement {
                    event_id: row.get("event_id"),
                    kind: row.get("kind"),
                    target_id: row.get("target_id"),
                    target_pubkey: row.get("target_pubkey"),
                    source_pubkey: String::new(), // not stored in seen_events
                    created_at: row.get("created_at"),
                })
                .await?;
        }
        insert.end().await?;

        total_inserted += count as u64;
        offset += batch_size;
        if total_inserted % 100_000 == 0 {
            tracing::info!(
                inserted = total_inserted,
                total = seen_count.0,
                "engagement progress"
            );
        }
    }
    tracing::info!(total = total_inserted, "engagement backfill complete");

    // ── 3. Zap metadata ─────────────────────────────────────────────
    tracing::info!("backfilling zap_metadata...");
    let zap_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM zap_metadata")
        .fetch_one(&pg)
        .await?;
    tracing::info!(total = zap_count.0, "zap_metadata to backfill");

    offset = 0;
    total_inserted = 0;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT event_id, COALESCE(sender_pubkey, '') AS sender_pubkey,
                   COALESCE(recipient_pubkey, '') AS recipient_pubkey,
                   amount_msats,
                   COALESCE(zapped_event_id, '') AS zapped_event_id,
                   created_at
            FROM zap_metadata
            ORDER BY created_at ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&pg)
        .await?;

        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        let mut insert = ch.insert::<ChZapMetadata>("zap_metadata").await?;
        for row in &rows {
            insert
                .write(&ChZapMetadata {
                    event_id: row.get("event_id"),
                    sender_pubkey: row.get("sender_pubkey"),
                    recipient_pubkey: row.get("recipient_pubkey"),
                    amount_msats: row.get("amount_msats"),
                    zapped_event_id: row.get("zapped_event_id"),
                    created_at: row.get("created_at"),
                })
                .await?;
        }
        insert.end().await?;

        total_inserted += count as u64;
        offset += batch_size;
        if total_inserted % 100_000 == 0 {
            tracing::info!(
                inserted = total_inserted,
                total = zap_count.0,
                "zap_metadata progress"
            );
        }
    }
    tracing::info!(total = total_inserted, "zap_metadata backfill complete");

    // ── 4. Note hashtags ────────────────────────────────────────────
    tracing::info!("backfilling note_hashtags...");
    let ht_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM note_hashtags")
        .fetch_one(&pg)
        .await?;
    tracing::info!(total = ht_count.0, "note_hashtags to backfill");

    offset = 0;
    total_inserted = 0;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT event_id, hashtag, created_at
            FROM note_hashtags
            ORDER BY created_at ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&pg)
        .await?;

        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        let mut insert = ch.insert::<ChNoteHashtag>("note_hashtags").await?;
        for row in &rows {
            insert
                .write(&ChNoteHashtag {
                    event_id: row.get("event_id"),
                    hashtag: row.get("hashtag"),
                    created_at: row.get("created_at"),
                })
                .await?;
        }
        insert.end().await?;

        total_inserted += count as u64;
        offset += batch_size;
        if total_inserted % 100_000 == 0 {
            tracing::info!(
                inserted = total_inserted,
                total = ht_count.0,
                "note_hashtags progress"
            );
        }
    }
    tracing::info!(total = total_inserted, "note_hashtags backfill complete");

    // ── 5. Follows ──────────────────────────────────────────────────
    tracing::info!("backfilling follows...");
    let follow_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM follows")
        .fetch_one(&pg)
        .await?;
    tracing::info!(total = follow_count.0, "follows to backfill");

    offset = 0;
    total_inserted = 0;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT follower_pubkey, followed_pubkey, created_at
            FROM follows
            ORDER BY created_at ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&pg)
        .await?;

        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        let mut insert = ch.insert::<ChFollow>("follows").await?;
        for row in &rows {
            insert
                .write(&ChFollow {
                    follower_pubkey: row.get("follower_pubkey"),
                    followed_pubkey: row.get("followed_pubkey"),
                    created_at: row.get("created_at"),
                })
                .await?;
        }
        insert.end().await?;

        total_inserted += count as u64;
        offset += batch_size;
        if total_inserted % 100_000 == 0 {
            tracing::info!(
                inserted = total_inserted,
                total = follow_count.0,
                "follows progress"
            );
        }
    }
    tracing::info!(total = total_inserted, "follows backfill complete");

    // ── 6. Relay lists (from kind-10002 events) ─────────────────────
    tracing::info!("backfilling relay_lists...");
    let relay_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM events WHERE kind = 10002")
            .fetch_one(&pg)
            .await?;
    tracing::info!(total = relay_count.0, "kind-10002 events to process");

    offset = 0;
    total_inserted = 0;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT pubkey, tags, created_at
            FROM events
            WHERE kind = 10002
            ORDER BY created_at ASC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&pg)
        .await?;

        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        let mut insert = ch.insert::<ChRelayList>("relay_lists").await?;
        for row in &rows {
            let pubkey: String = row.get("pubkey");
            let created_at: i64 = row.get("created_at");
            let tags: serde_json::Value = row.get("tags");

            if let Some(tags_arr) = tags.as_array() {
                for tag in tags_arr {
                    if let Some(arr) = tag.as_array() {
                        if arr.len() >= 2
                            && arr[0].as_str() == Some("r")
                            && arr[1].as_str().map(|s| !s.is_empty()).unwrap_or(false)
                        {
                            insert
                                .write(&ChRelayList {
                                    pubkey: pubkey.clone(),
                                    relay_url: arr[1].as_str().unwrap().to_string(),
                                    created_at,
                                })
                                .await?;
                        }
                    }
                }
            }
        }
        insert.end().await?;

        total_inserted += count as u64;
        offset += batch_size;
        if total_inserted % 10_000 == 0 {
            tracing::info!(
                inserted = total_inserted,
                total = relay_count.0,
                "relay_lists progress"
            );
        }
    }
    tracing::info!(total = total_inserted, "relay_lists backfill complete");

    tracing::info!("all backfill complete!");
    Ok(())
}

// ── ClickHouse row types (standalone, no dependency on main crate) ──────

#[derive(serde::Serialize, clickhouse::Row)]
struct ChEvent {
    id: String,
    pubkey: String,
    created_at: i64,
    kind: i32,
    client_name: String,
}

#[derive(serde::Serialize, clickhouse::Row)]
struct ChEngagement {
    event_id: String,
    kind: i16,
    target_id: String,
    target_pubkey: String,
    source_pubkey: String,
    created_at: i64,
}

#[derive(serde::Serialize, clickhouse::Row)]
struct ChZapMetadata {
    event_id: String,
    sender_pubkey: String,
    recipient_pubkey: String,
    amount_msats: i64,
    zapped_event_id: String,
    created_at: i64,
}

#[derive(serde::Serialize, clickhouse::Row)]
struct ChNoteHashtag {
    event_id: String,
    hashtag: String,
    created_at: i64,
}

#[derive(serde::Serialize, clickhouse::Row)]
struct ChFollow {
    follower_pubkey: String,
    followed_pubkey: String,
    created_at: i64,
}

#[derive(serde::Serialize, clickhouse::Row)]
struct ChRelayList {
    pubkey: String,
    relay_url: String,
    created_at: i64,
}

fn extract_client_from_tags(tags: &serde_json::Value) -> String {
    if let Some(arr) = tags.as_array() {
        for tag in arr {
            if let Some(tag_arr) = tag.as_array() {
                if tag_arr.len() >= 2
                    && tag_arr[0].as_str() == Some("client")
                    && tag_arr[1].as_str().map(|s| !s.is_empty()).unwrap_or(false)
                {
                    return tag_arr[1].as_str().unwrap().to_string();
                }
            }
        }
    }
    String::new()
}
