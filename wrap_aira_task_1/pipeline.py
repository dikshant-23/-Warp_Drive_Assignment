"""
Recommendation Feature Pipeline — Task 2

Computes two feature sets from the events table:

1. customer_features  (one row per customer)
2. product_cooccurrence
"""

import asyncio
import argparse
import logging
from datetime import datetime, timezone

import asyncpg

ADMIN_DSN = "postgresql://aira_admin:secret@127.0.0.1:5432/aira"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

async def get_checkpoint(conn, tenant_id: str, pipeline_name: str) -> datetime | None:
    row = await conn.fetchrow(
        """
        SELECT last_processed_at
          FROM pipeline_checkpoints
         WHERE tenant_id = $1::uuid AND pipeline_name = $2
        """,
        tenant_id, pipeline_name,
    )
    return row["last_processed_at"] if row else None


async def update_checkpoint(conn, tenant_id: str, pipeline_name: str, ts: datetime):
    await conn.execute(
        """
        INSERT INTO pipeline_checkpoints (tenant_id, pipeline_name, last_processed_at)
        VALUES ($1::uuid, $2, $3)
        ON CONFLICT (tenant_id, pipeline_name)
        DO UPDATE SET last_processed_at = EXCLUDED.last_processed_at,
                      updated_at = now()
        """,
        tenant_id, pipeline_name, ts,
    )


# ---------------------------------------------------------------------------
# Customer features pipeline
# ---------------------------------------------------------------------------

async def compute_customer_features(conn, tenant_id: str, since: datetime | None):
    """
    Single CTE chain — all feature groups computed in one SQL statement,
    then bulk-upserted into customer_features.

    Window: 30 days back from now (or from checkpoint for incremental runs,
    but we still look back 30 days for velocity/session accuracy).
    """
    log.info("customer_features: since=%s", since)

    thirty_days_ago = "now() - interval '30 days'"
    if since is None:
        window_filter = f"e.timestamp >= {thirty_days_ago}"
        params = [tenant_id]
    else:
        # Incremental: re-process from 30 days ago or the checkpoint,
        # whichever is earlier, so velocity/session windows stay accurate.
        window_filter = f"e.timestamp >= LEAST($2, {thirty_days_ago})"
        params = [tenant_id, since]

    sql = f"""
        WITH raw_events AS (
            SELECT e.customer_id,
                   e.product_id,
                   e.event_type,
                   e.timestamp,
                   p.category,
                   p.price
              FROM events e
              LEFT JOIN products p ON p.id = e.product_id
             WHERE e.tenant_id = $1::uuid
               AND {window_filter}
        ),

        -- ── Per-customer event aggregates ─────────────────────────────────
        agg AS (
            SELECT customer_id,
                   COUNT(*) FILTER (WHERE event_type = 'page_view')
                       AS total_views,
                   COUNT(*) FILTER (WHERE event_type = 'add_to_cart')
                       AS total_cart_adds,
                   COUNT(*) FILTER (WHERE event_type = 'purchase')
                       AS total_purchases,
                   COUNT(DISTINCT product_id)
                       FILTER (WHERE event_type IN ('page_view', 'product_view', 'purchase')
                                 AND product_id IS NOT NULL)
                       AS distinct_products_viewed,
                   COUNT(DISTINCT category)
                       FILTER (WHERE event_type IN ('page_view', 'product_view', 'purchase')
                                 AND category IS NOT NULL)
                       AS distinct_categories_viewed
              FROM raw_events
             GROUP BY customer_id
        ),

        -- ── Days since first seen / last activity ─────────────────────────
        -- first_seen comes from the customers table; last_activity from events
        activity AS (
            SELECT re.customer_id,
                   EXTRACT(EPOCH FROM (now() - c.first_seen)) / 86400.0
                       AS days_since_first_seen,
                   EXTRACT(EPOCH FROM (now() - MAX(re.timestamp))) / 86400.0
                       AS days_since_last_activity
              FROM raw_events re
              JOIN customers c ON c.id = re.customer_id
                               AND c.tenant_id = $1::uuid
             GROUP BY re.customer_id, c.first_seen
        ),

        -- ── Category affinity (recency-weighted, normalised) ──────────────
        category_events AS (
            SELECT customer_id,
                   category,
                   CASE
                     WHEN timestamp >= now() - interval '7 days' THEN 3.0
                     ELSE 1.0
                   END AS weight
              FROM raw_events
             WHERE category IS NOT NULL
               AND event_type IN ('page_view', 'product_view', 'purchase')
        ),
        affinity_raw AS (
            SELECT customer_id,
                   category,
                   SUM(weight) AS raw_score
              FROM category_events
             GROUP BY customer_id, category
        ),
        affinity_norm AS (
            SELECT customer_id,
                   category,
                   raw_score,
                   SUM(raw_score) OVER (PARTITION BY customer_id) AS total_score
              FROM affinity_raw
        ),
        affinity_final AS (
            SELECT customer_id,
                   jsonb_object_agg(
                       category,
                       ROUND((raw_score / total_score)::numeric, 4)
                   ) AS category_affinity
              FROM affinity_norm
             GROUP BY customer_id
        ),

        -- ── Purchase velocity & revenue ───────────────────────────────────
        purchase_rows AS (
            SELECT customer_id,
                   timestamp,
                   price,
                   LAG(timestamp) OVER (PARTITION BY customer_id ORDER BY timestamp)
                       AS prev_ts
              FROM raw_events
             WHERE event_type = 'purchase'
        ),
        purchase_stats AS (
            SELECT customer_id,
                   COUNT(*)  / 30.0                                      AS purchase_velocity_30d,
                   AVG(EXTRACT(EPOCH FROM (timestamp - prev_ts)) / 86400.0)
                                                                          AS avg_days_between_purchases,
                   SUM(price)                                             AS total_revenue,
                   AVG(price)                                             AS avg_order_value
              FROM purchase_rows
             GROUP BY customer_id
        ),

        -- ── Session features (30-min idle gap = new session) ──────────────
        session_gaps AS (
            SELECT customer_id,
                   product_id,
                   timestamp,
                   LAG(timestamp) OVER (PARTITION BY customer_id ORDER BY timestamp)
                       AS prev_ts
              FROM raw_events
        ),
        session_starts AS (
            SELECT customer_id,
                   product_id,
                   timestamp,
                   SUM(
                       CASE WHEN prev_ts IS NULL
                                 OR timestamp - prev_ts > interval '30 minutes'
                            THEN 1 ELSE 0 END
                   ) OVER (PARTITION BY customer_id ORDER BY timestamp)
                       AS session_id
              FROM session_gaps
        ),
        session_metrics AS (
            SELECT customer_id,
                   session_id,
                   EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60.0
                       AS length_min,
                   COUNT(DISTINCT product_id) FILTER (WHERE product_id IS NOT NULL)
                       AS products_viewed
              FROM session_starts
             GROUP BY customer_id, session_id
        ),
        session_stats AS (
            SELECT customer_id,
                   AVG(length_min)      AS avg_session_length_min,
                   AVG(products_viewed) AS avg_products_per_session
              FROM session_metrics
             GROUP BY customer_id
        ),

        -- ── Combine all signals ───────────────────────────────────────────
        combined AS (
            SELECT af.customer_id,
                   af.category_affinity,
                   COALESCE(a.total_views,               0) AS total_views,
                   COALESCE(a.total_cart_adds,           0) AS total_cart_adds,
                   COALESCE(a.total_purchases,           0) AS total_purchases,
                   COALESCE(a.distinct_products_viewed,  0) AS distinct_products_viewed,
                   COALESCE(a.distinct_categories_viewed,0) AS distinct_categories_viewed,
                   COALESCE(act.days_since_first_seen,   0) AS days_since_first_seen,
                   COALESCE(act.days_since_last_activity,0) AS days_since_last_activity,
                   COALESCE(ps.purchase_velocity_30d,    0) AS purchase_velocity_30d,
                   COALESCE(ps.avg_days_between_purchases, NULL) AS avg_days_between_purchases,
                   COALESCE(ps.total_revenue,            0) AS total_revenue,
                   COALESCE(ps.avg_order_value,          NULL) AS avg_order_value,
                   COALESCE(ss.avg_session_length_min,   0) AS avg_session_length_min,
                   COALESCE(ss.avg_products_per_session, 0) AS avg_products_per_session
              FROM affinity_final af
              LEFT JOIN agg           a   USING (customer_id)
              LEFT JOIN activity      act USING (customer_id)
              LEFT JOIN purchase_stats ps  USING (customer_id)
              LEFT JOIN session_stats  ss  USING (customer_id)
        )

        INSERT INTO customer_features (
            tenant_id, customer_id,
            category_affinity,
            total_views, total_cart_adds, total_purchases,
            distinct_products_viewed, distinct_categories_viewed,
            days_since_first_seen, days_since_last_activity,
            purchase_velocity_30d, avg_days_between_purchases,
            total_revenue, avg_order_value,
            avg_session_length_min, avg_products_per_session,
            computed_at
        )
        SELECT $1::uuid,
               customer_id,
               category_affinity,
               total_views, total_cart_adds, total_purchases,
               distinct_products_viewed, distinct_categories_viewed,
               days_since_first_seen, days_since_last_activity,
               purchase_velocity_30d, avg_days_between_purchases,
               total_revenue, avg_order_value,
               avg_session_length_min, avg_products_per_session,
               now()
          FROM combined
        ON CONFLICT (tenant_id, customer_id)
        DO UPDATE SET
            category_affinity            = EXCLUDED.category_affinity,
            total_views                  = EXCLUDED.total_views,
            total_cart_adds              = EXCLUDED.total_cart_adds,
            total_purchases              = EXCLUDED.total_purchases,
            distinct_products_viewed     = EXCLUDED.distinct_products_viewed,
            distinct_categories_viewed   = EXCLUDED.distinct_categories_viewed,
            days_since_first_seen        = EXCLUDED.days_since_first_seen,
            days_since_last_activity     = EXCLUDED.days_since_last_activity,
            purchase_velocity_30d        = EXCLUDED.purchase_velocity_30d,
            avg_days_between_purchases   = EXCLUDED.avg_days_between_purchases,
            total_revenue                = EXCLUDED.total_revenue,
            avg_order_value              = EXCLUDED.avg_order_value,
            avg_session_length_min       = EXCLUDED.avg_session_length_min,
            avg_products_per_session     = EXCLUDED.avg_products_per_session,
            computed_at                  = now()
    """

    result = await conn.execute(sql, *params)
    rows = int(result.split()[-1])
    log.info("customer_features: upserted %d rows", rows)
    return rows


# ---------------------------------------------------------------------------
# Product co-occurrence pipeline
# ---------------------------------------------------------------------------

async def compute_cooccurrence(conn, tenant_id: str, since: datetime | None):
    """
    Self-join on purchases: find all customer pairs that bought
    two different products within 30 days of each other.

    Stores BOTH (A→B) and (B→A) so top-N lookup is always a forward
    index scan on product_a_id.

    Confidence = co_count / distinct customers who bought A.
    """
    log.info("product_cooccurrence: since=%s", since)

    thirty_days_ago = "now() - interval '30 days'"
    if since is None:
        window_filter = f"timestamp >= {thirty_days_ago}"
        params = [tenant_id]
    else:
        window_filter = f"timestamp >= LEAST($2, {thirty_days_ago})"
        params = [tenant_id, since]

    sql = f"""
        WITH purchases AS (
            SELECT customer_id, product_id, timestamp
              FROM events
             WHERE tenant_id = $1::uuid
               AND event_type = 'purchase'
               AND {window_filter}
        ),

        pairs AS (
            SELECT a.product_id AS product_a_id,
                   b.product_id AS product_b_id,
                   a.customer_id
              FROM purchases a
              JOIN purchases b
                ON a.customer_id = b.customer_id
               AND a.product_id <> b.product_id
               AND ABS(EXTRACT(EPOCH FROM (a.timestamp - b.timestamp))) <= 2592000
        ),

        co_counts AS (
            SELECT product_a_id,
                   product_b_id,
                   COUNT(DISTINCT customer_id) AS co_count
              FROM pairs
             GROUP BY product_a_id, product_b_id
            HAVING COUNT(DISTINCT customer_id) >= 3
        ),

        product_buyers AS (
            SELECT product_id,
                   COUNT(DISTINCT customer_id) AS buyer_count
              FROM purchases
             GROUP BY product_id
        ),

        scored AS (
            SELECT c.product_a_id,
                   c.product_b_id,
                   c.co_count,
                   ROUND((c.co_count::numeric / pb.buyer_count), 4) AS confidence
              FROM co_counts c
              JOIN product_buyers pb ON pb.product_id = c.product_a_id
        )

        INSERT INTO product_cooccurrence
            (tenant_id, product_a_id, product_b_id, co_count, confidence, updated_at)
        SELECT $1::uuid,
               product_a_id, product_b_id, co_count, confidence, now()
          FROM scored
        ON CONFLICT (tenant_id, product_a_id, product_b_id)
        DO UPDATE SET
            co_count   = EXCLUDED.co_count,
            confidence = EXCLUDED.confidence,
            updated_at = now()
    """

    result = await conn.execute(sql, *params)
    rows = int(result.split()[-1])
    log.info("product_cooccurrence: upserted %d rows", rows)
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(tenant_id: str, full_refresh: bool):
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        now = datetime.now(tz=timezone.utc)

        if full_refresh:
            since_features     = None
            since_cooccurrence = None
            log.info("full refresh — ignoring checkpoints")
        else:
            since_features     = await get_checkpoint(conn, tenant_id, "customer_features")
            since_cooccurrence = await get_checkpoint(conn, tenant_id, "product_cooccurrence")

        await compute_customer_features(conn, tenant_id, since_features)
        await compute_cooccurrence(conn, tenant_id, since_cooccurrence)

        await update_checkpoint(conn, tenant_id, "customer_features",     now)
        await update_checkpoint(conn, tenant_id, "product_cooccurrence",  now)

        log.info("pipeline complete for tenant %s", tenant_id)
    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AIRA recommendation feature pipeline")
    parser.add_argument("tenant_id", help="Tenant UUID")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore checkpoints and recompute from scratch")
    args = parser.parse_args()

    asyncio.run(run(args.tenant_id, args.full_refresh))
