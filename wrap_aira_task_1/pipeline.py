"""
Customer feature computation pipeline.

Reads from the events table, computes behavioural features,
and upserts into customer_features.
"""

import asyncio
import sys
from datetime import datetime, timezone

import asyncpg

DSN = "postgresql://aira_admin:secret@127.0.0.1:5432/aira"
PIPELINE_NAME = "customer_features_v1"


async def get_tenants(conn):
    return await conn.fetch("SELECT id, slug FROM tenants ORDER BY created_at")


async def get_checkpoint(conn, tenant_id: str) -> datetime:
    row = await conn.fetchrow(
        """
        SELECT last_processed_at FROM pipeline_checkpoints
        WHERE tenant_id = $1 AND pipeline_name = $2
        """,
        tenant_id, PIPELINE_NAME,
    )
    return row["last_processed_at"] if row else datetime(2000, 1, 1, tzinfo=timezone.utc)


async def update_checkpoint(conn, tenant_id: str, ts: datetime):
    await conn.execute(
        """
        INSERT INTO pipeline_checkpoints (tenant_id, pipeline_name, last_processed_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (tenant_id, pipeline_name)
        DO UPDATE SET last_processed_at = EXCLUDED.last_processed_at,
                      updated_at        = now()
        """,
        tenant_id, PIPELINE_NAME, ts,
    )


async def compute_customer_features(conn, tenant_id: str, since: datetime) -> int:
    """
    Compute all features for customers active since the checkpoint.

    Uses a single CTE chain — one round trip to the DB per tenant.
    The CTEs build on each other: events → counts → affinity → velocity → sessions.
    """
    # Find customers with activity since last checkpoint (scopes the work)
    active = await conn.fetch(
        "SELECT DISTINCT customer_id FROM events WHERE tenant_id = $1 AND timestamp > $2",
        tenant_id, since,
    )
    if not active:
        return 0

    active_ids = [r["customer_id"] for r in active]

    result = await conn.execute(
        """
        WITH
        -- All events for active customers (full history, not just since checkpoint)
        base AS (
            SELECT
                e.customer_id,
                e.event_type,
                e.product_id,
                e.timestamp,
                p.category,
                p.price
            FROM events e
            LEFT JOIN products p
                   ON p.id = e.product_id AND p.tenant_id = e.tenant_id
            WHERE e.tenant_id = $1
              AND e.customer_id = ANY($2)
        ),

        -- Core behavioural counts
        behavioral AS (
            SELECT
                customer_id,
                COUNT(*)          FILTER (WHERE event_type = 'page_view')   AS total_views,
                COUNT(*)          FILTER (WHERE event_type = 'add_to_cart') AS total_cart_adds,
                COUNT(*)          FILTER (WHERE event_type = 'purchase')    AS total_purchases,
                COUNT(DISTINCT product_id)                                  AS distinct_products_viewed,
                COUNT(DISTINCT category)                                    AS distinct_categories_viewed,
                SUM(price)        FILTER (WHERE event_type = 'purchase')    AS total_revenue,
                MIN(timestamp)                                              AS first_event_at,
                MAX(timestamp)                                              AS last_event_at
            FROM base
            GROUP BY customer_id
        ),

        -- Category affinity: views in last 7d = 3x weight, last 30d = 1x weight.
        category_raw AS (
            SELECT
                customer_id,
                category,
                SUM(CASE
                    WHEN timestamp > now() - INTERVAL '7 days'  THEN 3
                    WHEN timestamp > now() - INTERVAL '30 days' THEN 1
                    ELSE 0
                END) AS weighted_views
            FROM base
            WHERE event_type IN ('page_view', 'product_view')
              AND category IS NOT NULL
            GROUP BY customer_id, category
        ),
        category_totals AS (
            SELECT customer_id, SUM(weighted_views) AS total_weight
            FROM category_raw GROUP BY customer_id
        ),
        category_affinity AS (
            SELECT
                r.customer_id,
                jsonb_object_agg(
                    r.category,
                    ROUND((r.weighted_views::NUMERIC / NULLIF(t.total_weight, 0))::NUMERIC, 4)
                ) AS affinity_vector
            FROM category_raw r
            JOIN category_totals t USING (customer_id)
            WHERE r.weighted_views > 0
            GROUP BY r.customer_id
        ),

        -- Purchase velocity: gap between consecutive purchases using LAG
        purchase_events AS (
            SELECT
                customer_id,
                timestamp,
                price,
                LAG(timestamp) OVER (PARTITION BY customer_id ORDER BY timestamp) AS prev_purchase
            FROM base
            WHERE event_type = 'purchase'
        ),
        purchase_velocity AS (
            SELECT
                customer_id,
                AVG(EXTRACT(EPOCH FROM (timestamp - prev_purchase)) / 86400) AS avg_days_between_purchases,
                AVG(price) AS avg_order_value
            FROM purchase_events
            WHERE prev_purchase IS NOT NULL
            GROUP BY customer_id
        ),

        -- Session features: events within 30-min gaps = same session
        session_boundaries AS (
            SELECT
                customer_id,
                timestamp,
                product_id,
                CASE
                    WHEN timestamp - LAG(timestamp) OVER (PARTITION BY customer_id ORDER BY timestamp)
                         > INTERVAL '30 minutes'
                    THEN 1 ELSE 0
                END AS is_new_session
            FROM base
        ),
        session_ids AS (
            SELECT
                customer_id,
                timestamp,
                product_id,
                SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY timestamp) AS session_num
            FROM session_boundaries
        ),
        per_session AS (
            SELECT
                customer_id,
                session_num,
                EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60 AS duration_mins,
                COUNT(DISTINCT product_id) AS products_viewed
            FROM session_ids
            GROUP BY customer_id, session_num
        ),
        session_agg AS (
            SELECT
                customer_id,
                AVG(duration_mins)    AS avg_session_duration_mins,
                AVG(products_viewed)  AS avg_products_per_session
            FROM per_session
            WHERE products_viewed > 1  -- single-product sessions have no useful signal
            GROUP BY customer_id
        )

        INSERT INTO customer_features (
            tenant_id, customer_id,
            total_views, total_cart_adds, total_purchases,
            distinct_products_viewed, distinct_categories_viewed,
            days_since_first_seen, days_since_last_activity,
            category_affinity,
            avg_days_between_purchases, total_revenue, avg_order_value,
            avg_session_duration_mins, avg_products_per_session,
            computed_at
        )
        SELECT
            $1::uuid,
            b.customer_id,
            b.total_views,
            b.total_cart_adds,
            b.total_purchases,
            b.distinct_products_viewed,
            b.distinct_categories_viewed,
            EXTRACT(EPOCH FROM (now() - b.first_event_at)) / 86400,
            EXTRACT(EPOCH FROM (now() - b.last_event_at))  / 86400,
            COALESCE(ca.affinity_vector, '{}'::jsonb),
            pv.avg_days_between_purchases,
            COALESCE(b.total_revenue, 0),
            pv.avg_order_value,
            sa.avg_session_duration_mins,
            sa.avg_products_per_session,
            now()
        FROM behavioral b
        LEFT JOIN category_affinity ca USING (customer_id)
        LEFT JOIN purchase_velocity  pv USING (customer_id)
        LEFT JOIN session_agg        sa USING (customer_id)
        ON CONFLICT ON CONSTRAINT uq_customer_features_tenant_customer
        DO UPDATE SET
            total_views                 = EXCLUDED.total_views,
            total_cart_adds             = EXCLUDED.total_cart_adds,
            total_purchases             = EXCLUDED.total_purchases,
            distinct_products_viewed    = EXCLUDED.distinct_products_viewed,
            distinct_categories_viewed  = EXCLUDED.distinct_categories_viewed,
            days_since_first_seen       = EXCLUDED.days_since_first_seen,
            days_since_last_activity    = EXCLUDED.days_since_last_activity,
            category_affinity           = EXCLUDED.category_affinity,
            avg_days_between_purchases  = EXCLUDED.avg_days_between_purchases,
            total_revenue               = EXCLUDED.total_revenue,
            avg_order_value             = EXCLUDED.avg_order_value,
            avg_session_duration_mins   = EXCLUDED.avg_session_duration_mins,
            avg_products_per_session    = EXCLUDED.avg_products_per_session,
            computed_at                 = EXCLUDED.computed_at
        """,
        tenant_id, active_ids,
    )
    return int(result.split()[-1])


async def run_pipeline(tenant_id: str = None, full_refresh: bool = False):
    conn = await asyncpg.connect(DSN)
    try:
        tenants = [{"id": tenant_id}] if tenant_id else await get_tenants(conn)

        for tenant in tenants:
            tid = str(tenant["id"])
            print(f"\nTenant {tid}")

            since = (
                datetime(2000, 1, 1, tzinfo=timezone.utc)
                if full_refresh
                else await get_checkpoint(conn, tid)
            )
            print(f"  checkpoint: {since.isoformat()}")

            run_started = datetime.now(timezone.utc)
            n = await compute_customer_features(conn, tid, since)
            print(f"  customer_features: {n} rows upserted")

            await update_checkpoint(conn, tid, run_started)
            print(f"  checkpoint updated")
    finally:
        await conn.close()


if __name__ == "__main__":
    tid = None
    full_refresh = "--full-refresh" in sys.argv
    for arg in sys.argv[1:]:
        if not arg.startswith("--"):
            tid = arg
    asyncio.run(run_pipeline(tid, full_refresh))