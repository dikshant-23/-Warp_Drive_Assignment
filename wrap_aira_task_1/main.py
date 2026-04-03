"""
AIRA Platform API — Task 1: Event Ingestion

POST /api/v1/events/batch
  Accepts up to 1,000 events per request.
  Validates with Pydantic (rejects entire batch on any error).
  Bulk inserts using executemany.
"""

import json
import time
from datetime import datetime
import uuid as uuid_lib
import asyncpg
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import text

from db import get_session

app = FastAPI(title="AIRA Platform")

VALID_EVENT_TYPES = {
    "page_view", "product_view", "add_to_cart",
    "purchase", "click", "dismiss",
}


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class EventIn(BaseModel):
    customer_id: str
    event_type: str
    product_id: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    timestamp: datetime

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        if v not in VALID_EVENT_TYPES:
            raise ValueError(f"'{v}' is not a valid event_type. Must be one of {sorted(VALID_EVENT_TYPES)}")
        return v


class BatchRequest(BaseModel):
    events: List[EventIn]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/v1/events/batch")
async def ingest_events(
    body: BatchRequest,
    x_tenant_id: str = Header(..., description="Tenant UUID"),
):
    """
    Ingest a batch of events (max 1,000 per request).

    Pydantic validates every event before we touch the database.
    If any event is invalid the entire batch is rejected — this keeps
    the client accountable for sending clean data.

    Bulk insert strategy:
    We resolve customer/product external_ids via subqueries inside the
    INSERT. This trades a tiny bit of per-row overhead for the simplicity
    of a single round-trip. At 1,000 rows this is fast enough.
    For extreme throughput you'd switch to COPY protocol via asyncpg.

    Note: CAST() is used instead of :: PostgreSQL cast syntax because
    SQLAlchemy's named parameter parser (:param) conflicts with :: syntax,
    producing malformed queries when used with asyncpg.
    """
    if not body.events:
        raise HTTPException(status_code=400, detail="events list cannot be empty")
    if len(body.events) > 1000:
        raise HTTPException(status_code=400, detail="max 1000 events per batch")

    start = time.perf_counter()

    # Build parameter list for executemany
    params = []
    for e in body.events:
        params.append({
            "tenant_id":    x_tenant_id,
            "customer_ext": e.customer_id,
            "event_type":   e.event_type,
            "product_ext":  e.product_id,
            "properties":   json.dumps(e.properties) if e.properties else None,
            "ts":           e.timestamp,
        })

    # CAST() instead of :: to avoid conflict with SQLAlchemy's :param syntax
    insert_sql = text("""
        INSERT INTO events
            (tenant_id, customer_id, event_type, product_id, properties, timestamp)
        VALUES (
            CAST(:tenant_id AS uuid),
            (SELECT id FROM customers
             WHERE tenant_id = CAST(:tenant_id AS uuid) AND external_id = :customer_ext
             LIMIT 1),
            CAST(:event_type AS event_type_enum),
            (SELECT id FROM products
             WHERE tenant_id = CAST(:tenant_id AS uuid) AND external_id = :product_ext
             LIMIT 1),
            CAST(:properties AS jsonb),
            :ts
        )
    """)

    async with get_session(x_tenant_id) as db:
        await db.execute(insert_sql, params)

    elapsed_ms = (time.perf_counter() - start) * 1000
    return {
        "accepted": len(body.events),
        "processing_time_ms": round(elapsed_ms, 2),
    }

# ---------------------------------------------------------------------------
# Task 2: Recommendation endpoint
# ---------------------------------------------------------------------------



# Direct asyncpg pool for the recommend endpoint
APP_DSN = "postgresql://app_user:appuser_secret@127.0.0.1:5432/aira"
MODEL_VERSION = "v1-cf-20260326"

_pg_pool: asyncpg.Pool = None


async def get_pg_pool() -> asyncpg.Pool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(APP_DSN, min_size=2, max_size=10)
    return _pg_pool


class RecommendRequest(BaseModel):
    customer_id: str              # external_id
    surface: str
    limit: int = 8
    exclude_product_ids: List[str] = []


@app.post("/api/v1/recommend")
async def recommend(
    body: RecommendRequest,
    x_tenant_id: str = Header(..., description="Tenant UUID"),
):
    """
    Blend category affinity (60%) and co-occurrence (40%) signals.
    """
    start = time.perf_counter()

    import redis.asyncio as aioredis
    redis_client = aioredis.from_url("redis://127.0.0.1:6379", decode_responses=True)

    cache_key = f"rec:{x_tenant_id}:{body.customer_id}:{body.surface}:{body.limit}"
    cached = await redis_client.get(cache_key)
    if cached:
        data = json.loads(cached)
        data["latency_ms"] = round((time.perf_counter() - start) * 1000, 2)
        await redis_client.aclose()
        return data

    pool = await get_pg_pool()
    items = []

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Set tenant context for RLS
            await conn.execute(f"SET LOCAL app.current_tenant = '{x_tenant_id}'")

            # Look up customer internal UUID from external_id
            customer = await conn.fetchrow(
                "SELECT id FROM customers WHERE tenant_id = $1 AND external_id = $2",
                x_tenant_id, body.customer_id,
            )

            if customer:
                customer_uuid = customer["id"]

                # --- Signal 1: Category affinity ---
                # Get top 3 categories from the customer's affinity vector,
                # then find active products in those categories.
                affinity_rows = await conn.fetch(
                    """
                    SELECT p.external_id, p.category,
                           (cf.category_affinity ->> p.category)::numeric AS affinity_score
                    FROM customer_features cf
                    CROSS JOIN LATERAL (
                        SELECT key AS top_cat
                        FROM jsonb_each_text(cf.category_affinity)
                        ORDER BY value::numeric DESC
                        LIMIT 3
                    ) top_cats
                    JOIN products p
                      ON p.category = top_cats.top_cat
                     AND p.tenant_id = $1
                     AND p.active = true
                    WHERE cf.tenant_id = $1
                      AND cf.customer_id = $2
                      AND p.external_id != ALL($3::text[])
                    ORDER BY affinity_score DESC
                    LIMIT $4
                    """,
                    x_tenant_id, customer_uuid,
                    body.exclude_product_ids, body.limit * 2,
                )

                # --- Signal 2: Co-occurrence ---
                # Products co-purchased with items recently viewed/bought.
                cooc_rows = await conn.fetch(
                    """
                    SELECT p.external_id, pc.confidence
                    FROM product_cooccurrence pc
                    JOIN products p ON p.id = pc.product_b_id AND p.tenant_id = $1
                    WHERE pc.tenant_id = $1
                      AND pc.product_a_id IN (
                          SELECT product_id FROM events
                          WHERE tenant_id  = $1
                            AND customer_id = $2
                            AND event_type  IN ('product_view', 'purchase')
                            AND product_id IS NOT NULL
                            AND timestamp > now() - INTERVAL '30 days'
                          LIMIT 10
                      )
                      AND p.active = true
                      AND p.external_id != ALL($3::text[])
                    ORDER BY pc.confidence DESC
                    LIMIT $4
                    """,
                    x_tenant_id, customer_uuid,
                    body.exclude_product_ids, body.limit * 2,
                )

                # Blend: affinity 60%, cooccurrence 40%
                scores: dict[str, dict] = {}
                for row in affinity_rows:
                    pid = row["external_id"]
                    scores[pid] = {
                        "score": round(float(row["affinity_score"] or 0) * 0.6, 4),
                        "reason": "category_affinity",
                    }
                for row in cooc_rows:
                    pid = row["external_id"]
                    cooc_score = round(float(row["confidence"] or 0) * 0.4, 4)
                    if pid in scores:
                        # Product in both signals — sum scores, keep better reason
                        scores[pid]["score"] = round(scores[pid]["score"] + cooc_score, 4)
                        scores[pid]["reason"] = "affinity+cooccurrence"
                    else:
                        scores[pid] = {"score": cooc_score, "reason": "cooccurrence"}

                items = [
                    {"product_id": pid, **data}
                    for pid, data in sorted(scores.items(), key=lambda x: -x[1]["score"])
                ][: body.limit]

            # Cold-start fallback: no features or empty result → global popularity
            if not items:
                popular = await conn.fetch(
                    """
                    SELECT p.external_id, COUNT(*) AS cnt
                    FROM events e
                    JOIN products p ON p.id = e.product_id AND p.tenant_id = $1
                    WHERE e.tenant_id = $1
                      AND e.event_type IN ('product_view', 'purchase')
                      AND e.timestamp > now() - INTERVAL '30 days'
                      AND p.active = true
                      AND p.external_id != ALL($2::text[])
                    GROUP BY p.external_id
                    ORDER BY cnt DESC
                    LIMIT $3
                    """,
                    x_tenant_id, body.exclude_product_ids, body.limit,
                )
                items = [
                    {"product_id": r["external_id"],
                     "score": round(r["cnt"] / max(r["cnt"] for r in popular), 4),
                     "reason": "popularity"}
                    for r in popular
                ]

    decision_id = str(uuid_lib.uuid4())
    latency_ms  = round((time.perf_counter() - start) * 1000, 2)

    response = {
        "items":          items,
        "model_version":  MODEL_VERSION,
        "decision_id":    decision_id,
        "latency_ms":     latency_ms,
    }

    # Cache for 5 minutes
    await redis_client.setex(cache_key, 300, json.dumps(response))
    await redis_client.aclose()

    return response