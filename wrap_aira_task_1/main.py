"""
AIRA Platform API

Task 1:  POST /api/v1/events/batch
  Accepts up to 1,000 events per request.
  Validates with Pydantic (rejects entire batch on any error).
  Bulk inserts using executemany.

Task 2:  POST /api/v1/recommend
  Returns top-N product recommendations for a customer.
  Blends category affinity (60%) with co-occurrence confidence (40%).
  Falls back to global popularity when no features exist (cold start).
  Caches results in Redis for 5 minutes.
  Logs every decision to recommendation_decisions asynchronously.
"""

import json
import time
import uuid
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import asyncpg
import redis.asyncio as redis_lib
from fastapi import FastAPI, Header, HTTPException, BackgroundTasks
from pydantic import BaseModel, field_validator
from sqlalchemy import text

from db import get_session

app = FastAPI(title="AIRA Platform")

log = logging.getLogger(__name__)

VALID_EVENT_TYPES = {
    "page_view", "product_view", "add_to_cart",
    "purchase", "click", "dismiss",
}

# DSNs — match docker-compose / db.py
ADMIN_DSN = "postgresql://aira_admin:secret@127.0.0.1:5432/aira"
REDIS_URL = "redis://127.0.0.1:6379/0"

# Shared asyncpg pool (initialised on first use)
_pg_pool: asyncpg.Pool | None = None
_redis: redis_lib.Redis | None = None


async def get_pg_pool() -> asyncpg.Pool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(ADMIN_DSN, min_size=2, max_size=10)
    return _pg_pool


async def get_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.from_url(REDIS_URL, decode_responses=True)
    return _redis


# ---------------------------------------------------------------------------
# Task 1 schemas
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
            raise ValueError(
                f"'{v}' is not a valid event_type. "
                f"Must be one of {sorted(VALID_EVENT_TYPES)}"
            )
        return v


class BatchRequest(BaseModel):
    events: List[EventIn]


# ---------------------------------------------------------------------------
# Task 2 schemas
# ---------------------------------------------------------------------------

class RecommendRequest(BaseModel):
    customer_id: str
    surface: str = "homepage"               # e.g. homepage_carousel, email, sms
    limit: int = 10
    exclude_product_ids: Optional[List[str]] = None
    context_product_id: Optional[str] = None   # e.g. current product page

MODEL_VERSION = "v1-cf-20260326"


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
    If any event is invalid the entire batch is rejected.

    Bulk insert strategy:
    We resolve customer/product external_ids via subqueries inside the INSERT.
    For extreme throughput you'd switch to COPY protocol via asyncpg.
    """
    if not body.events:
        raise HTTPException(status_code=400, detail="events list cannot be empty")
    if len(body.events) > 1000:
        raise HTTPException(status_code=400, detail="max 1000 events per batch")

    start = time.perf_counter()

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

    insert_sql = text("""
        INSERT INTO events
            (tenant_id, customer_id, event_type, product_id, properties, timestamp)
        VALUES (
            CAST(:tenant_id AS uuid),
            (SELECT id FROM customers
             WHERE tenant_id = CAST(:tenant_id AS uuid)
               AND external_id = :customer_ext
             LIMIT 1),
            CAST(:event_type AS event_type_enum),
            (SELECT id FROM products
             WHERE tenant_id = CAST(:tenant_id AS uuid)
               AND external_id = :product_ext
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


@app.post("/api/v1/recommend")
async def recommend(
    body: RecommendRequest,
    background_tasks: BackgroundTasks,
    x_tenant_id: str = Header(..., description="Tenant UUID"),
):
    """
    Return top-N product recommendations for a customer.

    Strategy (blended):
      60% weight  — category affinity from customer_features
      40% weight  — co-occurrence confidence from product_cooccurrence

    Cold-start (no features for this customer):
      Falls back to global purchase popularity over the last 30 days.

    Caching:
      Results cached in Redis for 5 min. Cache key includes tenant,
      customer, limit, and the exclude list so different calls don't
      collide.

    Logging:
      Every decision is written to recommendation_decisions in the
      background so it doesn't add latency.
    """
    start = time.perf_counter()
    pool = await get_pg_pool()
    r    = await get_redis()
    limit = min(body.limit, 50)   # cap at 50

    # Build cache key — surface is included so email vs homepage don't collide
    exclude_key = ",".join(sorted(body.exclude_product_ids or []))
    cache_key = f"rec:{x_tenant_id}:{body.customer_id}:{body.surface}:{limit}:{exclude_key}"

    cached = await r.get(cache_key)
    if cached:
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        return {
            "items":         json.loads(cached),
            "model_version": MODEL_VERSION,
            "decision_id":   str(uuid.uuid4()),
            "latency_ms":    latency_ms,
            "cached":        True,
        }

    exclude_ids = body.exclude_product_ids or []

    async with pool.acquire() as conn:
        # ── Step 1: fetch customer features ──────────────────────────────
        features_row = await conn.fetchrow(
            """
            SELECT cf.category_affinity
              FROM customer_features cf
              JOIN customers c ON c.id = cf.customer_id
             WHERE cf.tenant_id = $1::uuid
               AND c.external_id = $2
            """,
            x_tenant_id, body.customer_id,
        )

        cold_start = features_row is None
        items = []

        if not cold_start:
            category_affinity: dict = json.loads(features_row["category_affinity"] or "{}")

            # ── Step 2a: affinity-based candidates ───────────────────────
            # Score each product by the customer's affinity for its category
            affinity_rows = await conn.fetch(
                """
                SELECT p.external_id AS product_id,
                       p.category,
                       p.title,
                       p.price
                  FROM products p
                 WHERE p.tenant_id = $1::uuid
                   AND ($2::text[] IS NULL OR p.external_id <> ALL($2::text[]))
                 ORDER BY p.title   -- stable sort; we'll re-rank below
                 LIMIT 200
                """,
                x_tenant_id, exclude_ids or None,
            )

            affinity_scores = {}
            for row in affinity_rows:
                score = category_affinity.get(row["category"], 0.0)
                affinity_scores[row["product_id"]] = {
                    "product_id": row["product_id"],
                    "title":      row["title"],
                    "score":      float(score) * 0.6,
                    "reason":     "affinity",
                }

            # ── Step 2b: co-occurrence boost ──────────────────────────────
            # If there's a context product, boost products that co-occur with it
            if body.context_product_id:
                cooc_rows = await conn.fetch(
                    """
                    SELECT p.external_id AS product_id,
                           pc.confidence
                      FROM product_cooccurrence pc
                      JOIN products pa ON pa.id = pc.product_a_id
                      JOIN products pb ON pb.id = pc.product_b_id
                      JOIN products p  ON p.id  = pc.product_b_id
                     WHERE pc.tenant_id = $1::uuid
                       AND pa.external_id = $2
                       AND ($3::text[] IS NULL OR p.external_id <> ALL($3::text[]))
                     ORDER BY pc.confidence DESC
                     LIMIT 50
                    """,
                    x_tenant_id, body.context_product_id, exclude_ids or None,
                )
                for row in cooc_rows:
                    pid = row["product_id"]
                    boost = float(row["confidence"]) * 0.4
                    if pid in affinity_scores:
                        affinity_scores[pid]["score"] += boost
                        affinity_scores[pid]["reason"] = "blended"
                    else:
                        affinity_scores[pid] = {
                            "product_id": pid,
                            "score":      boost,
                            "reason":     "cooccurrence",
                        }

            # Sort by blended score, take top-N
            items = sorted(
                affinity_scores.values(),
                key=lambda x: x["score"],
                reverse=True,
            )[:limit]

        # ── Cold-start fallback ───────────────────────────────────────────
        if cold_start or not items:
            popular = await conn.fetch(
                """
                SELECT p.external_id AS product_id,
                       p.title,
                       COUNT(e.id) AS purchase_count
                  FROM events e
                  JOIN products p ON p.id = e.product_id
                 WHERE e.tenant_id = $1::uuid
                   AND e.event_type = 'purchase'
                   AND e.timestamp  >= now() - interval '30 days'
                   AND ($2::text[] IS NULL OR p.external_id <> ALL($2::text[]))
                 GROUP BY p.external_id, p.title
                 ORDER BY purchase_count DESC
                 LIMIT $3
                """,
                x_tenant_id, exclude_ids or None, limit,
            )
            items = [
                {
                    "product_id": r["product_id"],
                    "title":      r["title"],
                    "score":      float(r["purchase_count"]),
                    "reason":     "popularity",
                }
                for r in popular
            ]

    # Round scores for clean output
    for item in items:
        item["score"] = round(item["score"], 4)

    # Cache for 5 minutes
    await r.setex(cache_key, 300, json.dumps(items))

    # Generate a decision_id and log asynchronously
    decision_id = str(uuid.uuid4())
    background_tasks.add_task(
        _log_decision,
        x_tenant_id, body.customer_id, items, decision_id,
        "cold_start" if cold_start else "blended",
    )

    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    return {
        "items":         items,
        "model_version": MODEL_VERSION,
        "decision_id":   decision_id,
        "latency_ms":    latency_ms,
        "cached":        False,
    }


async def _log_decision(
    tenant_id: str,
    customer_external_id: str,
    items: list,
    decision_id: str,
    strategy: str,
):
    """Write recommendation decision to the audit table (fire-and-forget)."""
    try:
        pool = await get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO recommendation_decisions
                    (decision_id, tenant_id, customer_id, items, strategy)
                SELECT $1::uuid,
                       $2::uuid,
                       c.id,
                       $3::jsonb,
                       $4
                  FROM customers c
                 WHERE c.tenant_id = $2::uuid
                   AND c.external_id = $5
                """,
                decision_id, tenant_id, json.dumps(items), strategy, customer_external_id,
            )
    except Exception:
        log.exception("Failed to log recommendation decision %s", decision_id)
