"""
AIRA Platform API - Task 1: Event Ingestion
"""

import json
import time
from datetime import datetime
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
            raise ValueError(f"'{v}' is not valid. Must be one of {sorted(VALID_EVENT_TYPES)}")
        return v


class BatchRequest(BaseModel):
    events: List[EventIn]


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/v1/events/batch")
async def ingest_events(
    body: BatchRequest,
    x_tenant_id: str = Header(...),
):
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
            :tenant_id::uuid,
            (SELECT id FROM customers
             WHERE tenant_id = :tenant_id::uuid AND external_id = :customer_ext LIMIT 1),
            :event_type::event_type_enum,
            (SELECT id FROM products
             WHERE tenant_id = :tenant_id::uuid AND external_id = :product_ext LIMIT 1),
            :properties::jsonb,
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