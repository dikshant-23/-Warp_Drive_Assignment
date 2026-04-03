# AIRA Platform

Multi-tenant event ingestion and recommendation API. Built with FastAPI, PostgreSQL 16 (RLS + partitioning), Redis, and SQLAlchemy async.

## Quick start

```bash
docker-compose up -d
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
uvicorn main:app --reload --port 8000
```

## Running tests

```bash
pytest tests/ -v
```

Covers RLS tenant isolation, event ingestion, the feature pipeline, and the recommendation endpoint.

## Event ingestion

```bash
curl -X POST http://localhost:8000/api/v1/events/batch \
  -H "x-tenant-id: <tenant-uuid>" \
  -H "Content-Type: application/json" \
  -d '{"events": [{"customer_id": "cust-001", "event_type": "purchase", "product_id": "prod-001", "timestamp": "2026-04-01T10:00:00Z"}]}'
```

Accepts up to 1,000 events per request. Valid event types: `page_view`, `product_view`, `add_to_cart`, `purchase`, `click`, `dismiss`.

## Feature pipeline

Run once per tenant before serving recommendations:

```bash
python pipeline.py <tenant-uuid>              # incremental
python pipeline.py <tenant-uuid> --full-refresh  # recompute from scratch
```

## Recommendations

```bash
curl -X POST http://localhost:8000/api/v1/recommend \
  -H "x-tenant-id: <tenant-uuid>" \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-001", "surface": "homepage", "limit": 8, "exclude_product_ids": []}'
```

Blends category affinity (60%) with co-occurrence confidence (40%). Falls back to global popularity for new customers. Results cached in Redis for 5 minutes.

## Benchmark

With the server running:

```bash
python benchmark.py
```

Results are saved to `benchmark_results.txt`. See that file for throughput and latency numbers.

## Migrations

```
0001 — core tables (tenants, users, products, customers, events)
0002 — row-level security policies and roles
0003 — partition grants
0004 — pipeline_checkpoints, customer_features, recommendation_decisions
0005 — product_cooccurrence
```

All migrations have `upgrade()` and `downgrade()`. Roll back with `alembic downgrade -1`.
