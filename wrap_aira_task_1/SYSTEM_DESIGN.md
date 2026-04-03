# AIRA Platform: System Design at Scale

**Author:** Dikshant | **Date:** March 2026 | **Status:** Proposal

---

## Context

AIRA has grown to 500 tenants, 50M events/day, 10M customers, 5,000 recommendation requests/second at peak, and 2M direct mail pieces/month. This document outlines the architectural evolution needed to support this scale while maintaining the multi-tenant isolation guarantees and sub-100ms recommendation latency that AIRA's customers depend on.

---

## A. Event Pipeline at Scale (50M events/day)

### Current Bottleneck

At 50M events/day (~580 events/second average, ~3,000/second peak), direct PostgreSQL INSERT becomes the bottleneck. The synchronous HTTP-to-DB path creates back-pressure during peak hours and risks data loss on transient DB failures.

### Proposed Architecture

```
Clients → API Gateway → Kafka (events topic, 12 partitions)
                              ↓
              ┌───────────────┼───────────────┐
              ↓               ↓               ↓
        Flink Consumer  Flink Consumer  Flink Consumer
              ↓               ↓               ↓
         PostgreSQL      S3 (Parquet)    Redis (real-time)
         (hot, 90d)      (cold, Iceberg)  (streaming features)
```

**Message Queue: Kafka** over SQS for this workload. Kafka provides ordered, partitioned delivery essential for event deduplication and exactly-once semantics (via idempotent producers + transactional consumers). Partition by `tenant_id` hash to preserve per-tenant ordering and enable tenant-scoped consumers.

**Back-of-envelope math:**
- 50M events/day = ~580 events/sec average, ~3K/sec peak (5x burst)
- Average event payload: ~500 bytes → 50M * 500B = 25 GB/day raw
- Kafka retention: 7 days → 175 GB on disk (3x replication = 525 GB across cluster)
- 12 partitions handle 3K/sec comfortably (each partition sustains ~10K msg/sec)

**Stream Processing: Apache Flink** for exactly-once semantics with Kafka. The Flink job performs:
1. Schema validation and enrichment (resolve external IDs to UUIDs).
2. Deduplication via a keyed state window (event_id + timestamp).
3. Dual-write: hot path (PostgreSQL for queries) and cold path (S3 Parquet via Iceberg for analytics).

**Partitioning Strategy:**
- Kafka: hash by `tenant_id` (12 partitions, expandable).
- PostgreSQL: continue monthly range partitioning on `timestamp`. Drop partitions older than 90 days; Iceberg retains full history.
- Iceberg on S3: partition by `(tenant_id, date)` for efficient analytical queries.

**What stays in PostgreSQL vs. what goes to the analytical store:**

| PostgreSQL (hot, 90-day window) | S3/Iceberg (cold, full history) |
|---|---|
| Recent events for real-time features | Historical events for model training |
| Customer & product records (source of truth) | Event archives for compliance/audit |
| Feature tables, co-occurrence matrix | Aggregated analytics datasets |
| Recommendation decision logs (30 days) | Full decision log history |

**Exactly-once guarantees:** Kafka idempotent producers (per-partition sequence numbers) + Flink checkpointing with Kafka transactional consumers. For the PostgreSQL sink, we use upsert (ON CONFLICT) with event UUIDs as the deduplication key.

**Rollout plan:** Deploy Kafka and Flink alongside the existing direct-insert path. Dual-write for 2 weeks to validate parity, then cut over. Keep the direct-insert endpoint as a fallback for single-event ingestion during Kafka outages.

---

## B. Feature Store Design (4 hours → Near Real-Time)

### Current Bottleneck

The monolithic CTE pipeline scans the entire events table per-tenant, recomputing all features from scratch. At 50M events/day, this becomes I/O bound on PostgreSQL.

### Proposed Architecture: Lambda (Batch + Streaming)

```
                        ┌──────────────────┐
                        │  Batch Pipeline  │ (Flink Batch, every 6h)
                        │  Full recompute  │
                        └───────┬──────────┘
                                ↓
Events → Kafka → Flink ──→ Redis (Feature Store) ←── Reco API (<100ms)
              │                  ↑
              │          ┌───────┴──────────┐
              └────────→ │ Stream Pipeline  │ (Flink Streaming)
                         │ Incremental      │
                         │ updates          │
                         └──────────────────┘
```

**Streaming features (sub-minute latency):**
- Flink streaming job consumes from Kafka and maintains running counters (views in last 5 minutes, last 1 hour, last 24 hours) using tumbling/sliding windows.
- Updates written directly to Redis hashes: `features:{tenant_id}:{customer_id}`.
- These capture recency signals that batch features miss (e.g., "customer is actively browsing right now").

**Batch features (every 6 hours):**
- Flink batch job reads from Iceberg (S3) — no load on PostgreSQL.
- Computes heavier aggregates: category affinity, purchase velocity, session stats.
- Writes to Redis, overwriting the batch portion of the feature hash.

**Feature Store schema in Redis:**
```
HSET features:{tenant}:{customer}
  # Batch features (updated every 6h)
  total_views 142
  total_purchases 8
  category_affinity '{"footwear":0.65,"outerwear":0.25}'
  avg_order_value 89.50

  # Streaming features (updated sub-minute)
  views_5m 3
  views_1h 12
  cart_adds_1h 1
  last_event_ts 1711440600
```

**How the Reco API reads with sub-100ms latency:**
- Single `HGETALL` call to Redis: ~0.2ms at p99.
- Scoring logic runs in-process (no DB round-trip for features).
- Total p95 budget: Redis (1ms) + scoring (2ms) + product lookup (5ms from Redis cache) = ~8ms.

**Back-of-envelope:**
- 10M customers * average 200 bytes per feature hash = 2 GB in Redis.
- With 3x headroom for TTLs and working set: 6 GB — fits in a single Redis instance.
- For HA: Redis Sentinel or ElastiCache with read replicas.

---

## C. Recommendation Serving Architecture (5,000 req/s)

### Pre-computation vs. Real-time Scoring

**Hybrid approach:** Pre-compute candidate sets (top-100 products per customer segment), score in real-time using fresh streaming features.

Pre-computation alone becomes stale too quickly (product inventory changes, customer behavior shifts). Pure real-time scoring is too expensive at 5K req/s if it requires complex model inference.

**Architecture:**

```
Request → Load Balancer → Reco Service (stateless, 4 replicas)
                              │
                    ┌─────────┼─────────┐
                    ↓         ↓         ↓
              Redis Cache  Feature    Product
              (reco cache) Store     Catalog Cache
```

**Caching strategy:**
- L1 cache: In-process LRU (per-replica, 10K entries, 60s TTL). Handles repeat requests for the same customer/surface.
- L2 cache: Redis cache of pre-scored recommendations (5-minute TTL). Reduces computation load by ~80%.
- Cache invalidation: Product changes (price, stock, active status) publish to a Kafka topic. Reco service subscribes and invalidates affected cache entries by product_id pattern.

**A/B testing infrastructure:**
- Consistent hashing for stable assignment: `hash(customer_id + experiment_id) % 100` maps to a bucket.
- Bucket → variant mapping stored in a config service (e.g., LaunchDarkly or in-house).
- Decision logs include `experiment_id` and `variant` for offline analysis.
- Stable assignment means a customer always sees the same variant within an experiment, preventing inconsistent experiences.

**Graceful degradation:**
1. Model unavailable → serve from pre-computed cache (stale but safe).
2. Feature store unavailable → fall back to global popularity (no personalization).
3. Redis unavailable → fall back to PostgreSQL with aggressive timeouts.
4. Complete degradation → return empty recommendations with a `degraded: true` flag. The frontend shows editorial picks.

Each level is instrumented with a circuit breaker (e.g., using `tenacity` or `pybreaker`) and fires an alert.

**Capacity planning:**
- 5,000 req/s * 8ms avg processing = 40 CPU-seconds/s → ~4 vCPUs at saturation.
- 4 replicas * 2 vCPU each = 8 vCPUs (50% utilization target for headroom).
- Redis: 5K reads/s is trivial (single instance handles 100K+ ops/s).

---

## D. Multi-Tenant Data Isolation at Scale (500 Tenants)

### RLS at Scale: Where It Breaks

RLS works well at our current scale, but at 500 tenants with 10x data volume variance, several issues emerge:

**Problem 1: PgBouncer + RLS in transaction mode.**
PgBouncer in transaction mode reuses connections across requests. `SET LOCAL app.current_tenant` scopes to the *transaction*, but if PgBouncer pools connections between transactions, a connection might retain stale session state. Mitigation: use `SET LOCAL` (not `SET`) and ensure each transaction explicitly sets the tenant. Alternatively, use PgBouncer's `server_reset_query` to clear state.

**Problem 2: Noisy neighbors.**
A tenant with 40M of the 50M daily events dominates I/O. Their analytical queries slow down all tenants sharing the same PostgreSQL instance.

**Problem 3: Connection pooling pressure.**
500 tenants with bursty traffic patterns. If each tenant's peak requires 5 connections, naive pooling needs 2,500 connections — well beyond PostgreSQL's practical limit (~500 effective connections).

### Proposed: Tiered Isolation Model

```
┌───────────────────────────────────────────┐
│  Tier 1: Shared Schema (RLS)             │
│  ~480 small/medium tenants               │
│  Shared PostgreSQL cluster with RLS      │
│  PgBouncer pool: 200 connections         │
└───────────────────────────────────────────┘

┌───────────────────────────────────────────┐
│  Tier 2: Tenant-per-Schema               │
│  ~15 large tenants                       │
│  Dedicated schema per tenant, same DB    │
│  Separate connection pools               │
└───────────────────────────────────────────┘

┌───────────────────────────────────────────┐
│  Tier 3: Dedicated Instance              │
│  ~5 enterprise tenants                   │
│  Dedicated PostgreSQL instance           │
│  Full resource isolation                 │
└───────────────────────────────────────────┘
```

**Why not tenant-per-schema for everyone?** Migration complexity and operational overhead. 500 schemas means 500x Alembic migrations, 500x index maintenance windows, and complex cross-tenant analytics. The shared-schema + RLS approach is operationally simpler for the long tail of small tenants.

**Connection pooling strategy:**
- PgBouncer in transaction mode with `server_reset_query = 'DISCARD ALL'`.
- Per-tenant connection limits enforced at the application layer (token bucket).
- Tier 2/3 tenants get dedicated PgBouncer pools.

**Noisy-neighbor mitigation:**
- Per-tenant query timeout: `SET LOCAL statement_timeout = '5s'` for OLTP queries.
- Rate limiting on event ingestion per tenant (token bucket, configurable per plan tier).
- Heavy analytical queries routed to read replicas (tenant-aware routing).
- Resource quotas: PostgreSQL `pg_stat_statements` monitoring with alerts at per-tenant thresholds.

**Migration path from current architecture:**
1. Deploy PgBouncer with `server_reset_query` (week 1).
2. Add per-tenant rate limiting and query timeouts (week 2).
3. Identify top-5 tenants by data volume, migrate to Tier 2 schemas (week 3-4).
4. Offer Tier 3 dedicated instances to enterprise customers (month 2).
5. Build a tenant router in the application layer that directs connections to the correct pool/schema based on tenant tier.

**Observability:** Every query tagged with `tenant_id` in OpenTelemetry traces. Grafana dashboards show per-tenant query latency, connection count, and I/O usage. Alerts fire when any tenant exceeds 2x their historical baseline, indicating either growth (upgrade tier) or a runaway query (kill + investigate).

---

## Summary of Key Decisions

| Decision | Rationale |
|---|---|
| Kafka over SQS | Ordering guarantees, exactly-once semantics, higher throughput |
| Flink over Spark Streaming | True streaming (not micro-batch), exactly-once with Kafka |
| Iceberg on S3 over Redshift | Cost-effective cold storage, schema evolution, time travel |
| Redis feature store over PostgreSQL | Sub-ms reads, natural hash structure for features |
| Hybrid pre-compute + real-time scoring | Balances freshness with computational cost |
| Tiered tenant isolation | Right-sizes isolation to tenant needs without operational explosion |
| PgBouncer with DISCARD ALL | Safe connection pooling with RLS |

---

*This document is intended as a living design — specific technology choices should be validated with proof-of-concept benchmarks before commitment.*
