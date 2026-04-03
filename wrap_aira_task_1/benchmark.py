"""
Benchmark script — Task 1 & Task 2

Measures:
  1. Event ingestion throughput (events/sec) via POST /api/v1/events/batch
  2. Recommendation API p50/p95/p99 latency via POST /api/v1/recommend
"""

import asyncio
import time
import statistics
import json
from datetime import datetime, timezone, timedelta

import httpx
import asyncpg

BASE_URL    = "http://127.0.0.1:8000"
ADMIN_DSN   = "postgresql://aira_admin:secret@127.0.0.1:5432/aira"

# Benchmark tenant — created fresh each run
BENCH_TENANT_ID  = "cccccccc-cccc-cccc-cccc-cccccccccccc"
BENCH_TENANT_NAME = "bench-tenant"

BATCH_SIZE    = 500     # events per HTTP request
TOTAL_EVENTS  = 10_000  # total events to ingest
RECOMMEND_N   = 200     # number of /recommend calls to make


# ── Seed helpers ────────────────────────────────────────────────────────────

async def setup_bench_tenant(conn):
    """Create a fresh bench tenant, customer, and product."""
    await conn.execute(
        "DELETE FROM tenants WHERE id = $1::uuid", BENCH_TENANT_ID
    )
    await conn.execute(
        """
        INSERT INTO tenants (id, name, slug)
        VALUES ($1::uuid, $2, 'bench')
        ON CONFLICT (id) DO NOTHING
        """,
        BENCH_TENANT_ID, BENCH_TENANT_NAME,
    )
    await conn.execute(
        """
        INSERT INTO customers (id, tenant_id, external_id, email)
        VALUES (gen_random_uuid(), $1::uuid, 'bench-cust-1', 'bench@bench.com')
        ON CONFLICT DO NOTHING
        """,
        BENCH_TENANT_ID,
    )
    await conn.execute(
        """
        INSERT INTO products (id, tenant_id, external_id, title, price, category)
        VALUES (gen_random_uuid(), $1::uuid, 'bench-prod-1', 'Bench Widget', 9.99, 'widgets')
        ON CONFLICT DO NOTHING
        """,
        BENCH_TENANT_ID,
    )


async def teardown_bench_tenant(conn):
    await conn.execute(
        "DELETE FROM tenants WHERE id = $1::uuid", BENCH_TENANT_ID
    )


# ── Benchmark 1: event ingestion ────────────────────────────────────────────

async def bench_ingestion(client: httpx.AsyncClient) -> dict:
    """
    Send TOTAL_EVENTS events in batches of BATCH_SIZE.
    Measures wall-clock time and calculates events/sec.
    """
    print(f"\n[Ingestion] Sending {TOTAL_EVENTS:,} events in batches of {BATCH_SIZE}...")

    num_batches = TOTAL_EVENTS // BATCH_SIZE
    now = datetime.now(tz=timezone.utc)

    def make_batch(batch_idx: int) -> list:
        return [
            {
                "customer_id": "bench-cust-1",
                "event_type":  "product_view",
                "product_id":  "bench-prod-1",
                "timestamp":   (now - timedelta(seconds=batch_idx * BATCH_SIZE + i)).isoformat(),
            }
            for i in range(BATCH_SIZE)
        ]

    # Warm up (1 batch, not counted)
    await client.post(
        f"{BASE_URL}/api/v1/events/batch",
        headers={"x-tenant-id": BENCH_TENANT_ID},
        json={"events": make_batch(0)},
        timeout=30,
    )

    start = time.perf_counter()
    errors = 0
    for i in range(num_batches):
        resp = await client.post(
            f"{BASE_URL}/api/v1/events/batch",
            headers={"x-tenant-id": BENCH_TENANT_ID},
            json={"events": make_batch(i)},
            timeout=30,
        )
        if resp.status_code != 200:
            errors += 1

    elapsed = time.perf_counter() - start
    throughput = (num_batches * BATCH_SIZE) / elapsed
    TARGET_SEC = 2.0
    passed = elapsed < TARGET_SEC

    result = {
        "total_events":      num_batches * BATCH_SIZE,
        "batch_size":        BATCH_SIZE,
        "elapsed_sec":       round(elapsed, 2),
        "events_per_sec":    round(throughput, 0),
        "errors":            errors,
        "target_sec":        TARGET_SEC,
        "target_passed":     passed,
    }

    print(f"  Total events : {result['total_events']:,}")
    print(f"  Elapsed      : {result['elapsed_sec']}s  (target: < {TARGET_SEC}s  {'✓ PASSED' if passed else '✗ FAILED'})")
    print(f"  Throughput   : {result['events_per_sec']:,.0f} events/sec")
    print(f"  Errors       : {result['errors']}")

    return result


# ── Benchmark 2: recommendation latency ─────────────────────────────────────

async def bench_recommend(client: httpx.AsyncClient) -> dict:
    """
    Call /recommend RECOMMEND_N times sequentially and measure latency.
    Clears Redis cache before each call to get real DB latency.
    """
    print(f"\n[Recommend] Making {RECOMMEND_N} sequential /recommend calls...")

    latencies_ms = []
    errors = 0
    first_error_body = None

    for i in range(RECOMMEND_N):
        # Use a unique customer_id per call so we bypass the Redis cache
        # and measure real DB query latency, not cache hit latency.
        # Cold-start customers fall through to the popularity fallback,
        # which is the realistic worst-case path.
        unique_customer = f"bench-cust-{i}"
        start = time.perf_counter()
        resp = await client.post(
            f"{BASE_URL}/api/v1/recommend",
            headers={"x-tenant-id": BENCH_TENANT_ID},
            json={"customer_id": unique_customer, "surface": "homepage", "limit": 10},
            timeout=10,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        if resp.status_code == 200:
            latencies_ms.append(elapsed_ms)
        else:
            errors += 1
            if first_error_body is None:
                first_error_body = resp.text

    if errors:
        print(f"  WARNING: {errors}/{RECOMMEND_N} calls failed")
        print(f"  First error response: {first_error_body}")

    if not latencies_ms:
        print("  SKIPPED — all recommend calls failed, cannot compute latency")
        return {"n": 0, "p50_ms": None, "p95_ms": None,
                "p99_ms": None, "mean_ms": None, "max_ms": None, "errors": errors}

    latencies_ms.sort()
    n = len(latencies_ms)

    def percentile(data, p):
        idx = max(0, int(len(data) * p / 100) - 1)
        return round(data[idx], 2)

    result = {
        "n":        n,
        "p50_ms":   percentile(latencies_ms, 50),
        "p95_ms":   percentile(latencies_ms, 95),
        "p99_ms":   percentile(latencies_ms, 99),
        "mean_ms":  round(statistics.mean(latencies_ms), 2),
        "max_ms":   round(max(latencies_ms), 2),
        "errors":   errors,
    }

    print(f"  Calls : {result['n']}")
    print(f"  p50   : {result['p50_ms']} ms")
    print(f"  p95   : {result['p95_ms']} ms")
    print(f"  p99   : {result['p99_ms']} ms")
    print(f"  mean  : {result['mean_ms']} ms")
    print(f"  max   : {result['max_ms']} ms")

    return result


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        await setup_bench_tenant(conn)
    finally:
        await conn.close()

    async with httpx.AsyncClient() as client:
        # Verify server is up
        try:
            r = await client.get(f"{BASE_URL}/health", timeout=3)
            assert r.status_code == 200
        except Exception:
            print(f"ERROR: Could not reach {BASE_URL}/health")
            print("Make sure uvicorn main:app --port 8000 is running.")
            return

        ingestion = await bench_ingestion(client)
        recommend = await bench_recommend(client)

    # Write results file
    results = {
        "timestamp":  datetime.now(tz=timezone.utc).isoformat(),
        "ingestion":  ingestion,
        "recommend":  recommend,
    }

    with open("benchmark_results.txt", "w") as f:
        f.write("AIRA Platform — Benchmark Results\n")
        f.write("=" * 50 + "\n\n")
        passed_str = "PASSED" if ingestion.get("target_passed") else "FAILED"
        f.write("Event Ingestion\n")
        f.write("-" * 30 + "\n")
        f.write(f"  Total events   : {ingestion['total_events']:,}\n")
        f.write(f"  Batch size     : {ingestion['batch_size']}\n")
        f.write(f"  Elapsed        : {ingestion['elapsed_sec']}s\n")
        f.write(f"  Target         : < {ingestion['target_sec']}s  [{passed_str}]\n")
        f.write(f"  Throughput     : {ingestion['events_per_sec']:,.0f} events/sec\n")
        f.write(f"  Errors         : {ingestion['errors']}\n\n")

        p95_ok = recommend.get("p95_ms") is not None and recommend["p95_ms"] < 100
        p95_status = "PASSED" if p95_ok else ("N/A" if recommend.get("p95_ms") is None else "FAILED")
        f.write("Recommendation API Latency (uncached — unique customer per call)\n")
        f.write("-" * 30 + "\n")
        f.write(f"  Calls          : {recommend['n']}\n")
        f.write(f"  p50            : {recommend['p50_ms']} ms\n")
        f.write(f"  p95            : {recommend['p95_ms']} ms  [target: < 100ms  {p95_status}]\n")
        f.write(f"  p99            : {recommend['p99_ms']} ms\n")
        f.write(f"  mean           : {recommend['mean_ms']} ms\n")
        f.write(f"  max            : {recommend['max_ms']} ms\n")
        f.write(f"\n  Note: Each call uses a unique customer_id to bypass Redis cache\n")
        f.write(f"  and measure actual PostgreSQL query latency (cold-start path).\n")

    print("\n✓ Results written to benchmark_results.txt")

    # Cleanup
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        await teardown_bench_tenant(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
