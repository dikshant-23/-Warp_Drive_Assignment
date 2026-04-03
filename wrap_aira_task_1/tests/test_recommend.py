"""
Tests for POST /api/v1/recommend.
"""
import pytest
from httpx import AsyncClient, ASGITransport
from main import app

TENANT_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


@pytest.mark.asyncio
async def test_recommend_returns_items(seed):
    """Valid request returns a list of items with score and reason."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post(
            "/api/v1/recommend",
            headers={"x-tenant-id": TENANT_A},
            json={"customer_id": "cust-alpha-1", "surface": "homepage", "limit": 4},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "decision_id" in data
    assert "latency_ms" in data
    assert data["latency_ms"] < 1000  # sanity check — not actually p95


@pytest.mark.asyncio
async def test_recommend_respects_exclude_list(seed):
    """Excluded product IDs must not appear in results."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post(
            "/api/v1/recommend",
            headers={"x-tenant-id": TENANT_A},
            json={
                "customer_id": "cust-alpha-1",
                "surface": "homepage",
                "limit": 8,
                "exclude_product_ids": ["prod-alpha-1"],
            },
        )
    assert resp.status_code == 200
    product_ids = [item["product_id"] for item in resp.json()["items"]]
    assert "prod-alpha-1" not in product_ids


@pytest.mark.asyncio
async def test_recommend_cold_start(seed):
    """Unknown customer gets popularity fallback (no crash)."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post(
            "/api/v1/recommend",
            headers={"x-tenant-id": TENANT_A},
            json={"customer_id": "unknown-customer-xyz", "surface": "email"},
        )
    assert resp.status_code == 200
    data = resp.json()
    # Either items from fallback or empty list — both are valid
    assert isinstance(data["items"], list)