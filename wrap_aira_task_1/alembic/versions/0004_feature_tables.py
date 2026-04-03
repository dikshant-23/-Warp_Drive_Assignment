"""Add feature tables for recommendation pipeline

Task 2 — first migration:
  - pipeline_checkpoints      : watermark store for incremental pipeline runs
  - customer_features         : per-customer feature blob (affinity, velocity, session)
  - recommendation_decisions  : audit log of every recommendation served
"""

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB


def upgrade():
    # ------------------------------------------------------------------
    # pipeline_checkpoints
    # Tracks the last-processed timestamp per (tenant, pipeline).
    # Composite PK — no surrogate key needed.
    # ------------------------------------------------------------------
    op.create_table(
        "pipeline_checkpoints",
        sa.Column("tenant_id",           UUID(as_uuid=True), nullable=False),
        sa.Column("pipeline_name",       sa.Text(),          nullable=False),
        sa.Column("last_processed_at",   sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("updated_at",          sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "pipeline_name"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
    )

    # ------------------------------------------------------------------
    # customer_features
    # One row per (tenant, customer) — upserted on each pipeline run.
    #
    # Per-customer aggregates (last 30 days):
    #   total_views, total_cart_adds, total_purchases,
    #   distinct_products_viewed, distinct_categories_viewed,
    #   days_since_first_seen, days_since_last_activity
    #
    # Category affinity (JSONB):
    #   category → normalised score (7-day events weighted 3x vs 30-day)
    #
    # Purchase velocity / revenue:
    #   purchase_velocity_30d, avg_days_between_purchases,
    #   total_revenue, avg_order_value
    #
    # Session features (30-min idle gap):
    #   avg_session_length_min, avg_products_per_session
    # ------------------------------------------------------------------
    op.create_table(
        "customer_features",
        sa.Column("id",          UUID(as_uuid=True),
                  server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("tenant_id",   UUID(as_uuid=True), nullable=False),
        sa.Column("customer_id", UUID(as_uuid=True), nullable=False),

        # Category affinity vector
        sa.Column("category_affinity",          JSONB,        nullable=True),

        # Per-customer event aggregates
        sa.Column("total_views",                sa.Integer(), nullable=True),
        sa.Column("total_cart_adds",            sa.Integer(), nullable=True),
        sa.Column("total_purchases",            sa.Integer(), nullable=True),
        sa.Column("distinct_products_viewed",   sa.Integer(), nullable=True),
        sa.Column("distinct_categories_viewed", sa.Integer(), nullable=True),

        # Activity recency
        sa.Column("days_since_first_seen",      sa.Float(),   nullable=True),
        sa.Column("days_since_last_activity",   sa.Float(),   nullable=True),

        # Purchase velocity / revenue
        sa.Column("purchase_velocity_30d",      sa.Float(),   nullable=True),
        sa.Column("avg_days_between_purchases", sa.Float(),   nullable=True),
        sa.Column("total_revenue",              sa.Float(),   nullable=True),
        sa.Column("avg_order_value",            sa.Float(),   nullable=True),

        # Session features
        sa.Column("avg_session_length_min",     sa.Float(),   nullable=True),
        sa.Column("avg_products_per_session",   sa.Float(),   nullable=True),

        sa.Column("computed_at", sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),

        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "customer_id",
                            name="uq_customer_features_tenant_customer"),
        sa.ForeignKeyConstraint(["tenant_id"],   ["tenants.id"],   ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], ondelete="CASCADE"),
    )

    op.create_index(
        "ix_customer_features_tenant_customer",
        "customer_features",
        ["tenant_id", "customer_id"],
    )

    # ------------------------------------------------------------------
    # recommendation_decisions
    # Every /recommend call logs here for offline evaluation / feedback.
    # items column: [{"product_id": ..., "score": ..., "reason": ...}]
    # strategy: "blended" | "cold_start"
    # ------------------------------------------------------------------
    op.create_table(
        "recommendation_decisions",
        sa.Column("decision_id",  UUID(as_uuid=True),
                  server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("tenant_id",    UUID(as_uuid=True), nullable=False),
        sa.Column("customer_id",  UUID(as_uuid=True), nullable=False),
        sa.Column("items",        JSONB,     nullable=False),
        sa.Column("strategy",     sa.Text(), nullable=True),
        sa.Column("created_at",   sa.TIMESTAMP(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("decision_id"),
        sa.ForeignKeyConstraint(["tenant_id"],   ["tenants.id"],   ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], ondelete="CASCADE"),
    )

    op.create_index(
        "ix_recommendation_decisions_tenant_customer",
        "recommendation_decisions",
        ["tenant_id", "customer_id", "created_at"],
    )

    op.execute("GRANT SELECT, INSERT ON pipeline_checkpoints TO app_user")
    op.execute("GRANT SELECT, INSERT, UPDATE ON customer_features TO app_user")
    op.execute("GRANT SELECT, INSERT ON recommendation_decisions TO app_user")


def downgrade():
    op.drop_table("recommendation_decisions")
    op.drop_table("customer_features")
    op.drop_table("pipeline_checkpoints")
