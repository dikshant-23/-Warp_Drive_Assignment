"""Add customer_features, pipeline_checkpoints, recommendation_decisions.

customer_features: one row per customer, updated incrementally by the pipeline.
pipeline_checkpoints: watermark table so re-runs only process new events.
recommendation_decisions: audit log for every recommendation served.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade():
    # Watermark table — tracks last processed timestamp per pipeline per tenant.
    # Composite PK means one checkpoint row per (tenant, pipeline_name) pair.
    op.create_table(
        "pipeline_checkpoints",
        sa.Column("tenant_id",         UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("pipeline_name",      sa.String(100), primary_key=True),
        sa.Column("last_processed_at",  sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at",         sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )

    # One row per customer — upserted by the pipeline on every run.
    # category_affinity is a JSONB map: {category: normalised_score}
    # scores sum to ~1.0, weighted by recency (7d = 3x, 30d = 1x).
    op.create_table(
        "customer_features",
        sa.Column("tenant_id",                  UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("customer_id",                UUID(), sa.ForeignKey("customers.id", ondelete="CASCADE"), nullable=False),
        sa.Column("total_views",                sa.Integer(), server_default="0"),
        sa.Column("total_cart_adds",            sa.Integer(), server_default="0"),
        sa.Column("total_purchases",            sa.Integer(), server_default="0"),
        sa.Column("distinct_products_viewed",   sa.Integer(), server_default="0"),
        sa.Column("distinct_categories_viewed", sa.Integer(), server_default="0"),
        sa.Column("days_since_first_seen",      sa.Numeric(10, 2), nullable=True),
        sa.Column("days_since_last_activity",   sa.Numeric(10, 2), nullable=True),
        sa.Column("category_affinity",          JSONB(), nullable=True),
        sa.Column("avg_days_between_purchases", sa.Numeric(10, 2), nullable=True),
        sa.Column("total_revenue",              sa.Numeric(12, 2), server_default="0"),
        sa.Column("avg_order_value",            sa.Numeric(12, 2), nullable=True),
        sa.Column("avg_session_duration_mins",  sa.Numeric(10, 2), nullable=True),
        sa.Column("avg_products_per_session",   sa.Numeric(10, 2), nullable=True),
        sa.Column("computed_at",                sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.UniqueConstraint("tenant_id", "customer_id", name="uq_customer_features_tenant_customer"),
    )
    op.create_index("ix_customer_features_tenant_customer", "customer_features", ["tenant_id", "customer_id"])

    # Every recommendation served gets logged here.
    # decision_id ties this to future click/purchase feedback.
    op.create_table(
        "recommendation_decisions",
        sa.Column("id",            UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id",     UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("customer_id",   sa.String(255), nullable=False),  # external_id string, not FK
        sa.Column("surface",       sa.String(100), nullable=False),
        sa.Column("items",         JSONB(), nullable=False),
        sa.Column("model_version", sa.String(50), nullable=False),
        sa.Column("decision_id",   UUID(), nullable=False),
        sa.Column("latency_ms",    sa.Numeric(10, 2), nullable=True),
        sa.Column("created_at",    sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_decisions_tenant_customer", "recommendation_decisions", ["tenant_id", "customer_id"])
    op.create_index("ix_decisions_decision_id",     "recommendation_decisions", ["decision_id"], unique=True)

    # Grant read/write to app_user on tables the API needs
    op.execute("GRANT SELECT, INSERT ON recommendation_decisions TO app_user")
    op.execute("GRANT SELECT ON customer_features TO app_user")
    op.execute("GRANT SELECT ON pipeline_checkpoints TO app_user")


def downgrade():
    op.drop_table("recommendation_decisions")
    op.drop_table("customer_features")
    op.drop_table("pipeline_checkpoints")