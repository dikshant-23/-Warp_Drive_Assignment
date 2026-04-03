"""Add product_cooccurrence table.

Revision ID: 0005
Revises: 0004
Create Date: 2026-03-28

Stores bidirectional co-purchase pairs: both (A→B) and (B→A) are inserted
so top-N lookup for any product is a simple index scan on product_a_id.

confidence = P(B|A) = co_count / count(customers who bought A)
Pairs with co_count < 3 are filtered at compute time (too noisy).
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "product_cooccurrence",
        sa.Column("tenant_id",    UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("product_a_id", UUID(), sa.ForeignKey("products.id", ondelete="CASCADE"), nullable=False),
        sa.Column("product_b_id", UUID(), sa.ForeignKey("products.id", ondelete="CASCADE"), nullable=False),
        sa.Column("co_count",     sa.Integer(), nullable=False),
        sa.Column("confidence",   sa.Numeric(6, 4), nullable=False),
        sa.Column("last_updated", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("tenant_id", "product_a_id", "product_b_id"),
    )
    # Top-N lookup: given product_a, rank by confidence descending
    op.create_index("ix_cooccurrence_lookup", "product_cooccurrence",
                    ["tenant_id", "product_a_id", "confidence"])

    op.execute("GRANT SELECT ON product_cooccurrence TO app_user")


def downgrade():
    op.drop_table("product_cooccurrence")