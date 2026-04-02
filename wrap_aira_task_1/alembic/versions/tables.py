"""Create all tables and event partitions.

Revision ID: 0001
Revises: None
Create Date: 2026-03-26
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB, JSON, ENUM as PgENUM

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Create enum types via raw SQL — avoids SQLAlchemy's unreliable
    # create_type lifecycle when used inside op.create_table()
    op.execute("CREATE TYPE user_role AS ENUM ('admin', 'power_user', 'operator', 'viewer')")
    op.execute("CREATE TYPE event_type_enum AS ENUM ('page_view', 'product_view', 'add_to_cart', 'purchase', 'click', 'dismiss')")

    op.create_table(
        "tenants",
        sa.Column("id",         UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name",       sa.String(255), nullable=False),
        sa.Column("slug",       sa.String(100), nullable=False),
        sa.Column("plan_tier",  sa.String(50),  server_default="free"),
        sa.Column("created_at", sa.DateTime(),  server_default=sa.text("now()")),
    )
    op.create_index("ix_tenants_slug", "tenants", ["slug"], unique=True)

    op.create_table(
        "users",
        sa.Column("id",            UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id",     UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("email",         sa.String(320), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("display_name",  sa.String(255), nullable=False),
        sa.Column("role",          PgENUM(name="user_role", create_type=False),
                  nullable=False, server_default="viewer"),
        sa.Column("created_at",    sa.DateTime(), server_default=sa.text("now()")),
    )
    op.create_index("ix_users_tenant_id",    "users", ["tenant_id"])
    op.create_index("ix_users_tenant_email", "users", ["tenant_id", "email"], unique=True)

    op.create_table(
        "api_keys",
        sa.Column("id",           UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id",      UUID(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("key_hash",     sa.String(64), nullable=False),
        sa.Column("name",         sa.String(255), nullable=False),
        sa.Column("scopes",       JSON(), server_default="[]"),
        sa.Column("created_at",   sa.DateTime(), server_default=sa.text("now()")),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("revoked_at",   sa.DateTime(), nullable=True),
    )
    op.create_index("ix_api_keys_user_id", "api_keys", ["user_id"])

    op.create_table(
        "products",
        sa.Column("id",          UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id",   UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("title",       sa.String(500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("image_url",   sa.String(2048), nullable=True),
        sa.Column("price",       sa.Numeric(12, 2), nullable=False),
        sa.Column("currency",    sa.String(3), server_default="USD"),
        sa.Column("category",    sa.String(255), nullable=True),
        sa.Column("tags",        JSON(), server_default="[]"),
        sa.Column("stock_qty",   sa.Integer(), server_default="0"),
        sa.Column("active",      sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("metadata",    JSONB(), nullable=True),
        sa.Column("created_at",  sa.DateTime(), server_default=sa.text("now()")),
        sa.Column("updated_at",  sa.DateTime(), server_default=sa.text("now()")),
    )
    op.create_index("ix_products_tenant_external", "products", ["tenant_id", "external_id"], unique=True)
    op.create_index("ix_products_tenant_category", "products", ["tenant_id", "category"])

    op.create_table(
        "customers",
        sa.Column("id",          UUID(), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id",   UUID(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("email",       sa.String(320), nullable=True),
        sa.Column("attributes",  JSONB(), nullable=True),
        sa.Column("first_seen",  sa.DateTime(), server_default=sa.text("now()")),
        sa.Column("last_seen",   sa.DateTime(), server_default=sa.text("now()")),
    )
    op.create_index("ix_customers_tenant_external", "customers", ["tenant_id", "external_id"], unique=True)
    op.create_index("ix_customers_tenant_email",    "customers", ["tenant_id", "email"])

    # Events table uses raw SQL — SQLModel/SQLAlchemy ORM cannot express
    # PARTITION BY RANGE. Partitions must be created individually.
    op.execute("""
        CREATE TABLE events (
            id          UUID            NOT NULL DEFAULT gen_random_uuid(),
            tenant_id   UUID            NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            customer_id UUID            NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            event_type  event_type_enum NOT NULL,
            product_id  UUID,
            properties  JSONB,
            timestamp   TIMESTAMPTZ     NOT NULL,
            PRIMARY KEY (id, timestamp)
        ) PARTITION BY RANGE (timestamp);
    """)

    partitions = [
        ("events_2026_01", "2026-01-01", "2026-02-01"),
        ("events_2026_02", "2026-02-01", "2026-03-01"),
        ("events_2026_03", "2026-03-01", "2026-04-01"),
        ("events_2026_04", "2026-04-01", "2026-05-01"),
        ("events_2026_05", "2026-05-01", "2026-06-01"),
        ("events_2026_06", "2026-06-01", "2026-07-01"),
        ("events_2026_07", "2026-07-01", "2026-08-01"),
        ("events_2026_08", "2026-08-01", "2026-09-01"),
        ("events_2026_09", "2026-09-01", "2026-10-01"),
        ("events_2026_10", "2026-10-01", "2026-11-01"),
        ("events_2026_11", "2026-11-01", "2026-12-01"),
        ("events_2026_12", "2026-12-01", "2027-01-01"),
        ("events_2027_01", "2027-01-01", "2027-02-01"),
        ("events_2027_02", "2027-02-01", "2027-03-01"),
        ("events_2027_03", "2027-03-01", "2027-04-01"),
    ]
    for name, start, end in partitions:
        op.execute(f"""
            CREATE TABLE {name} PARTITION OF events
            FOR VALUES FROM ('{start}') TO ('{end}');
        """)

    op.execute("CREATE INDEX ix_events_tenant_customer_ts ON events (tenant_id, customer_id, timestamp);")
    op.execute("CREATE INDEX ix_events_tenant_type_ts    ON events (tenant_id, event_type, timestamp);")
    op.execute("CREATE INDEX ix_events_product           ON events (tenant_id, product_id) WHERE product_id IS NOT NULL;")


def downgrade():
    op.execute("DROP TABLE IF EXISTS events CASCADE;")
    op.drop_table("customers")
    op.drop_table("products")
    op.drop_table("api_keys")
    op.drop_table("users")
    op.drop_table("tenants")
    op.execute("DROP TYPE IF EXISTS event_type_enum;")
    op.execute("DROP TYPE IF EXISTS user_role;")