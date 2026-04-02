"""Enable Row-Level Security on all tenant-scoped tables.

Separate migration from table creation - schema correctness and
security are different concerns, easier to review independently.

How it works:
- App sets: SET LOCAL app.current_tenant = '<uuid>' per transaction
- Policies read that via current_setting('app.current_tenant', true)
- 'true' means return NULL if unset -> NULL never matches a UUID -> fail-closed

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-26
"""
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

TENANT_ID_TABLES = ["users", "products", "customers", "events"]


def _create_policies(table, tenant_col):
    condition = f"{tenant_col}::text = current_setting('app.current_tenant', true)"
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(f"CREATE POLICY {table}_select ON {table} FOR SELECT TO app_user USING ({condition});")
    op.execute(f"CREATE POLICY {table}_insert ON {table} FOR INSERT TO app_user WITH CHECK ({condition});")
    op.execute(f"CREATE POLICY {table}_update ON {table} FOR UPDATE TO app_user USING ({condition}) WITH CHECK ({condition});")
    op.execute(f"CREATE POLICY {table}_delete ON {table} FOR DELETE TO app_user USING ({condition});")


def upgrade():
    # Tenants table filters on id (tenant sees only itself)
    _create_policies("tenants", "id")

    for table in TENANT_ID_TABLES:
        _create_policies(table, "tenant_id")

    # Grant DML to app_user
    for table in ["tenants"] + TENANT_ID_TABLES + ["api_keys"]:
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO app_user;")
        op.execute(f"GRANT ALL ON {table} TO app_admin;")



def downgrade():
    for table in ["tenants"] + TENANT_ID_TABLES + ["api_keys"]:
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY;")
        op.execute(f"DROP POLICY IF EXISTS {table}_select ON {table};")
        op.execute(f"DROP POLICY IF EXISTS {table}_insert ON {table};")
        op.execute(f"DROP POLICY IF EXISTS {table}_update ON {table};")
        op.execute(f"DROP POLICY IF EXISTS {table}_delete ON {table};")
        op.execute(f"REVOKE ALL ON {table} FROM app_user;")