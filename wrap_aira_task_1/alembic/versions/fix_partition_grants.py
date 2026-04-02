"""Grant app_user permissions on individual event partition tables.

Postgres propagates RLS policies to partitions but NOT grants.
Discovered when INSERT to events failed with:
  permission denied for table events_2026_04

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-26
"""
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

PARTITIONS = (
    [f"events_2026_{m:02d}" for m in range(1, 13)] +
    [f"events_2027_{m:02d}" for m in range(1, 4)]
)


def upgrade():
    for p in PARTITIONS:
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {p} TO app_user;")
        op.execute(f"GRANT ALL ON {p} TO app_admin;")


def downgrade():
    for p in PARTITIONS:
        op.execute(f"REVOKE ALL ON {p} FROM app_user;")
        op.execute(f"REVOKE ALL ON {p} FROM app_admin;")