"""update script paths to warehouse structure

Revision ID: 6aaecfa0b827
Revises: 8d79800cd999
Create Date: 2025-01-03 09:08:13.037957

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column


# revision identifiers, used by Alembic.
revision = '6aaecfa0b827'
down_revision = '8d79800cd999'
branch_labels = None
depends_on = None

# Define a temporary table representation for the scripts table
scripts_table = table('script',
                      column('id', sa.Integer),
                      column('filename', sa.String),
                      column('name', sa.String),
                      column('warehouse_id', sa.Integer)
                      )


def map_filename(old_filename):
    """Map old filename to new warehouse-based structure"""
    filename_mapping = {
        'base_byggmakker.py': 'byggmakker/base_data.py',
        'store_byggmakker.py': 'byggmakker/store_data.py',
        'store_prices.py': 'byggmakker/prices.py',
        'retailer_byggmakker.py': 'byggmakker/retailer_data.py'
    }
    return filename_mapping.get(old_filename, old_filename)


def upgrade():
    # Create a connection
    connection = op.get_bind()

    # Get all existing scripts
    scripts = connection.execute(
        scripts_table.select()
    ).fetchall()

    # Update each script's filename
    for script in scripts:
        new_filename = map_filename(script.filename)
        if new_filename != script.filename:
            connection.execute(
                scripts_table.update()
                .where(scripts_table.c.id == script.id)
                .values(filename=new_filename)
            )


def downgrade():
    # Create a connection
    connection = op.get_bind()

    # Reverse filename mapping
    reverse_mapping = {
        'byggmakker/base_data.py': 'base_byggmakker.py',
        'byggmakker/store_data.py': 'store_byggmakker.py',
        'byggmakker/prices.py': 'store_prices.py',
        'byggmakker/retailer_data.py': 'retailer_byggmakker.py'
    }

    # Get all scripts
    scripts = connection.execute(
        scripts_table.select()
    ).fetchall()

    # Revert each script's filename
    for script in scripts:
        old_filename = reverse_mapping.get(script.filename, script.filename)
        if old_filename != script.filename:
            connection.execute(
                scripts_table.update()
                .where(scripts_table.c.id == script.id)
                .values(filename=old_filename)
            )