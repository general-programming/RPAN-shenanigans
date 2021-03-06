"""streams folder name, files

Revision ID: 52c47e38c3f4
Revises: 42903ff520ee
Create Date: 2019-08-25 00:32:47.292746

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '52c47e38c3f4'
down_revision = '42903ff520ee'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('streams', sa.Column('files', sa.ARRAY(sa.String()), nullable=True))
    op.add_column('streams', sa.Column('raw_foldername', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('streams', 'raw_foldername')
    op.drop_column('streams', 'files')
    # ### end Alembic commands ###
