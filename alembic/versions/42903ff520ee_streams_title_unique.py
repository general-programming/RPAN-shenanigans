"""streams title, unique

Revision ID: 42903ff520ee
Revises: f39e55083124
Create Date: 2019-08-24 23:59:52.783300

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '42903ff520ee'
down_revision = 'f39e55083124'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('streams', sa.Column('title', sa.String(), nullable=False))
    op.create_unique_constraint(None, 'streams', ['post_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'streams', type_='unique')
    op.drop_column('streams', 'title')
    # ### end Alembic commands ###
