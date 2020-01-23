# This Python file uses the following encoding: utf-8
import os

from sqlalchemy import Column, Integer, String, create_engine, inspect, DateTime, ForeignKey, Enum, Boolean, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, relation
from sqlalchemy.schema import Index

debug = os.environ.get('DEBUG', False)

engine = create_engine(os.environ["DB_URL"], convert_unicode=True, pool_recycle=3600)

if debug:
    engine.echo = True

sm = sessionmaker(autocommit=False,
                  autoflush=False,
                  bind=engine)

base_session = scoped_session(sm)

Base = declarative_base()
Base.query = base_session.query_property()


class SeedResponse(Base):
    __tablename__ = 'seed_responses'

    id = Column(Integer, primary_key=True)

    time = Column(DateTime, nullable=False)
    checked = Column(Boolean, default=False, server_default='f')
    response = Column(JSONB, nullable=False)

class Stream(Base):
    __tablename__ = "streams"

    id = Column(Integer, primary_key=True)

    author = Column(String, nullable=False)
    post_id = Column(String, nullable=False, unique=True)
    title = Column(String, nullable=False)
    hls_id = Column(String)

    raw_foldername = Column(String)
    files = Column(ARRAY(String))

class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True)

    time = Column(DateTime, nullable=False)
    response = Column(JSONB, nullable=False)

# Indexes

# Index("index_events_event_type", Event.event_type)