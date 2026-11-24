from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from src.config.settings import DB_URL

class Base(DeclarativeBase):
    pass

engine = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=240)
Session = sessionmaker(engine, expire_on_commit=False)


def connection():
    with Session() as db:
        yield db


def migrate():
    """建表并补齐既有库缺失的列，见 migrations/002_run_progress.md。"""
    Base.metadata.create_all(engine)
    if 'progress' not in {column['name'] for column in inspect(engine).get_columns('runs')}:
        with engine.begin() as connection:
            connection.execute(text('ALTER TABLE runs ADD COLUMN progress JSON'))
