# api/deps.py
from typing import Generator
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://testuser:test123@localhost:5432/testdb",
)

# Single engine for the process  
engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

def get_conn() -> Generator[Connection, None, None]:
    """
    FastAPI dependency that yields a SQLAlchemy Connection.
    IMPORTANT: Do NOT decorate with @contextmanager.
    """
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
