"""Database connection management."""
import os
import asyncpg
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://ghostkitchen:ghostkitchen@localhost:5432/ghostkitchen",
)

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


def get_sync_conn():
    """Synchronous psycopg2 connection for pipeline thread."""
    return psycopg2.connect(DATABASE_URL)


async def init_schema():
    """Run schema.sql once at startup."""
    pool = await get_pool()
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    async with pool.acquire() as conn:
        await conn.execute(sql)
