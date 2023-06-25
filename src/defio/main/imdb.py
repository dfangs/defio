"""
Generates the "normalized" version of the IMDB dataset.
"""
import asyncio

from defio.dataset.imdb import IMDB_DIR
from defio.dataset.imdb.normalized.generate import generate_tables

SOURCE_TSV_DIR = IMDB_DIR / "source" / "tsv"
TARGET_TSV_DIR = IMDB_DIR / "normalized" / "tsv"
TARGET_GZ_DIR = IMDB_DIR / "normalized" / "gz"


async def main() -> None:
    # Create directories if not exist
    for dirpath in (SOURCE_TSV_DIR, TARGET_TSV_DIR, TARGET_GZ_DIR):
        dirpath.mkdir(parents=True, exist_ok=True)

    # Generate all tables and compress them into gzip
    # Both operations are pipelined and done concurrently
    await generate_tables(SOURCE_TSV_DIR, TARGET_TSV_DIR, TARGET_GZ_DIR, verbose=True)


if __name__ == "__main__":
    asyncio.run(main())
