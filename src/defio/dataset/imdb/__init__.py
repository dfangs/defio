import csv
from pathlib import Path
from typing import Final

from defio.dataset import Dataset, DatasetLoadConfig

IMDB_DIR: Final = Path(__file__).parent

# NOTE:
# (1) pandas `read_csv()` is only compatible with `\N` as the null value
#     if escape ('\') is disabled.
# (2) Postgres defaults to `\N` as the null value for non-CSV format.
# (3) Redshift accepts both `\N` and `\\N`, but is more annoying to work with
#     if we use the latter.
# (4) This dataset conveniently does not use escapes, so it works well for us
#     as we can simply use '\N'.

# Only expose normalized datasets

IMDB_TSV: Final = Dataset(
    name="imdb-tsv",
    directory=IMDB_DIR / "normalized",
    schema_filename="schema.sql",
    stats_filename="stats.json",
    tables_dirname="tsv",
    load_config=DatasetLoadConfig(
        delimiter="\t",
        skip_header=False,
        na_value=r"\N",
        quoting=csv.QUOTE_NONE,
        escape_char=None,
    ),
)

IMDB_GZ: Final = Dataset(
    name="imdb-gz",
    directory=IMDB_DIR / "normalized",
    schema_filename="schema.sql",
    stats_filename="stats.json",
    tables_dirname="gz",
    load_config=DatasetLoadConfig(
        delimiter="\t",
        skip_header=False,
        na_value=r"\N",
        quoting=csv.QUOTE_NONE,
        escape_char=None,
    ),
)
