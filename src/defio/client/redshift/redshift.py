import asyncio
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Final, final

from attrs import define
from typing_extensions import override

from defio.client.config import PulumiDbConfig
from defio.client.postgres import PostgresClient
from defio.client.utils import get_tables_to_load
from defio.dataset import Dataset
from defio.infra.project.output import REDSHIFT_KEY_PREFIX
from defio.sql.schema import Table
from defio.utils.time import log_time

# TODO: Parameterize
_REDSHIFT_TABLE_IMPORT_FROM_S3: Final = """
COPY {table_name}
FROM 's3://{bucket_name}/{object_key}'
REGION '{region_name}'
IAM_ROLE '{iam_role_arn}'
DELIMITER '\t'
ESCAPE
IGNOREHEADER 1
NULL AS '\\\\N'
GZIP
;
"""

_REDSHIFT_VACUUM_ANALYZE: Final = "VACUUM {table_name}; ANALYZE {table_name};"


@final
@define(frozen=True)
class RedshiftClient(PostgresClient):
    """Asynchronous client for Amazon Redshift."""

    def __attrs_post_init__(self) -> None:
        # Workaround for Redshift when using `psycopg` instead of `redshift-connector`
        # See https://github.com/psycopg/psycopg/issues/122#issuecomment-1281703414
        os.environ["PGCLIENTENCODING"] = "utf-8"

    async def load_from_s3(
        self,
        /,
        *,
        dataset: Dataset,
        tables_to_load: Sequence[Table | str] | None = None,
        bucket_name: str,
        region_name: str,
        iam_role_arn: str,
        update_statistics: bool = False,
        verbose: bool = False,
    ) -> None:
        """
        Loads some tables from the given S3 bucket into this client's
        connected Redshift cluster.

        If `update_statistics` is `True`, then update the corresponding
        table statistics as well (i.e. via `VACUUM` + `ANALYZE`).
        """

        async def load(table: Table) -> None:
            with log_time(
                verbose,
                start=f"Loading table `{table.name}` from S3",
                end=lambda m: (
                    f"Finished loading table `{table.name}` "
                    f"in {m.elapsed_time.total_seconds():.2f} seconds"
                ),
            ):
                async with await self.connect() as aconn:
                    await aconn.execute_one(
                        _REDSHIFT_TABLE_IMPORT_FROM_S3.format(
                            table_name=table.name,
                            bucket_name=bucket_name,
                            object_key=f"{dataset.name}/{table.name}.tsv.gz",
                            region_name=region_name,
                            iam_role_arn=iam_role_arn,
                        )
                    )

                    # Optionally update the table statistics
                    if update_statistics:
                        await aconn.execute_one(
                            _REDSHIFT_VACUUM_ANALYZE.format(table_name=table.name)
                        )

        with log_time(
            verbose,
            start=f"Loading dataset `{dataset.name}` from S3\n---",
            end=lambda m: (
                f"---\nFinished loading dataset `{dataset.name}` "
                f"in {m.elapsed_time.total_seconds():.2f} seconds"
            ),
        ):
            # Load tables concurrently
            async with asyncio.TaskGroup() as tg:
                for table in get_tables_to_load(dataset, tables_to_load):
                    tg.create_task(load(table))


@final
@define
class PulumiRedshiftConfig(PulumiDbConfig):
    """Pulumi DB config for Amazon Redshift."""

    def __init__(
        self,
        stack_name: str,
        db_identifier: str,
        db_name: str | None = None,
    ) -> None:
        super().__init__(
            stack_name=stack_name,
            key_prefix=REDSHIFT_KEY_PREFIX,
            db_identifier=db_identifier,
            db_name=db_name,
        )

    @property
    @override
    def ssl_root_cert_path(self) -> Path:
        return Path(__file__).parent / "redshift-ca-bundle.crt"
