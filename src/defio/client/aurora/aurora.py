import asyncio
from collections.abc import Sequence
from pathlib import Path
from typing import Final, final

from attrs import define
from typing_extensions import override

from defio.client.config import PulumiDbConfig
from defio.client.postgres import PostgresClient
from defio.client.utils import get_tables_to_load
from defio.dataset import Dataset
from defio.infra.project.output import AURORA_KEY_PREFIX
from defio.sql.schema import Table
from defio.utils.time import log_time

_AURORA_CREATE_AWS_S3: Final = "CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;"

_AURORA_DISABLE_FOREIGN_KEY_CHECKS: Final = "SET session_replication_role = 'replica';"

_AURORA_ENABLE_FOREIGN_KEY_CHECKS: Final = "SET session_replication_role = 'origin';"

# TODO: Parameterize
_AURORA_TABLE_IMPORT_FROM_S3: Final = """
SELECT aws_s3.table_import_from_s3(
    '{table_name}',
    '',
    '(FORMAT text, DELIMITER ''\t'', NULL ''\\N'', HEADER)',
    aws_commons.create_s3_uri('{bucket_name}', '{object_key}', '{region_name}')
);
"""

_AURORA_VACUUM_ANALYZE: Final = "VACUUM ANALYZE {table_name};"


@final
@define(frozen=True)
class AuroraClient(PostgresClient):
    """Asynchronous client for Amazon Aurora."""

    async def load_from_s3(
        self,
        /,
        *,
        dataset: Dataset,
        tables_to_load: Sequence[Table | str] | None = None,
        bucket_name: str,
        region_name: str,
        update_statistics: bool = False,
        verbose: bool = False,
    ) -> None:
        """
        Loads some tables from the given S3 bucket into this client's
        connected Aurora cluster.

        If `update_statistics` is `True`, then update the corresponding
        table statistics as well (i.e. via `VACUUM ANALYZE`).
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
                    # Temporarily disable all foreign key checks
                    # This allows for concurrent loading of the tables
                    # See https://stackoverflow.com/a/49584660
                    await aconn.execute_one(_AURORA_DISABLE_FOREIGN_KEY_CHECKS)

                    # Load table from S3
                    await aconn.execute_one(
                        _AURORA_TABLE_IMPORT_FROM_S3.format(
                            table_name=table.name,
                            bucket_name=bucket_name,
                            object_key=f"{dataset.name}/{table.name}.tsv.gz",
                            region_name=region_name,
                        )
                    )

                    # Re-enable foreign key checks
                    await aconn.execute_one(_AURORA_ENABLE_FOREIGN_KEY_CHECKS)

                    # Optionally update the table statistics
                    if update_statistics:
                        await aconn.execute_one(
                            _AURORA_VACUUM_ANALYZE.format(table_name=table.name)
                        )

        with log_time(
            verbose,
            start=f"Loading dataset `{dataset.name}` from S3\n---",
            end=lambda m: (
                f"---\nFinished loading dataset `{dataset.name}` "
                f"in {m.elapsed_time.total_seconds():.2f} seconds"
            ),
        ):
            # Install the `aws_s3` extension beforehand
            async with await self.connect() as aconn:
                await aconn.execute_one(_AURORA_CREATE_AWS_S3)

            # Load tables concurrently
            async with asyncio.TaskGroup() as tg:
                for table in get_tables_to_load(dataset, tables_to_load):
                    tg.create_task(load(table))


@final
@define
class PulumiAuroraConfig(PulumiDbConfig):
    """Pulumi DB config for Amazon Aurora."""

    def __init__(
        self,
        stack_name: str,
        db_identifier: str,
        db_name: str | None = None,
    ) -> None:
        super().__init__(
            stack_name=stack_name,
            key_prefix=AURORA_KEY_PREFIX,
            db_identifier=db_identifier,
            db_name=db_name,
        )

    @property
    @override
    def ssl_root_cert_path(self) -> Path:
        return Path(__file__).parent / "aurora-ca-bundle.pem"
