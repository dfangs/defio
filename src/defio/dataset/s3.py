import asyncio
from pathlib import Path

import humanize
from aiobotocore.session import get_session

from defio.dataset import Dataset
from defio.utils.logging import log_around


async def upload_dataset_to_s3(
    *,
    dataset: Dataset,
    bucket_name: str,
    region_name: str,
    verbose: bool = False,
) -> None:
    """
    Uploads all tables of the given schema into the specified S3 bucket.

    Each table must be an uncompressed or gzip-compressed TSV file
    (i.e. with a `.tsv` or `.tsv.gz` file extension).
    """
    assert dataset.tables_dirpath.exists()

    async with asyncio.TaskGroup() as tg:
        for path in dataset.tables_dirpath.iterdir():
            if ".tsv" not in path.suffixes:
                continue
            tg.create_task(
                upload_table_to_s3(
                    table_path=path,
                    dataset_name=dataset.name,
                    bucket_name=bucket_name,
                    region_name=region_name,
                    verbose=verbose,
                )
            )


async def upload_table_to_s3(
    *,
    table_path: Path,
    dataset_name: str,
    bucket_name: str,
    region_name: str,
    verbose: bool = False,
) -> None:
    """
    Uploads the given DB table into the specified S3 bucket, namespaced by
    the name of the dataset it belongs to.

    The given table must be an uncompressed or gzip-compressed TSV file
    (i.e. with a `.tsv` or `.tsv.gz` file extension).

    Raises a `ValueError` if the file has the wrong extension.
    """
    assert table_path.exists()

    session = get_session()

    with open(table_path, mode="rb") as f:
        table_name = table_path.name
        size_in_bytes = table_path.stat().st_size

        with log_around(
            verbose,
            start=(
                f"Uploading table `{table_name}` "
                f"({humanize.naturalsize(size_in_bytes, binary=True)}) to S3"
            ),
            end=f"Finished uploading table `{table_name}`",
        ):
            if table_path.suffix == ".tsv":
                content_args = {
                    "ContentType": "text/tab-separated-values",
                }
            elif table_path.suffixes == [".tsv", ".gz"]:
                content_args = {
                    "ContentEncoding": "gzip",
                    "ContentType": "application/x-gzip",
                }
            else:
                raise ValueError("File must be compressed/uncompressed TSV")

            async with session.create_client("s3", region_name=region_name) as client:
                await client.put_object(
                    Bucket=bucket_name,
                    Key=f"{dataset_name}/{table_name}",
                    Body=f,
                    ChecksumAlgorithm="SHA256",
                    **content_args,
                )
