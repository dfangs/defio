import asyncio
import sys

from defio.client.redshift import PulumiRedshiftConfig, RedshiftClient
from defio.dataset.imdb import IMDB_GZ
from defio.infra.project.output import (
    AWS_REGION_NAME,
    REDSHIFT_S3_IMPORT_ROLE_ARN,
    S3_DATASETS_BUCKET_NAME,
    PulumiStackOutputs,
)


async def main():
    client = RedshiftClient.from_config(
        PulumiRedshiftConfig(
            stack_name="main",
            db_identifier=f"defio-redshift-{sys.argv[1]}",
        ).with_overrides(host="localhost")
    )

    stack_outputs = PulumiStackOutputs("main")

    await client.create_tables(schema_path=IMDB_GZ.schema_path, verbose=True)
    await client.load_from_s3(
        dataset=IMDB_GZ,
        bucket_name=stack_outputs.get(S3_DATASETS_BUCKET_NAME),
        region_name=stack_outputs.get(AWS_REGION_NAME),
        iam_role_arn=stack_outputs.get(REDSHIFT_S3_IMPORT_ROLE_ARN),
        update_statistics=True,
        verbose=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
