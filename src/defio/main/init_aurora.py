import asyncio
import sys

from defio.client.aurora import AuroraClient, PulumiAuroraConfig
from defio.dataset.imdb import IMDB_GZ
from defio.infra.project.output import (
    AWS_REGION_NAME,
    S3_DATASETS_BUCKET_NAME,
    PulumiStackOutputs,
)


async def main():
    client = AuroraClient.from_config(
        PulumiAuroraConfig(
            stack_name="main",
            db_identifier=f"defio-aurora-{sys.argv[1]}",
        ).with_overrides(host="localhost")
    )

    stack_outputs = PulumiStackOutputs("main")

    await client.create_tables(schema_path=IMDB_GZ.schema_path, verbose=True)
    await client.load_from_s3(
        dataset=IMDB_GZ,
        bucket_name=stack_outputs.get(S3_DATASETS_BUCKET_NAME),
        region_name=stack_outputs.get(AWS_REGION_NAME),
        update_statistics=True,
        verbose=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
