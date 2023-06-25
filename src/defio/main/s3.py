import asyncio

from defio.dataset.imdb import IMDB_GZ
from defio.dataset.s3 import upload_dataset_to_s3
from defio.infra.project.output import (
    AWS_REGION_NAME,
    S3_DATASETS_BUCKET_NAME,
    PulumiStackOutputs,
)


async def main() -> None:
    stack_outputs = PulumiStackOutputs("main")

    await upload_dataset_to_s3(
        dataset=IMDB_GZ,
        bucket_name=stack_outputs.get(S3_DATASETS_BUCKET_NAME),
        region_name=stack_outputs.get(AWS_REGION_NAME),
        verbose=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
