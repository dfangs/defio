from pathlib import Path
from typing import Final

# Used by both `client` and `infra` subpackages to relay Pulumi stack outputs
PULUMI_PROJECT_PATH: Final = Path(__file__).parent
AURORA_KEY_PREFIX: Final = "aurora"
REDSHIFT_KEY_PREFIX: Final = "redshift"

AWS_REGION_NAME: Final = "aws:region-name"
S3_DATASETS_BUCKET_NAME: Final = "s3:datasets-bucket-name"
REDSHIFT_S3_IMPORT_ROLE_ARN: Final = "redshift:s3-import-role-arn"

HOST_KEY_SUFFIX: Final = "host"
PORT_KEY_SUFFIX: Final = "port"
USERNAME_KEY_SUFFIX: Final = "username"
PASSWORD_KEY_SUFFIX: Final = "password"
INITIAL_DBNAME_KEY_SUFFIX: Final = "initial-dbname"
