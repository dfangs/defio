from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, assert_never

from attrs import define
from immutables import Map
from pulumi import automation as auto

from defio.infra.components.aurora import AuroraCluster
from defio.infra.components.redshift import RedshiftCluster
from defio.infra.constants import PROJECT_NAME

# Used by both `client` and `infra` subpackages to relay Pulumi stack outputs
PULUMI_PROJECT_PATH: Final = Path(__file__).parent
AURORA_KEY_PREFIX: Final = "aurora"
REDSHIFT_KEY_PREFIX: Final = "redshift"

AWS_REGION_NAME: Final = "aws:region-name"
S3_DATASETS_BUCKET_NAME: Final = "s3:datasets-bucket-name"
REDSHIFT_S3_IMPORT_ROLE_ARN: Final = "redshift:s3-import-role-arn"
EC2_INSTANCE_PUBLIC_DNS: Final = "ec2:instance-public-dns"

HOST_KEY_SUFFIX: Final = "host"
PORT_KEY_SUFFIX: Final = "port"
USERNAME_KEY_SUFFIX: Final = "username"
PASSWORD_KEY_SUFFIX: Final = "password"
INITIAL_DBNAME_KEY_SUFFIX: Final = "initial-dbname"


def create_dbconn_param_export_key(
    cluster: AuroraCluster | RedshiftCluster,
    suffix: str,
    *,
    for_ssm: bool = False,
) -> str:
    match cluster:
        case AuroraCluster():
            key_prefix = AURORA_KEY_PREFIX
        case RedshiftCluster():
            key_prefix = REDSHIFT_KEY_PREFIX
        case _:
            assert_never(cluster)

    if for_ssm:
        # Note the leading slash
        return f"/{PROJECT_NAME}/{key_prefix}/{cluster.get_id()}/{suffix}"

    return f"{key_prefix}:{cluster.get_id()}:{suffix}"


@define(frozen=True)
class PulumiStackOutputs:
    """
    Represents the stack outputs of the (only) Pulumi project
    in this package.
    """

    _outputs: Mapping[str, Any]

    def __init__(self, stack_name: str) -> None:
        try:
            stack = auto.select_stack(
                stack_name=stack_name, work_dir=str(PULUMI_PROJECT_PATH.resolve())
            )
        except auto.errors.StackNotFoundError as exc:
            raise ValueError(
                f"Stack `{stack_name}` does not exist in this Pulumi project"
            ) from exc

        object.__setattr__(
            self,
            "_outputs",
            # Note: The call to `stack.outputs()` is unfortunately slow (about 1-2 s)
            Map({key: output.value for key, output in stack.outputs().items()}),
        )

    def get(self, key: str) -> Any:
        """
        Gets the corresponding stack output value of the given key.

        Raises a `KeyError` if the given key does not exist.
        """
        try:
            return self._outputs[key]
        except KeyError as exc:
            raise KeyError(f"Key `{key}` does not exist in this stack outputs") from exc
