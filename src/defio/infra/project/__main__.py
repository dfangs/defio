import ipaddress
from pathlib import Path

import pulumi

from defio.infra.components.aurora import AuroraCluster, AuroraSubnetGroup
from defio.infra.components.ec2 import Instance, KeyPair
from defio.infra.components.iam import InstanceProfile, ManagedPolicy, Role
from defio.infra.components.redshift import RedshiftCluster, RedshiftSubnetGroup
from defio.infra.components.s3 import Bucket
from defio.infra.components.vpc import Vpc
from defio.infra.constants import (
    ALL_NETWORK,
    DEFAULT_PORT_POSTGRESQL,
    DEFAULT_PORT_REDSHIFT,
)
from defio.infra.helper.aurora import AuroraEngine, ClusterRoleFeature, DbInstanceClass
from defio.infra.helper.ec2 import Ami, AmiArch, AmiVariant, AmiVersion
from defio.infra.helper.iam import (
    Condition,
    PolicyDocument,
    Principal,
    Statement,
    StatementEffect,
)
from defio.infra.helper.redshift import RedshiftNodeType
from defio.infra.helper.vpc import (
    SELF_TARGET,
    GatewayEndpointService,
    SecurityGroupEgressRule,
    SecurityGroupIngressRule,
)
from defio.infra.project.output import (
    AURORA_KEY_PREFIX,
    AWS_REGION_NAME,
    HOST_KEY_SUFFIX,
    INITIAL_DBNAME_KEY_SUFFIX,
    PASSWORD_KEY_SUFFIX,
    PORT_KEY_SUFFIX,
    REDSHIFT_KEY_PREFIX,
    REDSHIFT_S3_IMPORT_ROLE_ARN,
    S3_DATASETS_BUCKET_NAME,
    USERNAME_KEY_SUFFIX,
)
from defio.infra.utils import get_aws_account_id, get_aws_region

VPC_CIDR_BLOCK = ipaddress.IPv4Network("10.0.0.0/16")
NUM_PUBLIC_SUBNETS = 2
NUM_PRIVATE_SUBNETS = 2

config = pulumi.Config()

## Core infrastructure (VPC)

with Vpc("vpc", cidr_block=VPC_CIDR_BLOCK) as vpc:
    # Create endpoints to the outside of VPC
    igw = vpc.add_internet_gateway("igw")
    s3_endpoint = vpc.add_gateway_endpoint(
        "gateway-endpoint-s3", service=GatewayEndpointService.S3
    )

    # Private subnets are connected only to the S3 endpoint
    public_route_table = vpc.add_route_table("route-table-public", targets=[igw])
    private_route_table = vpc.add_route_table(
        "route-table-private", targets=[s3_endpoint]
    )

    # Create all subnets
    public_subnets, private_subnets = vpc.add_subnets(
        "subnet",
        num_public_subnets=NUM_PUBLIC_SUBNETS,
        num_private_subnets=NUM_PRIVATE_SUBNETS,
        public_route_table=public_route_table,
        private_route_table=private_route_table,
    )

    # Create security groups
    ec2_security_group = vpc.add_security_group(
        "ec2",
        ingress_rules=[SecurityGroupIngressRule.for_ssh(ALL_NETWORK)],
        egress_rules=[SecurityGroupEgressRule.for_all_traffic(ALL_NETWORK)],
    )

    aurora_security_group = vpc.add_security_group(
        "aurora",
        ingress_rules=[
            SecurityGroupIngressRule.for_custom_tcp(
                ec2_security_group, port=DEFAULT_PORT_POSTGRESQL
            ),
            SecurityGroupIngressRule.for_all_traffic(SELF_TARGET),
        ],
        egress_rules=[SecurityGroupEgressRule.for_all_traffic(ALL_NETWORK)],
    )

    redshift_security_group = vpc.add_security_group(
        "redshift",
        ingress_rules=[
            SecurityGroupIngressRule.for_custom_tcp(
                ec2_security_group, port=DEFAULT_PORT_REDSHIFT
            ),
            SecurityGroupIngressRule.for_all_traffic(SELF_TARGET),
        ],
        egress_rules=[SecurityGroupEgressRule.for_all_traffic(ALL_NETWORK)],
    )

## Single EC2 instance with access to the databases

defio_instance = Instance(
    "defio-instance",
    subnet=public_subnets[0],
    instance_type="m6g.large",
    ami=Ami(AmiVersion.AL2023, AmiVariant.DEFAULT, AmiArch.ARM64),
    key_pair=KeyPair.from_file(
        "defio-key-pair", Path(config.require("public-key-path"))
    ),
    instance_profile=InstanceProfile("defio-instance-profile"),
    security_groups=[ec2_security_group],
)

## S3 bucket to store all datasets and related files + related IAM role & policy

defio_bucket = Bucket("defio-datasets")

defio_s3_import_policy = ManagedPolicy(
    "defio-s3-import-policy",
    policy_document=PolicyDocument(
        Statement=[
            Statement(
                Sid="s3Import",
                Effect=StatementEffect.ALLOW,
                Action=[
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                Resource=[
                    defio_bucket.get_arn(),
                    f"{defio_bucket.get_arn()}/*",
                ],
            )
        ]
    ),
)

aurora_s3_import_role = Role(
    "defio-aurora-s3-import-role",
    trust_policy=PolicyDocument(
        Statement=[
            Statement(
                Effect=StatementEffect.ALLOW,
                Principal=Principal(Service="rds.amazonaws.com"),
                Action="sts:AssumeRole",
                Condition=Condition(
                    StringLike={
                        "aws:SourceArn": [
                            f"arn:aws:rds:{get_aws_region()}:{get_aws_account_id()}:cluster:defio-*",
                        ]
                    }
                ),
            )
        ]
    ),
    managed_policies=[
        defio_s3_import_policy,
    ],
)

redshift_s3_import_role = Role(
    "defio-redshift-s3-import-role",
    trust_policy=PolicyDocument(
        Statement=[
            Statement(
                Effect=StatementEffect.ALLOW,
                Principal=Principal(Service="redshift.amazonaws.com"),
                Action="sts:AssumeRole",
                Condition=Condition(
                    StringLike={
                        "sts:ExternalId": [
                            f"arn:aws:redshift:{get_aws_region()}:{get_aws_account_id()}:dbuser:defio-*/*",
                        ]
                    }
                ),
            )
        ]
    ),
    managed_policies=[
        defio_s3_import_policy,
    ],
)

## Database clusters

aurora_clusters = [
    AuroraCluster(
        "defio-aurora",
        num_instances=1,
        engine=AuroraEngine.POSTGRESQL_15,
        instance_class=DbInstanceClass.R6G_LARGE,
        subnet_group=AuroraSubnetGroup("defio-aurora-subnets", subnets=private_subnets),
        master_username=config.require_secret("db-username"),
        master_password=config.require_secret("db-password"),
        skip_final_snapshot=True,
        publicly_accessible=False,
        initial_database_name="defio",
        security_groups=[aurora_security_group],
        iam_roles={aurora_s3_import_role: ClusterRoleFeature.S3_IMPORT},
        apply_immediately=True,
    )
]

redshift_clusters = [
    RedshiftCluster(
        "defio-redshift",
        num_nodes=1,
        node_type=RedshiftNodeType.DC2_LARGE,
        subnet_group=RedshiftSubnetGroup(
            "defio-redshift-subnets", subnets=private_subnets
        ),
        master_username=config.require_secret("db-username"),
        master_password=config.require_secret("db-password"),
        skip_final_snapshot=True,
        publicly_accessible=False,
        initial_database_name="defio",
        security_groups=[redshift_security_group],
        iam_roles=[redshift_s3_import_role],
        apply_immediately=True,
    )
]

# Export values
pulumi.export(AWS_REGION_NAME, get_aws_region())
pulumi.export(S3_DATASETS_BUCKET_NAME, defio_bucket.name)
pulumi.export(REDSHIFT_S3_IMPORT_ROLE_ARN, redshift_s3_import_role.arn)

for key_prefix, clusters in (
    (AURORA_KEY_PREFIX, aurora_clusters),
    (REDSHIFT_KEY_PREFIX, redshift_clusters),
):
    for cluster in clusters:
        pulumi.export(
            f"{key_prefix}:{cluster.get_id()}:{HOST_KEY_SUFFIX}",
            cluster.endpoint,
        )
        pulumi.export(
            f"{key_prefix}:{cluster.get_id()}:{PORT_KEY_SUFFIX}",
            cluster.port,
        )
        pulumi.export(
            f"{key_prefix}:{cluster.get_id()}:{USERNAME_KEY_SUFFIX}",
            cluster.username,
        )
        pulumi.export(
            f"{key_prefix}:{cluster.get_id()}:{PASSWORD_KEY_SUFFIX}",
            cluster.password,
        )
        pulumi.export(
            f"{key_prefix}:{cluster.get_id()}:{INITIAL_DBNAME_KEY_SUFFIX}",
            cluster.initial_database_name,
        )
