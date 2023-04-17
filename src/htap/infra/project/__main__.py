import ipaddress
from pathlib import Path

import pulumi

from htap.infra.components.aurora import AuroraCluster, AuroraSubnetGroup
from htap.infra.components.ec2 import Instance, KeyPair
from htap.infra.components.iam import InstanceProfile, ManagedPolicy, Role
from htap.infra.components.redshift import RedshiftCluster, RedshiftSubnetGroup
from htap.infra.components.s3 import Bucket
from htap.infra.components.vpc import Vpc
from htap.infra.constants import (
    ALL_NETWORK,
    DEFAULT_PORT_POSTGRESQL,
    DEFAULT_PORT_REDSHIFT,
)
from htap.infra.helper.aurora import AuroraEngine, ClusterRoleFeature, DbInstanceClass
from htap.infra.helper.ec2 import Ami, AmiArch, AmiVariant, AmiVersion
from htap.infra.helper.iam import (
    Condition,
    PolicyDocument,
    Principal,
    Statement,
    StatementEffect,
)
from htap.infra.helper.redshift import RedshiftNodeType
from htap.infra.helper.vpc import (
    SELF_TARGET,
    GatewayEndpointService,
    SecurityGroupEgressRule,
    SecurityGroupIngressRule,
)
from htap.infra.utils import get_aws_account_id, get_aws_region

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

htap_instance = Instance(
    "htap-instance",
    subnet=public_subnets[0],
    instance_type="m6g.large",
    ami=Ami(AmiVersion.AL2023, AmiVariant.DEFAULT, AmiArch.ARM64),
    key_pair=KeyPair.from_file(
        "htap-key-pair", Path(config.require("public-key-path"))
    ),
    instance_profile=InstanceProfile("htap-instance-profile"),
    security_groups=[ec2_security_group],
)

## S3 bucket to store all datasets and related files + related IAM role & policy

htap_bucket = Bucket("htap-datasets")

htap_s3_import_policy = ManagedPolicy(
    "htap-s3-import-policy",
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
                    htap_bucket.arn,
                    f"{htap_bucket.arn}/*",
                ],
            )
        ]
    ),
)

htap_s3_import_role = Role(
    "htap-s3-import-role",
    trust_policy=PolicyDocument(
        Statement=[
            Statement(
                Effect=StatementEffect.ALLOW,
                Principal=Principal(
                    Service=[
                        "rds.amazonaws.com",
                        "redshift.amazonaws.com",
                    ]
                ),
                Action="sts:AssumeRole",
                Condition=Condition(
                    StringLike={
                        "aws:SourceArn": [
                            f"arn:aws:rds:{get_aws_region()}:{get_aws_account_id()}:cluster:htap-*",
                            f"arn:aws:redshift:{get_aws_region()}:{get_aws_account_id()}:cluster:htap-*",
                        ]
                    }
                ),
            )
        ]
    ),
    managed_policies=[
        htap_s3_import_policy,
    ],
)

## Database clusters

aurora_cluster = AuroraCluster(
    "htap-aurora",
    num_instances=1,
    engine=AuroraEngine.POSTGRESQL_15,
    instance_class=DbInstanceClass.R6G_LARGE,
    subnet_group=AuroraSubnetGroup("htap-aurora-subnets", subnets=private_subnets),
    master_username=config.require("db-username"),
    master_password=config.require("db-password"),
    skip_final_snapshot=True,
    publicly_accessible=False,
    initial_database_name="htap",
    security_groups=[aurora_security_group],
    iam_roles={htap_s3_import_role: ClusterRoleFeature.S3_IMPORT},
    apply_immediately=True,
)

redshift_cluster = RedshiftCluster(
    "htap-redshift",
    num_nodes=1,
    node_type=RedshiftNodeType.DC2_LARGE,
    subnet_group=RedshiftSubnetGroup("htap-redshift-subnets", subnets=private_subnets),
    master_username=config.require("db-username"),
    master_password=config.require("db-password"),
    skip_final_snapshot=True,
    publicly_accessible=False,
    initial_database_name="htap",
    security_groups=[redshift_security_group],
    iam_roles=[htap_s3_import_role],
    apply_immediately=True,
)
