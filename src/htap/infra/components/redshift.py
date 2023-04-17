from collections.abc import Sequence

import pulumi
import pulumi_aws as aws

from htap.infra.components.iam import Role
from htap.infra.components.vpc import SecurityGroup, Subnet
from htap.infra.helper.redshift import RedshiftNodeType
from htap.infra.utils import ComponentMixin


class RedshiftSubnetGroup(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for Amazon Redshift Cluster Subnet Group.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        subnets: Sequence[Subnet],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        if len(subnets) == 0:
            raise ValueError("Subnets cannot be empty")

        self._subnet_group = aws.redshift.SubnetGroup(
            name,
            subnet_ids=[subnet.id for subnet in subnets],
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"name": self.name})

    @property
    def name(self) -> pulumi.Output[str]:
        """Returns the name of this cluster subnet group."""
        return self._subnet_group.name


class RedshiftCluster(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for Amazon Redshift Cluster.
    """

    def __init__(
        self,
        cluster_identifier: str,
        /,
        *,
        num_nodes: int,
        node_type: RedshiftNodeType,
        subnet_group: RedshiftSubnetGroup,
        master_username: pulumi.Input[str],
        master_password: pulumi.Input[str],
        skip_final_snapshot: bool,
        publicly_accessible: bool,
        initial_database_name: str | None = None,
        security_groups: Sequence[SecurityGroup] = (),
        iam_roles: Sequence[Role] = (),
        availability_zone: str | None = None,
        enable_availability_zone_relocation: bool = False,
        enable_encryption: bool = True,
        automated_snapshot_retention_period: int = 1,
        apply_immediately: bool = False,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), cluster_identifier, opts=opts)

        if publicly_accessible and enable_availability_zone_relocation:
            raise ValueError(
                "Incompatible combination: Both `publicly_accessible` and "
                "`enable_availability_zone_relocation` cannot be `True`"
            )

        self._cluster = aws.redshift.Cluster(
            cluster_identifier,
            cluster_identifier=cluster_identifier,
            number_of_nodes=num_nodes,
            node_type=node_type,
            cluster_subnet_group_name=subnet_group.name,
            master_username=master_username,
            master_password=master_password,
            skip_final_snapshot=skip_final_snapshot,
            final_snapshot_identifier=f"{cluster_identifier}-final-snapshot",
            publicly_accessible=publicly_accessible,
            database_name=initial_database_name,
            # cluster_parameter_group_name=cluster_parameter_group.name,
            vpc_security_group_ids=(
                [sg.id for sg in security_groups] if len(security_groups) > 0 else None
            ),
            iam_roles=(
                [role.arn for role in iam_roles] if len(iam_roles) > 0 else None
            ),
            default_iam_role_arn=(iam_roles[0].arn if len(iam_roles) > 0 else None),
            availability_zone=availability_zone,
            availability_zone_relocation_enabled=enable_availability_zone_relocation,
            encrypted=enable_encryption,
            automated_snapshot_retention_period=automated_snapshot_retention_period,
            enhanced_vpc_routing=True,
            apply_immediately=apply_immediately,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this Redshift cluster."""
        return self._cluster.cluster_identifier
