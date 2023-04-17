from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import pulumi
import pulumi_aws as aws

from htap.infra.components.iam import AwsManagedPolicy, Role
from htap.infra.components.vpc import SecurityGroup, Subnet
from htap.infra.helper.aurora import AuroraEngine, DbEngineMode, DbInstanceClass
from htap.infra.helper.iam import (
    Condition,
    PolicyDocument,
    Principal,
    Statement,
    StatementEffect,
)
from htap.infra.utils import ComponentMixin, get_aws_account_id, get_aws_region


class AuroraSubnetGroup(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for Amazon RDS DB Subnet Group.
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

        self._subnet_group = aws.rds.SubnetGroup(
            name,
            subnet_ids=[subnet.id for subnet in subnets],
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"name": self.name})

    @property
    def name(self) -> pulumi.Output[str]:
        """Returns the name of this DB subnet group."""
        return self._subnet_group.name


class AuroraInstance(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for Amazon Aurora DB Instance.
    """

    def __init__(
        self,
        instance_identifier: str,
        /,
        *,
        cluster: AuroraCluster,
        engine: AuroraEngine,
        instance_class: DbInstanceClass,
        subnet_group: AuroraSubnetGroup,
        publicly_accessible: bool,
        availability_zone: str | None = None,
        enable_performance_insights: bool = True,
        performance_insights_retention_period: int = 7,
        enable_enhanced_monitoring: bool = False,
        monitoring_interval: Literal[0, 1, 5, 10, 15, 30, 60] = 60,
        monitoring_role: Role | None = None,
        ca_cert_identifier: str = "rds-ca-rsa2048-g1",
        apply_immediately: bool = False,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), instance_identifier, opts=opts)

        if (
            performance_insights_retention_period not in {7, 731}
            and performance_insights_retention_period % 31 != 0
        ):
            raise ValueError("Invalid value performance insights retention period")

        if enable_enhanced_monitoring and monitoring_role is None:
            raise ValueError(
                "Must provide monitoring Role to enable enhanced monitoring"
            )

        self._instance = aws.rds.ClusterInstance(
            instance_identifier,
            identifier=instance_identifier,
            cluster_identifier=cluster.id,
            engine=engine.engine_type,
            engine_version=engine.engine_version,
            instance_class=instance_class,
            db_subnet_group_name=subnet_group.name,
            publicly_accessible=publicly_accessible,
            availability_zone=availability_zone,
            performance_insights_enabled=enable_performance_insights,
            performance_insights_retention_period=performance_insights_retention_period,
            monitoring_interval=(
                monitoring_interval if enable_enhanced_monitoring else 0
            ),
            monitoring_role_arn=(
                monitoring_role.arn
                if enable_enhanced_monitoring and monitoring_role is not None
                else None
            ),
            ca_cert_identifier=ca_cert_identifier,
            apply_immediately=apply_immediately,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this DB instance."""
        return self._instance.identifier


class AuroraCluster(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for provisioned Amazon Aurora cluster.
    """

    def __init__(
        self,
        cluster_identifier: str,
        /,
        *,
        num_instances: int,
        engine: AuroraEngine,
        instance_class: DbInstanceClass,
        subnet_group: AuroraSubnetGroup,
        master_username: pulumi.Input[str],
        master_password: pulumi.Input[str],
        skip_final_snapshot: bool,
        publicly_accessible: bool,
        initial_database_name: str | None = None,
        security_groups: Sequence[SecurityGroup] = (),
        iam_roles: Sequence[Role] = (),
        enable_encryption: bool = True,
        enable_cloudwatch_logs_exports: bool = True,
        backup_retention_period: int = 1,
        deletion_protection: bool = False,
        availability_zone: str | None = None,
        enable_performance_insights: bool = True,
        performance_insights_retention_period: int = 7,
        enable_enhanced_monitoring: bool = False,
        monitoring_interval: Literal[0, 1, 5, 10, 15, 30, 60] = 60,
        ca_cert_identifier: str = "rds-ca-rsa2048-g1",
        apply_immediately: bool = False,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), cluster_identifier, opts=opts)

        if num_instances < 1:
            raise ValueError("There must be at least one instance")

        if master_username in {"admin"}:
            # Note: There are many other reserved words
            raise ValueError(
                f"`{master_username}` cannot be used as a username as it is a reserved word"
            )

        if not 1 <= backup_retention_period <= 35:
            raise ValueError("Backup retention period must be in [1, 35]")

        # Note: It's fine to create one for each cluster we create
        # (1) AWS quota for parameter groups > the quota for clusters
        # (2) This allows for cluster-specific customization
        cluster_parameter_group = aws.rds.ClusterParameterGroup(
            f"{cluster_identifier}-cluster-parameter-group",
            family=engine.parameter_group_family,
            parameters=[
                # Load these useful libraries
                aws.rds.ClusterParameterGroupParameterArgs(
                    name="shared_preload_libraries",
                    value="auto_explain,pg_stat_statements,pg_hint_plan,pgaudit",
                    apply_method="pending-reboot",  # Required for static parameters
                ),
                # TODO: Only allow connections via SSL (i.e. with certificates)
                aws.rds.ClusterParameterGroupParameterArgs(
                    name="rds.force_ssl",
                    value="0",
                ),
            ],
            opts=pulumi.ResourceOptions(parent=self),
        )

        # Share the same monitoring role for all DB instances
        monitoring_role = (
            Role(
                f"{cluster_identifier}-monitoring-role",
                trust_policy=PolicyDocument(
                    Statement=[
                        Statement(
                            Effect=StatementEffect.ALLOW,
                            Principal=Principal(Service="monitoring.rds.amazonaws.com"),
                            Action="sts:AssumeRole",
                            Condition=Condition(
                                StringLike={
                                    "aws:SourceArn": f"arn:aws:rds:{get_aws_region()}:{get_aws_account_id()}:db:*"
                                }
                            ),
                        )
                    ]
                ),
                managed_policies=[
                    AwsManagedPolicy(
                        arn="arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
                    )
                ],
                opts=pulumi.ResourceOptions(parent=self),
            )
            if enable_enhanced_monitoring
            else None
        )

        self._cluster = aws.rds.Cluster(
            cluster_identifier,
            cluster_identifier=cluster_identifier,
            engine=engine.engine_type,
            engine_version=engine.engine_version,
            engine_mode=DbEngineMode.PROVISIONED,
            db_subnet_group_name=subnet_group.name,
            master_username=master_username,
            master_password=master_password,
            skip_final_snapshot=skip_final_snapshot,
            final_snapshot_identifier=f"{cluster_identifier}-final-snapshot",
            database_name=initial_database_name,
            db_cluster_parameter_group_name=cluster_parameter_group.name,
            vpc_security_group_ids=(
                [sg.id for sg in security_groups] if len(security_groups) > 0 else None
            ),
            iam_roles=(
                [role.name for role in iam_roles] if len(iam_roles) > 0 else None
            ),
            storage_encrypted=enable_encryption,
            enabled_cloudwatch_logs_exports=(
                list(engine.log_types) if enable_cloudwatch_logs_exports else None
            ),
            backup_retention_period=backup_retention_period,
            deletion_protection=deletion_protection,
            apply_immediately=apply_immediately,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self._instances = [
            AuroraInstance(
                f"{cluster_identifier}-instance-{i}",
                cluster=self,
                engine=engine,
                instance_class=instance_class,
                subnet_group=subnet_group,
                publicly_accessible=publicly_accessible,
                availability_zone=availability_zone,
                enable_performance_insights=enable_performance_insights,
                performance_insights_retention_period=performance_insights_retention_period,
                enable_enhanced_monitoring=enable_enhanced_monitoring,
                monitoring_interval=monitoring_interval,
                monitoring_role=monitoring_role,
                ca_cert_identifier=ca_cert_identifier,
                apply_immediately=apply_immediately,
                opts=pulumi.ResourceOptions(parent=self),
            )
            for i in range(num_instances)
        ]

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this Aurora cluster."""
        return self._cluster.cluster_identifier
