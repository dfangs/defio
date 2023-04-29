from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal, cast

import pulumi
import pulumi_aws as aws
from immutables import Map

from defio.infra.components.iam import AwsManagedPolicy, Role
from defio.infra.components.vpc import SecurityGroup, Subnet
from defio.infra.helper.aurora import (
    AuroraEngine,
    ClusterRoleFeature,
    DbEngineMode,
    DbInstanceClass,
)
from defio.infra.helper.iam import (
    Condition,
    PolicyDocument,
    Principal,
    Statement,
    StatementEffect,
)
from defio.infra.utils import ComponentMixin, get_aws_account_id, get_aws_region


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
        enable_performance_insights: bool | None = None,
        performance_insights_retention_period: int | None = None,
        enable_enhanced_monitoring: bool | None = None,
        monitoring_interval: Literal[0, 1, 5, 10, 15, 30, 60] | None = None,
        monitoring_role: Role | None = None,
        ca_cert_identifier: str | None = None,
        apply_immediately: bool = False,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), instance_identifier, opts=opts)

        # Default values
        if enable_performance_insights is None:
            enable_performance_insights = True

        if performance_insights_retention_period is None:
            performance_insights_retention_period = 7  # Free-tier

        if enable_enhanced_monitoring is None:
            enable_enhanced_monitoring = False

        if monitoring_interval is None:
            monitoring_interval = 60

        if ca_cert_identifier is None:
            ca_cert_identifier = "rds-ca-rsa2048-g1"

        # Input validations
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

    NOTE: Has only been tested with Aurora PostgreSQL clusters.
    In particular, for some parameters, the set of allowable values may be different.
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
        iam_roles: Mapping[Role, ClusterRoleFeature] = Map(),
        enable_encryption: bool | None = None,
        enable_cloudwatch_logs_exports: bool | None = None,
        backup_retention_period: int | None = None,
        deletion_protection: bool | None = None,
        availability_zone: str | None = None,
        enable_performance_insights: bool | None = None,
        performance_insights_retention_period: int | None = None,
        enable_enhanced_monitoring: bool | None = None,
        monitoring_interval: Literal[0, 1, 5, 10, 15, 30, 60] | None = None,
        ca_cert_identifier: str | None = None,
        apply_immediately: bool = False,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), cluster_identifier, opts=opts)

        # Default values
        if enable_encryption is None:
            enable_encryption = True

        if enable_cloudwatch_logs_exports is None:
            enable_cloudwatch_logs_exports = True

        if backup_retention_period is None:
            backup_retention_period = 1  # Free-tier

        if deletion_protection is None:
            deletion_protection = False  # Otherwise can't delete without disabling this

        # Input validations
        # if num_instances < 1:
        #     raise ValueError("There must be at least one instance")

        if master_username in {"admin"}:
            # Note: There are many other reserved words
            raise ValueError(
                f"`{master_username}` cannot be used as a username as it is a reserved word"
            )

        if not 1 <= backup_retention_period <= 35:
            raise ValueError("Backup retention period must be in [1, 35]")

        # Expose with a `str` getter (on top of as a Pulumi Output)
        self._cluster_identifier = cluster_identifier

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
                # Only allow connections via SSL (i.e. with CA certificates)
                aws.rds.ClusterParameterGroupParameterArgs(
                    name="rds.force_ssl",
                    value="1",
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
            storage_encrypted=enable_encryption,
            enabled_cloudwatch_logs_exports=(
                list(engine.log_types) if enable_cloudwatch_logs_exports else None
            ),
            backup_retention_period=backup_retention_period,
            deletion_protection=deletion_protection,
            apply_immediately=apply_immediately,
            opts=pulumi.ResourceOptions(
                parent=self,
                # See https://stackoverflow.com/a/71641828
                ignore_changes=["db_cluster_parameter_group_name"],
            ),
        )

        # Use `ClusterRoleAssociation` instead of the `iam_roles` attribute
        # since it is more robust (the latter had a bug with resource updates)
        for i, (role, feature) in enumerate(iam_roles.items()):
            aws.rds.ClusterRoleAssociation(
                f"{cluster_identifier}-cluster-role-association-{i}",
                db_cluster_identifier=self.id,
                role_arn=role.arn,
                feature_name=feature,
            )

        self._instances = [
            AuroraInstance(
                f"{cluster_identifier}-instance-{i + 1}",  # Use 1-based indexing
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

        self.register_outputs(
            {
                "id": self.id,
                "endpoint": self.endpoint,
                "port": self.port,
                "username": self.username,
                "password": self.password,
                "initial-dbname": self.initial_database_name,
            }
        )

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this Aurora cluster."""
        return self._cluster.cluster_identifier

    @property
    def endpoint(self) -> pulumi.Output[str]:
        """Returns the (writer) endpoint of this Aurora cluster."""
        return self._cluster.endpoint

    @property
    def port(self) -> pulumi.Output[int]:
        """Returns the connection port of this Aurora cluster."""
        return self._cluster.port

    @property
    def username(self) -> pulumi.Output[str]:
        """Returns the master username of this Aurora cluster."""
        return self._cluster.master_username

    @property
    def password(self) -> pulumi.Output[str]:
        """Returns the master password of this Aurora cluster."""
        # Password cannot be `None` in our setup
        return cast(pulumi.Output[str], self._cluster.master_password)

    @property
    def initial_database_name(self) -> pulumi.Output[str | None]:
        """Returns the initial database name of this Aurora cluster."""
        # Aurora doesn't create a database by default, unlike Redshift
        return cast(pulumi.Output[str | None], self._cluster.database_name)

    def get_id(self) -> str:
        """
        Returns the identifier of this Aurora cluster.

        This returns a `str` instead of `pulumi.Output[str]` so that
        it can be used immediately (e.g., as part of stack output keys).
        """
        return self._cluster_identifier
