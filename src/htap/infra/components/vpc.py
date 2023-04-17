from __future__ import annotations

import ipaddress
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import Self, TypeAlias, assert_never

import pulumi
import pulumi_aws as aws

from htap.constants import ALL_NETWORK
from htap.infra.helper.vpc import (
    GatewayEndpointService,
    SecurityGroupAbc,
    SecurityGroupEgressRule,
    SecurityGroupIngressRule,
)
from htap.infra.utils import ComponentMixin, get_az

RouteTarget: TypeAlias = (
    aws.ec2.InternetGateway | aws.ec2.NatGateway | aws.ec2.VpcEndpoint
)


class Vpc(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS VPC (Virtual Private Cloud).

    Can (and should) be used as a context manager.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        cidr_block: ipaddress.IPv4Network,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        self._vpc = aws.ec2.Vpc(
            name,
            cidr_block=str(cidr_block),
            enable_dns_hostnames=True,
            enable_dns_support=True,
            instance_tenancy=aws.ec2.Tenancy.DEFAULT.value,
            opts=pulumi.ResourceOptions(parent=self),
        )
        self._cidr_block = cidr_block

        # There should only be one Internet Gateway
        self._internet_gateway: aws.ec2.InternetGateway | None = None

        # Multiple gateway endpoints are allowed
        self._gateway_endpoints = set[GatewayEndpointService]()

        # Keep track of children component resources
        self._route_tables = list[RouteTable]()
        self._public_subnets = list[Subnet]()
        self._private_subnets = list[Subnet]()
        self._security_groups = list[SecurityGroup]()  # TODO: `dict` instead?

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Cannot call `register_outputs()` multiple times, so handle on exit
        # See https://github.com/pulumi/pulumi/issues/2394
        self.register_outputs(
            {
                "id": self.id,
                "vpc_cidr_block": str(self.cidr_block),
                "route_table_ids": [
                    route_table.id for route_table in self.route_tables
                ],
                "public_subnet_ids": [subnet.id for subnet in self.public_subnets],
                "public_subnet_cidr_blocks": [
                    str(subnet.cidr_block) for subnet in self.public_subnets
                ],
                "private_subnet_ids": [subnet.id for subnet in self.private_subnets],
                "private_subnet_cidr_blocks": [
                    str(subnet.cidr_block) for subnet in self.private_subnets
                ],
                "security_group_ids": [sg.id for sg in self.security_groups],
            }
        )

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this VPC."""
        return self._vpc.id

    @property
    def cidr_block(self) -> ipaddress.IPv4Network:
        """Returns the CIDR block of this VPC."""
        return self._cidr_block

    @property
    def route_tables(self) -> Sequence[RouteTable]:
        """Returns the list of route tables in this VPC."""
        return tuple(self._route_tables)

    @property
    def public_subnets(self) -> Sequence[Subnet]:
        """Returns the list of public subnets in this VPC."""
        return tuple(self._public_subnets)

    @property
    def private_subnets(self) -> Sequence[Subnet]:
        """Returns the list of private subnets in this VPC."""
        return tuple(self._private_subnets)

    @property
    def security_groups(self) -> Sequence[SecurityGroup]:
        """Returns the list of security groups in this VPC."""
        return tuple(self._security_groups)

    def add_internet_gateway(self, name: str, /) -> aws.ec2.InternetGateway:
        """
        Creates an Internet Gateway and adds it to this VPC.
        """
        if self._internet_gateway is not None:
            raise ValueError("There should only be one Internet Gateway")

        igw = aws.ec2.InternetGateway(name, opts=pulumi.ResourceOptions(parent=self))
        self._internet_gateway = igw

        # Could have used the `vpc_id` attribute of the Internet Gateway,
        # but this better reflects the actual AWS resource created
        aws.ec2.InternetGatewayAttachment(
            name,
            vpc_id=self.id,
            internet_gateway_id=igw.id,
            opts=pulumi.ResourceOptions(parent=self),
        )

        return igw

    def add_nat_gateway(self, name: str, /) -> aws.ec2.NatGateway:
        raise NotImplementedError

    def add_gateway_endpoint(
        self, name: str, /, *, service: GatewayEndpointService
    ) -> aws.ec2.VpcEndpoint:
        """
        Creates a Gateway Endpoint to the given service and adds it into this VPC.
        Note that routes to the given endpoint must be added separately.

        For simplicity, we don't attach an inline policy to the endpoint.
        Use other methods such as bucket policy if more access control is desired.
        """
        if service in self._gateway_endpoints:
            raise ValueError(f"Gateway endpoint for {service} already exists")

        self._gateway_endpoints.add(service)

        return aws.ec2.VpcEndpoint(
            name,
            vpc_id=self.id,
            vpc_endpoint_type="Gateway",
            service_name=service.qualified_name,
            opts=pulumi.ResourceOptions(parent=self),
        )

    def add_route_table(
        self, name: str, /, *, targets: Iterable[RouteTarget]
    ) -> RouteTable:
        """
        Creates a route table with some routes that direct all outbound traffic
        to the specified targets. If `targets` is empty, the route table will not
        contain any default routes.
        """
        route_table = RouteTable(
            name, vpc=self, targets=targets, opts=pulumi.ResourceOptions(parent=self)
        )
        self._route_tables.append(route_table)

        return route_table

    def add_subnet(
        self,
        name: str,
        /,
        *,
        availability_zone: str,
        cidr_block: ipaddress.IPv4Network,
        map_public_ip: bool,
        route_table: RouteTable,
    ) -> Subnet:
        """
        Creates a new subnet with the given CIDR block and route table
        and adds it to this VPC.
        """
        subnet = Subnet(
            name,
            vpc=self,
            availability_zone=availability_zone,
            cidr_block=cidr_block,
            map_public_ip=map_public_ip,
            route_table=route_table,
            opts=pulumi.ResourceOptions(parent=self),
        )

        if subnet.is_public:
            self._public_subnets.append(subnet)
        else:
            self._private_subnets.append(subnet)

        return subnet

    def add_subnets(
        self,
        name_prefix: str,
        /,
        *,
        num_public_subnets: int,
        num_private_subnets: int,
        public_route_table: RouteTable,
        private_route_table: RouteTable,
        subnet_prefixlen: int = 20,
    ) -> tuple[Sequence[Subnet], Sequence[Subnet]]:
        """
        Convenience method to add multiple public & private subnets to this VPC.

        This method creates subnets with equal size (based on `subnet_prefixlen`)
        and uses round-robin strategy to assign the Availability Zone for each subnet.
        Additionally, all public/private subnets share the same public/private route table.
        """
        if num_public_subnets < 0 or num_private_subnets < 0:
            raise ValueError("Number of subnets must be a nonnegative integer")

        if subnet_prefixlen <= self.cidr_block.prefixlen + 1:
            raise ValueError(
                "VPC CIDR block is not large enough to accomodate all subnets"
            )

        # Split VPC CIDR block into two, so that the Subnet resources
        # are more resilient against changes in the number of subnets
        public_cidr_block, private_cidr_block = tuple(
            self.cidr_block.subnets(prefixlen_diff=1)
        )

        public_subnets = [
            self.add_subnet(
                f"{name_prefix}-public-{i}",
                availability_zone=get_az(i, wrap_around=True),
                cidr_block=subnet_cidr_block,
                map_public_ip=True,
                route_table=public_route_table,
            )
            for i, subnet_cidr_block in zip(
                range(num_public_subnets),
                public_cidr_block.subnets(new_prefix=subnet_prefixlen),
            )
        ]

        private_subnets = [
            self.add_subnet(
                f"{name_prefix}-private-{i}",
                availability_zone=get_az(i, wrap_around=True),
                cidr_block=subnet_cidr_block,
                map_public_ip=False,
                route_table=private_route_table,
            )
            for i, subnet_cidr_block in zip(
                range(num_private_subnets),
                private_cidr_block.subnets(new_prefix=subnet_prefixlen),
            )
        ]

        return public_subnets, private_subnets

    def add_security_group(
        self,
        name: str,
        /,
        *,
        ingress_rules: Iterable[SecurityGroupIngressRule],
        egress_rules: Iterable[SecurityGroupEgressRule],
    ) -> SecurityGroup:
        """
        Creates a security group with the given ingress and egress rules.

        After creation, it can be added to an EC2 instance or network interface.
        """
        sg = SecurityGroup(
            name,
            vpc=self,
            ingress_rules=ingress_rules,
            egress_rules=egress_rules,
            opts=pulumi.ResourceOptions(parent=self),
        )
        self._security_groups.append(sg)

        return sg


class RouteTable(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS Route Table.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        vpc: Vpc,
        targets: Iterable[RouteTarget],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        self._route_table = aws.ec2.RouteTable(
            name, vpc_id=vpc.id, opts=pulumi.ResourceOptions(parent=self)
        )

        self._has_route_to_internet_gateway = False
        self._has_route_to_nat_gateway = False

        for i, target in enumerate(targets):
            self._create_route(f"{name}-route-{i}", target=target)

        self.register_outputs({"id": self.id})

    def _create_route(
        self, name: str, /, *, target: RouteTarget
    ) -> aws.ec2.Route | aws.ec2.VpcEndpointRouteTableAssociation:
        """
        Creates a route that directs all outbound traffic to the given target
        and associates it with this route table.
        """
        if isinstance(target, (aws.ec2.InternetGateway, aws.ec2.NatGateway)):
            if self._has_route_to_internet_gateway or self._has_route_to_nat_gateway:
                raise ValueError(
                    "Route table already has a route to an internet gateway or NAT gateway"
                )
        if isinstance(target, aws.ec2.InternetGateway):
            self._has_route_to_internet_gateway = True
        if isinstance(target, aws.ec2.NatGateway):
            self._has_route_to_nat_gateway = True

        match target:
            case aws.ec2.InternetGateway():
                return aws.ec2.Route(
                    name,
                    route_table_id=self.id,
                    destination_cidr_block=str(ALL_NETWORK),
                    gateway_id=target.id,
                    opts=pulumi.ResourceOptions(parent=self),
                )

            case aws.ec2.NatGateway():
                return aws.ec2.Route(
                    name,
                    route_table_id=self.id,
                    destination_cidr_block=str(ALL_NETWORK),
                    nat_gateway_id=target.id,
                    opts=pulumi.ResourceOptions(parent=self),
                )

            case aws.ec2.VpcEndpoint():
                return aws.ec2.VpcEndpointRouteTableAssociation(
                    name,
                    route_table_id=self.id,
                    vpc_endpoint_id=target.id,
                    opts=pulumi.ResourceOptions(parent=self),
                )

            case _:
                assert_never(target)

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this route table."""
        return self._route_table.id

    @property
    def has_public_route(self) -> bool:
        """Returns `True` iff this route table can direct traffic to the internet."""
        # By definition, only consider the Internet Gateway here, and not NAT gateway
        return self._has_route_to_internet_gateway


class Subnet(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS Subnet.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        vpc: Vpc,
        availability_zone: str,
        cidr_block: ipaddress.IPv4Network,
        map_public_ip: bool,
        route_table: RouteTable,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        if not cidr_block.subnet_of(vpc.cidr_block):
            raise ValueError("CIDR block must be a subnet of the VPC's CIDR block")

        for subnet in [*vpc.public_subnets, *vpc.private_subnets]:
            if cidr_block.overlaps(subnet.cidr_block):
                raise ValueError("CIDR block must not overlap with other subnets")

        if route_table not in vpc.route_tables:
            raise ValueError("Route table has not been added to this VPC")

        self._cidr_block = cidr_block
        self._is_public = map_public_ip and route_table.has_public_route

        self._subnet = aws.ec2.Subnet(
            name,
            vpc_id=vpc.id,
            availability_zone=availability_zone,
            cidr_block=str(cidr_block),
            map_public_ip_on_launch=map_public_ip,
            opts=pulumi.ResourceOptions(parent=self),
        )

        # Note: Route Table - Subnet is a 1-to-many relationship,
        # so it makes sense to associate the Route Table here
        aws.ec2.RouteTableAssociation(
            f"{name}-route-table-association",
            subnet_id=self.id,
            route_table_id=route_table.id,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs(
            {
                "id": self.id,
                "cidr_block": str(self.cidr_block),
                "is_public": self.is_public,
            }
        )

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this subnet."""
        return self._subnet.id

    @property
    def cidr_block(self) -> ipaddress.IPv4Network:
        """Returns the CIDR block of this subnet."""
        return self._cidr_block

    @property
    def is_public(self) -> bool:
        """
        Returns `True` iff this subnet is accessible to the internet.

        This depends not only on the value of `map_public_ip`, but also on
        whether the associated route table has a route to an internet gateway
        (by definition; see AWS Documentation for more details).
        """
        return self._is_public


class SecurityGroup(pulumi.ComponentResource, ComponentMixin, SecurityGroupAbc):
    """
    Pulumi component resource for AWS EC2 Security Group.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        vpc: Vpc,
        ingress_rules: Iterable[SecurityGroupIngressRule],
        egress_rules: Iterable[SecurityGroupEgressRule],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        # Note: Since Security Groups are bound to some VPC, I decided to
        # put the class  here instead of in the sibling `ec2` module
        self._sg = aws.ec2.SecurityGroup(
            name, vpc_id=vpc.id, opts=pulumi.ResourceOptions(parent=self)
        )

        # Note: Terraform recommends migrating to `AwsVpcSecurityGroupIngressRule`
        # (and similarly for egress), but Pulumi doesn't have the support yet,
        # so for now, use `SecurityGroupRule` to attach the ingress/egress rules

        for i, ingress_rule in enumerate(ingress_rules):
            aws.ec2.SecurityGroupRule(
                f"{name}-ingress-rule-{i}",
                security_group_id=self.id,
                type="ingress",
                protocol=ingress_rule.protocol,
                from_port=ingress_rule.port_range[0],
                to_port=ingress_rule.port_range[1],
                description=ingress_rule.description,
                **ingress_rule.get_pulumi_target_arg(),
                opts=pulumi.ResourceOptions(parent=self),
            )

        for i, egress_rule in enumerate(egress_rules):
            aws.ec2.SecurityGroupRule(
                f"{name}-egress-rule-{i}",
                security_group_id=self.id,
                type="egress",
                protocol=egress_rule.protocol,
                from_port=egress_rule.port_range[0],
                to_port=egress_rule.port_range[1],
                description=egress_rule.description,
                **egress_rule.get_pulumi_target_arg(),
                opts=pulumi.ResourceOptions(parent=self),
            )

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this security group."""
        return self._sg.id
