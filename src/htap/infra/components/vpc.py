from __future__ import annotations

import ipaddress
from collections.abc import Iterable
from types import TracebackType
from typing import Self, TypeAlias, assert_never

import pulumi
import pulumi_aws as aws

from htap.infra.constants import ALL_NETWORK
from htap.infra.helper.vpc import (
    GatewayEndpointService,
    SecurityGroupEgressRule,
    SecurityGroupIngressRule,
)
from htap.infra.utils import ComponentMixin

RouteTarget: TypeAlias = (
    aws.ec2.InternetGateway | aws.ec2.NatGateway | aws.ec2.VpcEndpoint
)


class Vpc(pulumi.ComponentResource, ComponentMixin):
    """
    AWS VPC (Virtual Private Cloud) component resource for Pulumi.

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
        self._vpc_cidr_block = cidr_block

        # There should only be one Internet Gateway
        self._internet_gateway: aws.ec2.InternetGateway | None = None

        # Multiple gateway endpoints are allowed
        self._gateway_endpoints = set[GatewayEndpointService]()

        # Keep track of whether a route table already has a route
        self._route_tables = dict[aws.ec2.RouteTable, bool]()

        # Keep track of the subnets' CIDR blocks
        self._subnets = dict[aws.ec2.Subnet, ipaddress.IPv4Network]()

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
        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of the VPC."""
        return self._vpc.id

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
    ) -> aws.ec2.RouteTable:
        """
        Creates a Route Table with some routes that direct all outbound traffic
        to the specified targets. If `targets` is empty, the route table will not
        contain any default routes.
        """
        route_table = aws.ec2.RouteTable(
            name, vpc_id=self.id, opts=pulumi.ResourceOptions(parent=self)
        )
        self._route_tables[route_table] = False

        for i, target in enumerate(targets):
            self._add_route(f"{name}-route-{i}", route_table=route_table, target=target)

        return route_table

    def _add_route(
        self,
        name: str,
        /,
        *,
        route_table: aws.ec2.RouteTable,
        target: RouteTarget,
    ) -> aws.ec2.Route | aws.ec2.VpcEndpointRouteTableAssociation:
        """
        Creates a route that directs all outbound traffic to the given target
        and associates it with the given Route Table.
        """
        if route_table not in self._route_tables:
            raise ValueError("Route Table has not been added to this VPC")

        if isinstance(target, (aws.ec2.InternetGateway, aws.ec2.NatGateway)):
            if self._route_tables[route_table]:
                raise ValueError(
                    "Route Table already has a route to an internet gateway or NAT gateway"
                )

            self._route_tables[route_table] = True

        match target:
            case aws.ec2.InternetGateway():
                return aws.ec2.Route(
                    name,
                    route_table_id=route_table.id,
                    destination_cidr_block=str(ALL_NETWORK),
                    gateway_id=target.id,
                    opts=pulumi.ResourceOptions(parent=self),
                )

            case aws.ec2.NatGateway():
                return aws.ec2.Route(
                    name,
                    route_table_id=route_table.id,
                    destination_cidr_block=str(ALL_NETWORK),
                    nat_gateway_id=target.id,
                    opts=pulumi.ResourceOptions(parent=self),
                )

            case aws.ec2.VpcEndpoint():
                return aws.ec2.VpcEndpointRouteTableAssociation(
                    name,
                    route_table_id=route_table.id,
                    vpc_endpoint_id=target.id,
                    opts=pulumi.ResourceOptions(parent=self),
                )

            case _:
                assert_never(target)

    def add_subnet(
        self,
        name: str,
        /,
        *,
        availability_zone: str,
        cidr_block: ipaddress.IPv4Network,
        map_public_ip: bool,
        route_table: aws.ec2.RouteTable,
    ) -> aws.ec2.Subnet:
        """
        Creates a new Subnet with the given CIDR block and Route Table
        and adds it to this VPC.

        To make the Subnet public, in addition to setting `map_public_ip`
        to `True`, make sure that the associated Route Table and Security Group
        are also configured correctly.
        """
        if not cidr_block.subnet_of(self._vpc_cidr_block):
            raise ValueError("CIDR block must be a subnet of the VPC's CIDR block")

        for existing_cidr_block in self._subnets.values():
            if cidr_block.overlaps(existing_cidr_block):
                raise ValueError("CIDR block must not overlap with other subnets")

        if route_table not in self._route_tables:
            raise ValueError("Route Table has not been added to this VPC")

        subnet = aws.ec2.Subnet(
            name,
            vpc_id=self.id,
            availability_zone=availability_zone,
            cidr_block=str(cidr_block),
            map_public_ip_on_launch=map_public_ip,
            opts=pulumi.ResourceOptions(parent=self),
        )
        self._subnets[subnet] = cidr_block

        # Note: Route Table - Subnet is a 1-to-many relationship,
        # so it makes sense to associate the Route Table here
        aws.ec2.RouteTableAssociation(
            f"{name}-route-table-assoc",
            subnet_id=subnet.id,
            route_table_id=route_table.id,
            opts=pulumi.ResourceOptions(parent=self),
        )

        return subnet

    def add_security_group(
        self,
        name: str,
        /,
        *,
        ingress_rules: Iterable[SecurityGroupIngressRule],
        egress_rules: Iterable[SecurityGroupEgressRule],
    ) -> aws.ec2.SecurityGroup:
        """
        Creates a Security Group with the given ingress and egress rules.
        After creation, it can be added to an EC2 instance or network interface.
        """
        # Note: Since Security Groups are bound to some VPC, I decided to
        # put the functionality here instead of in the sibling `ec2` module
        sg = aws.ec2.SecurityGroup(
            name, vpc_id=self.id, opts=pulumi.ResourceOptions(parent=self)
        )

        # Note: Terraform recommends migrating to `AwsVpcSecurityGroupIngressRule`
        # (and similarly for egress), but Pulumi doesn't have the support yet,
        # so for now, use `SecurityGroupRule` to attach the ingress/egress rules

        for i, ingress_rule in enumerate(ingress_rules):
            aws.ec2.SecurityGroupRule(
                f"{name}-ingress-rule-{i}",
                security_group_id=sg.id,
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
                security_group_id=sg.id,
                type="egress",
                protocol=egress_rule.protocol,
                from_port=egress_rule.port_range[0],
                to_port=egress_rule.port_range[1],
                description=egress_rule.description,
                **egress_rule.get_pulumi_target_arg(),
                opts=pulumi.ResourceOptions(parent=self),
            )

        return sg
