import ipaddress
from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Any, Self, TypeAlias, overload

import pulumi
import pulumi_aws as aws
from attrs import define

from htap.infra.utils import get_aws_region
from htap.utils.sentinel import Sentinel


@unique
class GatewayEndpointService(Enum):
    """
    Represents the various types of VPC Gateway Endpoints.

    Note that this is a strict subset of VPC Endpoints in general.
    """

    S3 = "s3"
    DYNAMODB = "dynamodb"

    @property
    def qualified_name(self) -> str:
        # `name` cannot be use since it is a default attribute name for Enum
        return f"com.amazonaws.{get_aws_region()}.{self.value}"


class SecurityGroupAbc(ABC):
    """
    Workaround for a circular import between the VPC component module
    and helper module.
    """

    @property
    @abstractmethod
    def id(self) -> pulumi.Output[str]:
        raise NotImplementedError


# pylint: disable-next=invalid-name
class SELF_TARGET(Sentinel):
    """Sentinel value representing a self-target for Security Group Rules."""


# Type alias
SecurityGroupRuleTarget: TypeAlias = (
    ipaddress.IPv4Network | SecurityGroupAbc | type[SELF_TARGET]
)


@define(frozen=True)
class _SecurityGroupRule(ABC):
    """
    Abstract base class for Security Group Rules.

    This class serves to contain the shared fields and factory methods
    for both Ingress Rule and Egress Rule.
    """

    protocol: aws.ec2.ProtocolType
    port_range: tuple[int, int]
    target: SecurityGroupRuleTarget
    description: str | None = None

    def get_pulumi_target_arg(self) -> dict[str, Any]:
        """
        Returns the relevant keyword arguments needed when constructing
        an `aws.ec2.SecurityGroupRule` Pulumi resource.
        """
        match self.target:
            case ipaddress.IPv4Network():
                return {"cidr_blocks": [str(self.target)]}
            case SecurityGroupAbc():
                return {"source_security_group_id": self.target.id}
            case _ if self.target is SELF_TARGET:
                return {"self": True}
            case _:
                # Cannot use `assert_never` here since the case for `SELF_TARGET`
                # doesn't narrow its type, so `_` is not guaranteed to be `Never`
                raise ValueError("Should not reach here")

    @classmethod
    def for_all_traffic(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        # Setting `protocol=ALL` will ignore the port range
        return cls(aws.ec2.ProtocolType.ALL, (-1, -1), target, description)

    @classmethod
    def for_all_tcp(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        return cls(aws.ec2.ProtocolType.TCP, (0, 65535), target, description)

    @classmethod
    def for_all_udp(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        return cls(aws.ec2.ProtocolType.UDP, (0, 65535), target, description)

    @classmethod
    def for_all_icmp(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        return cls(aws.ec2.ProtocolType.ICMP, (-1, -1), target, description)

    @overload
    @classmethod
    def for_custom_tcp(
        cls,
        target: SecurityGroupRuleTarget,
        *,
        port: int,
        description: str | None = None,
    ) -> Self:
        ...

    @overload
    @classmethod
    def for_custom_tcp(
        cls,
        target: SecurityGroupRuleTarget,
        *,
        port_range: tuple[int, int],
        description: str | None = None,
    ) -> Self:
        ...

    @classmethod
    def for_custom_tcp(
        cls,
        target: SecurityGroupRuleTarget,
        *,
        port: int | None = None,
        port_range: tuple[int, int] | None = None,
        description: str | None = None,
    ) -> Self:
        if port is not None:
            return cls(aws.ec2.ProtocolType.TCP, (port, port), target, description)
        if port_range is not None:
            return cls(aws.ec2.ProtocolType.TCP, port_range, target, description)
        raise RuntimeError("Should not reach here")

    @overload
    @classmethod
    def for_custom_udp(
        cls,
        target: SecurityGroupRuleTarget,
        *,
        port: int,
        description: str | None = None,
    ) -> Self:
        ...

    @overload
    @classmethod
    def for_custom_udp(
        cls,
        target: SecurityGroupRuleTarget,
        *,
        port_range: tuple[int, int],
        description: str | None = None,
    ) -> Self:
        ...

    @classmethod
    def for_custom_udp(
        cls,
        target: SecurityGroupRuleTarget,
        *,
        port: int | None = None,
        port_range: tuple[int, int] | None = None,
        description: str | None = None,
    ) -> Self:
        if port is not None:
            return cls(aws.ec2.ProtocolType.UDP, (port, port), target, description)
        if port_range is not None:
            return cls(aws.ec2.ProtocolType.UDP, port_range, target, description)
        raise RuntimeError("Should not reach here")

    @classmethod
    def for_ssh(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        return cls.for_custom_tcp(target=target, port=22, description=description)

    @classmethod
    def for_http(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        return cls.for_custom_tcp(target=target, port=80, description=description)

    @classmethod
    def for_https(
        cls, target: SecurityGroupRuleTarget, *, description: str | None = None
    ) -> Self:
        return cls.for_custom_tcp(target=target, port=443, description=description)


@define(frozen=True)
class SecurityGroupIngressRule(_SecurityGroupRule):
    def __init__(
        self,
        protocol: aws.ec2.ProtocolType,
        port_range: tuple[int, int],
        source: SecurityGroupRuleTarget,
        description: str | None = None,
    ) -> None:
        super().__init__(protocol, port_range, source, description)


@define(frozen=True)
class SecurityGroupEgressRule(_SecurityGroupRule):
    def __init__(
        self,
        protocol: aws.ec2.ProtocolType,
        port_range: tuple[int, int],
        destination: SecurityGroupRuleTarget,
        description: str | None = None,
    ) -> None:
        super().__init__(protocol, port_range, destination, description)
