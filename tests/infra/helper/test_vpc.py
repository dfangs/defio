import ipaddress

from pulumi_aws.ec2 import ProtocolType, SecurityGroup
from pytest_mock import MockerFixture

from htap.infra.helper.vpc import (
    SELF_TARGET,
    SecurityGroupEgressRule,
    SecurityGroupIngressRule,
)


def test_security_group_ingress_rule() -> None:
    target = ipaddress.IPv4Network("10.0.0.0/16")
    description = "SSH ingress rule"

    ingress_rule = SecurityGroupIngressRule.for_ssh(
        target=target, description=description
    )
    expected = SecurityGroupIngressRule(
        protocol=ProtocolType.TCP,
        port_range=(22, 22),
        source=target,
        description=description,
    )

    assert ingress_rule == expected
    assert ingress_rule.get_pulumi_args() == {"cidr_blocks": ["10.0.0.0/16"]}


def test_security_group_egress_rule(mocker: MockerFixture) -> None:
    mocked_sg = mocker.Mock(spec=SecurityGroup)
    type(mocked_sg).id = mocker.PropertyMock(return_value="sg-1234")
    description = "SSH egress rule"

    egress_rule = SecurityGroupEgressRule.for_all_traffic(
        target=mocked_sg, description=description
    )

    # For ALL rule, no need to check for port range
    assert egress_rule.protocol == ProtocolType.ALL
    assert egress_rule.target == mocked_sg
    assert egress_rule.description == description
    assert egress_rule.get_pulumi_args() == {"source_security_group_id": "sg-1234"}


def test_security_group_rule_self_target() -> None:
    rule = SecurityGroupIngressRule.for_ssh(
        target=SELF_TARGET, description="Self target rule"
    )

    assert rule.get_pulumi_args() == {"self": True}
