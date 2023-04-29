from collections.abc import Sequence

import pulumi
import pulumi_aws as aws

from defio.infra.helper.iam import PolicyDocument, Principal, Statement, StatementEffect
from defio.infra.utils import ComponentMixin


class ManagedPolicy(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS IAM Managed Policy.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        policy_document: PolicyDocument,
        description: str | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        self._policy = aws.iam.Policy(
            name,
            policy=policy_document.to_json(),
            description=description,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"arn": self.arn})

    @property
    def arn(self) -> pulumi.Output[str]:
        """Returns the ARN of this Managed Policy."""
        return self._policy.arn


class AwsManagedPolicy:
    """
    Convenience class that wraps an existing AWS IAM Managed Policy.

    This class shares the same interface with the `ManagedPolicy`
    Pulumi component resource.
    """

    def __init__(self, arn: str) -> None:
        self._policy = aws.iam.get_policy(arn=arn)

    @property
    def arn(self) -> pulumi.Output[str]:
        """Returns the ARN of this Managed Policy."""
        return self._policy.arn


class Role(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS IAM Role.

    All policies associated with the Role (i.e. trust policy and
    permissions policy, both inline and managed) must be specified
    at creation time.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        trust_policy: PolicyDocument,
        inline_policies: Sequence[PolicyDocument] = (),
        managed_policies: Sequence[ManagedPolicy | AwsManagedPolicy] = (),
        description: str | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        self._role = aws.iam.Role(
            name,
            assume_role_policy=trust_policy.to_json(),
            description=description,
            opts=pulumi.ResourceOptions(parent=self),
        )

        for i, inline_policy in enumerate(inline_policies):
            aws.iam.RolePolicy(
                f"{name}-permissions-policy-inline-{i}",
                role=self.name,
                policy=inline_policy.to_json(),
                opts=pulumi.ResourceOptions(parent=self),
            )

        for i, managed_policy in enumerate(managed_policies):
            aws.iam.RolePolicyAttachment(
                f"{name}-permissions-policy-managed-{i}",
                role=self.name,
                policy_arn=managed_policy.arn,
                opts=pulumi.ResourceOptions(parent=self),
            )

        self.register_outputs({"arn": self.arn, "name": self.name})

    @property
    def arn(self) -> pulumi.Output[str]:
        """Returns the ARN of this Role."""
        return self._role.arn

    @property
    def name(self) -> pulumi.Output[str]:
        """Returns the name of this Role."""
        return self._role.name


class InstanceProfile(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS IAM Instance Profile.

    An IAM Instance Profile is essentially a container for an IAM Role
    that can be assumed by EC2 instances.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        inline_policies: Sequence[PolicyDocument] = (),
        managed_policies: Sequence[ManagedPolicy] = (),
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        # Design decision: Always create a new Role instead of asking
        # the client to provide it as a parameter, which helps enforce
        # the right trust policy (this also mimics how AWS Console does it)

        self._role = Role(
            f"{name}-role",
            trust_policy=PolicyDocument(
                Statement=[
                    Statement(
                        Effect=StatementEffect.ALLOW,
                        Principal=Principal(Service="ec2.amazonaws.com"),
                        Action="sts:AssumeRole",
                    )
                ]
            ),
            inline_policies=inline_policies,
            managed_policies=managed_policies,
            opts=pulumi.ResourceOptions(parent=self),
            # TODO: Description?
        )

        self._instance_profile = aws.iam.InstanceProfile(
            name,
            role=self._role.name,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"name": self.name})

    @property
    def name(self) -> pulumi.Output[str]:
        """Returns the name of this Instance Profile."""
        return self._instance_profile.name
