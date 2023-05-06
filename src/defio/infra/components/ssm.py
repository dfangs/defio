import re

import pulumi
import pulumi_aws as aws

from defio.infra.utils import ComponentMixin


class Parameter(pulumi.ComponentResource, ComponentMixin):
    """Pulumi component resource for AWS SSM Parameter."""

    def __init__(
        self,
        /,
        *,
        name: str,
        value: pulumi.Input[str],
        secure: bool = False,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        assert re.match("[a-zA-Z0-9_.-/]+", name) is not None

        super().__init__(self.get_type_name(), name, opts=opts)

        self._parameter = aws.ssm.Parameter(
            name,
            name=name,  # If omitted, Pulumi will append a random suffix to the name
            type=(
                aws.ssm.ParameterType.SECURE_STRING
                if secure
                else aws.ssm.ParameterType.STRING
            ),
            value=value,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"name": self.name, "value": self.value})

    @property
    def name(self) -> pulumi.Output[str]:
        """Returns the name of this SSM parameter."""
        return self._parameter.name

    @property
    def value(self) -> pulumi.Output[str]:
        """Returns the value of this SSM parameter."""
        return self._parameter.value
