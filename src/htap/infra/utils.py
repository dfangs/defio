import inspect
from collections.abc import Sequence
from functools import cache
from pathlib import Path

import pulumi_aws as aws

from htap.constants import PACKAGE_NAME


class ComponentMixin:
    """
    Mixin class to automatically infer the name of a Pulumi component resource
    based on the module name and class name.
    """

    def get_type_name(self) -> str:
        """
        Returns the Pulumi resource type of this component resource.

        Reference: https://www.pulumi.com/docs/intro/concepts/resources/names/#types
        """
        # Use reflection to get the module's filename and class name
        package_name = PACKAGE_NAME
        module_name = Path(inspect.getfile(self.__class__)).stem
        class_name = self.__class__.__name__

        assert module_name != "__main__.py"

        return f"{package_name}:{module_name}:{class_name}"


def get_aws_account_id() -> str:
    """
    Returns the current AWS Account ID that Pulumi is authorized to use.
    """
    return aws.get_caller_identity().account_id


def get_aws_region() -> str:
    """Returns the current AWS Region configured within Pulumi."""
    return aws.get_region().name


@cache
def get_available_azs() -> Sequence[str]:
    """
    Returns all the available (i.e. usable) Availability Zones
    in the current Region.

    Implementation note: In order to cache the result of this function,
    put it in the global scope (i.e. not as an inner function).
    """
    return aws.get_availability_zones(state="available").names


def get_az(index: int, wrap_around: bool = False) -> str:
    """
    Returns the `index`-th Availability Zone (AZ) in the current Region.
    E.g., `index=0` means `us-east-1a` in the `us-east-1` Region.

    Raises an IndexError if `wrap_around` is `False` and `index`
    is outside of [0, number of AZs in the current Region).

    If `wrap_around` is `True`, use `index % len(AZs)` to get the AZ.
    """
    availability_zones = get_available_azs()

    if wrap_around:
        return availability_zones[index % len(availability_zones)]

    if index < 0 or index >= len(availability_zones):
        raise IndexError("Given index is out of range of the available AZs")

    return availability_zones[index]
