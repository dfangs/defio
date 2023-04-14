from enum import StrEnum, unique
from functools import cache

import pulumi_aws as aws
from attrs import define


@unique
class AmiVersion(StrEnum):
    AL2023 = "2023"


@unique
class AmiVariant(StrEnum):
    DEFAULT = "ami"
    MINIMAL = "ami-minimal"
    ECS_OPTIMIZED = "ami-ecs-hvm"


@unique
class AmiArch(StrEnum):
    ARM64 = "arm64"
    X86_64 = "x86_64"


@define(frozen=True)
class Ami:
    """Represents an AMI (Amazon Machine Image) for use with EC2 instances."""

    version: AmiVersion
    variant: AmiVariant
    arch: AmiArch

    @property
    def id(self) -> str:
        """Returns the identifier of this AMI."""
        return Ami._get_ami(self.version, self.variant, self.arch).id

    @property
    def root_device_name(self) -> str:
        """Returns the root device name of this AMI."""
        return Ami._get_ami(self.version, self.variant, self.arch).root_device_name

    @property
    def root_volume_size(self) -> int:
        """Returns the volume size of the root device of this AMI."""
        return int(
            Ami._get_ami(self.version, self.variant, self.arch)
            .block_device_mappings[0]
            .ebs["volume_size"]
        )

    @cache
    @staticmethod
    def _get_ami(
        version: AmiVersion, variant: AmiVariant, arch: AmiArch
    ) -> aws.ec2.GetAmiResult:
        """
        Gets the information of the specified AMI using AWS API.

        Implementation note: Use cache to prevent multiple calls to AWS.
        """
        return aws.ec2.get_ami(
            owners=["amazon"],
            filters=[
                aws.ec2.GetAmiFilterArgs(
                    name="name",
                    values=[
                        # TODO: Strengthen pattern? Wilcard?
                        f"al{version}-{variant}-{version}.*-kernel-*-{arch}"
                    ],
                )
            ],
            most_recent=True,
        )
