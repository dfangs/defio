from __future__ import annotations

import ipaddress
from collections.abc import Sequence
from pathlib import Path

import pulumi
import pulumi_aws as aws

from htap.infra.components.iam import InstanceProfile
from htap.infra.components.vpc import SecurityGroup, Subnet
from htap.infra.helper.ec2 import Ami
from htap.infra.utils import ComponentMixin


class KeyPair(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS EC2 Key Pair.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        public_key_openssh: str,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        self._key_pair = aws.ec2.KeyPair(
            name,
            public_key=public_key_openssh,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"key_name": self.key_name})

    @staticmethod
    def from_file(
        name: str,
        /,
        path_to_public_key: Path,
        *,
        opts: pulumi.ResourceOptions | None = None,
    ) -> KeyPair:
        """
        Alternative constructor to create a Key Pair from an existing public key file.
        """
        try:
            with open(path_to_public_key, mode="r", encoding="utf-8") as f:
                return KeyPair(name, public_key_openssh=f.read(), opts=opts)
        except OSError as exc:
            raise ValueError("Error when reading from file") from exc

    @property
    def key_name(self) -> pulumi.Output[str]:
        """Returns the key name of this key pair."""
        return self._key_pair.key_name


class NetworkInterface(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS Network Interface.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        subnet: Subnet,
        security_groups: Sequence[SecurityGroup] = (),
        private_ip: ipaddress.IPv4Address | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        self._network_interface = aws.ec2.NetworkInterface(
            name,
            subnet_id=subnet.id,
            security_groups=(
                [sg.id for sg in security_groups] if len(security_groups) > 0 else None
            ),
            private_ip=(str(private_ip) if private_ip is not None else None),
            source_dest_check=True,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this network interface."""
        return self._network_interface.id


class LaunchTemplate(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS EC2 Launch Template.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        ami: Ami,
        key_pair: KeyPair | None = None,
        instance_profile: InstanceProfile | None = None,
        network_interface: NetworkInterface | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        """
        Instance type is better to be specified outside of the launch template.

        """
        super().__init__(self.get_type_name(), name, opts=opts)

        # Note: Instance type is better specified outside of the launch template
        self._launch_template = aws.ec2.LaunchTemplate(
            name,
            image_id=ami.id,
            key_name=(key_pair.key_name if key_pair is not None else None),
            network_interfaces=(
                [
                    # For simplicity, only allow at most one network interface
                    aws.ec2.LaunchTemplateNetworkInterfaceArgs(
                        device_index=0, network_interface_id=network_interface.id
                    )
                ]
                if network_interface is not None
                else None
            ),
            block_device_mappings=[
                aws.ec2.LaunchTemplateBlockDeviceMappingArgs(
                    device_name=ami.root_device_name,
                    ebs=aws.ec2.LaunchTemplateBlockDeviceMappingEbsArgs(
                        # Keep the original volume size (can't decrease since the AMI snapshot has to fit in)
                        volume_size=ami.root_volume_size,
                        # Default to `gp3` (although AL2023 already uses it as default)
                        volume_type="gp3",
                        # No need to keep a snapshot of the root volume (TODO)
                        delete_on_termination=str(True),
                        # Enable at-rest encryption by default
                        encrypted=str(True),
                        # TODO: Need ARN instead of alias
                        # kms_key_id=None,
                    ),
                )
            ],
            iam_instance_profile=(
                aws.ec2.LaunchTemplateIamInstanceProfileArgs(name=instance_profile.name)
                if instance_profile is not None
                else None
            ),
            metadata_options=aws.ec2.LaunchTemplateMetadataOptionsArgs(
                # Default value; enable the EC2 instance metadata service
                http_endpoint="enabled",
                # Force the use of IMDSv2 instead of v1
                http_tokens="required",
                # Enable access to EC2 instance tags
                instance_metadata_tags="enabled",
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this Launch Template."""
        return self._launch_template.id


class Instance(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for AWS EC2 Instance.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        subnet: Subnet,
        instance_type: str,
        ami: Ami,
        security_groups: Sequence[SecurityGroup] = (),
        private_ip: ipaddress.IPv4Address | None = None,
        key_pair: KeyPair | None = None,
        instance_profile: InstanceProfile | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), name, opts=opts)

        network_interface = NetworkInterface(
            f"{name}-network-interface",
            subnet=subnet,
            security_groups=security_groups,
            private_ip=private_ip,
            opts=pulumi.ResourceOptions(parent=self),
        )

        launch_template = LaunchTemplate(
            f"{name}-launch-template",
            ami=ami,
            key_pair=key_pair,
            instance_profile=instance_profile,
            network_interface=network_interface,
            opts=pulumi.ResourceOptions(parent=self),
        )

        self._instance = aws.ec2.Instance(
            name,
            launch_template=aws.ec2.InstanceLaunchTemplateArgs(id=launch_template.id),
            instance_type=instance_type,  # TODO: Not validated
            tags={"Name": name},  # Use Pulumi logical name
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.register_outputs({"id": self.id})

    @property
    def id(self) -> pulumi.Output[str]:
        """Returns the identifier of this EC2 instance."""
        return self._instance.id
