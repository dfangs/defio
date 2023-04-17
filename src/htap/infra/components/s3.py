import pulumi
import pulumi_aws as aws

from htap.infra.helper.iam import PolicyDocument
from htap.infra.utils import ComponentMixin


class Bucket(pulumi.ComponentResource, ComponentMixin):
    """
    Pulumi component resource for Amazon S3 Bucket.

    NOTE: For now, default to a private bucket.
    """

    def __init__(
        self,
        bucket_name: str,
        /,
        *,
        bucket_policy: PolicyDocument | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        super().__init__(self.get_type_name(), bucket_name, opts=opts)

        self._name = bucket_name

        self._bucket = aws.s3.BucketV2(
            bucket_name,
            bucket=bucket_name,
            opts=pulumi.ResourceOptions(parent=self),
        )

        aws.s3.BucketPublicAccessBlock(
            f"{bucket_name}-block-public-access",
            bucket=self._bucket.id,
            block_public_acls=True,
            ignore_public_acls=True,
            block_public_policy=True,
            restrict_public_buckets=True,
            opts=pulumi.ResourceOptions(parent=self),
        )

        aws.s3.BucketOwnershipControls(
            f"{bucket_name}-ownership-controls",
            bucket=self._bucket.id,
            rule=aws.s3.BucketOwnershipControlsRuleArgs(
                object_ownership="BucketOwnerEnforced"
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        # Note: No need to create `BucketAclV2` resource
        # When `BucketOwnerEnforced` is applied, use bucket policies to control access instead

        aws.s3.BucketServerSideEncryptionConfigurationV2(
            f"{bucket_name}-encryption-config",
            bucket=self._bucket.id,
            rules=[
                aws.s3.BucketServerSideEncryptionConfigurationV2RuleArgs(
                    apply_server_side_encryption_by_default=aws.s3.BucketServerSideEncryptionConfigurationV2RuleApplyServerSideEncryptionByDefaultArgs(
                        sse_algorithm="AES256"
                    )
                )
            ],
            opts=pulumi.ResourceOptions(parent=self),
        )

        if bucket_policy is not None:
            aws.s3.BucketPolicy(
                f"{bucket_name}-bucket-policy",
                bucket=self._bucket.id,
                policy=bucket_policy.to_json(),
                opts=pulumi.ResourceOptions(parent=self),
            )

        self.register_outputs({"arn": self.arn})

    @property
    def arn(self) -> str:
        """
        Returns the ARN of this S3 bucket.

        Note:
        Return a `str` instead of `pulumi.Output[str]` so that it can be
        used immediately (e.g., inside `PolicyDocument`).
        This may cause Pulumi to not infer the resource dependency if this
        value is treated as a `pulumi.Input`, and thus do not use it as such.
        """
        return f"arn:aws:s3:::{self._name}"
