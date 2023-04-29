import json
from typing import Any

import pytest

from defio.infra.helper.iam import (
    WILDCARD,
    Condition,
    PolicyDocument,
    Principal,
    Statement,
    StatementEffect,
)


@pytest.mark.parametrize(
    "policy_document, expected_dict",
    [
        pytest.param(
            PolicyDocument(
                Statement=[
                    Statement(
                        Effect=StatementEffect.ALLOW, Action=WILDCARD, Resource=WILDCARD
                    ),
                ]
            ),
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
            },
            id="administrator_access_policy",
        ),
        # Adapted from https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html
        pytest.param(
            PolicyDocument(
                Statement=[
                    Statement(
                        Sid="ListObjectsInBucket",
                        Effect=StatementEffect.ALLOW,
                        Action=["s3:ListBucket"],
                        Resource=["arn:aws:s3:::bucket-name"],
                    ),
                    Statement(
                        Sid="AllObjectActions",
                        Effect=StatementEffect.ALLOW,
                        Action="s3:*Object",
                        Resource=["arn:aws:s3:::bucket-name/*"],
                    ),
                ]
            ),
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "ListObjectsInBucket",
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": ["arn:aws:s3:::bucket-name"],
                    },
                    {
                        "Sid": "AllObjectActions",
                        "Effect": "Allow",
                        "Action": "s3:*Object",
                        "Resource": ["arn:aws:s3:::bucket-name/*"],
                    },
                ],
            },
            id="s3_bucket_policy",
        ),
        pytest.param(
            PolicyDocument(
                Statement=[
                    Statement(
                        Effect=StatementEffect.ALLOW,
                        Action="sts:AssumeRole",
                        Principal=Principal(Service="ec2.amazonaws.com"),
                    ),
                ]
            ),
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "ec2.amazonaws.com"},
                    }
                ],
            },
            id="instance_profile_trust_policy",
        ),
        pytest.param(
            PolicyDocument(
                Statement=[
                    Statement(
                        Sid="s3import",
                        Effect=StatementEffect.ALLOW,
                        Action=[
                            "s3:GetObject",
                            "s3:ListBucket",
                        ],
                        Resource=[
                            "arn:aws:s3:::defio-datasets",
                            "arn:aws:s3:::defio-datasets/*",
                        ],
                    ),
                ]
            ),
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "s3import",
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket",
                        ],
                        "Resource": [
                            "arn:aws:s3:::defio-datasets",
                            "arn:aws:s3:::defio-datasets/*",
                        ],
                    }
                ],
            },
            id="rds_s3_import_permission_policy",
        ),
        pytest.param(
            PolicyDocument(
                Statement=[
                    Statement(
                        Effect=StatementEffect.ALLOW,
                        Action="sts:AssumeRole",
                        Principal=Principal(Service="rds.amazonaws.com"),
                        Condition=Condition(
                            StringLike={
                                "aws:SourceArn": "arn:aws:rds:us-east-1:0123456789:cluster:defio*",
                            }
                        ),
                    ),
                ]
            ),
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "rds.amazonaws.com"},
                        "Condition": {
                            "StringLike": {
                                "aws:SourceArn": "arn:aws:rds:us-east-1:0123456789:cluster:defio*",
                            }
                        },
                    }
                ],
            },
            id="rds_s3_import_trust_policy",
        ),
    ],
)
def test_unstructure(
    policy_document: PolicyDocument, expected_dict: dict[str, Any]
) -> None:
    assert policy_document.to_dict() == expected_dict
    assert json.loads(policy_document.to_json()) == expected_dict
