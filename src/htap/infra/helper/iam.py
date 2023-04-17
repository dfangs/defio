from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from enum import StrEnum, unique
from typing import Any, TypeAlias

from attrs import define, fields

from htap.utils.sentinel import Sentinel


class WILDCARD(Sentinel):
    """Sentinel value representing a wilcard."""


@define(kw_only=True)
class PolicyDocument:
    """AWS IAM Policy Document."""

    Version: str = "2012-10-17"
    Statement: Sequence[Statement]

    def to_dict(self) -> dict[str, Any]:
        """Converts this instance into its dict/JSON representation."""
        return {
            "Version": self.Version,
            "Statement": [statement.to_dict() for statement in self.Statement],
        }

    def to_json(self) -> str:
        """Converts this instance into its JSON string representation."""
        return json.dumps(self.to_dict())


@define(kw_only=True)
class Statement:
    """
    TODO: Validation (e.g., Resource must be valid ARN)
    """

    Sid: str | None = None
    Effect: StatementEffect
    Principal: type[WILDCARD] | Principal | None = None
    Action: type[WILDCARD] | str | Sequence[str]
    Resource: type[WILDCARD] | Sequence[str] | None = None
    Condition: Condition | None = None

    def to_dict(self) -> dict[str, Any]:
        """Converts this instance into its dict/JSON representation."""
        result = {}

        if self.Sid is not None:
            result["Sid"] = self.Sid

        result["Effect"] = self.Effect

        if self.Principal is WILDCARD:
            result["Principal"] = "*"
        elif isinstance(self.Principal, Principal):
            result["Principal"] = self.Principal.to_dict()

        if self.Action is WILDCARD:
            result["Action"] = "*"
        elif isinstance(self.Action, str):
            result["Action"] = self.Action
        elif isinstance(self.Action, Sequence):
            result["Action"] = list(self.Action)

        if self.Resource is WILDCARD:
            result["Resource"] = "*"
        elif isinstance(self.Resource, Sequence):
            result["Resource"] = list(self.Resource)

        if self.Condition is not None:
            result["Condition"] = self.Condition.to_dict()

        return result


@unique
class StatementEffect(StrEnum):
    ALLOW = "Allow"
    DENY = "Deny"


_PrincipalItem: TypeAlias = str | Sequence[str]


@define(kw_only=True)
class Principal:
    """
    TODO: Validation
    """

    AWS: _PrincipalItem | None = None
    Federated: _PrincipalItem | None = None
    Service: _PrincipalItem | None = None
    CanonicalUser: _PrincipalItem | None = None

    def to_dict(self) -> dict[str, Any]:
        """Converts this instance into its dict/JSON representation."""
        result = {}

        for f in fields(self.__class__):
            value = getattr(self, f.name)

            if isinstance(value, str):
                result[f.name] = value
            elif isinstance(value, Sequence):
                result[f.name] = list(value)

        return result


_ConditionItemMap: TypeAlias = Mapping[str, str | Sequence[str]]


@define(kw_only=True)
class Condition:
    """
    TODO: Validation + more conditions (these are still not exhaustive)
    """

    StringEquals: _ConditionItemMap | None = None
    StringNotEquals: _ConditionItemMap | None = None
    StringLike: _ConditionItemMap | None = None
    StringNotLike: _ConditionItemMap | None = None
    ArnEquals: _ConditionItemMap | None = None

    def to_dict(self) -> dict[str, Any]:
        """Converts this instance into its dict/JSON representation."""
        result = {}

        for f in fields(self.__class__):
            operator_name = f.name
            operator = getattr(self, operator_name)

            if isinstance(operator, Mapping):
                result[operator_name] = {
                    key: (value if isinstance(value, str) else list(value))
                    for key, value in operator.items()
                }

        return result
