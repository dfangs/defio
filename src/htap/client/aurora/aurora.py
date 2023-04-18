from __future__ import annotations

from pathlib import Path
from typing import final

from attrs import define
from typing_extensions import override

from htap.client.config import PulumiDbConfig
from htap.client.postgres import PostgresClient
from htap.constants import AURORA_KEY_PREFIX


@final
@define(frozen=True)
class AuroraClient(PostgresClient):
    """
    Asynchronous client for Amazon Aurora.

    Implementation note:
    This class simply wraps the default `PostgresClient`.
    """


@final
@define(frozen=True)
class PulumiAuroraConfig(PulumiDbConfig):
    """Pulumi DB config for Amazon Aurora."""

    @override
    def ssl_root_cert_path(self) -> Path:
        return Path(__file__).parent / "aurora-ca-bundle.pem"

    @property
    @override
    def key_prefix(self) -> str:
        return AURORA_KEY_PREFIX
