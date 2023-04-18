from __future__ import annotations

from pathlib import Path
from typing import final

from attrs import define
from typing_extensions import override

from htap.client.config import PulumiDbConfig
from htap.client.postgres import PostgresClient
from htap.constants import REDSHIFT_KEY_PREFIX


@final
@define(frozen=True)
class RedshiftClient(PostgresClient):
    """
    Asynchronous client for Amazon Redshift.

    Implementation note:
    This class simply wraps the default `PostgresClient`.
    """


@final
@define(frozen=True)
class PulumiRedshiftConfig(PulumiDbConfig):
    """Pulumi DB config for Amazon Redshift."""

    @override
    def ssl_root_cert_path(self) -> Path:
        return Path(__file__).parent / "redshift-ca-bundle.pem"

    @property
    @override
    def key_prefix(self) -> str:
        return REDSHIFT_KEY_PREFIX
