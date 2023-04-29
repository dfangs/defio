from collections.abc import Set
from enum import Enum, StrEnum, auto, unique
from typing import TypeVar

from defio.infra.constants import DEFAULT_PORT_MYSQL, DEFAULT_PORT_POSTGRESQL

_T = TypeVar("_T")


@unique
class DbEngineType(StrEnum):
    AURORA_MYSQL = "aurora-mysql"
    AURORA_POSTGRESQL = "aurora-postgresql"
    MYSQL = "mysql"
    POSTGRESQL = "postgres"


@unique
class DbEngineMode(StrEnum):
    PROVISIONED = "provisioned"
    SERVERLESS = "serverless"


@unique
class AuroraLogType(StrEnum):
    AUDIT = "audit"
    ERROR = "error"
    GENERAL = "general"
    SLOW_QUERY = "slowquery"
    POSTGRESQL = "postgresql"


@unique
class AuroraEngine(Enum):
    AURORA_MYSQL_2 = ("5.7.mysql_aurora.2.11.2", "aurora-mysql5.7")
    AURORA_MYSQL_3 = ("8.0.mysql_aurora.3.03.0", "aurora-mysql8.0")
    POSTGRESQL_11 = ("11.19", "aurora-postgresql11")
    POSTGRESQL_12 = ("12.14", "aurora-postgresql12")
    POSTGRESQL_13 = ("13.10", "aurora-postgresql13")
    POSTGRESQL_14 = ("14.7", "aurora-postgresql14")
    POSTGRESQL_15 = ("15.2", "aurora-postgresql15")

    def __init__(self, engine_version: str, parameter_group_family: str) -> None:
        self.engine_version = engine_version
        self.parameter_group_family = parameter_group_family

    def _branch(self, mysql_value: _T, postgresql_value: _T) -> _T:
        if "MYSQL" in self.name:
            return mysql_value
        if "POSTGRESQL" in self.name:
            return postgresql_value
        raise RuntimeError("Should not reach here")

    @property
    def engine_type(self) -> DbEngineType:
        return self._branch(
            mysql_value=DbEngineType.AURORA_MYSQL,
            postgresql_value=DbEngineType.AURORA_POSTGRESQL,
        )

    @property
    def default_port(self) -> int:
        return self._branch(
            mysql_value=DEFAULT_PORT_MYSQL,
            postgresql_value=DEFAULT_PORT_POSTGRESQL,
        )

    @property
    def log_types(self) -> Set[AuroraLogType]:
        postgres_log = frozenset({AuroraLogType.POSTGRESQL})

        return self._branch(
            mysql_value=frozenset(AuroraLogType) - postgres_log,
            postgresql_value=postgres_log,
        )


@unique
class DbInstanceClass(StrEnum):
    @staticmethod
    def _generate_next_value_(
        name: str, start: str, count: int, last_values: list[str]
    ) -> str:
        return f"db.{name.lower().replace('_', '.')}"

    T3_SMALL = auto()
    T3_MEDIUM = auto()
    T3_LARGE = auto()
    T4G_MEDIUM = auto()
    T4G_LARGE = auto()
    R5_LARGE = auto()
    R5_XLARGE = auto()
    R5_2XLARGE = auto()
    R5_4XLARGE = auto()
    R5_8XLARGE = auto()
    R5_12XLARGE = auto()
    R5_16XLARGE = auto()
    R5_24XLARGE = auto()
    R6G_LARGE = auto()
    R6G_XLARGE = auto()
    R6G_2XLARGE = auto()
    R6G_4XLARGE = auto()
    R6G_8XLARGE = auto()
    R6G_12XLARGE = auto()
    R6G_16XLARGE = auto()
    R6I_LARGE = auto()
    R6I_XLARGE = auto()
    R6I_2XLARGE = auto()
    R6I_4XLARGE = auto()
    R6I_8XLARGE = auto()
    R6I_12XLARGE = auto()
    R6I_16XLARGE = auto()
    R6I_24XLARGE = auto()
    R6I_32XLARGE = auto()
    X2G_LARGE = auto()
    X2G_XLARGE = auto()
    X2G_2XLARGE = auto()
    X2G_4XLARGE = auto()
    X2G_8XLARGE = auto()
    X2G_12XLARGE = auto()
    X2G_16XLARGE = auto()


@unique
class ClusterRoleFeature(StrEnum):
    COMPREHEND = "Comprehend"
    LAMBDA = "Lambda"
    S3_EXPORT = "s3Export"
    S3_IMPORT = "s3Import"
    SAGE_MAKER = "SageMaker"
