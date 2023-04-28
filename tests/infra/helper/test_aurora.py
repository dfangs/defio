from collections.abc import Set

import pytest

from htap.infra.helper.aurora import (
    AuroraEngine,
    AuroraLogType,
    DbEngineType,
    DbInstanceClass,
)


@pytest.mark.parametrize(
    "engine, expected_engine_type",
    [
        (AuroraEngine.AURORA_MYSQL_2, DbEngineType.AURORA_MYSQL),
        (AuroraEngine.AURORA_MYSQL_3, DbEngineType.AURORA_MYSQL),
        (AuroraEngine.POSTGRESQL_14, DbEngineType.AURORA_POSTGRESQL),
        (AuroraEngine.POSTGRESQL_15, DbEngineType.AURORA_POSTGRESQL),
    ],
)
def test_aurora_engine_type(
    engine: AuroraEngine, expected_engine_type: DbEngineType
) -> None:
    assert engine.engine_type is expected_engine_type


@pytest.mark.parametrize(
    "engine, expected_default_port",
    [
        (AuroraEngine.AURORA_MYSQL_2, 3306),
        (AuroraEngine.AURORA_MYSQL_3, 3306),
        (AuroraEngine.POSTGRESQL_14, 5432),
        (AuroraEngine.POSTGRESQL_15, 5432),
    ],
)
def test_aurora_engine_default_port(
    engine: AuroraEngine, expected_default_port: int
) -> None:
    assert engine.default_port == expected_default_port


@pytest.mark.parametrize(
    "engine, expected_log_types",
    [
        (
            AuroraEngine.AURORA_MYSQL_2,
            {
                AuroraLogType.AUDIT,
                AuroraLogType.ERROR,
                AuroraLogType.GENERAL,
                AuroraLogType.SLOW_QUERY,
            },
        ),
        (
            AuroraEngine.AURORA_MYSQL_3,
            {
                AuroraLogType.AUDIT,
                AuroraLogType.ERROR,
                AuroraLogType.GENERAL,
                AuroraLogType.SLOW_QUERY,
            },
        ),
        (AuroraEngine.POSTGRESQL_14, {AuroraLogType.POSTGRESQL}),
        (AuroraEngine.POSTGRESQL_15, {AuroraLogType.POSTGRESQL}),
    ],
)
def test_aurora_engine_log_types(
    engine: AuroraEngine, expected_log_types: Set[AuroraLogType]
) -> None:
    assert engine.log_types == expected_log_types


@pytest.mark.parametrize(
    "db_instance_class, expected_str_value",
    [
        (DbInstanceClass.T3_MEDIUM, "db.t3.medium"),
        (DbInstanceClass.T4G_LARGE, "db.t4g.large"),
        (DbInstanceClass.R5_XLARGE, "db.r5.xlarge"),
        (DbInstanceClass.R6G_2XLARGE, "db.r6g.2xlarge"),
        (DbInstanceClass.X2G_16XLARGE, "db.x2g.16xlarge"),
    ],
)
def test_db_instance_class(
    db_instance_class: DbInstanceClass, expected_str_value: str
) -> None:
    assert db_instance_class == expected_str_value
