from collections import Counter
from unittest.mock import Mock, PropertyMock

import numpy as np
import pandas as pd
import pytest

from defio.dataset.column_stats import (
    CategoricalColumnStats,
    ColumnType,
    KeyColumnStats,
    NumericalColumnStats,
    RawStringColumnStats,
)
from defio.sql.schema import DataType


@pytest.fixture(name="base_int_values")
def fixture_base_int_values() -> list[int | None]:
    return [0, 1, None, 0, 1, 0, None, 1, 0, None]


@pytest.fixture(name="notna_int_values")
def fixture_notna_int_values(base_int_values: list[int | None]) -> list[int]:
    return [value for value in base_int_values if value is not None]


@pytest.fixture(name="nan_ratio")
def fixture_nan_ratio(base_int_values: list[int | None]) -> float:
    na_values = [value for value in base_int_values if value is None]
    return len(na_values) / len(base_int_values)


@pytest.fixture(name="num_unique")
def fixture_num_unique(notna_int_values: list[int]) -> int:
    return len(set(notna_int_values))


@pytest.fixture(name="boolean_series")
def fixture_boolean_series(base_int_values: list[int | None]) -> pd.Series:
    return pd.Series(base_int_values, dtype=DataType.BOOLEAN.pandas_dtype)


@pytest.fixture(name="integer_series")
def fixture_integer_series(base_int_values: list[int | None]) -> pd.Series:
    return pd.Series(base_int_values, dtype=DataType.INTEGER.pandas_dtype)


@pytest.fixture(name="float_series")
def fixture_float_series(base_int_values: list[int | None]) -> pd.Series:
    return pd.Series(base_int_values, dtype=DataType.FLOAT.pandas_dtype)


@pytest.fixture(name="base_str_values")
def fixture_base_str_values() -> list[str | None]:
    return ["a b", "a c", "b d", None]


@pytest.fixture(name="notna_str_values")
def fixture_notna_str_values(base_str_values: list[str]) -> list[str | None]:
    return [value for value in base_str_values if value is not None]


@pytest.fixture(name="string_series")
def fixture_string_series(base_str_values: list[str | None]) -> pd.Series:
    return pd.Series(base_str_values, dtype=DataType.STRING.pandas_dtype)


class TestColumnType:
    def test_integer(self, integer_series: pd.Series) -> None:
        column = Mock()
        type(column).dtype = PropertyMock(return_value=DataType.INTEGER)

        inferred_column_type = ColumnType.infer_from_series(integer_series, column)
        assert inferred_column_type in (
            ColumnType.CATEGORICAL,
            ColumnType.KEY,
            ColumnType.NUMERICAL,
        )

    def test_float(self, float_series: pd.Series) -> None:
        column = Mock()
        type(column).dtype = PropertyMock(return_value=DataType.FLOAT)

        inferred_column_type = ColumnType.infer_from_series(float_series, column)
        assert inferred_column_type is ColumnType.NUMERICAL

    def test_string(self, string_series: pd.Series) -> None:
        column = Mock()
        type(column).dtype = PropertyMock(return_value=DataType.STRING)

        inferred_column_type = ColumnType.infer_from_series(string_series, column)
        assert inferred_column_type in (
            ColumnType.CATEGORICAL,
            ColumnType.KEY,
            ColumnType.RAW_STRING,
        )

    def test_boolean(self, boolean_series: pd.Series) -> None:
        column = Mock()
        type(column).dtype = PropertyMock(return_value=DataType.BOOLEAN)

        inferred_column_type = ColumnType.infer_from_series(boolean_series, column)
        assert inferred_column_type is ColumnType.CATEGORICAL


class TestCategoricalColumnStats:
    @pytest.fixture(name="value_freqs")
    def fixture_value_freqs(
        self, base_int_values: list[int | None]
    ) -> dict[bool, float]:
        notna_values = [bool(value) for value in base_int_values if value is not None]

        return {
            value: count / len(base_int_values)
            for value, count in Counter(notna_values).items()
        }

    def test_from_series(
        self, boolean_series: pd.Series, value_freqs: dict[bool, float]
    ) -> None:
        stats = CategoricalColumnStats[bool](boolean_series)

        for value, freq in stats.most_frequent_values.items():
            assert value is not None
            assert value in value_freqs
            assert freq == value_freqs[value]

    def test_from_dict(
        self,
        nan_ratio: float,
        num_unique: int,
        value_freqs: dict[bool, float],
    ) -> None:
        stats = CategoricalColumnStats[str](
            {
                "column_type": str(ColumnType.CATEGORICAL),
                "nan_ratio": nan_ratio,
                "num_unique": num_unique,
                "most_frequent_values": value_freqs,
            }
        )

        assert stats.nan_ratio == nan_ratio
        assert stats.num_unique == num_unique
        assert stats.most_frequent_values == value_freqs

    def test_to_dict(
        self, boolean_series: pd.Series, nan_ratio: float, num_unique: int
    ) -> None:
        stats = CategoricalColumnStats[bool](boolean_series)

        # Assert everything but `most_frequent_values`
        # since the spec is not fully determined
        stats_dict = stats.to_dict()
        assert stats_dict == {
            "column_type": str(ColumnType.CATEGORICAL),
            "nan_ratio": nan_ratio,
            "num_unique": num_unique,
            "most_frequent_values": stats_dict["most_frequent_values"],
        }


class TestKeyColumnStats:
    def test_from_series(self, integer_series: pd.Series) -> None:
        stats = KeyColumnStats[int](integer_series)

        values: list[int] = integer_series.dropna().to_list()

        assert len(stats.sampled_values) <= len(values)

        for value in stats.sampled_values:
            assert value is not None
            assert value in values

    def test_from_dict(
        self, integer_series: pd.Series, nan_ratio: float, num_unique: int
    ) -> None:
        samples_values = [int(integer_series.dropna().loc[0])]

        stats = KeyColumnStats[int](
            {
                "column_type": str(ColumnType.KEY),
                "nan_ratio": nan_ratio,
                "num_unique": num_unique,
                "sampled_values": samples_values,
            }
        )

        assert stats.nan_ratio == nan_ratio
        assert stats.num_unique == num_unique
        assert stats.sampled_values == samples_values

    def test_to_dict(
        self, integer_series: pd.Series, nan_ratio: float, num_unique: int
    ) -> None:
        stats = KeyColumnStats[int](integer_series)

        # Assert everything but `sampled_values` since it is nondeterministic
        stats_dict = stats.to_dict()
        assert stats_dict == {
            "column_type": str(ColumnType.KEY),
            "nan_ratio": nan_ratio,
            "num_unique": num_unique,
            "sampled_values": stats_dict["sampled_values"],
        }


class TestNumericalColumnStats:
    @pytest.fixture(name="min_value")
    def fixture_min_value(self, notna_int_values: list[int]) -> float:
        return float(min(notna_int_values))

    @pytest.fixture(name="max_value")
    def fixture_max_value(self, notna_int_values: list[int]) -> float:
        return float(max(notna_int_values))

    @pytest.fixture(name="mean_value")
    def fixture_mean_value(self, notna_int_values: list[int]) -> float:
        return sum(notna_int_values) / len(notna_int_values)

    @pytest.fixture(name="percentiles")
    def fixture_percentiles(self, integer_series: pd.Series) -> list[float]:
        # NOTE: Too lazy to find out how to properly compute percentiles
        return (
            integer_series.dropna()
            .quantile(q=np.linspace(0, 1, 101), interpolation="nearest")
            .to_list()
        )

    def test_from_series(
        self,
        float_series: pd.Series,
        min_value: float,
        max_value: float,
        mean_value: float,
        percentiles: list[float],
    ) -> None:
        stats = NumericalColumnStats(float_series)

        assert stats.min == min_value
        assert stats.max == max_value
        assert stats.mean == mean_value
        assert stats.percentiles == percentiles

    def test_from_dict(
        self,
        nan_ratio: float,
        num_unique: int,
        min_value: float,
        max_value: float,
        mean_value: float,
        percentiles: list[float],
    ) -> None:
        stats = NumericalColumnStats(
            {
                "column_type": str(ColumnType.NUMERICAL),
                "nan_ratio": nan_ratio,
                "num_unique": num_unique,
                "min": min_value,
                "max": max_value,
                "mean": mean_value,
                "percentiles": percentiles,
            }
        )

        assert stats.nan_ratio == nan_ratio
        assert stats.num_unique == num_unique
        assert stats.min == min_value
        assert stats.max == max_value
        assert stats.mean == mean_value
        assert stats.percentiles == percentiles

    def test_to_dict(
        self,
        float_series: pd.Series,
        nan_ratio: float,
        num_unique: int,
        min_value: float,
        max_value: float,
        mean_value: float,
        percentiles: list[float],
    ) -> None:
        stats = NumericalColumnStats(float_series)

        assert stats.to_dict() == {
            "column_type": str(ColumnType.NUMERICAL),
            "nan_ratio": nan_ratio,
            "num_unique": num_unique,
            "min": min_value,
            "max": max_value,
            "mean": mean_value,
            "percentiles": percentiles,
        }


class TestRawStringColumnStats:
    @pytest.fixture(name="nan_ratio")
    def fixture_nan_ratio(self, base_str_values: list[str | None]) -> float:
        # Override the outer-scope `nan_ratio`
        na_values = [value for value in base_str_values if value is None]
        return len(na_values) / len(base_str_values)

    @pytest.fixture(name="num_unique")
    def fixture_num_unique(self, notna_str_values: list[str]) -> int:
        # Override the outer-scope `num_unique`
        return len(set(notna_str_values))

    @pytest.fixture(name="word_freqs")
    def fixture_word_freqs(self, base_str_values: list[str]) -> dict[str, float]:
        notna_values = [value for value in base_str_values if value is not None]

        return {
            word: count / len(base_str_values)
            for word, count in Counter(
                word for value in notna_values for word in value.split()
            ).items()
        }

    def test_from_series(
        self, string_series: pd.Series, word_freqs: dict[str, float]
    ) -> None:
        stats = RawStringColumnStats(string_series)

        for word, freq in stats.frequent_words.items():
            assert word is not None
            assert word in word_freqs
            assert freq == word_freqs[word]

    def test_from_dict(
        self,
        nan_ratio: float,
        num_unique: int,
        word_freqs: dict[str, float],
    ) -> None:
        stats = RawStringColumnStats(
            {
                "column_type": str(ColumnType.RAW_STRING),
                "nan_ratio": nan_ratio,
                "num_unique": num_unique,
                "frequent_words": word_freqs,
            }
        )

        assert stats.nan_ratio == nan_ratio
        assert stats.num_unique == num_unique
        assert stats.frequent_words == word_freqs

    def test_to_dict(
        self,
        string_series: pd.Series,
        nan_ratio: float,
        num_unique: int,
    ) -> None:
        stats = RawStringColumnStats(string_series)

        # Assert everything but `frequent_words`
        # since the spec is not fully determined
        stats_dict = stats.to_dict()
        assert stats_dict == {
            "column_type": str(ColumnType.RAW_STRING),
            "nan_ratio": nan_ratio,
            "num_unique": num_unique,
            "frequent_words": stats_dict["frequent_words"],
        }
