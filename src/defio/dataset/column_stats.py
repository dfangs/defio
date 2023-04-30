# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from enum import StrEnum, auto, unique
from typing import Any, Generic, TypeVar, assert_never, final

import numpy as np
import pandas as pd
from attrs import define
from typing_extensions import override

from defio.sql.schema import Column, DataType

_T = TypeVar("_T")


@unique
class ColumnType(StrEnum):
    """
    General categories or types of the data in a column.

    A column type determines the kinds of filter predicates
    that can be performed on the column.
    """

    CATEGORICAL = auto()
    KEY = auto()
    NUMERICAL = auto()
    RAW_STRING = auto()

    @staticmethod
    def infer_from_series(series: pd.Series, column: Column) -> ColumnType:
        """Infers the type of the column based on some heuristics."""
        match dtype := column.dtype:
            case DataType.INTEGER:
                # Check for key constraints first, since it's user-defined
                if column.is_primary_key or column.is_foreign_key:
                    return ColumnType.KEY
                if _is_column_categorical(series):
                    return ColumnType.CATEGORICAL
                return ColumnType.NUMERICAL

            case DataType.FLOAT:
                return ColumnType.NUMERICAL

            case DataType.STRING:
                if column.is_primary_key or column.is_foreign_key:
                    return ColumnType.KEY
                if _is_column_categorical(series):
                    return ColumnType.CATEGORICAL
                return ColumnType.RAW_STRING

            case DataType.BOOLEAN:
                return ColumnType.CATEGORICAL

            case _:
                assert_never(dtype)


def _is_column_categorical(series: pd.Series) -> bool:
    # Arbitrary threshold
    num_unique_threshold = 50
    unique_ratio_threshold = 0.01

    num_unique = len(series.dropna().unique())
    num_notna = len(series.dropna())

    # Infer a column as "categorical" based on the number of unique values
    return (
        num_unique <= num_unique_threshold
        or num_unique / num_notna <= unique_ratio_threshold
    )


@define(kw_only=True)
class ColumnStats(ABC):
    """Abstract base class for column statistics."""

    nan_ratio: float
    num_unique: int

    @final
    def __init__(
        self,
        source: pd.Series | dict[str, Any],
    ) -> None:
        """
        Initializes this column stats from a pandas Series or a JSON dict.

        All subclasses should override the corresponding helper init methods
        instead of this.
        """
        if isinstance(source, pd.Series):
            self._init_from_series(source)
        else:
            self._init_from_dict(source)

    @abstractmethod
    def _init_from_series(self, series: pd.Series) -> None:
        self.nan_ratio = sum(series.isna()) / len(series)
        self.num_unique = len(series.dropna().unique())

    @abstractmethod
    def _init_from_dict(self, data: dict[str, Any]) -> None:
        assert data["column_type"] == self._column_type
        self.nan_ratio = data["nan_ratio"]
        self.num_unique = data["num_unique"]

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        """
        Converts this column stats into a JSON dict.

        All subclasses must override this method and call this base implementation.
        """
        return {
            "column_type": str(self._column_type),
            "nan_ratio": self.nan_ratio,
            "num_unique": self.num_unique,
        }

    @property
    @abstractmethod
    def _column_type(self) -> ColumnType:
        raise NotImplementedError

    @staticmethod
    def from_series(
        series: pd.Series, column: Column, column_type: ColumnType | None = None
    ) -> ColumnStats:
        """
        Returns the stats of the given column based on the given data.

        The computed stats depend on the "type" of the given column,
        which will be automatically inferred if not provided.

        Raises a `ValueError` if the column type is not appropriate
        for the column's data type.
        """
        column_type = (
            column_type
            if column_type is not None
            else ColumnType.infer_from_series(series, column)
        )

        # Dynamic check (can't really add more type-safety here)
        match dtype := column.dtype:
            case DataType.INTEGER:
                if column_type is ColumnType.RAW_STRING:
                    raise ValueError("Invalid column type for Integer")
            case DataType.FLOAT:
                if column_type is not ColumnType.NUMERICAL:
                    raise ValueError("Invalid column type for Float")
            case DataType.STRING:
                if column_type is ColumnType.NUMERICAL:
                    raise ValueError("Invalid column type for String")
            case DataType.BOOLEAN:
                if column_type is not ColumnType.CATEGORICAL:
                    raise ValueError("Invalid column type for Boolean")
            case _:
                assert_never(dtype)

        match column_type:
            case ColumnType.CATEGORICAL:
                return CategoricalColumnStats(series)
            case ColumnType.KEY:
                return KeyColumnStats(series)
            case ColumnType.NUMERICAL:
                return NumericalColumnStats(series)
            case ColumnType.RAW_STRING:
                return RawStringColumnStats(series)
            case _:
                assert_never(column_type)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> ColumnStats:
        """Converts the given JSON dict into column stats."""
        match column_type := ColumnType(data["column_type"]):
            case ColumnType.CATEGORICAL:
                return CategoricalColumnStats(data)
            case ColumnType.NUMERICAL:
                return NumericalColumnStats(data)
            case ColumnType.KEY:
                return KeyColumnStats(data)
            case ColumnType.RAW_STRING:
                return RawStringColumnStats(data)
            case _:
                assert_never(column_type)


@final
@define(init=False, kw_only=True)
class CategoricalColumnStats(ColumnStats, Generic[_T]):
    """
    Column statistics for numerical values.

    Supported data types: `Integer`, `String`, `Boolean`.

    Reasonable operations: `=`, `!=`, `IN`.
    """

    most_frequent_values: Mapping[_T, float]

    @override
    def _init_from_series(self, series: pd.Series) -> None:
        super()._init_from_series(series)

        # Take into account null values when dividing by # of entries
        value_counts = series.value_counts()

        max_threshold = 50  # Arbitrary threshold (but seems reasonable)
        value_freqs = value_counts[:max_threshold] / len(series)

        self.most_frequent_values = _convert_series_to_dict(value_freqs)

    @override
    def _init_from_dict(self, data: dict[str, Any]) -> None:
        super()._init_from_dict(data)
        self.most_frequent_values = dict(data["most_frequent_values"])

    @override
    def to_dict(self) -> dict[str, Any]:
        print(self.most_frequent_values)
        return {
            **super().to_dict(),
            # Convert to list instead of dict, since `_T` can be non-string values
            # but JSON keys must be strings
            "most_frequent_values": list(self.most_frequent_values.items()),
        }

    @property
    @override
    def _column_type(self) -> ColumnType:
        return ColumnType.CATEGORICAL


@final
@define(init=False, kw_only=True)
class KeyColumnStats(ColumnStats, Generic[_T]):
    """
    Column statistics for key values (i.e. columns with primary key
    or foreign key constraint).

    Supported data types: `Integer` and `String` (commonly used keys).

    Reasonable operations: Point query (`=`).
    """

    sampled_values: Sequence[_T]

    @override
    def _init_from_series(self, series: pd.Series) -> None:
        super()._init_from_series(series)

        # Arbitrary thresholds
        # Make sure that we sample enough for categorical tables as well
        fraction_threshold = 0.1
        number_threshold = 1000

        notna_series = series.dropna()
        num_samples = min(
            len(notna_series),
            max(
                number_threshold,
                min(number_threshold, int(fraction_threshold * len(notna_series))),
            ),
        )

        self.sampled_values = notna_series.sample(num_samples).to_list()

    @override
    def _init_from_dict(self, data: dict[str, Any]) -> None:
        super()._init_from_dict(data)
        self.sampled_values = data["sampled_values"]

    @override
    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "sampled_values": list(self.sampled_values),
        }

    @property
    @override
    def _column_type(self) -> ColumnType:
        return ColumnType.KEY


@final
@define(init=False, kw_only=True)
class NumericalColumnStats(ColumnStats):
    """
    Column statistics for numerical values.

    Supported data types: `Integer`, `Float`.

    Reasonable operations: Range query (`<`, `<=`, `>`, `>=`, and `BETWEEN`).

    Note:
    Using generic type for int/float is troublesome,
    so just use float for all statistics.
    """

    min: float
    max: float
    mean: float
    percentiles: Sequence[float]

    @override
    def _init_from_series(self, series: pd.Series) -> None:
        super()._init_from_series(series)

        notna_series = series.dropna()

        self.min = float(notna_series.min()) if len(notna_series) > 0 else math.nan
        self.max = float(notna_series.max()) if len(notna_series) > 0 else math.nan
        self.mean = float(notna_series.mean()) if len(notna_series) > 0 else math.nan

        # NOTE:
        # For some reason, `dropna()` magically convert `pd.NA` into `NaN`
        # so we can use `to_list()` directly without extra conversion
        self.percentiles = notna_series.quantile(
            q=np.linspace(0, 1, 101), interpolation="nearest"
        ).to_list()

    @override
    def _init_from_dict(self, data: dict[str, Any]) -> None:
        super()._init_from_dict(data)
        self.min = data["min"]
        self.max = data["max"]
        self.mean = data["mean"]
        self.percentiles = data["percentiles"]

    @override
    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "percentiles": list(self.percentiles),
        }

    @property
    @override
    def _column_type(self) -> ColumnType:
        return ColumnType.NUMERICAL


@final
@define(init=False, kw_only=True)
class RawStringColumnStats(ColumnStats):
    """
    Column statistics for raw string values.

    Supported data types: `String`.

    Reasonable operations: `LIKE`.
    """

    frequent_words: Mapping[str, float]

    @override
    def _init_from_series(self, series: pd.Series) -> None:
        super()._init_from_series(series)
        df_column_name = "words"
        word_row_counts: pd.Series = (
            series.dropna()  # Drop NA values
            .str.split()  # Split each string into list of words
            .reset_index(name=df_column_name)  # Convert Series into DataFrame
            .explode(df_column_name)  # Convert each word into its own row
            .drop_duplicates()  # Drop duplicates, so we count the number of rows
            .groupby(df_column_name)  # Group by unique words
            .count()  # Count the number of occurrences (ignoring missing values)
            .squeeze()  # Convert back to Series
            .sort_values(ascending=False)
        )

        max_threshold = 100  # Arbitrary threshold
        word_row_freqs = word_row_counts[:max_threshold] / len(series)

        self.frequent_words = _convert_series_to_dict(word_row_freqs)

    @override
    def _init_from_dict(self, data: dict[str, Any]) -> None:
        super()._init_from_dict(data)
        self.frequent_words = data["frequent_words"]

    @override
    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "frequent_words": dict(self.frequent_words),
        }

    @property
    @override
    def _column_type(self) -> ColumnType:
        return ColumnType.RAW_STRING


def _convert_series_to_dict(series: pd.Series) -> dict[Any, Any]:
    # NOTE:
    # Use this roundabout way instead of `series.to_dict()`
    # since the latter doesn't un-convert pandas nullable types
    return dict(zip(series.index.to_list(), series.to_list()))
