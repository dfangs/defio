from abc import abstractmethod
from collections.abc import Iterator
from typing import final

from attrs import define
from typing_extensions import override

from defio.dataset import Dataset
from defio.sqlgen.ast.statement import GenSelectStatement
from defio.sqlgen.sampler.aggregate import AggregateSampler, AggregateSamplerConfig
from defio.sqlgen.sampler.join import JoinSampler, JoinSamplerConfig
from defio.sqlgen.sampler.predicate import PredicateSampler, PredicateSamplerConfig
from defio.utils.generator import ImmutableGenerator
from defio.utils.random import Randomizer


class SqlGenerator(ImmutableGenerator[str]):
    """
    Abstract base class for an immutable "generator" that yields
    a (potentially unbounded) sequence of SQL strings.

    This is particularly useful for lazily generating SQL queries
    in memory (as opposed to, for example, eagerly loading a list of
    pre-generated SQL queries from disk).
    """

    @override
    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        raise NotImplementedError


@final
@define(frozen=True, eq=False, kw_only=True)
class RandomSqlGenerator(SqlGenerator):
    """
    Lazy SQL generator based on random sampling from an actual dataset.

    Currently, this generator only supports a subset of PostgreSQL syntax.
    In particular, it can only generate `SELECT` statements with joins,
    filters, and aggregates. More advanced features such as group-bys,
    `HAVING` clauses, and subqueries are not yet supported.
    """

    dataset: Dataset
    rng: Randomizer
    join_config: JoinSamplerConfig
    predicate_config: PredicateSamplerConfig
    aggregate_config: AggregateSamplerConfig
    num_queries: int = 10_000

    @override
    def __iter__(self) -> Iterator[str]:
        join_sampler = JoinSampler(
            schema=self.dataset.schema,
            rng=self.rng,
            config=self.join_config,
        )

        predicate_sampler = PredicateSampler(
            schema=self.dataset.schema,
            stats=self.dataset.stats,
            rng=self.rng,
            config=self.predicate_config,
        )

        aggregate_sampler = AggregateSampler(
            schema=self.dataset.schema,
            rng=self.rng,
            config=self.aggregate_config,
        )

        for _ in range(self.num_queries):
            joins = join_sampler.sample_joins()
            predicates = predicate_sampler.sample_predicates(joins)
            aggregates = aggregate_sampler.sample_aggregates(joins)

            statement = GenSelectStatement(
                target_list=aggregates, from_clause=joins, where_clause=predicates
            )

            yield str(statement.to_sql())
