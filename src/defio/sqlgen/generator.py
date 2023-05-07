from abc import abstractmethod
from collections.abc import Iterator
from typing import final

from attrs import define, field
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
@define(frozen=True, kw_only=True)
class RandomSqlGenerator(SqlGenerator):
    """
    Lazy SQL generator based on random sampling from an actual dataset.

    Currently, this generator only supports a subset of PostgreSQL syntax.
    In particular, it can only generate `SELECT` statements with joins,
    filters, and aggregates. More advanced features such as group-bys,
    `HAVING` clauses, and subqueries are not yet supported.

    Additionally, while this generator produces nondeterministic outputs
    (more specifically, pseudo-random based on the provided `seed`),
    multiple iterations of the same generator instance will always yield
    the same sequence of outputs.
    """

    dataset: Dataset
    join_config: JoinSamplerConfig
    predicate_config: PredicateSamplerConfig
    aggregate_config: AggregateSamplerConfig
    num_queries: int = 1_000
    seed: int = field(factory=Randomizer.create_entropy)

    @override
    def __iter__(self) -> Iterator[str]:
        # NOTE:
        # To ensure repeatability across multiple iterations,
        # `seed` cannot be `None` and has to be fixed at creation time
        # (or otherwise each sampler will generate its own entropy)

        join_sampler = JoinSampler(
            schema=self.dataset.schema,
            config=self.join_config,
            seed=self.seed,
        )

        predicate_sampler = PredicateSampler(
            schema=self.dataset.schema,
            stats=self.dataset.stats,
            config=self.predicate_config,
            seed=self.seed,
        )

        aggregate_sampler = AggregateSampler(
            schema=self.dataset.schema,
            config=self.aggregate_config,
            seed=self.seed,
        )

        for _ in range(self.num_queries):
            joins = join_sampler.sample_joins()
            predicates = predicate_sampler.sample_predicates(joins)
            aggregates = aggregate_sampler.sample_aggregates(joins)

            statement = GenSelectStatement(
                target_list=aggregates, from_clause=joins, where_clause=predicates
            )

            yield str(statement.to_sql())
