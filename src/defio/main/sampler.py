from defio.dataset.imdb import IMDB_GZ
from defio.sql.ast.from_clause import JoinType
from defio.sqlgen.generator import RandomSqlGenerator
from defio.sqlgen.sampler.aggregate import AggregateSamplerConfig
from defio.sqlgen.sampler.join import JoinSamplerConfig
from defio.sqlgen.sampler.predicate import PredicateSamplerConfig

if __name__ == "__main__":
    generator = RandomSqlGenerator(
        dataset=IMDB_GZ,
        seed=1,
        join_config=JoinSamplerConfig(
            max_num_joins=6,
            join_types=[
                JoinType.INNER_JOIN,
                JoinType.LEFT_OUTER_JOIN,
                JoinType.RIGHT_OUTER_JOIN,
            ],
            join_types_weights=[0.9, 0.05, 0.05],
        ),
        predicate_config=PredicateSamplerConfig(
            max_num_predicates=8,
            p_drop_point_query=0.9,
        ),
        aggregate_config=AggregateSamplerConfig(
            max_num_aggregates=2,
        ),
        num_queries=3,
    )

    for query in generator:
        print(query, end="\n\n")
