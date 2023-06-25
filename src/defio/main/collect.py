import asyncio
from datetime import timedelta
from pathlib import Path

from defio.client.aurora import AuroraClient, SsmAuroraConfig
from defio.client.redshift import RedshiftClient, SsmRedshiftConfig
from defio.dataset.imdb import IMDB_GZ
from defio.sql.ast.from_clause import JoinType
from defio.sqlgen.generator import RandomSqlGenerator
from defio.sqlgen.sampler.aggregate import AggregateSamplerConfig
from defio.sqlgen.sampler.join import JoinSamplerConfig
from defio.sqlgen.sampler.predicate import PredicateSamplerConfig
from defio.utils.generator import chunk
from defio.workload import Workload
from defio.workload.query import QueryGenerator
from defio.workload.reporter import FileQueryReporter
from defio.workload.runner import run_workload


async def main():
    num_clusters = 2
    num_queries = 10000
    reports_path = Path(__file__).parent / "reports"
    timeout = timedelta(seconds=90)

    sql_source = RandomSqlGenerator(
        dataset=IMDB_GZ,
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
            p_drop_point_query=0.75,
        ),
        aggregate_config=AggregateSamplerConfig(
            max_num_aggregates=2,
            p_count_star=0.2,
        ),
        num_queries=num_queries,
        seed=0,
    )

    sql_source_chunks = chunk(
        sql_source,
        num_chunks=num_clusters,
        chunk_size=(num_queries // num_clusters),
    )

    async with asyncio.TaskGroup() as tg:
        for i in range(num_clusters):
            aurora_client = AuroraClient.from_config(
                SsmAuroraConfig(
                    db_identifier=f"defio-aurora-{i}",
                )
            )
            redshift_client = RedshiftClient.from_config(
                SsmRedshiftConfig(
                    db_identifier=f"defio-redshift-{i}",
                )
            )

            workload = Workload.serial(
                QueryGenerator.with_fixed_interval(
                    sql_source=sql_source_chunks[i],
                    # Not too small to cause reordering, not too big to slow things down
                    interval=timedelta(milliseconds=100),
                )
            )

            tg.create_task(
                run_workload(
                    workload=workload,
                    client=aurora_client,
                    reporter=FileQueryReporter(reports_path, f"aurora-{i}"),
                    statement_timeout=timeout,
                )
            )

            tg.create_task(
                run_workload(
                    workload=workload,
                    client=redshift_client,
                    reporter=FileQueryReporter(reports_path, f"redshift-{i}"),
                    statement_timeout=timeout,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
