import subprocess
from pathlib import Path
from typing import Any, Final

import dagster as dg
import polars as pl

from .resources import BlsApiResource, PostgresResource
from .configs import YearMonthConfig
from .partitions import MONTHLY_PARTITIONS

GROUP_NAME: Final[str] = "bls_laus"

# Used when wanting to create html file from jupyterlab notebook using CLI quarto render command
# See commented index_html asset below
NOTEBOOK_DIR: Final[Path] = Path("/path/to/your/notebook/folder")

RETRY_POLICY: Final[dg.RetryPolicy] = dg.RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=dg.Backoff.EXPONENTIAL,
)


def _df_result(df: pl.DataFrame, table_name: str) -> dg.MaterializeResult:
    """
    Helper function that allows dataframe metadata to be outputted in the dagster UI for each asset that produces
    a dataframe.  Without this helper function, you would have to return dg.MaterializeResult for each
    dataframe-producing asset.  So this is to prevent DRY (Don't Repeat Yourself).
    """

    return dg.MaterializeResult(
        metadata={
            "table_name": dg.MetadataValue.text(table_name),
            "row_count": dg.MetadataValue.int(df.height),
            "columns": dg.MetadataValue.json(df.columns),
            "preview": dg.MetadataValue.md(f"```\n{df.head(10)}\n```"),
        }
    )

def _sql_result(postgres: PostgresResource, table_name: str) -> dg.MaterializeResult:
    """
    Helper function that allows SQL result metadata to be outputted in the dagster UI for each asset that executes
    a table-creating SQL query.  Without this helper function, you would have to return dg.MaterializeResult for each
    table-creating asset.  So this is to prevent DRY (Don't Repeat).
    """

    return dg.MaterializeResult(
        metadata={
            "table_name": dg.MetadataValue.text(table_name),
            "row_count": dg.MetadataValue.int(postgres.row_count(table_name)),
        }
    )


@dg.asset(
    name="area_type_codes",
    group_name=GROUP_NAME,
    description="BLS LAUS area type code lookup (la.area_type).",
    kinds={"csv", "polars", "postgres"},
    retry_policy=RETRY_POLICY,
)
def area_type_codes(
    context: dg.AssetExecutionContext,
    bls_api: BlsApiResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.area_type_codes"
    df: pl.DataFrame = bls_api.fetch_tsv("la.area_type").rename(
        {"areatype_text": "area_type_text"}
    )
    postgres.write_table(df, table_name)
    context.log.info(f"Wrote {df.height} rows to {table_name}")
    return _df_result(df, table_name)


@dg.asset(
    name="area_codes",
    group_name=GROUP_NAME,
    description="BLS LAUS area code lookup (la.area) with derived FIPS code.",
    kinds={"csv", "polars", "postgres"},
    retry_policy=RETRY_POLICY,
)
def area_codes(
    context: dg.AssetExecutionContext,
    bls_api: BlsApiResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.area_codes"
    df: pl.DataFrame = bls_api.fetch_tsv("la.area").with_columns(
        fips_code=pl.col("area_code").str.slice(2, 2)
        + pl.col("area_code").str.slice(4, 3)
    )
    postgres.write_table(df, table_name)
    context.log.info(f"Wrote {df.height} rows to {table_name}")
    return _df_result(df, table_name)


@dg.asset(
    name="measure_codes",
    group_name=GROUP_NAME,
    description="BLS LAUS measure code lookup (la.measure).",
    kinds={"csv", "polars", "postgres"},
    retry_policy=RETRY_POLICY,
)
def measure_codes(
    context: dg.AssetExecutionContext,
    bls_api: BlsApiResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.measure_codes"
    schema: dict[str, Any] = {"measure_code": pl.String, "measure_text": pl.String}
    df: pl.DataFrame = bls_api.fetch_tsv("la.measure", schema=schema)
    postgres.write_table(df, table_name)
    context.log.info(f"Wrote {df.height} rows to {table_name}")
    return _df_result(df, table_name)


@dg.asset(
    name="unemployment_by_counties",
    group_name=GROUP_NAME,
    description="Monthly county-level unemployment data (la.data.64.County).",
    kinds={"csv", "polars", "postgres"},
    retry_policy=RETRY_POLICY,
)
def unemployment_by_counties(
    context: dg.AssetExecutionContext,
    bls_api: BlsApiResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.unemployment_by_counties"
    df: pl.DataFrame = (
        bls_api.fetch_tsv("la.data.64.County")
        .with_columns(
            pl.col("value").cast(pl.Float64, strict=False),
            fips_code=pl.col("series_id").str.slice(5, 2)
            + pl.col("series_id").str.slice(7, 3),
            measure_code=pl.col("series_id").str.slice(-2),
        )
    )
    postgres.write_table(df, table_name)
    context.log.info(f"Wrote {df.height} rows to {table_name}")
    return _df_result(df, table_name)


@dg.asset(
    name="unemployment_by_states_s",
    group_name=GROUP_NAME,
    description="Monthly state unemployment data, seasonally adjusted (la.data.3.AllStatesS).",
    kinds={"csv", "polars", "postgres"},
    retry_policy=RETRY_POLICY,
)
def unemployment_by_states_s(
    context: dg.AssetExecutionContext,
    bls_api: BlsApiResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.unemployment_by_states_s"
    df: pl.DataFrame = (
        bls_api.fetch_tsv(
            "la.data.3.AllStatesS"
        )
        .with_columns(
            pl.col("value").cast(pl.Float64, strict=False)
        )
    )
    postgres.write_table(df, table_name)
    context.log.info(f"Wrote {df.height} rows to {table_name}")
    return _df_result(df, table_name)


@dg.asset(
    name="unemployment_by_states_u",
    group_name=GROUP_NAME,
    description="Monthly state unemployment data, not seasonally adjusted (la.data.2.AllStatesU).",
    kinds={"csv", "polars", "postgres"},
    retry_policy=RETRY_POLICY,
)
def unemployment_by_states_u(
    context: dg.AssetExecutionContext,
    bls_api: BlsApiResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.unemployment_by_states_u"
    df: pl.DataFrame = (
        bls_api
        .fetch_tsv("la.data.2.AllStatesU")
        .with_columns(
            pl.col("value").cast(pl.Float64, strict=False),
        )
    )
    postgres.write_table(df, table_name)
    context.log.info(f"Wrote {df.height} rows to {table_name}")
    return _df_result(df, table_name)


# @dg.asset(
#     name="county_unemployment_summary",
#     group_name=GROUP_NAME,
#     description="county unemployment rate for specific year-month",
#     kinds={"postgres"},
#     deps=[unemployment_by_counties, area_codes]
# )
# def county_unemployment_summary(
#     context: dg.AssetExecutionContext,
#     config: YearMonthConfig,
#     postgres: PostgresResource,
# ) -> dg.MaterializeResult:
#     table_name: str = "public.county_unemployment_summary"
#
#     postgres.execute(f"DROP TABLE IF EXISTS {table_name}")
#     context.log.info(f"DROP TABLE IF EXISTS {table_name}")
#
#     postgres.execute(
#         f"""
#         create table public.county_unemployment_summary as
#         select
#             fips_code
#             , area_text
#             , year_month
#             , value
#         from (
#             select
#                 ubc.fips_code
#                 , ac.area_text
#                 , cast(ubc.year as text) || '-' || substr(ubc.period,2,2) as year_month
#                 , ubc.value
#             from
#                 -- To obtain county level unemployment data
#                 public.unemployment_by_counties ubc
#             left join
#                 -- To obtain state county names
#                 public.area_codes ac
#             on
#                 ubc.fips_code = ac.fips_code
#             where
#                 ubc.measure_code = '03'   -- To obtain unemployment rate measure
#                 and ac.area_type_code = 'F'   -- Limit to county area codes
#         ) as ubc_summary
#         where
#             ubc_summary.year_month = '{config.year_month}'
#         order by
#             ubc_summary.fips_code
#         """
#     )
#     context.log.info(f"CREATED TABLE {table_name}")
#
#     return _sql_result(postgres, table_name)

@dg.asset(
    name="county_unemployment_summary_partitioned",
    group_name=GROUP_NAME,
    description="County unemployment rate, one month per partition.",
    kinds={"postgres"},
    partitions_def=MONTHLY_PARTITIONS,
    deps=[unemployment_by_counties, area_codes],
)
def county_unemployment_summary_partitioned(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    table_name: str = "public.county_unemployment_summary"

    # Partition keys look like "2026-09-01"; the table stores "2026-09"
    year_month: str = context.partition_key[:7]

    # 1. Create the table once, if it doesn't exist yet
    postgres.execute(
        f"""
        create table if not exists {table_name} (
            fips_code   text,
            area_text   text,
            year_month  text,
            value       double precision
        )
        """
    )

    # 2. Remove any existing rows for this month, so re-runs are idempotent
    postgres.execute(f"delete from {table_name} where year_month = '{year_month}'")
    context.log.info(f"Deleted existing rows for {year_month} from {table_name}")

    # 3. Insert the month's data
    postgres.execute(
        f"""
        insert into {table_name} (fips_code, area_text, year_month, value)
        select
            ubc.fips_code
            , ac.area_text
            , cast(ubc.year as text) || '-' || substr(ubc.period, 2, 2) as year_month
            , ubc.value
        from
            public.unemployment_by_counties ubc
        left join
            public.area_codes ac
        on
            ubc.fips_code = ac.fips_code
        where
            ubc.measure_code = '03'        -- unemployment rate measure
            and ac.area_type_code = 'F'    -- county area codes only
            and cast(ubc.year as text) || '-' || substr(ubc.period, 2, 2) = '{year_month}'
        order by
            ubc.fips_code
        """
    )
    context.log.info(f"Inserted {year_month} rows into {table_name}")

    return _sql_result(postgres, table_name)

# @dg.asset(
#     name="index_html",
#     group_name=GROUP_NAME,
#     description="Renders index.ipynb to index.html with Quarto.",
#     kinds={"file", "ipynb"},
#     deps=[ UPDATE_ME ],
# )
# def index_html(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#     """
#     An index.html asset that is created from executing CLI command: quarto render index.ipynb
#     """
#
#     command: list[str] = ["quarto", "render", "index.ipynb"]
#     result: subprocess.CompletedProcess[str] = subprocess.run(
#         command,
#         cwd=NOTEBOOK_DIR,
#         capture_output=True,
#         text=True,
#         check=False,
#     )
#     if result.stdout:
#         context.log.info(result.stdout)
#     if result.stderr:
#         context.log.info(result.stderr)
#     result.check_returncode()  # raises, failing the asset, on a nonzero exit
#
#     output: Path = NOTEBOOK_DIR / "index.html"
#     return dg.MaterializeResult(
#         metadata={
#             "output_path": dg.MetadataValue.path(str(output)),
#             "size_bytes": dg.MetadataValue.int(output.stat().st_size),
#         }
#     )
