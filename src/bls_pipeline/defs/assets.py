import subprocess
from pathlib import Path
from typing import Any, Final

import dagster as dg
import polars as pl

from .resources import BlsApiResource, PostgresResource
from .configs import YearMonthConfig

GROUP_NAME: Final[str] = "bls_laus"

# Used when wanting to create html file from jupyterlab notebook using CLI quarto render command
# See commented index_html asset below
NOTEBOOK_DIR: Final[Path] = Path("/path/to/your/notebook/folder")

RETRY_POLICY: Final[dg.RetryPolicy] = dg.RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=dg.Backoff.EXPONENTIAL,
)


def _result(df: pl.DataFrame, table_name: str) -> dg.MaterializeResult:
    """
    Helper function that allows for same metadata to be outputted in the dagster UI for each and every asset.
    Without this helper function, then you would have to return dg.MaterializeResult for each and every asset.
    So this is to prevent DRY (Don't Repeat Yourself).
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
    kinds={"python", "postgres"},
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
    return _result(df, table_name)


@dg.asset(
    name="area_codes",
    group_name=GROUP_NAME,
    description="BLS LAUS area code lookup (la.area) with derived FIPS code.",
    kinds={"python", "postgres"},
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
    return _result(df, table_name)


@dg.asset(
    name="measure_codes",
    group_name=GROUP_NAME,
    description="BLS LAUS measure code lookup (la.measure).",
    kinds={"python", "postgres"},
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
    return _result(df, table_name)


@dg.asset(
    name="unemployment_by_counties",
    group_name=GROUP_NAME,
    description="Monthly county-level unemployment data (la.data.64.County).",
    kinds={"python", "postgres"},
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
    return _result(df, table_name)

@dg.asset(
    name="unemployment_by_states_s",
    group_name=GROUP_NAME,
    description="Monthly state unemployment data, seasonally adjusted (la.data.3.AllStatesS).",
    kinds={"python", "postgres"},
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
    return _result(df, table_name)


@dg.asset(
    name="unemployment_by_states_u",
    group_name=GROUP_NAME,
    description="Monthly state unemployment data, not seasonally adjusted (la.data.2.AllStatesU).",
    kinds={"python", "postgres"},
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
    return _result(df, table_name)


# @dg.asset(
#     name="index_html",
#     group_name=GROUP_NAME,
#     description="Renders index.ipynb to index.html with Quarto.",
#     kinds={"python"},
#     deps=[ UPDATE_ME ],
# )
# def index_html(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
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
