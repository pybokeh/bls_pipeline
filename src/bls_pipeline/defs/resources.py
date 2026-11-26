import io
from typing import Any, Final

import dagster as dg
import polars as pl
import requests
from adbc_driver_postgresql import dbapi

BLS_BASE_URL: Final[str] = "https://download.bls.gov/pub/time.series/la"

class BlsApiResource(dg.ConfigurableResource):
    """Downloads tab-separated BLS files and returns cleaned Polars DataFrames."""

    user_agent: str
    base_url: str = BLS_BASE_URL
    timeout_seconds: int = 120

    def fetch_tsv(
        self,
        file_name: str,
        schema: dict[str, Any] | None = None,
        infer_schema_length: int | None = 10000,
    ) -> pl.DataFrame:
        url: str = f"{self.base_url}/{file_name}"
        response: requests.Response = requests.get(
            url,
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        if schema is not None:
            df: pl.DataFrame = pl.read_csv(
                io.BytesIO(response.content),
                separator="\t",
                schema=schema,
            )
        else:
            df = pl.read_csv(
                io.BytesIO(response.content),
                separator="\t",
                infer_schema=True,
                infer_schema_length=infer_schema_length,
            )

        # Remove whitespace around column names
        df = df.rename({c: c.strip() for c in df.columns})
        # Remove whitespace around column values
        df = df.with_columns(pl.col(pl.String).str.strip_chars())
        return df


class PostgresResource(dg.ConfigurableResource):
    """Writes Polars DataFrames to Postgres using the ADBC engine."""

    host: str
    port: str
    user: str
    password: str
    database: str

    @property
    def connection_uri(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def read_query(self, query: str) -> pl.DataFrame:
        return pl.read_database_uri(
            query=query,
            uri=self.connection_uri,
            engine="adbc",
        )

    def execute(self, statement: str, parameters: tuple | None = None) -> None:
        with dbapi.connect(self.connection_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(statement, parameters)
            conn.commit()

    def write_table(self, df: pl.DataFrame, table_name: str) -> None:
        df.write_database(
            table_name=table_name,
            connection=self.connection_uri,
            engine="adbc",
            if_table_exists="replace",
        )

    def row_count(self, table_name: str) -> int:
        df: pl.DataFrame = self.read_query(f"SELECT COUNT(*) AS n FROM {table_name}")
        return int(df.item())