import dagster as dg
from pydantic import Field

class YearMonthConfig(dg.Config):
    year_month: str | None = Field(
        default = "YYYY-MM",
        pattern = r"^\d{4}-(0[1-9]|1[0-2])$",
        description = "The year and month in YYYY-MM format",
    )

