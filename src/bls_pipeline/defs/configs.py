import dagster as dg
from datetime import date

from dateutil.relativedelta import relativedelta
from pydantic import Field

# BLS is usually 2 months behind in reporting unemployment rate at certain levels (ex. county level)
default_year_month = (date.today() - relativedelta(months=2)).strftime("%Y-%m")

class YearMonthConfig(dg.Config):
    year_month: str = Field(
        default = default_year_month,
        pattern = r"^\d{4}-(0[1-9]|1[0-2])$",
        description = "The year and month in YYYY-MM format",
    )
