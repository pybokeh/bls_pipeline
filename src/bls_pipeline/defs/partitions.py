import dagster as dg
from typing import Final

MONTHLY_PARTITIONS: Final[dg.MonthlyPartitionsDefinition] = dg.MonthlyPartitionsDefinition(
    start_date="2026-01-01",
)