from datetime import datetime
from typing import Final
from zoneinfo import ZoneInfo

import dagster as dg

from .jobs import bls_laus_job

SCHEDULE_TIMEZONE: Final[str] = "America/New_York"
LAST_DAY_OF_FIRST_WEEK: Final[int] = 7


# Cron cannot express "first Friday of the month": when both day-of-month and
# day-of-week are restricted, cron runs when EITHER matches. So the cron fires
# every Friday at 06:00 and the function below skips all but the first one
# (a Friday falling on day 1-7 of the month is always the first Friday).
# Optional: enable in the UI (default status is STOPPED).
@dg.schedule(
    job=bls_laus_job,
    cron_schedule="0 6 * * 5",
    execution_timezone=SCHEDULE_TIMEZONE,
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
def bls_laus_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    tz: ZoneInfo = ZoneInfo(SCHEDULE_TIMEZONE)
    scheduled_time: datetime = (
        context.scheduled_execution_time.astimezone(tz)
        if context.scheduled_execution_time is not None
        else datetime.now(tz)  # e.g. when tested manually
    )

    if scheduled_time.day > LAST_DAY_OF_FIRST_WEEK:
        return dg.SkipReason(
            f"{scheduled_time:%Y-%m-%d} is not the first Friday of the month."
        )

    return dg.RunRequest(run_key=f"{scheduled_time:%Y-%m}")  # one run per month