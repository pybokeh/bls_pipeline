import dagster as dg

from .defs import assets
from .defs.jobs import bls_laus_job
from .defs.resources import BlsApiResource, PostgresResource
from .defs.schedules import bls_laus_schedule

defs: dg.Definitions = dg.Definitions(
    assets=dg.load_assets_from_modules([assets]),
    jobs=[bls_laus_job],
    schedules=[bls_laus_schedule],
    resources={
        "bls_api": BlsApiResource(
            user_agent=dg.EnvVar("BLS_USER_AGENT"),  # e.g. "Your Name your.email@example.com"
        ),
        "postgres": PostgresResource(
            host=dg.EnvVar("DB_HOST"),
            port=dg.EnvVar("DB_PORT"),
            user=dg.EnvVar("DB_USER"),
            password=dg.EnvVar("DB_PASSWORD"),
            database=dg.EnvVar("BLS_DB_NAME"),
        ),
    },
)