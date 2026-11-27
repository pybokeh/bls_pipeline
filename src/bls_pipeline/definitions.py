import dagster as dg

from .defs import assets
from .defs.jobs import bls_laus_job
from .defs.resources import BlsApiResource, PostgresResource
from .defs.schedules import bls_laus_schedule
from .defs.assets import state_codes

# state_codes is an external asset defined with an AssetSpec
# It isn't discoverable with the load_assets_from_modules() method
# So have to add it explicitly in the assets= parameter

all_assets = dg.load_assets_from_modules([assets])

defs: dg.Definitions = dg.Definitions(
    assets=[*all_assets, state_codes],
    jobs=[bls_laus_job],
    schedules=[bls_laus_schedule],
    resources={
        "bls_api": BlsApiResource(
            user_agent=dg.EnvVar("BLS_USER_AGENT"),
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