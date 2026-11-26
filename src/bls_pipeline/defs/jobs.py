import dagster as dg

from .assets import GROUP_NAME

bls_laus_job: dg.UnresolvedAssetJobDefinition = dg.define_asset_job(
    name="bls_laus_job",
    selection=dg.AssetSelection.groups(GROUP_NAME),
    description="Refresh all BLS LAUS tables in Postgres.",
)