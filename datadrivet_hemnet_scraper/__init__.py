from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import assets_schedule

defs = Definitions(
        assets=load_assets_from_modules([assets]),
        schedules=[assets_schedule]
)
