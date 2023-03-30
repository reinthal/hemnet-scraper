from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from .assets import  (initial_hemnet_search_start_pages, 
                    hemnet_search_links, 
                    hemnet_initial_search_links_webpages,
                    hemnet_search_basic_listing_data,
                    hemnet_search_detailed_listing_data)

all_assets_job = define_asset_job(name="all_assets_job", selection="*")

assets_schedule = ScheduleDefinition(job=all_assets_job, cron_schedule="@daily")