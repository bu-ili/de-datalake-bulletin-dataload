from dagster import (
    schedule,
    RunRequest,
    SkipReason,
    ScheduleEvaluationContext,
    define_asset_job,
)
from de_datalake_bulletin_dataload.defs.resources import ConfigResource
from de_datalake_bulletin_dataload.defs.assets import (
    fetch_export_pages_data,
    fetch_export_media_data,
)
from de_datalake_bulletin_dataload.defs.utilities import fetch_latest_modified_date


bulletin_raw_job = define_asset_job(
    name="bulletin_raw_job",
    selection=[fetch_export_pages_data, fetch_export_media_data],
    tags={"dagster/asset_group": "bulletin_raw"},
    description="Job to materialize bulletin_raw asset group for Bulletin data lake.",
)


@schedule(
    cron_schedule="0 10 * * *",  # Daily at 10:00 AM UTC
    job=bulletin_raw_job,
    name="bulletin_daily_schedule",
    description="Daily schedule to check for new or modified content in Bulletin API and trigger incremental loads.",
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "image": "de-bulletin:latest",
                "resources": {
                    "requests": {"cpu": "500m", "memory": "1Gi"},
                    "limits": {"cpu": "1000m", "memory": "2Gi"},
                },
            },
            "pod_spec_config": {"imagePullSecrets": [{"name": "ghcr-creds"}]},
        },
    },
)
def bulletin_daily_schedule(context: ScheduleEvaluationContext, get_config: ConfigResource):
    """Daily schedule to check for new or modified content in Bulletin API and trigger incremental loads.

    Uses cursor to store last known modified dates for both pages and media endpoints
    in the format 'pages_modified|media_modified'. Runs daily at 10:00 AM UTC.

    Implements idempotent incremental loads by passing last_modified_date to assets,
    which filter API queries to only fetch records modified since the last run.

    Args:
        context (ScheduleEvaluationContext): Dagster ScheduleEvaluationContext for resources and logging.

    Returns:
        RunRequest: If new data is detected, triggers asset materialization with incremental config and updated cursor.
        SkipReason: If no new data is detected, skips the run with a reason.
    """
    default_baseline = get_config.get_config_value("default_baseline_datetime")
    cursor = context.cursor or f"{default_baseline}|{default_baseline}"
    last_pages_modified, last_media_modified = cursor.split("|")

    latest_pages = fetch_latest_modified_date("pages", get_config)
    latest_media = fetch_latest_modified_date("media", get_config)

    context.log.info(
        f"Last known - Pages: {last_pages_modified}, Media: {last_media_modified}"
    )
    context.log.info(f"Current latest - Pages: {latest_pages}, Media: {latest_media}")

    has_new_data = (latest_pages > last_pages_modified) or (
        latest_media > last_media_modified
    )

    if has_new_data:
        context.log.info(
            "New data detected! Triggering incremental asset materialization."
        )

        context.update_cursor(f"{latest_pages}|{latest_media}")

        return RunRequest(
            run_key=f"{latest_pages}_{latest_media}",
            run_config={
                "ops": {
                    "bulletin_raw_pages": {
                        "config": {
                            "upload_to_s3": False,
                            "last_modified_date": latest_pages,
                            "full_refresh": False,
                        }
                    },
                    "bulletin_raw_media": {
                        "config": {
                            "upload_to_s3": False,
                            "last_modified_date": latest_media,
                            "full_refresh": False,
                        }
                    },
                }
            },
        )

    context.log.info("No new data detected. Skipping run.")
    return SkipReason(
        f"No new data. Latest - Pages: {latest_pages}, Media: {latest_media}"
    )
