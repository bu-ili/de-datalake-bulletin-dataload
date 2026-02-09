from dagster import (
    schedule,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
)
from de_datalake_bulletin_dataload.defs.assets import (
    fetch_export_pages_data,
    fetch_export_media_data,
)


bulletin_raw_job = define_asset_job(
    name="bulletin_raw_job",
    selection=[fetch_export_pages_data, fetch_export_media_data],
    tags={"dagster/asset_group": "bulletin_raw"},
    description="Job to materialize bulletin_raw asset group for Bulletin data lake.",
)


@schedule(
    cron_schedule="*/10 * * * *",  # Every 10 minutes (for testing)
    job=bulletin_raw_job,
    name="bulletin_daily_schedule",
    description="Schedule to trigger incremental loads for Bulletin data. Assets auto-discover baseline from S3.",
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
def bulletin_daily_schedule(context: ScheduleEvaluationContext):
    """Trigger bulletin asset materialization with incremental loading.

    Assets auto-discover baseline from S3 by querying the most recent load_date partition.
    Runs every 3 minutes for testing (change to daily in production).

    Args:
        context (ScheduleEvaluationContext): Dagster ScheduleEvaluationContext for logging.

    Returns:
        RunRequest: Triggers asset materialization with incremental config.
    """
    context.log.info("Triggering bulletin asset materialization (incremental mode)")

    return RunRequest(
        run_key=context.scheduled_execution_time.isoformat(),
        run_config={
            "ops": {
                "bulletin_raw_pages": {
                    "config": {
                        "upload_to_s3": False,
                        "full_refresh": False,
                    }
                },
                "bulletin_raw_media": {
                    "config": {
                        "upload_to_s3": False,
                        "full_refresh": False,
                    }
                },
            }
        },
        tags={"source": "bulletin_daily_schedule"},
    )
