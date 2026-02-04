import httpx
from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
)
from de_datalake_bulletin_dataload.defs.resources import ConfigResource
from de_datalake_bulletin_dataload.defs.assets import (
    fetch_export_pages_data,
    fetch_export_media_data,
)


def fetch_latest_modified_date(endpoint: str, get_config: ConfigResource) -> str:
    """Fetch the most recent modified date from Bulletin Wordpress API for an endpoint.

    The sensor parameter defines the query to get the latest modified record. The response
    is compared against the last known modified date stored in the sensor cursor.

    Args:
        endpoint (str): API endpoint to query (e.g., 'pages', 'media').
        get_config (ConfigResource): ConfigResource for getting sensor parameters.

    Returns:
        str: Datetime string of the latest modified date, or '2000-01-01T00:00:00' if fetch fails.
    """
    base_url = get_config.get_base_url()
    sensor_param = get_config.get_sensor_param()
    url = f"{base_url}{endpoint}{sensor_param}"

    try:
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()
        data = response.json()

        if data and len(data) > 0:
            return data[0]["modified"]
    except Exception as e:
        print(f"Error fetching latest modified date for {endpoint}: {e}")

    return "2000-01-01T00:00:00"


@sensor(
    asset_selection=[fetch_export_pages_data, fetch_export_media_data],
    minimum_interval_seconds=86000,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"get_config"},
    name="bulletin_data_sensor",
    tags={
        "dagster/asset_group": "bulletin_raw",
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
def bulletin_data_sensor(context: SensorEvaluationContext):
    """Check for new or modified content in Bulletin API and trigger assets if found.

    Uses cursor to store last known modified dates for both pages and media endpoints
    in the format 'pages_modified|media_modified'. Runs every 24 hours.

    Args:
        context (SensorEvaluationContext): Dagster SensorEvaluationContext for resources and logging.

    Returns:
        RunRequest: If new data is detected, triggers asset materialization with updated cursor.
        SkipReason: If no new data is detected, skips the run with a reason.
    """

    cursor = context.cursor or "2000-01-01T00:00:00|2000-01-01T00:00:00"
    last_pages_modified, last_media_modified = cursor.split("|")

    latest_pages = fetch_latest_modified_date("pages", context.resources.get_config)
    latest_media = fetch_latest_modified_date("media", context.resources.get_config)

    context.log.info(
        f"Last known - Pages: {last_pages_modified}, Media: {last_media_modified}"
    )
    context.log.info(f"Current latest - Pages: {latest_pages}, Media: {latest_media}")

    has_new_data = (latest_pages > last_pages_modified) or (
        latest_media > last_media_modified
    )

    if has_new_data:
        context.log.info("New data detected! Triggering asset materialization.")

        context.update_cursor(f"{latest_pages}|{latest_media}")

        return RunRequest(
            run_key=f"{latest_pages}_{latest_media}",
            run_config={
                "ops": {
                    "bulletin_pages_dataload": {"config": {"upload_to_s3": False}},
                    "bulletin_media_dataload": {"config": {"upload_to_s3": False}},
                }
            },
        )

    context.log.info("No new data detected. Skipping run.")
    return SkipReason(
        f"No new data. Latest - Pages: {latest_pages}, Media: {latest_media}"
    )
