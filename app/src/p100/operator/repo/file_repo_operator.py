import os, logging
import shutil

PARAM_LOCAL_URL = "local_url"
PARAM_REMOTE_URL = "remote_url"
PARAM_EVENT = "event"
PARAM_OPERATION_MODE = "operation_mode"
SUPPORTED_EVENTS = {"checkout", "refresh", "branch"}

logger = logging.getLogger(__name__)

def execute(**context):
    # Get the context
    local_url = context[PARAM_LOCAL_URL]
    remote_url = context[PARAM_REMOTE_URL]
    event = context[PARAM_EVENT].lower()
    operation_mode = context[PARAM_OPERATION_MODE]

    logger.info(
        f"Copying files from {remote_url} to {local_url} for event: {event} and operation mode: {operation_mode}"
    )

    if event not in SUPPORTED_EVENTS:
        logger.error(
            f"Invalid event: {event}. Supported events are: {', '.join(SUPPORTED_EVENTS)}"
        )
        return -1

    if not os.path.isdir(remote_url):
        logger.error(f"Remote URL is not an existing directory: {remote_url}")
        return -2

    if os.path.isdir(local_url) and os.listdir(local_url):
        logger.error(f"Local repository exists and is not empty: {local_url}")
        return -3

    # Recursively copy all files from remote_url to local_url if the event is checkout or refresh
    if event in {"checkout", "refresh"}:
        try:
            shutil.copytree(remote_url, local_url, dirs_exist_ok=True)
        except Exception as e:
            logger.error(f"Error copying files from {remote_url} to {local_url}: {e}")
            return -4

    logger.info(f"Successfully copied files from {remote_url} to {local_url}")
    return 0
