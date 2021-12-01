import os
import logging
from google.cloud.logging import Client, Resource
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging


def setup_logger():
    """ルートロガーに CloudLoggingの設定をする"""

    logging_client = Client()
    resource = Resource(
        type="cloud_run_revision",
        labels={
            "service_name": os.environ.get("_SERVICE_NAME"),
            "project_id": os.environ.get("_PROJECT_ID"),
            "location": os.environ.get("_REGION"),
        },
    )
    handler = CloudLoggingHandler(logging_client, resource=resource)
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)

    return
