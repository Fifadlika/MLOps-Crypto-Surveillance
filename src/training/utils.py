import os


def get_tracking_uri() -> str:
    uri = os.getenv("MLFLOW_TRACKING_URI")
    if not uri:
        raise EnvironmentError("MLFLOW_TRACKING_URI is not set")
    return uri
