"""S3-backed lakehouse: GeoDataFrames as timestamped Parquet snapshots."""

import os

import dagster as dg

from .paths import format_size, location_of
from .store import s3_datastore

__all__ = ["s3_datastore", "location_of", "format_size"]


def _optional_env(name: str) -> dg.EnvVar | None:
    """Bind only when set: on Fargate the task role supplies credentials and the
    AWS key env vars don't exist (EnvVar hard-fails on unset variables)."""
    return dg.EnvVar(name) if os.getenv(name) else None


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "s3_datastore": s3_datastore(
                bucket_name=dg.EnvVar("S3_BUCKET"),
                region_name=dg.EnvVar("S3_REGION"),
                aws_access_key_id=_optional_env("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=_optional_env("AWS_SECRET_ACCESS_KEY"),
            ),
        }
    )
