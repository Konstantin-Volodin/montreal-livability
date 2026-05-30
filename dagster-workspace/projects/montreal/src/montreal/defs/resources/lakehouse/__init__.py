"""S3-backed lakehouse: GeoDataFrames as timestamped Parquet snapshots."""

import dagster as dg

from .paths import format_size, location_of
from .store import s3_datastore

__all__ = ["s3_datastore", "location_of", "format_size"]


@dg.definitions
def resources() -> dg.Definitions:
    """Bind the s3_datastore resource into the autoloaded defs folder."""
    return dg.Definitions(
        resources={
            "s3_datastore": s3_datastore(
                bucket_name=dg.EnvVar("S3_BUCKET"),
                region_name=dg.EnvVar("S3_REGION"),
                aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
            ),
        }
    )
