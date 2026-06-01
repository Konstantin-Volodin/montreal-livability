"""Pure addressing + time helpers for the lakehouse (no I/O)."""

from datetime import datetime, timezone

import dagster as dg

# Fixed-width UTC stamp: lexical order == chronological order, so two stamps
# compare as plain strings. The change-detection skip relies on that.
_STAMP = "%Y%m%dT%H%M%S_%fZ"


def format_size(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f} MB"


def location_of(asset: dg.AssetsDefinition) -> str:
    """Lakehouse directory an asset writes to: ``{layer}/{asset_name}``.

    Lets a consumer address an upstream asset by the object itself (already
    imported for ``deps=``) instead of restating its S3 path as a string.
    """
    layer = asset.metadata_by_key[asset.key].get("layer", "unknown_layer")
    return f"{layer}/{asset.key.path[-1]}"


def now_stamp() -> str:
    """Sortable UTC stamp used as both the snapshot filename and data version."""
    return f"{datetime.now(timezone.utc):{_STAMP}}"


def parse_stamp(stamp: str) -> datetime:
    return datetime.strptime(stamp, _STAMP).replace(tzinfo=timezone.utc)


def stamp_of(key: str) -> str:
    """Recover the stamp from a snapshot key ``.../{stamp}.parquet``."""
    return key.rsplit("/", 1)[-1].removesuffix(".parquet")
