"""Change-detection skip: decide whether an asset can reuse its existing S3 output.

Serverless caching with no database - the only state is the timestamped snapshots
and their manifests. ``should_skip`` compares stamps; ``reemit_latest`` re-emits the
existing output unchanged. Both take the ``s3_datastore`` they query.
"""

from typing import Optional

import dagster as dg

from .paths import location_of


def should_skip(store, context, upstreams, code_version: Optional[str] = None) -> bool:
    """Whether this asset can reuse its existing output instead of recomputing.

    True iff (a) it already has an output, (b) that output was produced by the
    same ``code_version``, and (c) every upstream's latest stamp is no newer than
    this asset's own output stamp - i.e. nothing upstream changed since. Each
    ``upstreams`` entry is a directory string, or a ``(directory, is_prefix)``
    tuple where ``is_prefix`` takes the newest stamp across the dir's shards.

    Because a bronze cache hit never rewrites its snapshot, an upstream stamp only
    advances on a real content change, so stamp dominance is a sound, DB-free
    "did my inputs change" test.
    """
    mine = store.output_stamp(context)
    if mine is None:
        return False

    provenance = store._read_manifest(store._own_dir(context)) or {}
    if provenance.get("code_version") != code_version:
        context.log.info(
            f"code_version changed ({provenance.get('code_version')!r} -> {code_version!r}); recomputing."
        )
        return False

    for entry in upstreams:
        directory, is_prefix = entry if isinstance(entry, tuple) else (entry, False)
        upstream = (
            store.latest_stamp_under_prefix(directory) if is_prefix else store.latest_stamp(directory)
        )
        if upstream is None or upstream > mine:
            context.log.info(f"upstream {directory} changed (upstream={upstream}, mine={mine}); recomputing.")
            return False

    context.log.info(f"inputs unchanged since {mine}; reusing existing output.")
    return True


def reemit_latest(store, context) -> dg.MaterializeResult:
    """Re-emit the existing output: stable DataVersion + cache-hit metadata, no S3 write.

    The manifest still names a valid snapshot, so downstream reads keep working;
    re-emitting the same DataVersion means no spurious invalidation.
    """
    stamp = store.output_stamp(context)
    try:
        if context.has_partition_key:
            store.describe_latest(context, store.asset_dir(context, context.partition_key))
        elif context.assets_def.metadata_by_key[context.asset_key].get("segmentation") in (None, "snapshot"):
            store.describe_latest(context, location_of(context.assets_def))
    except FileNotFoundError:
        pass  # sharded base dir has no snapshot of its own; the stable DataVersion is enough
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"skipped_unchanged": dg.MetadataValue.bool(True)},
    )
