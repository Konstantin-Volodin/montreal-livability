"""GeoDataFrame <-> Parquet helpers (no S3, no I/O beyond in-memory buffers)."""

import io

import geopandas as gpd
import pandas as pd


def to_wgs84(gdf):
    """Reproject a GeoDataFrame to EPSG:4326; non-geo frames pass through.

    Normalizing on write means consumers can assume lat/lng and never reproject on read.
    """
    if not isinstance(gdf, gpd.GeoDataFrame):
        return gdf
    if gdf.crs is None:
        return gdf.set_crs(4326, allow_override=True)
    if gdf.crs.to_epsg() != 4326:
        return gdf.to_crs(4326)
    return gdf


def to_parquet_bytes(gdf) -> bytes:
    buffer = io.BytesIO()
    gdf.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
    return buffer.getvalue()


def read_parquet_bytes(raw: bytes):
    """Parse Parquet bytes, falling back to plain pandas when there's no geometry (gold tabular assets)."""
    buffer = io.BytesIO(raw)
    try:
        return gpd.read_parquet(buffer)
    except (ValueError, AttributeError):
        buffer.seek(0)
        return pd.read_parquet(buffer)


def preview(gdf, n: int = 5) -> str:
    """Markdown preview of a frame, dropping the geometry column."""
    df = gdf.head(n).drop(columns="geometry", errors="ignore")
    try:
        return df.to_markdown()
    except ImportError:  # `to_markdown` needs `tabulate`; fall back to a plain table.
        return f"```\n{df.to_string()}\n```"
