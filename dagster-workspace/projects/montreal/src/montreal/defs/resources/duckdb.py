"""DuckDB query engine: read in any data and convert it into SQL.

`SQL` is a lazily-composed SQL fragment that can carry bound values, nested
`SQL` fragments, and in-memory ``pandas`` DataFrames. `DuckDB` materialises a
fragment by registering any DataFrames and running the rendered query against
an in-memory DuckDB connection (with ``httpfs`` loaded so S3 paths work).
"""

from string import Template
from typing import Mapping, Optional

import dagster as dg
import pandas as pd
from dagster import ConfigurableResource
from duckdb import connect
from sqlescapy import sqlescape


class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings


def sql_to_string(s: SQL) -> str:
    replacements = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            replacements[key] = f"df_{id(value)}"
        elif isinstance(value, SQL):
            replacements[key] = f"({sql_to_string(value)})"
        elif isinstance(value, str):
            replacements[key] = f"'{sqlescape(value)}'"
        elif isinstance(value, (int, float, bool)):
            replacements[key] = str(value)
        elif value is None:
            replacements[key] = "null"
        else:
            raise ValueError(f"Invalid type for {key}")
    return Template(s.sql).safe_substitute(replacements)


def collect_dataframes(s: SQL) -> Mapping[str, pd.DataFrame]:
    dataframes = {}
    for key, value in s.bindings.items():
        if isinstance(value, pd.DataFrame):
            dataframes[f"df_{id(value)}"] = value
        elif isinstance(value, SQL):
            dataframes.update(collect_dataframes(value))
    return dataframes


def read_data(path: str, format: Optional[str] = None) -> SQL:
    """Build a `SQL` fragment that reads any supported source into a table.

    ``path`` may be a local path or an ``s3://`` / ``http(s)://`` URL. The
    format is inferred from the extension when not given explicitly. Supported
    formats: ``parquet``, ``csv``, ``json``.
    """
    if format is None:
        ext = path.rsplit(".", 1)[-1].lower()
        format = {"pq": "parquet"}.get(ext, ext)

    readers = {
        "parquet": "read_parquet",
        "csv": "read_csv_auto",
        "json": "read_json_auto",
    }
    if format not in readers:
        raise ValueError(
            f"Unsupported format {format!r}; expected one of {sorted(readers)}"
        )

    return SQL(f"select * from {readers[format]}($path)", path=path)


class DuckDB(ConfigurableResource):
    """In-memory DuckDB engine that materialises `SQL` fragments.

    ``options`` is raw SQL run on every fresh connection (e.g. ``SET`` /
    ``CREATE SECRET`` statements). When ``aws_region`` /
    ``aws_access_key_id`` / ``aws_secret_access_key`` are provided, an S3
    secret is created so ``s3://`` reads and writes are authenticated.
    """

    options: str = ""
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    def _connect(self):
        db = connect(":memory:")
        db.query("install httpfs; load httpfs;")

        if self.aws_access_key_id and self.aws_secret_access_key:
            db.execute(
                """
                create or replace secret aws_s3 (
                    type s3,
                    key_id ?,
                    secret ?,
                    region ?
                )
                """,
                [
                    self.aws_access_key_id,
                    self.aws_secret_access_key,
                    self.aws_region or "us-east-1",
                ],
            )
        elif self.aws_region:
            db.query(f"set s3_region='{sqlescape(self.aws_region)}';")

        if self.options:
            db.query(self.options)
        return db

    def query(self, select_statement: SQL):
        db = self._connect()

        dataframes = collect_dataframes(select_statement)
        for key, value in dataframes.items():
            db.register(key, value)

        result = db.query(sql_to_string(select_statement))
        if result is None:
            return
        return result.df()


@dg.definitions
def resources() -> dg.Definitions:
    """Bind the duckdb resource into the autoloaded defs folder."""
    return dg.Definitions(
        resources={
            "duckdb": DuckDB(
                aws_region=dg.EnvVar("S3_REGION"),
                aws_access_key_id=dg.EnvVar("S3_ACCESS_KEY"),
                aws_secret_access_key=dg.EnvVar("S3_SECRET_KEY"),
            ),
        }
    )
