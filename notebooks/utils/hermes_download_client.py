import glob
import os
import secrets
import shutil

import connectorx
import polars as pl
import pyarrow.parquet

from utils import quickflow_blocks

TIMESCALE_CONNECTION_STRING = os.environ.get("TIMESCALE_CONNECTION_STRING")
DATA_DIRECTORY = os.environ.get("DATA_DIRECTORY")


def _extract(query):
    """Download data from PostgreSQL+TimescaleDB and return as pyarrow table."""
    return connectorx.read_sql(
        conn=TIMESCALE_CONNECTION_STRING,
        query=query,
        return_type="arrow2",
        protocol="binary",
    )


def _write(table, tags=["test"], directory="", overwrite=False):
    """Write pyarrow table to a parquet file."""
    path = os.path.join(directory, f"{'-'.join(map(str, tags))}.parquet")
    if not overwrite and os.path.isfile(path):
        raise FileExistsError()
    os.makedirs(directory, exist_ok=True)
    pyarrow.parquet.write_table(table, where=path, version="2.6")


def _load(pattern, directory=""):
    """Lazily load a polars DataFrame from a parquet file pattern."""
    return pl.scan_parquet(os.path.join(directory, f"{pattern}.parquet"))


class _ExtractMeasurements(quickflow_blocks.Component):
    """Extract measurements from PostgreSQL+TimescaleDB incrementally.

    First, we determine the latest measurement in the local parquet files. Then, we
    download everything that's newer than that. This works, because measurements are
    never updated in the database, only appended.
    """

    def __init__(self, chunksize=(2**12)):
        self.directory = os.path.join(DATA_DIRECTORY, "download", "chunks")
        self.chunksize = chunksize

    def _read_tail(self):
        """Get receipt_timestamp and index of latest measurement."""
        # get latest timestamp from last chunk
        try:
            return (
                _load("measurements-chunk-*", directory=self.directory)
                .sort("receipt_timestamp")
                .select("receipt_timestamp")
                .last()
                .collect()
                .row(0)
            )
        except FileNotFoundError:
            pass
        # get timestamp from acropolis.parquet if no chunk is available
        try:
            return (
                _load("acropolis", directory=os.path.join(DATA_DIRECTORY, "download"))
                .sort("receipt_timestamp")
                .select("receipt_timestamp")
                .last()
                .collect()
                .row(0)
            )
        except FileNotFoundError:
            # download from start
            return [None]

    def execute(self):
        # Delete (then recreate) fragment to avoid many small and incomplete chunks
        for path in glob.glob(
            os.path.join(self.directory, "measurements-chunk-*-fragment.parquet")
        ):
            os.remove(path)

        # Download chunks incrementally
        flag = True
        print("Start downloading from datetime: ")
        while flag:
            receipt_timestamp = self._read_tail()[0]
            print(receipt_timestamp)
            # Define query and download
            query = (
                f"""
                    SELECT *
                    FROM measurement
                    ORDER BY receipt_timestamp
                    LIMIT {self.chunksize}
                """
                if receipt_timestamp is None
                else f"""
                    SELECT *
                    FROM measurement
                    WHERE receipt_timestamp > '{receipt_timestamp}'
                    ORDER BY receipt_timestamp
                    LIMIT {self.chunksize}
                """
            )
            table = _extract(query)
            tags = ["measurements", "chunk", secrets.token_hex(4)]
            if len(table) < self.chunksize:
                if len(table) == 0:  # Stop in case the result is empty
                    break
                flag = False
                tags.append("fragment")  # Mark incomplete chunks as fragments
            _write(table=table, tags=tags, directory=self.directory)
            # Pivot with polars and overwrite
            path = os.path.join(self.directory, f"{'-'.join(map(str, tags))}.parquet")

            pl.read_parquet(path).pivot(
                values="value",
                index=[
                    "sensor_identifier",
                    "revision",
                    "creation_timestamp",
                    "receipt_timestamp",
                ],
                columns="attribute",
                aggregate_function="first",
            ).write_parquet(path, statistics=True)


class _ExtractMetadata(quickflow_blocks.Component):
    """Extract metadata from PostgreSQL+TimescaleDB.

    This includes the `sensors` and `configurations` table. The download is not
    incremental, because records can be updated in the database. The metadata is
    comparatively small, so this is not a problem.
    """

    def __init__(self):
        self.directory = os.path.join(DATA_DIRECTORY, "download", "metadata")

    def execute(self):
        for tablename in ["configuration", "sensor"]:
            table = _extract(f"SELECT * FROM {tablename}")
            _write(
                table=table,
                tags=[tablename + "s"],
                directory=self.directory,
                overwrite=True,
            )


class _Merge(quickflow_blocks.Component):
    """Merge measurements into a single table and file."""

    def __init__(self):
        self.directory = os.path.join(DATA_DIRECTORY, "download")

    def execute(self):
        acropolis = _load("acropolis", directory=self.directory)
        sensors = _load("sensors", directory=os.path.join(self.directory, "metadata"))

        # append all downloaded chunks to existing local acropolis copy
        pivots = [acropolis]
        paths = glob.glob(
            os.path.join(DATA_DIRECTORY, "download", "chunks", "*.parquet")
        )

        # Merge chunks & sensor metadata
        for path in paths:
            pivots.append(
                pl.scan_parquet(path)
                .join(
                    sensors.select("identifier", "name"),
                    how="left",
                    left_on="sensor_identifier",
                    right_on="identifier",
                )
                .drop("sensor_identifier")
                .rename({"name": "system_name"})
                .with_columns(pl.col("creation_timestamp").dt.cast_time_unit("us"))
            )

        # perform a diagonal concat for all parquets in pivots
        print("Performing merge.")
        pl.concat(pivots, how="diagonal").collect().write_parquet(
            os.path.join(DATA_DIRECTORY, "download", "acropolis.parquet"),
            statistics=True,
        )

        # Delete chunks that were merged
        print("Deleting merged chunks.")
        for path in paths:
            os.remove(path)

        # Return structured raw data as LazyFrame for downstream components
        return _load("acropolis", directory=self.directory)


class _RemoveDirectory(quickflow_blocks.Component):
    """Delete a directory (e.g. to reset after a change to the data schema)."""

    def __init__(self, directory):
        self.directory = directory

    def execute(self):
        shutil.rmtree(self.directory)


class Extract(quickflow_blocks.Composite):
    """Extract ACROPOLIS data from PostgreSQL+TimescaleDB and structure it.

    Returns a polars LazyFrame.
    """

    def __init__(self, chunksize=2**14, reset=False):
        self._components = [
            _ExtractMeasurements(chunksize=chunksize),
            _ExtractMetadata(),
            _Merge(),
        ]
        if reset:
            self._components.insert(0, _RemoveDirectory(directory="download"))
