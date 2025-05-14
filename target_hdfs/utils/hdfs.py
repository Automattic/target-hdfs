from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import cache
from pathlib import Path
from subprocess import DEVNULL, PIPE, run
from tempfile import NamedTemporaryFile
from typing import TypedDict

import pyarrow as pa

from target_hdfs.utils import convert_size_to_bytes

logger = logging.getLogger(__name__)


class SchemaChangedError(Exception):
    """Exception for schema change."""


class HDFSFile(TypedDict):
    """HDFS file content (pyarrow table) and path."""

    content: pa.Table
    path: str


class FileSize(TypedDict):
    """File size in bytes."""

    path: str
    size: int


@cache
def get_hdfs_block_size() -> int:
    """Run the HDFS getconf command to get HDFS blocksize."""
    cmd = ["hdfs", "getconf", "-confKey", "dfs.blocksize"]
    result = run(cmd, stdout=PIPE, stderr=DEVNULL, text=True, check=True)
    hdfs_block_size = int(convert_size_to_bytes(result.stdout.strip()))
    logger.info(f"HDFS block size: {hdfs_block_size} bytes")
    return hdfs_block_size


def download_from_hdfs(source_path_hdfs: str, local_path: str) -> None:
    """Download a file from HDFS."""
    # Removing local temp file as hdfs -get command does not overwrite it
    Path(local_path).unlink(missing_ok=True)
    logger.info(f"Download file from HDFS: {source_path_hdfs} ")
    cmd = ["hdfs", "dfs", "-get", source_path_hdfs, local_path]
    run(cmd, stdout=DEVNULL, stderr=DEVNULL, check=True)
    logger.debug(f"File {source_path_hdfs} downloaded from hdfs to {local_path}")


def upload_to_hdfs(local_file: str, destination_path_hdfs: str) -> None:
    """Upload a local file to HDFS."""
    logger.debug(f"Uploading file to HDFS: {destination_path_hdfs} ")
    new_hdfs_file = destination_path_hdfs + "_new"
    cmd = ["hdfs", "dfs", "-put", "-f", local_file, new_hdfs_file]
    run(cmd, stdout=DEVNULL, stderr=DEVNULL, check=True)
    replace_old_file_with_new_file(new_hdfs_file)
    logger.info(f"File {destination_path_hdfs} uploaded to HDFS")


def replace_old_file_with_new_file(new_file_path: str) -> None:
    """Replace the old file with the new file in HDFS."""
    old_file_path = new_file_path.replace("_new", "")
    logger.info(f"Replacing old file {old_file_path} with new file: {new_file_path}")
    cmd = ["hdfs", "dfs", "-mv", new_file_path, old_file_path]
    run(cmd, stdout=DEVNULL, stderr=DEVNULL, check=True)


def get_most_recent_file(hdfs_path: str) -> FileSize | None:
    """Get the most recent modified parquet file in a given HDFS path."""
    cmd = ["hdfs", "dfs", "-ls", hdfs_path]
    result = run(cmd, stdout=PIPE, stderr=DEVNULL, text=True, check=True)

    most_recent = None
    most_recent_date_time = datetime.min
    for line in result.stdout.splitlines():
        if line.startswith("-"):
            parts = line.split()
            path = " ".join(parts[7:])
            if path.endswith(".parquet"):
                file_size, date_str, time_str = parts[4:7]
                timestamp = datetime.strptime(
                    f"{date_str} {time_str}", "%Y-%m-%d %H:%M"
                ).replace(tzinfo=timezone.utc)
                if timestamp > most_recent_date_time:
                    most_recent = FileSize(path=path, size=int(file_size))

    logger.info(f"Most recent parquet file: {most_recent}")
    return most_recent


def create_hdfs_directory(hdfs_path: str) -> None:
    """Create a directory in HDFS."""
    logger.info(f"Creating directory in HDFS: {hdfs_path}")
    cmd = ["hdfs", "dfs", "-mkdir", "-p", hdfs_path]
    run(cmd, stdout=DEVNULL, stderr=DEVNULL, check=True)
    logger.info(f"Directory {hdfs_path} created in HDFS")


def read_most_recent_file(
    hdfs_file_path: str,
    pyarrow_schema: pa.Schema,
    hdfs_block_size_limit: str | None,
) -> HDFSFile | None:
    """Read the last file from HDFS."""
    block_size_limit = (
        convert_size_to_bytes(hdfs_block_size_limit)
        if hdfs_block_size_limit
        else get_hdfs_block_size() * 0.85
    )
    most_recent_file = get_most_recent_file(hdfs_file_path)

    # Force creates a new file if the last file is larger than 85% of the HDFS block size or does not exist
    if not most_recent_file or (most_recent_file["size"] >= block_size_limit):
        return None

    with NamedTemporaryFile("wb") as tmp_file:
        download_from_hdfs(most_recent_file["path"], tmp_file.name)
        parquet_df = pa.parquet.read_table(tmp_file.name)
        if set(parquet_df.schema).symmetric_difference(set(pyarrow_schema)):
            raise SchemaChangedError(
                f"Schema of the file {most_recent_file['path']} does not match the expected schema.\n"
                f"Difference: \n{set(parquet_df.schema).symmetric_difference(set(pyarrow_schema))}\n"
                f"Schema of the file: \n{parquet_df.schema}\n"
                f"Schema of the stream: \n{pyarrow_schema}"
            )
        if not parquet_df.schema.equals(pyarrow_schema):
            logger.info("Rearranging columns to match the schema")
            parquet_df = parquet_df.select(pyarrow_schema.names)
        return {"content": parquet_df, "path": most_recent_file["path"]}
