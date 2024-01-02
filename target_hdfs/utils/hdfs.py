import logging
from functools import cache
from subprocess import run
from tempfile import NamedTemporaryFile
from typing import Optional

import pyarrow as pa
from pyarrow._fs import FileInfo

from target_hdfs.utils import convert_size_to_bytes

logger = logging.getLogger(__name__)


@cache
def get_hdfs_client():
    """Get a HDFS client"""
    return pa.fs.HadoopFileSystem("default")


@cache
def get_hdfs_block_size():
    """Run the HDFS getconf command to get HDFS blocksize"""
    cmd = ["hdfs", "getconf", "-confKey", "dfs.blocksize"]
    result = run(cmd, capture_output=True, text=True)

    block_size = int(convert_size_to_bytes(result.stdout.strip()))
    return block_size


def download_from_hdfs(source_path_hdfs, local_path) -> None:
    logger.debug(f"Uploading file from HDFS: {source_path_hdfs} ")
    pa.fs.copy_files(
        source_path_hdfs,
        local_path,
        source_filesystem=get_hdfs_client(),
        destination_filesystem=pa.fs.LocalFileSystem(),
    )
    logger.debug(f"File {source_path_hdfs} downloaded from hdfs to {local_path} ")


def upload_to_hdfs(local_file, destination_path_hdfs) -> None:
    """Upload a local file to HDFS using RPC"""
    logger.debug(f"Uploading file to HDFS: {destination_path_hdfs} ")
    pa.fs.copy_files(
        local_file,
        destination_path_hdfs,
        source_filesystem=pa.fs.LocalFileSystem(),
        destination_filesystem=get_hdfs_client(),
    )
    logger.info(f"File {destination_path_hdfs} uploaded to HDFS")
    replace_old_file_with_new_file(destination_path_hdfs)


def replace_old_file_with_new_file(new_file_path) -> None:
    """Replace the old file with the new file in HDFS"""
    hdfs_client = get_hdfs_client()
    hdfs_client.move(new_file_path, new_file_path.replace("_new", ""))


def set_file_as_old(file_path) -> None:
    """Replace the old file with the new file in HDFS"""
    hdfs_client = get_hdfs_client()
    hdfs_client.move(file_path, f"{file_path}_old")


def get_files(hdfs_path, extension=".parquet") -> list[FileInfo]:
    """Get all parquet files in a given HDFS path"""
    hdfs_client = get_hdfs_client()
    file_list = hdfs_client.get_file_info(pa.fs.FileSelector(hdfs_path, recursive=True))
    files = [file for file in file_list if file.base_name.endswith(extension)]
    return files


def get_most_recent_file(hdfs_path) -> Optional[FileInfo]:
    """Get the most recent modified parquet file in a given HDFS path"""
    files = get_files(hdfs_path)
    return max(files, key=lambda file: file.mtime) if files else None


def read_most_recent_file(hdfs_file_path) -> Optional[pa.Table]:
    """Read the last file from HDFS"""
    most_recent_file = get_most_recent_file(hdfs_file_path)
    # Force creates a new file if the last file is larger than 85% of the HDFS block size or does not exist
    if not most_recent_file and most_recent_file.size >= get_hdfs_block_size() * 0.85:
        return None
    with NamedTemporaryFile("wb") as tmp_file:
        download_from_hdfs(most_recent_file.path, tmp_file.name)
        parquet_df = pa.parquet.read_table(most_recent_file.base_name)
        # To make sure that the file will be correctly processed, we set the file as old
        set_file_as_old(most_recent_file.path)
        return parquet_df


def delete_old_files(hdfs_path) -> None:
    """Delete old files in HDFS"""
    for file in get_files(hdfs_path, extension=".parquet_old"):
        hdfs_client = get_hdfs_client()
        hdfs_client.delete_file(file.path)
