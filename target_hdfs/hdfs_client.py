import pyarrow as pa
import singer

LOGGER = singer.get_logger()


class HDFSClient:
    def __init__(self):
        self._client = pa.fs.HadoopFileSystem('default')

    def create_hdfs_dir(self, hdfs_path):
        """Create HDFS Path"""
        LOGGER.info(f"Creating hdfs {hdfs_path} path")
        self._client.create_dir(hdfs_path)

    def upload_to_hdfs(self, local_file, destination_path_hdfs) -> None:
        """Upload a local file to HDFS using RPC"""
        pa.fs.copy_files(
            local_file,
            destination_path_hdfs,
            source_filesystem=pa.fs.LocalFileSystem(),
            destination_filesystem=self._client
        )


