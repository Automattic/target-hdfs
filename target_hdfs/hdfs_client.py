import os
import tempfile
from datetime import datetime

import requests
import singer
from hdfs import InsecureClient
from pyarrow.parquet import ParquetWriter
from requests.auth import HTTPBasicAuth

from target_hdfs.helpers import flatten_schema_to_pyarrow_schema

import pyarrow as pa

LOGGER = singer.get_logger()
LOGGER.setLevel(os.getenv("LOGGER_LEVEL", "INFO"))


class HDFSClient:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
    ):
        self.host = host
        self.user = user
        self.password = password
        session = requests.Session()
        session.auth = HTTPBasicAuth(self.user, self.password)
        self.client = InsecureClient(self.host, user=self.user)

    def upload(self, local_file, destination_path, overwrite) -> None:
        self.client.upload(hdfs_path=local_file, local_path=destination_path, overwrite=overwrite)

    @staticmethod
    def create_dataframe(list_dict, schema, force_output_schema_cast=True):
        fields = set()
        for d in list_dict:
            fields = fields.union(d.keys())
        data = {f: [row.get(f) for row in list_dict] for f in fields}
        dataframe = pa.table(data)
        if force_output_schema_cast:
            if schema:
                dataframe = dataframe.cast(flatten_schema_to_pyarrow_schema(schema, list(fields)))
            else:
                raise Exception("Not possible to force the cast because the schema was not provided.")
        return dataframe

    def write_file(
            self,
            current_stream_name,
            records,
            schema,
            hdfs_destination_path,
            compression_extension,
            compression_method,
            filename_separator='-',
            include_sync_ymd_partition=True,
            streams_in_separate_folder=True,
            force_output_schema_cast=True):
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S-%f")
        LOGGER.debug(f"Writing files from {current_stream_name} stream to HDFS")
        with tempfile.NamedTemporaryFile() as tmp:
            dataframe = self.create_dataframe(records, schema, force_output_schema_cast)
            ParquetWriter(tmp, dataframe.schema, compression=compression_method).write_table(dataframe)

            filename = (current_stream_name + filename_separator + timestamp + compression_extension + ".parquet")
            hdfs_filepath = hdfs_destination_path
            if streams_in_separate_folder:
                hdfs_filepath = os.path.join(hdfs_filepath, current_stream_name)
            if include_sync_ymd_partition:
                sync_ymd = datetime.utcnow().strftime("%Y%m%d")
                hdfs_filepath = os.path.join(hdfs_filepath, f"synced_ymd={sync_ymd}")
            hdfs_filepath = os.path.join(hdfs_filepath, filename)
            self.upload(tmp, hdfs_filepath)
