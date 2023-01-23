#!/usr/bin/env python3
import io
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

import singer
from jsonschema.validators import Draft4Validator

from .helpers import flatten, flatten_schema, concat_tables, write_file_to_hdfs, flatten_schema_to_pyarrow_schema, \
    bytes_to_mb

_all__ = ['main']

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'hdfs_destination_path'
]

EXTENSION_MAPPING = {
    'SNAPPY': '.snappy',
    'GZIP': '.gz',
    'BROTLI': '.br',
    'ZSTD': '.zstd',
    'LZ4': '.lz4',
}


@dataclass
class TargetConfig:
    hdfs_destination_path: str
    compression_method: str = 'gzip'
    compression_extension: str = ''
    streams_in_separate_folder: bool = True
    partitions: Optional[str] = None
    rows_per_file: Optional[int] = None
    file_size_mb: Optional[int] = None
    file_prefix: Optional[str] = None
    filename_separator: str = '-'

    def __post_init__(self):
        self.set_compression_extension()
        if self.streams_in_separate_folder:
            self.filename_separator = os.path.sep
        LOGGER.info(self)

    def set_compression_extension(self):
        if self.compression_method:
            # The target is prepared to accept all the compression methods provided by the pandas module
            self.compression_extension = EXTENSION_MAPPING.get(self.compression_method.upper())
            if self.compression_extension is None:
                raise Exception('Unsupported compression method.')

    def generate_file_name(self, stream_name):
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S-%f')
        file_name = f'{timestamp}{self.compression_extension}.parquet'
        if self.file_prefix:
            file_name = f'{self.file_prefix}-{file_name}'
        if not self.streams_in_separate_folder:
            file_name = f'{stream_name}{self.filename_separator}{file_name}'
        return file_name

    def generate_hdfs_path(self, stream_name):
        hdfs_filepath = self.hdfs_destination_path
        if self.streams_in_separate_folder:
            hdfs_filepath = os.path.join(hdfs_filepath, stream_name)
        if self.partitions:
            hdfs_filepath = os.path.join(hdfs_filepath, self.partitions)
        return os.path.join(hdfs_filepath, self.generate_file_name(stream_name))


class MessageType(Enum):
    RECORD = 1
    STATE = 2
    SCHEMA = 3
    EOF = 4


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug(f'Emitting state {line}')
        sys.stdout.write(f'{line}\n')
        sys.stdout.flush()


def persist_messages(messages, config: TargetConfig):
    state = None
    schemas = {}
    pyarrow_schemas = {}
    key_properties = {}
    validators = {}
    files_created = []
    records_count = defaultdict(int)
    records = defaultdict(list)
    pyarrow_tables = {}

    for message in messages:
        LOGGER.debug(f'target-parquet got message: {message}')
        try:
            message = singer.parse_message(message).asdict()
        except json.JSONDecodeError as exc:
            raise Exception(f'Unable to parse:\n{message}') from exc

        message_type = message['type']
        if message_type == 'RECORD':
            stream_name = message['stream']
            if stream_name not in schemas:
                raise ValueError(f'A record for stream {stream_name} was encountered '
                                 f'before a corresponding schema')
            validators[stream_name].validate(message['record'])
            records[stream_name].append(flatten(message['record'], schemas[stream_name]))
            records_count[stream_name] += 1

            # Update the pyarrow table on every 1000 records
            if not len(records[stream_name]) % 1000:
                concat_tables(stream_name, pyarrow_tables, records, pyarrow_schemas[stream_name])

            # Write the file to HDFS if the file size is greater than the specified size or
            # if the number of rows is greater than the specified number
            if ((config.file_size_mb and stream_name in pyarrow_tables and
                 bytes_to_mb(pyarrow_tables[stream_name].nbytes) >= config.file_size_mb > 0)
                    or (config.rows_per_file and not records_count[stream_name] % config.rows_per_file)):
                write_file_to_hdfs(current_stream_name=stream_name,
                                   pyarrow_tables=pyarrow_tables,
                                   records=records,
                                   pyarrow_schema=pyarrow_schemas[stream_name],
                                   config=config,
                                   files_created_list=files_created)

            state = None
        elif message_type == 'STATE':
            LOGGER.debug(f'Setting state to {message["value"]}')
            state = message['value']
        elif message_type == 'SCHEMA':
            stream = message['stream']
            validators[stream] = Draft4Validator(message['schema'])
            schemas[stream] = flatten_schema(message['schema']['properties'])
            LOGGER.debug(f'Schema: {schemas[stream]}')
            pyarrow_schemas[stream] = flatten_schema_to_pyarrow_schema(schemas[stream])
            key_properties[stream] = message['key_properties']
        else:
            LOGGER.warning(f'Unknown message type {message["type"]} in message {message}')

    # Write the file with the remaining records/pyarrow_tables
    for stream_name in schemas:
        if stream_name in pyarrow_tables or stream_name in records:
            write_file_to_hdfs(current_stream_name=stream_name,
                               pyarrow_tables=pyarrow_tables,
                               records=records,
                               pyarrow_schema=pyarrow_schemas[stream_name],
                               config=config,
                               files_created_list=files_created)

    LOGGER.info(f'Wrote {len(files_created)} files')
    LOGGER.debug(f'Wrote {files_created} files')

    return state


def main():
    config = singer.utils.parse_args(REQUIRED_CONFIG_KEYS).config

    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(
        messages=input_messages,
        config=TargetConfig(
            hdfs_destination_path=config.get('hdfs_destination_path', 'output'),
            compression_method=config.get('compression_method', 'gzip'),
            streams_in_separate_folder=config.get('streams_in_separate_folder', True),
            file_size_mb=config.get('file_size_mb'),
            rows_per_file=config.get('rows_per_file'),
            partitions=config.get('partitions'),
            file_prefix=config.get('file_prefix')
        )
    )

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':
    main()
