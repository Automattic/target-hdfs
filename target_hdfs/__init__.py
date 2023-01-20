#!/usr/bin/env python3
import io
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from multiprocessing import Queue, Process
from typing import Optional

import singer
from jsonschema.validators import Draft4Validator

from .helpers import flatten, flatten_schema, write_record_to_avro_file, flatten_schema_to_avro_schema, \
    close_writer_and_upload

_all__ = ['main']

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'hdfs_destination_path'
]

EXTENSION_MAPPING = {
    'null': '',
    'bzip2': '.bz2',
    'snappy': '.snappy',
    'deflate': '.zz',
    'zstandard': '.zstd'
}


@dataclass
class TargetConfig:
    hdfs_destination_path: str
    compression_method: Optional[str] = 'bzip2'
    compression_extension: str = ''
    streams_in_separate_folder: bool = True
    file_prefix: Optional[str] = None
    partitions: Optional[str] = None
    file_size_mb: Optional[int] = 250
    filename_separator: str = '-'

    def __post_init__(self):
        self.set_compression_extension()
        if self.streams_in_separate_folder:
            self.filename_separator = os.path.sep
        LOGGER.info(self)

    def set_compression_extension(self):
        self.compression_method = self.compression_method or 'null'
        # The target is prepared to accept all the compression methods provided by the pandas module
        self.compression_extension = EXTENSION_MAPPING.get(self.compression_method.lower())
        if self.compression_extension is None:
            raise Exception('Unsupported compression method.')

    def generate_file_name(self, stream_name):
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S-%f')
        file_name = f'{timestamp}{self.compression_extension}.avro'
        if not self.streams_in_separate_folder:
            file_name = f'{stream_name}{self.filename_separator}{file_name}'
        if self.file_prefix:
            file_name = f'{self.file_prefix}{self.filename_separator}{file_name}'
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
    # pylint: disable=too-many-statements
    # Object that signals shutdown
    _break_object = object()

    def producer(message_buffer: io.TextIOWrapper, w_queue: Queue):
        schemas = {}
        key_properties = {}
        validators = {}

        state = None
        try:
            for message in message_buffer:
                LOGGER.debug(f'target-parquet got message: {message}')
                try:
                    message = json.loads(message)
                except json.JSONDecodeError as exc:
                    raise Exception(f'Unable to parse:\n{message}') from exc

                message_type = message['type']
                if message_type == 'RECORD':
                    stream_name = message['stream']
                    if stream_name not in schemas:
                        raise ValueError(f'A record for stream {stream_name} was encountered '
                                         f'before a corresponding schema')
                    validators[stream_name].validate(message['record'])
                    flattened_record = flatten(message['record'], schemas[stream_name])
                    # Once the record is flattened, it is added to the final record list,
                    # which will be stored in the parquet file.
                    w_queue.put((MessageType.RECORD, stream_name, flattened_record))
                    state = None
                elif message_type == 'STATE':
                    LOGGER.debug(f'Setting state to {message["value"]}')
                    state = message['value']
                elif message_type == 'SCHEMA':
                    stream = message['stream']
                    validators[stream] = Draft4Validator(message['schema'])
                    schemas[stream] = flatten_schema(message['schema']['properties'])
                    LOGGER.debug(f'Schema: {schemas[stream]}')
                    key_properties[stream] = message['key_properties']
                    w_queue.put((MessageType.SCHEMA, stream, schemas[stream]))
                else:
                    LOGGER.warning(f'Unknown message type {message["type"]} in message {message}')
            w_queue.put((MessageType.EOF, _break_object, None))
            return state
        except Exception as err:
            w_queue.put((MessageType.EOF, _break_object, None))
            raise err

    def consumer(receiver):
        files_uploaded = []
        current_stream_name = None

        local_writers = {}
        avro_schemas = {}
        more_messages = True

        with tempfile.TemporaryDirectory() as tmpdir:
            while more_messages:
                (message_type, stream_name, record) = receiver.get()
                if message_type == MessageType.RECORD:
                    current_stream_name = stream_name
                    write_record_to_avro_file(current_stream_name, avro_schemas[stream_name], record, config,
                                              tmpdir, files_uploaded, local_writers)
                elif message_type == MessageType.SCHEMA:
                    avro_schemas[stream_name] = flatten_schema_to_avro_schema(stream_name, record)
                elif message_type == MessageType.EOF:
                    if current_stream_name in local_writers:
                        files_uploaded.append(close_writer_and_upload(local_writers, current_stream_name, config))
                    LOGGER.info(f'Wrote {len(files_uploaded)} files')
                    LOGGER.debug(f'Wrote {files_uploaded} files')
                    more_messages = False

    messages_queue = Queue()
    process_consumer = Process(target=consumer, args=(messages_queue,),)
    process_consumer.start()
    state = producer(messages, messages_queue)
    process_consumer.join()
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
            file_prefix=config.get('file_prefix', None),
            file_size_mb=config.get('file_size_mb', None),
            partitions=config.get('partitions')
        )
    )

    emit_state(state)
    LOGGER.debug('Exiting normally')


if __name__ == '__main__':
    main()
