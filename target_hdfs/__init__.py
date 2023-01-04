#!/usr/bin/env python3
import io
import json
import os
import sys
from multiprocessing import Queue, Process
from collections import defaultdict
from enum import Enum
from typing import Tuple

import singer
from jsonschema.validators import Draft4Validator

from .helpers import flatten, flatten_schema, concat_tables, write_file_to_hdfs

_all__ = ["main"]

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "hdfs_destination_path"
]

EXTENSION_MAPPING = {
    "SNAPPY": ".snappy",
    "GZIP": ".gz",
    "BROTLI": ".br",
    "ZSTD": ".zstd",
    "LZ4": ".lz4",
}


class MessageType(Enum):
    RECORD = 1
    STATE = 2
    SCHEMA = 3
    EOF = 4


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def get_compression_extension(compression_method) -> Tuple[str, str]:
    compression_extension = ""
    if compression_method:
        # The target is prepared to accept all the compression methods provided by the pandas module
        compression_extension = EXTENSION_MAPPING.get(compression_method.upper())
        if compression_extension is None:
            raise Exception("Unsupported compression method.")
    return compression_extension, compression_method


def persist_messages(messages,
                     hdfs_destination_path,
                     compression_method=None,
                     streams_in_separate_folder=False,
                     sync_ymd_partition=None,
                     file_size_mb=-1):
        ## Static information shared among processes
        schemas = {}
        key_properties = {}
        validators = {}

        compression_extension, compression_method = get_compression_extension(compression_method)

        filename_separator = "-"
        if streams_in_separate_folder:
            LOGGER.info("Writing streams in separate folders")
            filename_separator = os.path.sep

        LOGGER.info(f"Files will be save in HDFS path: {hdfs_destination_path}")
        ## End of Static information shared among processes

        # Object that signals shutdown
        _break_object = object()

        def producer(message_buffer: io.TextIOWrapper, w_queue: Queue):
            state = None
            try:
                for message in message_buffer:
                    LOGGER.debug(f"target-parquet got message: {message}")
                    try:
                        message = singer.parse_message(message).asdict()
                    except json.JSONDecodeError:
                        raise Exception(f'Unable to parse:\n{message}')

                    message_type = message["type"]
                    if message_type == "RECORD":
                        stream_name = message["stream"]
                        if stream_name not in schemas:
                            raise ValueError(f'A record for stream {stream_name} was encountered '
                                             f'before a corresponding schema')
                        validators[stream_name].validate(message["record"])
                        flattened_record = flatten(message["record"], schemas[stream_name])
                        # Once the record is flattened, it is added to the final record list,
                        # which will be stored in the parquet file.
                        w_queue.put((MessageType.RECORD, stream_name, flattened_record))
                        state = None
                    elif message_type == "STATE":
                        LOGGER.debug(f'Setting state to {message["value"]}')
                        state = message["value"]
                    elif message_type == "SCHEMA":
                        stream = message["stream"]
                        validators[stream] = Draft4Validator(message["schema"])
                        schemas[stream] = flatten_schema(message["schema"]["properties"])
                        LOGGER.debug(f"Schema: {schemas[stream]}")
                        key_properties[stream] = message["key_properties"]
                        w_queue.put((MessageType.SCHEMA, stream, schemas[stream]))
                    else:
                        LOGGER.warning(f'Unknown message type {message["type"]} in message {message}')
                w_queue.put((MessageType.EOF, _break_object, None))
                return state
            except Exception as Err:
                w_queue.put((MessageType.EOF, _break_object, None))
                raise Err

        def consumer(receiver):
            files_created = []
            current_stream_name = None

            records = defaultdict(list)
            records_count = defaultdict(int)
            dataframes = {}
            schemas = {}

            while True:
                (message_type, stream_name, record) = receiver.get()  # q.get()
                if message_type == MessageType.RECORD:
                    if stream_name != current_stream_name and current_stream_name is not None:
                        files_created.append(
                            write_file(current_stream_name, dataframes, records, schemas[current_stream_name]))
                    current_stream_name = stream_name
                    records[stream_name].append(record)
                    records_count[stream_name] += 1
                    # Update the pyarrow table on every 1000 records
                    if len(records[current_stream_name]) % 1000 == 0:
                        concat_tables(current_stream_name, dataframes, records, schemas)
                    if (file_size_mb > 0) and (dataframes[current_stream_name].nbytes / (1024 * 1024) >= file_size_mb):
                        files_created.append(
                            write_file(current_stream_name, dataframes, records, schemas[current_stream_name]))
                elif message_type == MessageType.SCHEMA:
                    schemas[stream_name] = record
                elif message_type == MessageType.EOF:
                    files_created.append(
                        write_file(current_stream_name, dataframes, records, schemas[current_stream_name]))
                    LOGGER.info(f"Wrote {len(files_created)} files")
                    LOGGER.debug(f"Wrote {files_created} files")
                    break

        def write_file(current_stream_name, dataframes, records, current_stream_schema):
            write_file_to_hdfs(current_stream_name,
                               dataframes,
                               records,
                               current_stream_schema,
                               hdfs_destination_path,
                               compression_extension,
                               compression_method,
                               filename_separator,
                               sync_ymd_partition,
                               streams_in_separate_folder)


        q = Queue()
        t2 = Process(target=consumer, args=(q,),)
        t2.start()
        state = producer(messages, q)
        t2.join()
        return state


def main():
    config = singer.utils.parse_args(REQUIRED_CONFIG_KEYS).config

    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_messages(
        messages=input_messages,
        hdfs_destination_path=config.get("hdfs_destination_path", "output"),
        compression_method=config.get("compression_method", "gzip"),
        streams_in_separate_folder=config.get("streams_in_separate_folder", True),
        file_size_mb=config.get("file_size_mb", -1),
        sync_ymd_partition=config.get("sync_ymd_partition")
    )

    emit_state(state)
    LOGGER.debug("Exiting normally")


if __name__ == "__main__":
    main()
