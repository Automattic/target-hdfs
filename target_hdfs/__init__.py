#!/usr/bin/env python3
import io
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Tuple

import singer
from jsonschema.validators import Draft4Validator

from target_hdfs.hdfs_client import HDFSClient
from target_hdfs.helpers import flatten

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "host"
    "user",
    "password",
]

EXTENSION_MAPPING = {
    "SNAPPY": ".snappy",
    "GZIP": ".gz",
    "BROTLI": ".br",
    "ZSTD": ".zstd",
    "LZ4": ".lz4",
}


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
            LOGGER.warning("unsuported compression method.")
            compression_method = None
    return compression_extension, compression_method


def persist_messages(hdfs_client, messages, destination_path,
                     compression_method=None, streams_in_separate_folder=False,
                     force_output_schema_cast=True):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    records = defaultdict(list)

    compression_extension, compression_method = get_compression_extension(compression_method)

    filename_separator = "-"
    if force_output_schema_cast:
        LOGGER.info("forcing the output cast to the provided schema")
    if streams_in_separate_folder:
        LOGGER.info("writing streams in separate folders")
        filename_separator = os.path.sep
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    for message in messages:
        try:
            message = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            raise Exception("Unable to parse:\n{}".format(message))

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        message_type = message["type"]
        if message_type == "RECORD":
            if message["stream"] not in schemas:
                raise Exception(f"A record for stream {message['stream']} was encountered "
                                "before a corresponding schema")
            stream_name = message["stream"]
            validators[message["stream"]].validate(message["record"])
            flattened_record = flatten(message["record"], schemas.get(stream_name, {}))
            # Once the record is flattened, it is added to the final record list,
            # which will be stored in the parquet file.
            records[stream_name].append(flattened_record)
            state = None
        elif message_type == "STATE":
            LOGGER.debug(f"Setting state to {message['value']}")
            state = message["value"]
        elif message_type == "SCHEMA":
            stream = message["stream"]
            schemas[stream] = message["schema"]
            LOGGER.debug(f"Schema: {schemas[stream]}")
            validators[stream] = Draft4Validator(message["schema"])
            key_properties[stream] = message["key_properties"]
        else:
            LOGGER.warning(f'Unknown message type {message["type"]} in message {message}')

        if len(records) == 0:
            # If there are not any records retrieved, it is not necessary to create a file.
            LOGGER.info("There were not any records retrieved.")
            return state

        # Saving Files
        for stream_name in records.keys():
            LOGGER.info(f"Saving file for {stream_name}.")
            hdfs_client.write_file(
                current_stream_name=stream_name,
                records=records.pop(stream_name),
                schema=schemas.get(stream_name, {}),
                hdfs_destination_path=destination_path,
                compression_extension=compression_extension,
                compression_method=compression_method,
                filename_separator=filename_separator,
                streams_in_separate_folder=streams_in_separate_folder,
                force_output_schema_cast=force_output_schema_cast)
        return state


if __name__ == '__main__':
    config = singer.utils.parse_args(REQUIRED_CONFIG_KEYS).config

    hdfs_client = HDFSClient(host=config.host, user=config.user, password=config.password)

    # The target expects that the tap generates UTF-8 encoded text.
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_messages(
        hdfs_client=hdfs_client,
        messages=input_messages,
        destination_path=config.get("destination_path", "output"),
        compression_method=config.get("compression_method", "gzip"),
        streams_in_separate_folder=config.get("streams_in_separate_folder", True),
        force_output_schema_cast=config.get("force_output_schema_cast", True),
    )

    emit_state(state)
    LOGGER.debug("Exiting normally")
