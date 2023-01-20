import json
import os
from typing import MutableMapping

import avro
import pyarrow as pa
import singer
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.schema import Schema

LOGGER = singer.get_logger()


def bytes_to_mb(x):
    return x / (1024 * 1024)


def flatten(dictionary, flat_schema, parent_key='', sep='__'):
    """Function that flattens a nested structure, using the separator given as parameter, or uses '__' as default
    E.g:
     dictionary =  {
                        'key_1': 1,
                        'key_2': {
                               'key_3': 2,
                               'key_4': {
                                      'key_5': 3,
                                      'key_6' : ['10', '11']
                                 }
                        }
                        'key_7': None
                    }
    flatten_schema = {
             'key_1': ['null', 'integer'],
             'key_2__key_3': ['null', 'string'],
             'key_2__key_4__key_5': ['null', 'integer'],
             'key_2__key_4__key_6': ['null', 'array']
             'key_7__key_8': ['null', 'integer']
             'key_7__key_9': ['null', 'string']
        }
    By calling the function with the dictionary above as parameter, you will get the following structure:
        {
             'key_1': 1,
             'key_2__key_3': 2,
             'key_2__key_4__key_5': 3,
             'key_2__key_4__key_6': "['10', '11']"
             'key_7__key_8': None
             'key_7__key_9': None
         }
    """
    items = {} if parent_key else flat_schema.fromkeys(flat_schema.keys(), None)
    if dictionary:
        for key, value in dictionary.items():
            new_key = parent_key + sep + key if parent_key else key
            if isinstance(value, MutableMapping):
                items.update(flatten(value, flat_schema, new_key, sep=sep))
            elif new_key in flat_schema:
                items[new_key] = str(value) if isinstance(value, list) else value
    return items


def flatten_schema(dictionary, parent_key='', sep='__'):
    """Function that flattens a nested structure, using the separater given as parameter, or uses '__' as default
    E.g:
     dictionary =  {
                        'key_1': {'type': ['null', 'integer']},
                        'key_2': {
                            'type': ['null', 'object'],
                            'properties': {
                                'key_3': {'type': ['null', 'string']},
                                'key_4': {
                                    'type': ['null', 'object'],
                                    'properties': {
                                        'key_5' : {'type': ['null', 'integer']},
                                        'key_6' : {
                                            'type': ['null', 'array'],
                                            'items': {
                                                'type': ['null', 'object'],
                                                'properties': {
                                                    'key_7': {'type': ['null', 'number']},
                                                    'key_8': {'type': ['null', 'string']}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
    By calling the function with the dictionary above as parameter, you will get the following structure:
        {
             'key_1': ['null', 'integer'],
             'key_2__key_3': ['null', 'string'],
             'key_2__key_4__key_5': ['null', 'integer'],
             'key_2__key_4__key_6': ['null', 'array']
        }
    """
    items = {}
    if dictionary:
        for key, value in dictionary.items():
            new_key = parent_key + sep + key if parent_key else key
            types = value.get('type', [])
            if 'type' not in value:
                if 'anyOf' in value:
                    types = [item['type'] for item in value['anyOf'] if 'type' in item]
                else:
                    LOGGER.warning(f'SCHEMA with limited support on field {key}: {value}')
            if 'object' in types:
                items.update(flatten_schema(value.get('properties'), new_key, sep=sep))
            else:
                items[new_key] = types
    return items


FIELD_TYPE_TO_AVRO_TYPES = {
    'BOOLEAN': 'boolean',
    'STRING': 'string',
    'ARRAY': 'string',
    '': 'string',  # string type will be considered as default
    'INTEGER': 'int',
    'NUMBER': 'double',
    'DATE-TIME': 'long',
    'NULL': 'null'
}


def flatten_schema_to_avro_schema(stream_name, flatten_schema_dictionary) -> Schema:
    """Function that converts a flatten schema to an avro schema
    E.g:
     steam_name = 'my_stream'
     flatten_schema_dictionary = {
             'key_1': ['null', 'integer'],
             'key_2__key_3': ['null', 'string'],
             'key_2__key_4__key_5': ['null', 'integer'],
             'key_2__key_4__key_6': ['null', 'array']
        }
    By calling the function with the dictionary above as parameter, you will get the following structure:
        {"namespace": "my_stream",
         "type": "record",
         "name": "my_stream",
         "fields": [
             {"name": "key_1", "type": ['null', 'int']},
             {"name": "key_2__key_3",  "type": ['null', 'string']},
             {"name": "key_2__key_4__key_5",  "type": ['null', 'int']},
             {"name": "key_2__key_4__key_6", "type": ['null', "string"]}
         ]
        }
    """
    flatten_schema_dictionary = flatten_schema_dictionary or {}
    try:
        avsc_fields = [
            {'name': field_name,
             'type': ([FIELD_TYPE_TO_AVRO_TYPES[field_type.upper()]
                       for field_type in field_input_types] if isinstance(field_input_types, list)
                      else FIELD_TYPE_TO_AVRO_TYPES[field_input_types.upper()])}
            for field_name, field_input_types in flatten_schema_dictionary.items()]
    except KeyError as key_exception:
        raise NotImplementedError(f'Data type not supported: {key_exception}') from key_exception

    return avro.schema.parse(json.dumps(
        {'namespace': stream_name,
         'type': 'record',
         'name': stream_name,
         'fields': list(avsc_fields)}))


def write_record_to_avro_file(stream_name: str, schema: Schema, record, config, tempdir, files_uploaded,
                              local_writers):

    """Append row to avro file (create the file if not exist)"""
    if stream_name not in local_writers:
        current_file_path = os.path.join(tempdir, config.generate_file_name(stream_name))
        # pylint: disable=consider-using-with
        local_writers[stream_name] = DataFileWriter(open(current_file_path, 'wb'), DatumWriter(), schema,
                                                    codec=config.compression_method)

    # Including record to avro file
    local_writers[stream_name].append(record)

    # Upload file when size reach the defined file size
    if bytes_to_mb(os.path.getsize(local_writers[stream_name].writer.name)) >= config.file_size_mb:
        files_uploaded.append(close_writer_and_upload(local_writers, stream_name, config))


def close_writer_and_upload(local_writers, stream_name, config):
    local_file = local_writers[stream_name].writer.name
    local_writers[stream_name].close()
    del local_writers[stream_name]
    return upload_to_hdfs(local_file, stream_name, config)


def upload_to_hdfs(local_file, stream_name, config) -> None:
    """Upload a local file to HDFS using RPC"""
    destination_path_hdfs = config.generate_hdfs_path(stream_name)
    LOGGER.debug(f'Uploading file to HDFS: {destination_path_hdfs} ')
    pa.fs.copy_files(
        local_file,
        destination_path_hdfs,
        source_filesystem=pa.fs.LocalFileSystem(),
        destination_filesystem=pa.fs.HadoopFileSystem('default')
    )
    LOGGER.info(f'File {destination_path_hdfs} uploaded to HDFS')
    return destination_path_hdfs
