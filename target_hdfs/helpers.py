import gc
import os
import tempfile
from datetime import datetime
from typing import List, Union, MutableMapping, Dict

import pyarrow as pa
import singer
from pyarrow.parquet import ParquetWriter

LOGGER = singer.get_logger()
LOGGER.setLevel(os.getenv("LOGGER_LEVEL", "INFO"))


def flatten(dictionary, flat_schema, parent_key="", sep="__"):
    """Function that flattens a nested structure, using the separater given as parameter, or uses '__' as default
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
            else:
                if new_key in flat_schema:
                    items[new_key] = str(value) if isinstance(value, list) else value
    return items


def flatten_schema(dictionary, parent_key="", sep="__"):
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
            if "type" not in value:
                LOGGER.warning(f"SCHEMA with limited support on field {key}: {value}")
            if "object" in value.get("type", []):
                items.update(flatten_schema(value.get("properties"), new_key, sep=sep))
            else:
                items[new_key] = value.get("type", None)
    return items


FIELD_TYPE_TO_PYARROW = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "": pa.string(),  # string type will be considered as default
    "INTEGER": pa.int64(),
    "NUMBER": pa.float64()
}


def _field_type_to_pyarrow_field(field_name: str, input_types: Union[List[str], str]):
    input_types = input_types or []
    if isinstance(input_types, str):
        input_types = [input_types]
    types_uppercase = {item.upper() for item in input_types}
    nullable = "NULL" in types_uppercase
    types_uppercase.discard("NULL")
    input_type = list(types_uppercase)[0] if types_uppercase else ""
    pyarrow_type = FIELD_TYPE_TO_PYARROW.get(input_type, None)

    if not pyarrow_type:
        raise NotImplementedError(f'Data types "{input_types}" for field {field_name} is not yet supported.')

    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(flatten_schema_dictionary, fields_ordered) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order
    E.g:
     dictionary = {
             'key_1': ['null', 'integer'],
             'key_2__key_3': ['null', 'string'],
             'key_2__key_4__key_5': ['null', 'integer'],
             'key_2__key_4__key_6': ['null', 'array']
        }
    By calling the function with the dictionary above as parameter, you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    flatten_schema_dictionary = flatten_schema_dictionary or {}
    return pa.schema(
        [_field_type_to_pyarrow_field(field_name, flatten_schema_dictionary[field_name])
         for field_name in fields_ordered]
    )


def create_dataframe(list_dict: List[Dict], schema: Dict):
    """"Create a pyarrow Table from a python list of dict"""
    fields = set()
    for d in list_dict:
        fields = fields.union(d.keys())
    data = {f: [row.get(f) for row in list_dict] for f in fields}
    dataframe = pa.table(data).cast(flatten_schema_to_pyarrow_schema(schema, list(fields)))
    return dataframe


def concat_tables(current_stream_name: str, dataframes: Dict[str, pa.Table],
                  records: Dict[str, List[Dict]], schema: Dict):
    """Create a dataframe from records and concatenate with the existing one"""
    dataframe = create_dataframe(records.pop(current_stream_name), schema)
    if current_stream_name not in dataframes:
        dataframes[current_stream_name] = dataframe
    else:
        dataframes[current_stream_name] = pa.concat_tables([dataframes[current_stream_name], dataframe])
    LOGGER.debug(f'Database[{current_stream_name}] size: '
                 f'{dataframes[current_stream_name].nbytes / 1024 / 1024} MB | '
                 f'{dataframes[current_stream_name].num_rows} rows')


def create_hdfs_dir(hdfs_path):
    """Create HDFS Path"""
    LOGGER.info(f"Creating hdfs {hdfs_path} path")
    pa.fs.HadoopFileSystem('default').create_dir(hdfs_path)


def upload_to_hdfs(local_file, destination_path_hdfs) -> None:
    """Upload a local file to HDFS using RPC"""
    pa.fs.copy_files(
        local_file,
        destination_path_hdfs,
        source_filesystem=pa.fs.LocalFileSystem(),
        destination_filesystem=pa.fs.HadoopFileSystem('default')
    )


def write_file_to_hdfs(
        current_stream_name,
        dataframes,
        records,
        schema,
        hdfs_destination_path,
        compression_extension,
        compression_method,
        filename_separator='-',
        sync_ymd_partition=None,
        streams_in_separate_folder=True
):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S-%f")

    # Converting the last records to pyarrow table
    if records[current_stream_name]:
        concat_tables(current_stream_name, dataframes, records, schema)

    hdfs_filepath = generate_hdfs_path(current_stream_name, hdfs_destination_path, streams_in_separate_folder,
                                       sync_ymd_partition)
    # create_hdfs_dir(hdfs_filepath)

    LOGGER.info(f"Writing files from {current_stream_name} stream to HDFS {hdfs_filepath}")
    with tempfile.NamedTemporaryFile("wb") as tmp_file:
        ParquetWriter(tmp_file.name, dataframes[current_stream_name].schema,
                      compression=compression_method).write_table(dataframes[current_stream_name])
        filename = f"{current_stream_name}{filename_separator}{timestamp}{compression_extension}.parquet"
        upload_to_hdfs(tmp_file.name, os.path.join(hdfs_filepath, filename))

    # explicit memory management. This can be usefull when working on very large data groups
    del dataframes[current_stream_name]
    gc.collect()

    return hdfs_filepath


def generate_hdfs_path(current_stream_name, hdfs_destination_path, streams_in_separate_folder, sync_ymd_partition):
    hdfs_filepath = hdfs_destination_path
    if streams_in_separate_folder:
        hdfs_filepath = os.path.join(hdfs_filepath, current_stream_name)
    if sync_ymd_partition:
        hdfs_filepath = os.path.join(hdfs_filepath, f"synced_ymd={sync_ymd_partition}")
    return hdfs_filepath
