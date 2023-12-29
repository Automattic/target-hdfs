import os
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from target_hdfs.utils import bytes_to_mb

FIELD_TYPE_TO_PYARROW = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "INTEGER": pa.int64(),
    "NUMBER": pa.float64(),
    "OBJECT": pa.string(),
}


EXTENSION_MAPPING = {
    "snappy": ".snappy",
    "gzip": ".gz",
    "brotli": ".br",
    "zstd": ".zstd",
    "lz4": ".lz4",
}


def _field_type_to_pyarrow_field(field_name: str, input_types: dict) -> pa.Field:
    types = input_types.get("type", [])
    # If type is not defined, check if anyOf is defined
    if not types:
        for any_type in input_types.get("anyOf", []):
            if t := any_type.get("type"):
                if isinstance(t, list):
                    types.extend(t)
                else:
                    types.append(t)
    types = [types] if isinstance(types, str) else types
    types_uppercase = {item.upper() for item in types}
    nullable = "NULL" in types_uppercase
    types_uppercase.discard("NULL")
    input_type = list(types_uppercase)[0] if types_uppercase else ""
    pyarrow_type = FIELD_TYPE_TO_PYARROW.get(input_type, pa.string())

    if not pyarrow_type:
        raise NotImplementedError(
            f'Data types "{input_types}" for field {field_name} is not yet supported.'
        )

    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(flatten_schema_dictionary: dict) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order
    E.g:
     dictionary = {
        'properties': {
             'key_1': {'type': ['null', 'integer']},
             'key_2__key_3': {'type': ['null', 'string']},
             'key_2__key_4__key_5': {'type': ['null', 'integer']},
             'key_2__key_4__key_6': {'type': ['null', 'array']}
           }
        }
    By calling the function with the dictionary above as parameter, you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    flatten_schema = flatten_schema_dictionary.get("properties", {})
    return pa.schema(
        [
            _field_type_to_pyarrow_field(field_name, field_input_types)
            for field_name, field_input_types in flatten_schema.items()
        ]
    )


def create_pyarrow_table(list_dict: List[dict], schema: pa.Schema) -> pa.Table:
    """Create a pyarrow Table from a python list of dict"""
    data = {f: [row.get(f) for row in list_dict] for f in schema.names}
    return pa.table(data).cast(schema)


def concat_tables(
    records: List[Dict], pyarrow_table: pa.Table, pyarrow_schema: pa.Schema
):
    """Create a dataframe from records and concatenate with the existing one"""
    if not records:
        return pyarrow_table
    new_table = create_pyarrow_table(records, pyarrow_schema)
    return pa.concat_tables([pyarrow_table, new_table]) if pyarrow_table else new_table


def write_parquet_file(
    table: pa.Table,
    path: str,
    compression_method="gzip",
    basename_template=None,
    partition_cols=None,
) -> None:
    """Write a pyarrow table to a parquet file"""
    pq.write_to_dataset(
        table,
        root_path=path,
        compression=compression_method,
        partition_cols=partition_cols or None,
        use_threads=True,
        use_legacy_dataset=False,
        basename_template=f"{basename_template}.{EXTENSION_MAPPING[compression_method.lower()]}.parquet"
        if basename_template
        else None,
    )


def get_single_parquet_file(dir_path: str) -> Optional[str]:
    """Return the path of the single parquet file in the directory"""
    files = [
        os.path.join(root, file)
        for root, dirs, files in os.walk(dir_path)
        for file in files
        if file.endswith(".parquet")
    ]
    assert (
        len(files) <= 1
    ), f"Expected 1 parquet file or None in local temp path {dir_path}, found {len(files)}"
    return files[0] if files else None


def read_parquet_file(dir_path: str) -> Optional[pa.Table]:
    """Read a single parquet file and return a pyarrow table"""
    file = get_single_parquet_file(dir_path)
    return pq.read_table(file) if file else None


def get_pyarrow_table_size(table: pa.Table) -> float:
    """Return the size of a pyarrow table in MB"""
    return bytes_to_mb(table.nbytes)
