import os
from typing import List, Union, MutableMapping

import pyarrow as pa
import singer

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
