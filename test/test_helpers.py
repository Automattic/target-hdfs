from datetime import datetime
from pprint import pprint
from unittest import TestCase, mock
from unittest.mock import patch

import pyarrow as pa
import logging

import pytest

from target_hdfs import TargetConfig
from target_hdfs.helpers import flatten, flatten_schema, flatten_schema_to_pyarrow_schema, create_dataframe


class TestHelpers(TestCase):
    def setUp(self):
        """Mocking datetime"""
        self.datetime_patcher = patch('target_hdfs.datetime')
        self.mock_datetime = self.datetime_patcher.start()
        self.mock_datetime.utcnow = mock.Mock(return_value=datetime(2023, 1, 1))

    def tearDown(self):
        self.datetime_patcher.stop()

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def test_flatten(self):
        in_dict = {
            "key_1": 1,
            "key_2": {"key_3": 2, "key_4": {"key_5": 3, "key_6": ["10", "11"]}},
        }
        expected = {
            "key_1": 1,
            "key_2__key_3": 2,
            "key_2__key_4__key_5": 3,
            "key_2__key_4__key_6": '["10", "11"]',
        }

        flat_schema = {
            "key_1": "integer",
            "key_2__key_3": ["null", "integer"],
            "key_2__key_4__key_5": ["null", "integer"],
            "key_2__key_4__key_6": "string"
        }
        output = flatten(in_dict, flat_schema)
        self.assertEqual(output, expected)

    def test_flatten_with_empty_object(self):
        in_dict = {
            "key_1": 1,
            "key_2": None,
        }
        expected = {
            "key_1": 1,
            "key_2__key_3": None,
            "key_2__key_4__key_5": None,
            "key_2__key_4__key_6": None,
        }

        flat_schema = {
            "key_1": "integer",
            "key_2__key_3": ["null", "integer"],
            "key_2__key_4__key_5": ["null", "integer"],
            "key_2__key_4__key_6": "string"
        }
        output = flatten(in_dict, flat_schema)
        self.assertEqual(output, expected)

    def test_flatten_schema(self):
        in_dict = {
            "key_1": {"type": ["null", "integer"]},
            "key_2": {
                "type": ["null", "object"],
                "properties": {
                    "key_3": {"type": ["null", "string"]},
                    "key_4": {
                        "type": ["null", "object"],
                        "properties": {
                            "key_5": {"type": ["null", "integer"]},
                            "key_6": {
                                "type": ["null", "array"],
                                "items": {
                                    "type": ["null", "object"],
                                    "properties": {
                                        "key_7": {"type": ["null", "number"]},
                                        "key_8": {"type": ["null", "string"]},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
        expected = {
            "key_1": ["null", "integer"],
            "key_2__key_3": ["null", "string"],
            "key_2__key_4__key_5": ["null", "integer"],
            "key_2__key_4__key_6": ["null", "array"]
        }

        output = flatten_schema(in_dict)
        self.assertEqual(output, expected)

    def test_flatten_schema_with_anyOf(self):
        in_dict = {
            "int": {"type": "integer"},
            "string_date": {"type": "string", "format": "date-time"},
            "string": {"type": "string"},
            "anyOf": {
                "anyOf": [{"type": "null"}, {"type": "string", "format": "date-time"}]
            },
            "int_null": {"type": ["null", "integer"]},
        }

        expected = {
            "int": ["integer"],
            "string_date": ["string"],
            "string": ["string"],
            "anyOf": ['null', 'string'],
            "int_null": ["null", "integer"],
        }

        with self._caplog.at_level(logging.WARNING):
            output = flatten_schema(in_dict)
            for record in self._caplog.records:
                self.assertIn("SCHEMA with limited support on field anyOf", record.message)
        self.assertEqual(output, expected)

    def test_flatten_schema_empty(self):
        in_dict = dict()
        self.assertEqual(dict(), flatten_schema(in_dict))

    def test_flatten_schema_to_pyarrow_schema(self):
        in_dict = {
            "id": "integer",
            "created_at": "string",
            "updated_at": "string",
            "email": "string",
            "email_list": ["array", "null"],
            "external_created_at": ["integer", "null"],
            "page_views_count": "integer",
            "only_null_datatype": ["null"],
            "page_views_avg": ["number", "null"],
        }

        expected = pa.schema([
            pa.field("id", pa.int64(), False),
            pa.field("created_at", pa.string(), False),
            pa.field("updated_at", pa.string(), False),
            pa.field("email", pa.string(), False),
            pa.field("email_list", pa.string(), True),
            pa.field("external_created_at", pa.int64(), True),
            pa.field("page_views_count", pa.int64(), False),
            pa.field("only_null_datatype", pa.string(), True),
            pa.field("page_views_avg", pa.float64(), True),
        ])
        result = flatten_schema_to_pyarrow_schema(in_dict)

        self.assertEqual(expected, result)

    def test_flatten_schema_to_pyarrow_schema_type_not_defined(self):
        in_dict = {"created_at": "new-type"}

        with self.assertRaises(NotImplementedError):
            flatten_schema_to_pyarrow_schema(in_dict)

    def test_create_dataframe(self):
        input_data = [{
            "key_1": 1,
            "key_2__key_4__key_5": 3,
            "key_2__key_3": 2,
            "key_2__key_4__key_6": "['10', '11']",
        }]

        schema = pa.schema([
            pa.field("key_1", pa.int64(), False),
            pa.field("key_2__key_4__key_6", pa.string(), False),
            pa.field("key_2__key_3", pa.string(), True),
            pa.field("key_2__key_4__key_5", pa.int64(), True)
        ])

        df = create_dataframe(input_data, schema, False)
        self.assertEqual(sorted(df.column_names), sorted(schema.names))
        for field in schema:
            self.assertEqual(df.schema.field(field.name).type, field.type)
        self.assertEqual(df.num_rows, 1)

    def test_create_dataframe_camelcase(self):
        input_data = [{
            "key 1": 1,
            "Key 2 > key 4 > key_5": 3,
            "key_2 # KEY_3": 2,
            "key_2__key 4##key_6": "['10', '11']",
        }]

        input_schema = pa.schema([
            pa.field("key 1", pa.int64(), False),
            pa.field("key_2__key 4##key_6", pa.string(), False),
            pa.field("key_2 # KEY_3", pa.string(), True),
            pa.field("Key 2 > key 4 > key_5", pa.int64(), True)
        ])

        expected_schema = pa.schema([
            pa.field("key_1", pa.int64(), False),
            pa.field("key_2__key_4_key_6", pa.string(), False),
            pa.field("key_2_key_3", pa.string(), True),
            pa.field("key_2_key_4_key_5", pa.int64(), True)
        ])

        df = create_dataframe(input_data, input_schema, True)
        self.assertEqual(sorted(df.column_names), sorted(expected_schema.names))
        for field in expected_schema:
            self.assertEqual(df.schema.field(field.name).type, field.type)
        self.assertEqual(df.num_rows, 1)

    def test_generate_file_name(self):
        # Test separated folder
        config = TargetConfig(hdfs_destination_path='', streams_in_separate_folder=True)
        self.assertEqual('20230101_000000-000000.gz.parquet', config.generate_file_name('my_stream'))

        # Test not separated folder
        config = TargetConfig(hdfs_destination_path='', streams_in_separate_folder=False)
        self.assertEqual('my_stream-20230101_000000-000000.gz.parquet', config.generate_file_name('my_stream'))

        # Test not separated folder with prefix
        config = TargetConfig(hdfs_destination_path='', streams_in_separate_folder=False, file_prefix='scope1')
        self.assertEqual('my_stream-scope1-20230101_000000-000000.gz.parquet', config.generate_file_name('my_stream'))

    def test_generate_hdfs_path(self):
        # Test separated folder
        config = TargetConfig(hdfs_destination_path='/path/1/', streams_in_separate_folder=True)
        self.assertEqual('/path/1/my_stream/20230101_000000-000000.gz.parquet', config.generate_hdfs_path('my_stream'))

        # Test not separated folder
        config = TargetConfig(hdfs_destination_path='/path/2/', streams_in_separate_folder=False)
        self.assertEqual('/path/2/my_stream-20230101_000000-000000.gz.parquet', config.generate_hdfs_path('my_stream'))

        # Test separated folder with partition
        config = TargetConfig(hdfs_destination_path='/path/3/', streams_in_separate_folder=True, partitions='my_partition=123')
        self.assertEqual('/path/3/my_stream/my_partition=123/20230101_000000-000000.gz.parquet', config.generate_hdfs_path('my_stream'))

        # Test not separated folder with multiple partitions
        config = TargetConfig(hdfs_destination_path='/path/4/', streams_in_separate_folder=False, partitions='my_partition1=1/my_partition2=2')
        self.assertEqual('/path/4/my_partition1=1/my_partition2=2/my_stream-20230101_000000-000000.gz.parquet', config.generate_hdfs_path('my_stream'))

    def test_flatten_array_fields(self):
        in_dict = {
            "int_array": [1, 2, 3],
            "array_of_int_array": [1, 2, [3, 4]],
            "array_of_mixed_types": ["a", {"b":"value"}, ["c", "d"]],
            "string_array": ["a", "b", "c"],
            "object_array": [{"int": 1}, {"string1": "aaa'aaa"}, {"string2": 'aaa"aaa'}, {"array": [1, 2, 3]}, {'true': True}, {'false': False}, {'null': None}],
        }
        expected = {
            'int_array': '[1, 2, 3]',
            'array_of_int_array': '[1, 2, [3, 4]]',
            "array_of_mixed_types": '["a", {"b": "value"}, ["c", "d"]]',
            'string_array': '["a", "b", "c"]',
            'object_array': '[{"int": 1}, {"string1": "aaa\'aaa"}, {"string2": "aaa\\"aaa"}, {"array": [1, 2, 3]}, {"true": true}, {"false": false}, {"null": null}]'
        }

        flat_schema = {
            "int_array": "array",
            "array_of_int_array": "array",
            "array_of_mixed_types": "array",
            "string_array": ["null", "array"],
            "object_array": "array",
        }
        output = flatten(in_dict, flat_schema)
        self.assertEqual(expected, output)
