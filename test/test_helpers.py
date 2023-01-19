import json
from datetime import datetime
from unittest import TestCase, mock

import logging
from unittest.mock import patch

import avro
import pytest

from target_hdfs import TargetConfig
from target_hdfs.helpers import flatten, flatten_schema, flatten_schema_to_avro_schema


class TestHelpers(TestCase):
    def setUp(self):
        """Mocking datetime"""
        self.datetime_patcher = patch('target_hdfs.datetime')
        self.mock_datetime = self.datetime_patcher.start()
        self.mock_datetime.utcnow = mock.Mock(return_value=datetime(2023, 1, 1))

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
            "key_2__key_4__key_6": "['10', '11']",
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
            "int": "integer",
            "string_date": "string",
            "string": "string",
            "anyOf": ["null", "string"],
            "int_null": ["null", "integer"],
        }

        output = flatten_schema(in_dict)
        self.assertEqual(output, expected)

    def test_flatten_schema_with_not_supported_type(self):
        in_dict = {
            "int": {"type": "integer"},
            "wrong_type_structure": {
                "wrong_type": {"type": ["null", "string"]}
            },
        }

        expected = {
            "int": "integer",
            "wrong_type_structure": [],
        }

        with self._caplog.at_level(logging.WARNING):
            output = flatten_schema(in_dict)
            for record in self._caplog.records:
                self.assertIn("SCHEMA with limited support on field wrong_type_structure", record.message)
        self.assertEqual(output, expected)

    def test_flatten_schema_empty(self):
        in_dict = dict()
        self.assertEqual(dict(), flatten_schema(in_dict))

    def test_flatten_schema_to_avro_schema(self):
        in_dict = {
            "int": "integer",
            "string": "string",
            "array_null": ["array", "null"],
            "int_null": ["integer", "null"],
            "null": ["null"],
            "number_null": ["number", "null"],
        }

        expected = avro.schema.parse(json.dumps({
            "namespace": "my_stream",
            "type": "record",
            "name": "my_stream",
            "fields": [
                {"name": "int", "type": 'int'},
                {"name": "string",  "type": 'string'},
                {"name": "array_null", "type": ['string', 'null']},
                {"name": "int_null", "type": ['int', 'null']},
                {"name": "null", "type": ['null']},
                {"name": "number_null", "type": ['double', 'null']},
            ]
        }))
        output = flatten_schema_to_avro_schema("my_stream", in_dict)

        self.assertEqual(expected, output)

    def test_flatten_schema_to_arrow_schema_type_not_defined(self):
        in_dict = {"non_existing_type": "new-type"}

        with self.assertRaises(NotImplementedError):
            flatten_schema_to_avro_schema("my_stream", in_dict)

    def test_generate_file_name(self):
        # Test separated folder
        config = TargetConfig(hdfs_destination_path='', streams_in_separate_folder=True)
        self.assertEqual('20230101_000000-000000.bz2.avro', config.generate_file_name('my_stream'))

        # Test not separated folder
        config = TargetConfig(hdfs_destination_path='', streams_in_separate_folder=False)
        self.assertEqual('my_stream-20230101_000000-000000.bz2.avro', config.generate_file_name('my_stream'))

        # Test not separated folder with prefix
        config = TargetConfig(hdfs_destination_path='', streams_in_separate_folder=False, file_prefix='scope1')
        self.assertEqual('scope1-my_stream-20230101_000000-000000.bz2.avro', config.generate_file_name('my_stream'))

    def test_generate_hdfs_path(self):
        # Test separated folder
        config = TargetConfig(hdfs_destination_path='/path/1/', streams_in_separate_folder=True)
        self.assertEqual('/path/1/my_stream/20230101_000000-000000.bz2.avro', config.generate_hdfs_path('my_stream'))

        # Test not separated folder
        config = TargetConfig(hdfs_destination_path='/path/2/', streams_in_separate_folder=False)
        self.assertEqual('/path/2/my_stream-20230101_000000-000000.bz2.avro', config.generate_hdfs_path('my_stream'))

        # Test separated folder with partition
        config = TargetConfig(hdfs_destination_path='/path/3/', streams_in_separate_folder=True, partitions='my_partition=123')
        self.assertEqual('/path/3/my_stream/my_partition=123/20230101_000000-000000.bz2.avro', config.generate_hdfs_path('my_stream'))

        # Test not separated folder with multiple partitions
        config = TargetConfig(hdfs_destination_path='/path/4/', streams_in_separate_folder=False, partitions='my_partition1=1/my_partition2=2')
        self.assertEqual('/path/4/my_partition1=1/my_partition2=2/my_stream-20230101_000000-000000.bz2.avro', config.generate_hdfs_path('my_stream'))
