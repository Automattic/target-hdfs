from unittest import TestCase

import pyarrow as pa
import logging

import pytest

from target_hdfs.helpers import flatten, flatten_schema, flatten_schema_to_pyarrow_schema, create_dataframe


class TestHelpers(TestCase):
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

    def test_flatten_schema_2(self):
        in_dict = {
            "id": {"type": "integer"},
            "created_at": {"type": "string", "format": "date-time"},
            "updated_at": {"type": "string", "format": "date-time"},
            "email": {"type": "string"},
            "last_surveyed": {
                "anyOf": [{"type": "null"}, {"type": "string", "format": "date-time"}]
            },
            "external_created_at": {"type": ["integer", "null"]},
            "page_views_count": {"type": "integer"},
        }

        expected = {
            "id": "integer",
            "created_at": "string",
            "updated_at": "string",
            "email": "string",
            "last_surveyed": None,
            "external_created_at": ["integer", "null"],
            "page_views_count": "integer",
        }

        with self._caplog.at_level(logging.WARNING):
            output = flatten_schema(in_dict)
            for record in self._caplog.records:
                self.assertIn("SCHEMA with limited support on field last_surveyed", record.message)
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

        df = create_dataframe(input_data, schema)
        self.assertEqual(sorted(df.column_names), sorted(schema.names))
        for field in schema:
            self.assertEqual(df.schema.field(field.name).type, field.type)
        self.assertEqual(df.num_rows, 1)
