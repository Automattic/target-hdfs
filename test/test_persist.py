import os
import shutil
import tempfile
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
import io
from decimal import Decimal
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from pandas.testing import assert_frame_equal
import glob

from target_hdfs import persist_messages, TargetConfig, bytes_to_mb

INPUT_MESSAGE_1 = """\
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

INPUT_MESSAGE_1_REORDERED = """\
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

EXPECTED_DF_1 = pa.table(
        {
            "str": ["value1", "value2", "value3"],
            "int": [1, None, 3],
            "decimal": [0.1, 0.2, 0.3],
            "date": ["2021-06-11", "2021-06-12", "2021-06-13"],
            "datetime": [
                "2021-06-11T00:00:00.000000Z",
                "2021-06-12T00:00:00.000000Z",
                "2021-06-13T00:00:00.000000Z",
            ],
            "boolean": [True, True, False],
        }
    ).to_pandas()


# date field have all values null
INPUT_MESSAGE_2 = """\
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

INPUT_MESSAGE_2_WITH_DIFFERENT_DATA_TYPES = """\
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"decimal2": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"decimal2": null,"date": null,"datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"decimal2": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"decimal2": null,"date": null,"datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"decimal2": null,"date": null,"datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

EXPECTED_DF_2 = pa.table(
        {
            "str": ["value1", "value2", "value3"],
            "int": [1, None, 3],
            "decimal": [Decimal("0.1"), Decimal("0.2"), Decimal("0.3")],
            "datetime": [
                "2021-06-11T00:00:00.000000Z",
                "2021-06-12T00:00:00.000000Z",
                "2021-06-13T00:00:00.000000Z",
            ],
            "boolean": [True, True, False],
        }
    ).to_pandas()

# With null fields
INPUT_MESSAGE_3 = """\
{"type": "SCHEMA","stream": "test","schema": { "type": ["null", "object"], "properties": { "field1": { "type": ["null", "object"], "additionalProperties": false, "properties": { "field2": { "type": ["null", "object"], "properties": { "field3": { "type": ["null", "string"] }, "field4": { "type": ["null", "string"] } } } } }, "field2": { "type": ["null", "object"], "properties": { "field3": { "type": ["null", "string"] }, "field4": { "type": ["null", "string"] }, "field5": { "type": ["null", "string"] } } }, "field6": { "type": ["null", "string"] } }, "additionalProperties": false }, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"field1": {"field2": {"field3": "test_field3", "field4": "test_field4"}}, "field2": null}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""

EXPECTED_DF_3 = pd.DataFrame(
        [
            {'field1__field2__field3': 'test_field3',
             'field1__field2__field4': 'test_field4',
             'field2__field3': None,
             'field2__field4': None,
             'field2__field5': None,
             'field6': None,
             }
        ]
    )


class TestPersist(TestCase):
    def setUp(self):
        """Mocking HDFS methods to run local tests"""
        self.upload_to_hdfs_patcher = patch('target_hdfs.helpers.upload_to_hdfs')
        self.mock_upload_to_hdfs = self.upload_to_hdfs_patcher.start()
        self.mock_upload_to_hdfs.side_effect = lambda local, destination: shutil.copy(local, destination)

    def tearDown(self):
        self.upload_to_hdfs_patcher.stop()

    @staticmethod
    def generate_input_message(message):
        return io.TextIOWrapper(io.BytesIO(message.encode()), encoding="utf-8")

    def test_persist_messages(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.makedirs(os.path.join(tmpdirname, 'test'))
            persist_messages(self.generate_input_message(INPUT_MESSAGE_1), TargetConfig(f"{tmpdirname}"))
            filename = [f for f in glob.glob(f"{tmpdirname}/test/*.parquet")]
            df = ParquetFile(filename[0]).read().to_pandas()
            assert_frame_equal(df, EXPECTED_DF_1, check_like=True)

    def test_persist_messages_null_field(self):
        """
        This tests checks if the null object fields are being correctly exploded according to the schema and
        if it doesn't replace the values if we have a conflict of the same field name in different levels of object.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.makedirs(os.path.join(tmpdirname, 'test'))
            persist_messages(self.generate_input_message(INPUT_MESSAGE_3), TargetConfig(f"{tmpdirname}"))
            filename = [f for f in glob.glob(f"{tmpdirname}/test/*.parquet")]
            df = ParquetFile(filename[0]).read().to_pandas()
            assert_frame_equal(df, EXPECTED_DF_3, check_like=True)

    def test_persist_messages_invalid_sort(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.makedirs(os.path.join(tmpdirname, 'test'))
            with self.assertRaises(ValueError) as e:
                persist_messages(self.generate_input_message(INPUT_MESSAGE_1_REORDERED), TargetConfig(f"{tmpdirname}"))

                self.assertEqual("A record for stream test was encountered before a corresponding schema", e.exception)

    def test_persist_with_schema_force(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.makedirs(os.path.join(tmpdirname, 'test'))
            persist_messages(self.generate_input_message(INPUT_MESSAGE_2_WITH_DIFFERENT_DATA_TYPES), TargetConfig(f"{tmpdirname}"))
            filename = [f for f in glob.glob(f"{tmpdirname}/test/*.parquet")]
            schema = pa.parquet.read_schema(filename[0])
            expected_schema = pa.schema([
                pa.field("decimal", pa.float64(), True),
                pa.field("datetime", pa.string(), True),
                pa.field("date", pa.string(), True),
                pa.field("int", pa.int64(), True),
                pa.field("boolean", pa.bool_(), True),
                pa.field("decimal2", pa.float64(), True),
                pa.field("str", pa.string(), True)
            ])

            # Testing each field in schema
            for field in expected_schema:
                self.assertEqual(schema.field(field.name).type, field.type)

    def test_rows_per_file(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.makedirs(os.path.join(tmpdirname, 'test'))
            persist_messages(self.generate_input_message(INPUT_MESSAGE_1 * 10000), TargetConfig(f"{tmpdirname}", rows_per_file=1000))
            filename = [f for f in glob.glob(f"{tmpdirname}/test/*.parquet")]
            df = ParquetFile(filename[0]).read().to_pandas()

            self.assertEqual(len(filename), 30)
            self.assertEqual(len(df), 1000)  # 1000 rows per file

    def test_file_size(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            os.makedirs(os.path.join(tmpdirname, 'test'))
            persist_messages(self.generate_input_message(INPUT_MESSAGE_1 * 50000), TargetConfig(f"{tmpdirname}", file_size_mb=2))
            filename = [f for f in glob.glob(f"{tmpdirname}/test/*.parquet")]
            df = ParquetFile(filename[0]).read()

            self.assertEqual(len(filename), 5)
            self.assertAlmostEqual(bytes_to_mb(df.nbytes), 2, places=1)  # approx 2MB per file
