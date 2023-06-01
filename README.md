# target-hdfs

A [Singer](https://singer.io) target that writes data to parquet files in HDFS. This target is based on [`target-parquet`] [targetparquet] 
and the code was adapted to generate parquet files and upload to HDFS using RPC.

## How to use it

`target-hdfs` works with a [Singer Tap] in order to move data ingested by the tap into parquet files and upload to HDFS.

### Install

We will use [`tap-exchangeratesapi`][exchangeratesapi] to pull currency exchange rate data from a public data set as an example.

First, make sure Python 3 is installed on your system or follow these installation instructions for [Linux] or [Mac] and you have a HDFS configured with `core-site.xml` file.

It is recommended to install each Tap and Target in a separate Python virtual environment to avoid conflicting dependencies between any Taps and Targets.

```bash
 # Install tap-exchangeratesapi in its own virtualenv
python3 -m venv ~/.virtualenvs/tap-exchangeratesapi
source ~/.virtualenvs/tap-exchangeratesapi/bin/activate
pip3 install tap-exchangeratesapi
deactivate

# Install target-hdfs in its own virtualenv
python3 -m venv ~/.virtualenvs/target-hdfs
source ~/.virtualenvs/target-hdfs/bin/activate
pip3 install target-hdfs
deactivate
```

### Run

We can now run `tap-exchangeratesapi` and pipe the output to `target-hdfs`.

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-hdfs/bin/target-hdfs
```

By default (if you use the config sample), the data will be written into a file called `/path/to/the/output/directory/exchange_rate/sync_ymd=20220101/{timestamp}.gz.parquet` in your HDFS.

### Optional Configuration

If you want to save the file in a specific location, you need to create a new configuration file, 
in which you specify the `hdfs_destination_path` to the directory you are interested in and pass the `-c` argument to the target.
Also, you can compress the parquet file by passing the `compression_method` argument in the configuration file. 
Note that, these compression methods have to be supported by `Pyarrow`, and at the moment (October, 2020), 
the only compression modes available are: snappy (recommended), zstd, brotli and gzip. The library will check these, 
and default to `gzip` if something else is provided.
For an example of the configuration file, see [config.sample.json](config.sample.json).
The `file_prefix` define a prefix to include in the file name when upload to HDFS.
There is also an `streams_in_separate_folder` option to create each stream in a different folder, as these are expected to come in different schema.
The `rows_per_file` and `file_size_mb` force a file to be saved every time the number of rows is reached or/and the pyarrow 
dataframe size reach the file size (without the compression).
The `partitions` config is an option to save the data in one or more partitions (e.g. `sync_ymd=20230101` or `sync_year=2023/sync_month=01/sync_day=01`).
To run `target-hdfs` with the configuration file, use this command:
The `force_header_snake_case` config convert all the fields in the header to snake case in lower case (e.g. `Key 1 > #1` to `key_1_1`), it's disable by default.

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-hdfs/bin/target-hdfs -c config.json
```

[singer tap]: https://singer.io
[targetparquet]: https://github.com/Automattic/target-parquet
[exchangeratesapi]: https://github.com/singer-io/tap-exchangeratesapi
[mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[linux]: https://docs.python-guide.org/starting/install3/linux/


### Development

To install development required packages run

```bash
pip install -e ".[dev]"
```

In order to run all tests run

```bash
pytest
```