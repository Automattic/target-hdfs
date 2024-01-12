# target-hdfs

`target-hdfs` is a Singer target for hdfs.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

Install from PyPi:

```bash
pipx install target-hdfs
```

Install from GitHub:

```bash
pipx install git+https://github.com/Automattic/target-hdfs.git@main
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-hdfs --about
```

| Setting                | Required | Default | Description                                                                                                                                                                                                                                                                                      |
|:-----------------------|:--------:|:-------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hdfs_destination_path  |   True   | None    | HDFS Destination Path                                                                                                                                                                                                                                                                            |
| compression_method     |  False   | gzip    | (Default - gzip) Compression methods have to be supported by Pyarrow, and currently the compression modes available are - snappy, zstd, brotli and gzip.                                                                                                                                         |
| max_pyarrow_table_size |  False   |     800 | Max size of pyarrow table in MB (before writing to parquet file). It can control the memory usage of the target.                                                                                                                                                                                 |
| max_batch_size         |  False   |   10000 | Max records to write in one batch. It can control the memory usage of the target.                                                                                                                                                                                                                |
| extra_fields           |  False   | None    | Extra fields to add to the flattened record. (e.g. extra_col1=value1,extra_col2=value2)                                                                                                                                                                                                          |
| extra_fields_types     |  False   | None    | Extra fields types. (e.g. extra_col1=string,extra_col2=integer)                                                                                                                                                                                                                                  |
| partition_cols         |  False   | None    | Extra fields to add to the flattened record. (e.g. extra_col1,extra_col2)                                                                                                                                                                                                                        |

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

The `target-hdfs` will use the configuration set in `core-sites.xml` and `hdfs-site.xml` files to authenticate and authorize the connection to HDFS.

## Usage

You can easily run `target-hdfs` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-hdfs --version
target-hdfs --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-hdfs --config /path/to/target-hdfs-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-hdfs` CLI interface directly using `poetry run`:

```bash
poetry run target-hdfs --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-hdfs
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-hdfs --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-hdfs
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
