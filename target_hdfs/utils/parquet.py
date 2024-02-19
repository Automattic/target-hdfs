import os


def get_parquet_files(dir_path: str) -> list[str]:
    """Return the path of parquet files in the local directory."""
    return [
        os.path.join(root, file)
        for root, dirs, files in os.walk(dir_path)
        for file in files
        if file.endswith(".parquet")
    ]
