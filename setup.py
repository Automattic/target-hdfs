#!/usr/bin/env python

from setuptools import setup

setup(
    name="target-hdfs",
    version="0.0.4",
    description="Singer.io target for writing parquet files in HDFS",
    author="Joao Amaral",
    url="https://singer.io",
    python_requires=">3.9",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_hdfs"],
    install_requires=["singer-python>=5.13.0,<6.0.0", "pyarrow>=10.0.1,<11.0.0"],
    extras_require={
        "dev": ["pytest<8.0.0", "pandas<2.0.0", "pylint<3.0.0", "pylint-quotes<1.0.0"]
    },
    entry_points="""
          [console_scripts]
          target-hdfs=target_hdfs:main
      """,
    packages=["target_hdfs"],
)
