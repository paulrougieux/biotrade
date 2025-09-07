#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani

Copyright (c) 2023 European Union
Licenced under the MIT licence
"""

# Imports #
from setuptools import setup, find_namespace_packages
from os import path

# Load the contents of the README file #
this_dir = path.abspath(path.dirname(__file__))
readme_path = path.join(this_dir, "README.md")
with open(readme_path, encoding="utf-8") as handle:
    readme = handle.read()

# Call setup #
setup(
    name="biotrade",
    version="0.4.0",
    description="Agriculture and forestry statistics.",
    license="MIT",
    url="https://gitlab.com/bioeconomy/forobs/biotrade/",
    author="Paul Rougieux, Selene Patani",
    author_email="paul.rougieux@gmail.com",
    packages=find_namespace_packages(exclude=["notebooks", "scripts"]),
    install_requires=[
        "pandas",
        "sqlalchemy",
        "sqlalchemy_utils",
        "psycopg2-binary",
        "requests",
        "scipy",
        "pymannkendall",
        "matplotlib",
        "comtradeapicall",
        "dbfread",
    ],
    extras_require={"api": ["fastapi", "uvicorn"]},
    python_requires=">=3.7",
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
    ],
    include_package_data=True,
)
