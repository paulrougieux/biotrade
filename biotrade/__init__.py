#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Specify the module dir to make it possible to access config data. Specify the
location of biotrade_data folder and the database connection URL for all
components of the system.

Usage:

    >>> from biotrade import module_dir
    >>> from biotrade import data_dir
    >>> from biotrade import database_url

"""

from pathlib import Path
import os

__version__ = "0.0.23"

module_dir = Path(__file__).resolve().parent

# Where is the data, default case
data_dir = Path.home() / "repos/forobs/biotrade_data/"

# But you can override that with an environment variable
if os.environ.get("BIOTRADE_DATA"):
    data_dir = Path(os.environ["BIOTRADE_DATA"])

# Create if not existing
if not data_dir.exists():
    if os.environ.get("BIOTRADE_SKIP_CONFIRMATION"):
        data_dir.mkdir(parents=True)
    else:
        msg = f"Create {data_dir}?"
        if input(msg + "Please confirm [y/n]:") == "y":
            data_dir.mkdir(parents=True)
        else:
            print("Directory creation cancelled.")

# Database connection URL, default case
database_url = None
# But you can override it with an environment variable
if os.environ.get("BIOTRADE_DATABASE_URL"):
    database_url = os.environ["BIOTRADE_DATABASE_URL"]
