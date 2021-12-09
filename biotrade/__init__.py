#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

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

module_dir = Path(__file__).resolve().parent

# Where is the data, default case
data_dir = Path.home() / "repos/biotrade_data/"

# But you can override that with an environment variable
if os.environ.get("BIOTRADE_DATA"):
    data_dir = Path(os.environ["BIOTRADE_DATA"])

# Database connection URL, default case
database_url = None
# But you can override it with an environment variable
if os.environ.get("BIOTRADE_DATABASE_URL"):
    database_url = os.environ["BIOTRADE_DATABASE_URL"]
