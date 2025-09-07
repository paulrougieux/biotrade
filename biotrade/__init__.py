#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

The `biotrade` package analyses international trade of bio-based products. It focuses on
the agriculture and forestry sectors, from primary production to secondary products
transformation. It loads bilateral trade data from UN Comtrade, production and trade
data from FAOSTAT and socio-economic indicators from the World Bank.

- For installation instructions see the
[README](https://pypi.org/project/biotrade/) on pypi.org or on the project git
repository.

The `biotrade` package is structured into sub-packages, one for each data
source: `biotrade.faostat`, `biotrade.comtrade` and `biotrade.world_bank`. In
addition, there is a `biotrade.common` sub-package.


# Update data from sources

Each data source has a "pump" and a "database" object. The "pump" loads data
from the source and the database stores data locally. The documentation of the
pump and database of each source provide information on how to update and
select data. Start with the FAOSTAT database which is the most light weight and
which will work correctly also without an API access token.

- `biotrade.faostat.pump` and `biotrade.faostat.database`

- `biotrade.comtrade.pump` and `biotrade.comtrade.database`

- `biotrade.world_bank.pump` and `biotrade.world_bank.database`


# Transform data

- Common data transformation functions are located in `biotrade.common`.

- Some transformation functions are specific to a data sources, you will find
the under the sub-module for that data source such as for example:
`biotrade.faostat.aggregate` or `biotrade.faostat.country_groups`


# Paths defined for biotrade data

Some paths are defined at the top level:

 - `module dir` makes it possible to access configuration data such as product
 codes and country codes mapping tables in `biotrade/config data`.

 - `data_dir` contains the location of the directory where data is stored (if
   SQLite databases are used, they will be stored there as well). By default it
   is located in the user's home directory, in a sub-directory called
   "$HOME/repos/forobs/biotrade_data/" but this can be changed with the
   environment variable BIOTRADE_DATA. To check that you have entered the
   environment variable correctly, you can call the following from python:

        >>> import os
        >>> os.environ["BIOTRADE_DATA"]

 - For more information on how to set up environment variables in Windows, Mac
   or Unix see
   https://superuser.com/questions/284342/what-are-path-and-other-environment-variables-and-how-can-i-set-or-use-them

 - `database_url` is equal to `None` by default and then a local on-disk SQLite
 database is used. It can be changed to a PostGreSQL database through the
 environment variable BIOTRADE_DATABASE_URL that specifies the location of the
 database connection URL.

 You can display these values with :

    >>> import biotrade
    >>> print("module_dir:", biotrade.module_dir)
    >>> print("data_dir:", biotrade.data_dir)
    >>> print("database_url:", biotrade.database_url)

"""

from pathlib import Path
import os

__version__ = "0.4.0"

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
