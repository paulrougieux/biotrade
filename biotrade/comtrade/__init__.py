#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Download only and return a data frame, for debugging purposes:

    >>> from biotrade.comtrade import comtrade
    >>> # Other sawnwood
    >>> swd99 = comtrade.pump.download(cc = "440799")
    >>> # Soy
    >>> soy = comtrade.pump.download(cc = "120190")

Download and store in the database as used when updating the database

    >>> comtrade.pump.append_to_db(cc = "440799")

Show the list of reporter countries

    >>> from biotrade.comtrade import comtrade
    >>> comtrade.country_groups.reporters

Display information on column names used for renaming
and dropping less important columns:

    >>> comtrade.column_names

"""
# Third party modules
import pandas

# Internal modules
from biotrade import module_dir, data_dir, database_url
from biotrade.comtrade.country_groups import CountryGroups
from biotrade.comtrade.database import DatabaseComtradePostgresql
from biotrade.comtrade.database import DatabaseComtradeSqlite
from biotrade.comtrade.dump import Dump
from biotrade.comtrade.products import Products
from biotrade.comtrade.pump import Pump

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.common.logger import create_logger

create_logger()


class Comtrade:
    """
    Parent to various objects dealing with data from the UN Comtrade API.
    """

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    df = pandas.read_csv(config_data_dir / "column_names.csv")
    # Select only relevant columns and remove incomplete mappings
    df = df[["biotrade", "comtrade_machine", "comtrade_human", "comtrade_apicall"]]
    column_names = df

    def __init__(self):
        # Location of the data
        self.data_dir = data_dir / "comtrade"
        if not self.data_dir.exists():
            self.data_dir.mkdir()
        # Location of the bilateral trade data grouped by commodities and products
        self.products_dir = self.data_dir / "products"

    @property
    def country_groups(self):
        """Identify reporter and partner countries and regions"""
        return CountryGroups(self)

    @property
    def db_pgsql(self):
        """Store Comtrade data and make it available for further processing"""
        return DatabaseComtradePostgresql(self)

    @property
    def db_sqlite(self):
        """Store Comtrade data and make it available for further processing"""
        return DatabaseComtradeSqlite(self)

    @property
    def db(self):
        """The generic database can be either a PostGreSQL or a SQLite database
        Depending of the value of the DATABASE_URL variable. If it's None
        then use SQLite otherwise use the PostGreSQL db defined in the
        environmental variable BIOTRADE_DATABASE_URL.
        """
        if database_url is None:
            return DatabaseComtradeSqlite(self)
        return DatabaseComtradePostgresql(self)

    @property
    def dump(self):
        """Dump data from the database into files"""
        return Dump(self)

    @property
    def products(self):
        """Identify Comtrade products (commodities) and metadata"""
        return Products(self)

    @property
    def pump(self):
        """Download data from Comtrade and send it to the database"""
        return Pump(self)


# Make a singleton #
comtrade = Comtrade()
