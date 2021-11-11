#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

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
from biotrade import module_dir, data_dir, DATABASE_URL
from biotrade.comtrade.country_groups import CountryGroups
from biotrade.comtrade.database import DatabaseComtradePostgresql
from biotrade.comtrade.database import DatabaseComtradeSqlite
from biotrade.comtrade.products import Products
from biotrade.comtrade.pump import Pump

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.common.logger import create_logger

create_logger()


class Comtrade:
    """
    Parent to various objects dealing with data from the UN Comtrade API.
    """

    # Location of the data
    data_dir = data_dir / "comtrade"

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    df = pandas.read_csv(config_data_dir / "column_names.csv")
    # Select only relevant columns and remove incomplete mappings
    df = df[["jrc", "comtrade_machine"]]
    column_names = df[df.isna().sum(axis=1) == 0]

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
        if DATABASE_URL is None:
            return DatabaseComtradeSqlite(self)
        else:
            return DatabaseComtradePostgresql(self)

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
