#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Read forestry production and trade data from the database

    >>> from biotrade.faostat import faostat

Read forestry production and trade data from the bulk FAOSTAT files directly
(loading large files takes a few seconds)

    >>> fp = faostat.pump.forestry_production
    >>> ft = faostat.pump.forestry_trade

Download or update zipped CSV files from FAOSTAT.

    >>> from biotrade.faostat import faostat
    >>> faostat.pump.download_zip_csv("Forestry_E_All_Data_(Normalized).zip")

Display information on column names used for renaming
and dropping less important columns:

    >>> faostat.column_names

Display the list of EU countries names in the FAOSTAT data

    >>> faostat.country_groups.eu_country_names

"""
# Third party modules
import pandas

# Internal modules
from biotrade import module_dir, data_dir, DATABASE_URL
from biotrade.faostat.products import Products
from biotrade.faostat.pump import Pump
from biotrade.faostat.country_groups import CountryGroups
from biotrade.faostat.database import DatabaseFaostatSqlite
from biotrade.faostat.database import DatabaseFaostatPostgresql

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.common.logger import create_logger

create_logger()


class Faostat:
    """
    Parent to various objects dealing with FAOSTAT data
    """

    # Location of the data
    data_dir = data_dir / "faostat"

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    df = pandas.read_csv(config_data_dir / "column_names.csv")
    df = df.filter(regex="jrc|faostat")
    non_na_values = (~df.filter(like="faostat").isna()).sum(axis=1)
    column_names = df[non_na_values > 0]

    @property
    def country_groups(self):
        """Identify reporter and partner countries and regions"""
        return CountryGroups(self)

    @property
    def products(self):
        """Identify Comtrade products (commodities) and metadata"""
        return Products(self)

    @property
    def pump(self):
        """Load data from FAOSTAT and read it into data frames"""
        return Pump(self)

    @property
    def db_sqlite(self):
        """Store Comtrade data and make it available for further processing"""
        return DatabaseFaostatSqlite(self)

    @property
    def db(self):
        """The generic database can be either a PostGreSQL or a SQLite database
        Depending of the value of the environmental variable
        BIOTRADE_DATABASE_URL. If it's not defined use an SQLite database, otherwise
        use a PostGreSQL database as defined in that URL. Note that the environment
        variables are read at the root of this module's directory. In
        particular BIOTRADE_DATABASE_URL is stored into the DATABASE_URL
        variable.
        """
        if DATABASE_URL is None:
            return DatabaseFaostatSqlite(self)
        return DatabaseFaostatPostgresql(self)


# Make a singleton #
faostat = Faostat()
