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

Display information on column names used for renaming
and dropping less important columns:

    >>> comtrade.column_names

"""
# Third party modules
import pandas

# Internal modules
from biotrade import module_dir
from biotrade.countries import Countries
from biotrade.database import Database
from biotrade.products import Products
from biotrade.pump import Pump

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.logger import create_logger

create_logger()


class Comtrade:
    """
    Parent to various objects dealing with data from the UN Comtrade API.
    """

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    column_names = pandas.read_csv(config_data_dir / "column_names.csv")

    @property
    def countries(self):
        """Identify reporter and partner countries and regions"""
        return Countries(self)

    @property
    def database(self):
        """Store Comtrade data and make it available for further processing"""
        return Database(self)

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
