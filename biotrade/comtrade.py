#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Download only and return a data frame, for debugging purposes:

    >>> from env_impact_imports.comtrade import comtrade
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
from env_impact_imports import module_dir
from env_impact_imports.countries import Countries
from env_impact_imports.database import Database
from env_impact_imports.products import Products
from env_impact_imports.pump import Pump


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
        """Identify Comtrade products (commodities) and metadata"""
        return Countries(self)

    @property
    def database(self):
        """Store downloaded data in a database for further processing"""
        return Database(self)

    @property
    def products(self):
        """Identify Comtrade products (commodities) and metadata"""
        return Products(self)

    @property
    def pump(self):
        """Download data from Comtrade"""
        return Pump(self)


# Make a singleton #
comtrade = Comtrade()
