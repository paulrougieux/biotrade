#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Read forestry production and trade data from FAOSTAT files

    >>> from biotrade.faostat import faostat
    >>> fp = faostat.pump.forestry_production
    >>> ft = faostat.pump.forestry_trade

Download or update zipped CSV files from FAOSTAT.

    >>> from biotrade.faostat import faostat
    >>> faostat.pump.download_zip_csv("Forestry_E_All_Data_(Normalized).zip")

Display information on column names used for renaming
and dropping less important columns:

    >>> faostat.column_names

Display the list of EU countries names in the FAOSTAT data

    >>> faostat.countries.eu_country_names

"""
# Third party modules
import pandas

# Internal modules
from biotrade import module_dir, data_dir
from biotrade.faostat.products import Products
from biotrade.faostat.pump import Pump
from biotrade.faostat.countries import Countries

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.logger import create_logger

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
    def countries(self):
        """Identify reporter and partner countries and regions"""
        return Countries(self)

    @property
    def products(self):
        """Identify Comtrade products (commodities) and metadata"""
        return Products(self)

    @property
    def pump(self):
        """Load data from FAOSTAT and read it into data frames"""
        return Pump(self)


# Make a singleton #
faostat = Faostat()
