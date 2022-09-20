#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
You can use this object at the ipython console with the following examples.

Download and return a data frame:

    >>> from biotrade.eurostat import eurostat
    >>> exch_rates = eurostat.pump.download_bulk_df("ert_bil_eur_m")
    >>> demo_gind = eurostat.pump.download_bulk_df("demo_gind")

"""
# Standard modules

# Third party modules
import pandas

# Internal modules
from biotrade.eurostat.pump import Pump

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.common.logger import create_logger
from biotrade import module_dir, data_dir

create_logger()


class Eurostat:
    """
    Parent to various objects dealing with data from Eurostat.
    """

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    df = pandas.read_csv(config_data_dir / "column_names.csv")
    # Select only relevant columns and remove incomplete mappings
    df = df[["biotrade", "eurostat_bulk"]]
    column_names = df[df.isna().sum(axis=1) == 0]

    def __init__(self):
        # Location of the data
        self.data_dir = data_dir / "eurostat"
        if not self.data_dir.exists():
            self.data_dir.mkdir()

    @property
    def pump(self):
        """Download data from Comtrade and send it to the database"""
        return Pump(self)


# Make a singleton #
eurostat = Eurostat()
