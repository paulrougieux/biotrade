#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Usage:

    from biotrade.world_bank import world_bank

"""
# External dependencies
import pandas as pd

# Internal modules
from biotrade import module_dir, data_dir, database_url
from biotrade.world_bank.pump import Pump
from biotrade.world_bank.database import DatabaseWorldBankSqlite
from biotrade.world_bank.database import DatabaseWorldBankPostgresql

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.common.logger import create_logger

create_logger()


class WorldBank:
    """
    Parent to various objects dealing with World Bank data
    """

    # Location of the data
    data_dir = data_dir / "world_bank"

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    df = pd.read_csv(config_data_dir / "column_names.csv")
    # Select only relevant columns and remove incomplete mappings
    df = df[["biotrade", "world_bank"]]
    column_names = df[df.isna().sum(axis=1) == 0]

    def __init__(self):
        if not self.data_dir.exists():
            self.data_dir.mkdir()

    @property
    def pump(self):
        """Load data from the World Bank and read it into data frames"""
        return Pump(self)

    @property
    def db_sqlite(self):
        """Database using the SQLite engine"""
        return DatabaseWorldBankSqlite(self)

    @property
    def db_postgresql(self):
        """Database using the PostGreSQL engine"""
        return DatabaseWorldBankPostgresql(self)

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
        if database_url is None:
            return DatabaseWorldBankSqlite(self)
        return DatabaseWorldBankPostgresql(self)


# Make a singleton #
world_bank = WorldBank()
