#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Download and store in the database as used when updating the database

    >>> comtrade.pump.append_to_db(cc = "440799")

"""

# Third party modules
from sqlalchemy import create_engine


class Database:
    """
    Database to store UN comtrade data.
    """

    # Database configuration
    engine = create_engine("postgresql://rdb@localhost/tradeflows")
    schema = "raw_comtrade"

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent

    def append_to_db(self, df, table):
        """Store a data frame inside a given database table"""
        df.to_sql(
            name="yearly",
            con=self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
        )
