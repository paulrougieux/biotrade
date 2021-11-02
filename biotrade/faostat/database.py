#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.
"""
# First party modules
import logging

# Third party modules
from sqlalchemy import Integer, Float, Text, UniqueConstraint
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import create_engine, inspect

# Internal modules
from biotrade import data_dir
from biotrade.common.database import Database


class DatabaseFaostat(Database):
    """
    Database to store UN Comtrade data.

    For example store forestry production data in the database:

        >>> from biotrade.faostat import faostat
        >>> fp = faostat.pump.forestry_production
        >>> faostat.db_sqlite.append(fp, "forestry_production")

    Store

    Select data for Italy using an SQL Alchemy select statement. Return results
    using an SQL Alchemy cursor or with a pandas data frame:

        >>> db = faostat.db_sqlite
        >>> reporter = db.forestry_production.columns.get("reporter")
        >>> statement = db.forestry_production.select().where(reporter == "Italy")
        >>> df_it = pandas.read_sql_query(statement, db.engine)

    Use a SQL select statement directly

        >>> df_it = pandas.read_sql_query("SELECT * FROM forestry_production WHERE reporter = 'Italy'",
        >>>                               db.engine)

    """

    # To be overwritten by the children
    database_url = None
    schema = None

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Database configuration
        self.engine = create_engine(self.database_url)
        # SQL Alchemy metadata
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine
        self.inspector = inspect(self.engine)
        # Describe table metadata and create them if they don't exist
        self.forestry_production = self.describe_trade_table(name="forestry_production")
        self.create_if_not_existing(self.forestry_production)

    def describe_trade_table(self, name):
        """Define the metadata of a table containing Comtrade data.

        The unique constraint is a very important part of the table structure.
        It makes sure that there will be no duplicated flows.

        Note the "product" column is left empty, removed from the data frame
        before it is stored in the database because it would be too large. The
        text description of a product (commodity) is available in the products table.

        Experiment on database size test: dropping the following text columns from the definition

             Column("reporter", Text),
             Column("product", Text),
             Column("element", Text),
             Column("unit", Text),
             Column("flag", Text),

        And dropping these columns from the data frame before loading it back to the database

            >>> from biotrade.faostat import faostat
            >>> fp = faostat.pump.forestry_production
            >>> fp.drop(columns=["reporter", "product", "element", "unit", "flag"], inplace=True)
            >>> faostat.db_sqlite.append(fp, "forestry_production")

        Leads to a size reduction from 250 Mb to 99 Mb

            ls -l ~/repos/biotrade_data/faostat/faostat.db
            -rw-r--r-- 1 paul paul 99246080 Nov  2 05:34 /home/paul/repos/biotrade_data/faostat/faostat.db
            -rw-r--r-- 1 paul paul 251060224 Nov  2 05:43 /home/paul/repos/biotrade_data/faostat/faostat.db

        As a result, it is probably not worth using index tables, because the gain will not be very large.
        """
        table = Table(
            name,
            self.metadata,
            Column("reporter_code", Integer),
            Column("reporter", Text),
            Column("product_code", Integer),
            Column("product", Text),
            Column("element_code", Integer),
            Column("element", Text),
            Column("period", Integer),
            Column("year", Integer),
            Column("unit", Text),
            Column("value", Float),
            Column("flag", Text),
            UniqueConstraint(
                "period",
                "reporter_code",
                "product_code",
                "element_code",
                "unit",
                "flag",
            ),
            schema=self.schema,
        )
        return table


class DatabaseFaostatSqlite(DatabaseFaostat):
    """Database using the SQLite engine"""

    database_url = f"sqlite:///{data_dir}/faostat/faostat.db"
    schema = "main"
