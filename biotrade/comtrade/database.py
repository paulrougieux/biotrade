#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Get yearly Comtrade data at the 2 digit level.
Load the complete table into a pandas data frame.

    >>> import pandas
    >>> from biotrade.comtrade import comtrade
    >>> db = comtrade.db_pgsql
    >>> df = pandas.read_sql_table("yearly_hs2", db.engine, schema="raw_comtrade")

Select data for the year 2017 using an SQL Alchemy select statement. Return results
using an SQL Alchemy cursor or with a pandas data frame:

    >>> year = db.yearly_hs2.columns.get("year")
    >>> statement = db.yearly_hs2.select().where(year == 2017)
    >>> # Get results as a list from the cursor
    >>> with db.engine.connect() as connection:
    >>>     result = connection.execute(statement)
    >>> for row in result:
    >>>     print(row)
    >>> # Legacy cursor
    >>> result_2 = db.engine.execute(statement).fetchall()
    >>> # Results as a data frame
    >>> df_2017 = pandas.read_sql_query(statement, db.engine)

Download and store in the database as used when updating the database
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


class DatabaseComtrade(Database):
    """
    Database to store UN Comtrade data.
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
        self.monthly = self.describe_trade_table(name="monthly")
        self.create_if_not_existing(self.monthly)
        self.yearly = self.describe_trade_table(name="yearly")
        self.create_if_not_existing(self.yearly)
        self.yearly_hs2 = self.describe_trade_table(name="yearly_hs2")
        self.create_if_not_existing(self.yearly_hs2)

    def describe_trade_table(self, name):
        """Define the metadata of a table containing Comtrade data.

        The unique constraint is a very important part of the table structure.
        It makes sure that there will be no duplicated flows.

        Alternatively a table metadata structure could be automatically loaded with:

            Table('yearly_hs2', self.metadata, autoload_with=self.engine)

        The python code below was originally generated with:

            sqlacodegen --schema raw_comtrade --tables yearly_hs2 postgresql://rdb@localhost/biotrade

        Note the "product" column is left empty, removed from the data frame
        before it is stored in the database because it would be too large. The
        text description of a product (commodity) is available in the products table.
        """
        table = Table(
            name,
            self.metadata,
            Column("classification", Text),
            Column("year", Integer),
            Column("period", Integer),
            Column("period_description", Text),
            Column("aggregate_level", Integer),
            Column("is_leaf", Integer),
            Column("flow_code", Integer),
            Column("flow", Text),
            Column("reporter_code", Integer),
            Column("reporter", Text),
            Column("reporter_iso", Text),
            Column("partner_code", Integer),
            Column("partner", Text),
            Column("partner_iso", Text),
            Column("partner_2_code", Text),
            Column("partner_2", Text),
            Column("partner_2_iso", Text),
            Column("customs_proc_code", Text),
            Column("customs", Text),
            Column("mode_of_transport_code", Text),
            Column("mode_of_transport", Text),
            Column("product_code", Text),
            Column("unit_code", Integer),
            Column("unit", Text),
            Column("quantity", Float),
            Column("alt_qty_unit_code", Text),
            Column("alt_qty_unit", Text),
            Column("alt_qty", Float),
            Column("net_weight", Float),
            Column("gross_weight", Float),
            Column("trade_value", Float),
            Column("cif_value", Float),
            Column("fob_value", Float),
            Column("flag", Integer),
            UniqueConstraint(
                "period",
                "flow_code",
                "reporter_code",
                "partner_code",
                "product_code",
                "unit_code",
                "flag",
            ),
            schema=self.schema,
        )
        return table


class DatabaseComtradePostgresql(DatabaseComtrade):
    """Database using the PostgreSQL engine"""

    database_url = "postgresql://rdb@localhost/biotrade"
    schema = "raw_comtrade"


class DatabaseComtradeSqlite(DatabaseComtrade):
    """Database using the SQLite engine"""

    database_url = f"sqlite:///{data_dir}/comtrade/comtrade.db"
    schema = "main"