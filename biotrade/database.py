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
    >>> db = comtrade.database_postgresql
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
from sqlalchemy import BigInteger, Float, Text, UniqueConstraint
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import create_engine, inspect

# Internal modules
from biotrade import data_dir


class Database:
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
        self.yearly_hs2 = self.describe_and_create_if_not_existing(name="yearly_hs2")
        self.monthly = self.describe_and_create_if_not_existing(name="monthly")
        self.yearly = self.describe_and_create_if_not_existing(name="yearly")

    def append(self, df, table, drop_description=True):
        """Store a data frame inside a given database table"""
        # Drop the lengthy product description
        if drop_description and "product_description" in df.columns:
            df.drop(columns=["product_description"], inplace=True)
        df.to_sql(
            name=table,
            con=self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
        )
        self.logger.info("Wrote %s rows to the database table %s", len(df), table)

    def describe_and_create_if_not_existing(self, name):
        """Create the table in the database if it doesn't exist already"""
        # Describe table metadata
        table = self.describe_table(name=name)
        #  Create the table if it doesn't exist
        if not self.inspector.has_table(table.name, schema=self.schema):
            table.create()
            self.logger.info("Created table %s in schema %s.", table.name, self.schema)
        return table

    def describe_table(self, name):
        """Define the metadata of a table containing Comtrade data.

        The unique constraint is a very important part of the table structure.
        It makes sure that there will be no duplicated flows.

        Alternatively a table metadata structure could be automatically loaded with:

            Table('yearly_hs2', self.metadata, autoload_with=self.engine)

        The python code below was originally generated with:

            sqlacodegen --schema raw_comtrade --tables yearly_hs2 postgresql://rdb@localhost/biotrade

        Note the "commodity" column is left empty, removed from the data frame
        before it is stored in the database because it would be too large. The
        text description of a commodity is available in the products table.
        """
        table = Table(
            name,
            self.metadata,
            Column("classification", Text),
            Column("year", BigInteger),
            Column("period", BigInteger),
            Column("period_description", Text),
            Column("aggregate_level", BigInteger),
            Column("is_leaf", BigInteger),
            Column("flow_code", BigInteger),
            Column("flow", Text),
            Column("reporter_code", BigInteger),
            Column("reporter", Text),
            Column("reporter_iso", Text),
            Column("partner_code", BigInteger),
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
            Column("unit_code", BigInteger),
            Column("unit", Text),
            Column("quantity", Text),
            Column("alt_qty_unit_code", Text),
            Column("alt_qty_unit", BigInteger),
            Column("alt_qty", Text),
            Column("net_weight", Float(53)),
            Column("gross_weight", Text),
            Column("trade_value", BigInteger),
            Column("cif_value", Text),
            Column("fob_value", Text),
            Column("flag", BigInteger),
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


class DatabasePostgresql(Database):
    """Database using the PostgreSQL engine"""

    database_url = "postgresql://rdb@localhost/biotrade"
    schema = "raw_comtrade"


class DatabaseSqlite(Database):
    """Database using the SQLite engine"""

    database_url = f"sqlite:///{data_dir}/trade.db"
    schema = "main"
