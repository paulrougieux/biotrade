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


class Database:
    """
    Database to store UN Comtrade data.
    """

    # Database schema configuration
    schema = "raw_comtrade"

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")

    def __init__(self, parent, database_url):
        # Default attributes #
        self.parent = parent
        # Database configuration
        self.engine = create_engine(database_url)
        # SQL Alchemy metadata
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine
        self.inspector = inspect(self.engine)
        # Describe table metadata
        self.yearly_hs2 = self.describe_table(name="yearly_hs2")
        #  Create tables if they don't exist
        if not self.inspector.has_table(self.yearly_hs2.name, schema=self.schema):
            self.yearly_hs2.create()
            self.logger.info(
                "Created table %s in schema %s.", self.yearly_hs2.name, self.schema
            )

    def append(self, df, table):
        """Store a data frame inside a given database table"""
        df.to_sql(
            name=table,
            con=self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
        )
        self.logger.info("Wrote %s rows to the database table %s", len(df), table)

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
            Column("period_desc", Text),
            Column("aggregate_level", BigInteger),
            Column("is_leaf_code", BigInteger),
            Column("trade_flow_code", BigInteger),
            Column("trade_flow", Text),
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
            Column("commodity_code", Text),
            Column("commodity", Text),
            Column("qty_unit_code", BigInteger),
            Column("qty_unit", Text),
            Column("qty", Text),
            Column("alt_qty_unit_code", Text),
            Column("alt_qty_unit", BigInteger),
            Column("alt_qty", Text),
            Column("netweight", Float(53)),
            Column("grossweight", Text),
            Column("tradevalue", BigInteger),
            Column("cifvalue", Text),
            Column("fobvalue", Text),
            Column("flag", BigInteger),
            UniqueConstraint(
                "period",
                "trade_flow_code",
                "reporter_code",
                "partner_code",
                "commodity_code",
                "qty_unit_code",
                "flag",
            ),
            schema=self.schema,
        )
        return table
