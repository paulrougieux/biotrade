#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.
Load population data https://data.worldbank.org/indicator/SP.POP.TOTL

    >>> from biotrade.world_bank import world_bank
    >>> pop = world_bank.db.select("indicator", indicator_code="SP.POP.TOTL")

Select available indicator time series for Italy and Germany and display those
that contain the word "gdp".

    >>> from biotrade.world_bank import world_bank
    >>> indicators = world_bank.db.select("indicator", reporter=["Germany", "Italy"])
    >>> icode = indicators["indicator_code"]
    >>> icode[icode.str.contains("gdp", case=False)].unique()

Select the GDP in constant USD of 2015 and display the country groups and
countries with the highest GDP
https://data.worldbank.org/indicator/NY.GDP.MKTP.KD

    >>> gdpk2015 = world_bank.db.select("indicator", indicator_code="NY.GDP.MKTP.KD")
    >>> gdpk2015.groupby('reporter')["value"].max().sort_values(ascending=False).head(20)

"""
# First party modules
import logging

# Third party modules
import pandas
from sqlalchemy import Integer, Float, Text, UniqueConstraint
from sqlalchemy import Table, Column
from sqlalchemy import MetaData
from sqlalchemy import create_engine, inspect

# from sqlalchemy.sql import func
from sqlalchemy.schema import CreateSchema
from sqlalchemy_utils import database_exists, create_database

# Internal modules
from biotrade import data_dir, database_url
from biotrade.common.database import Database


class DatabaseWorldBank(Database):
    """
    Database to store World Bank Indicators data

    Database update methods are in the world_bank.pump object.

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
        # Create the database if it doesn't exist
        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        # SQL Alchemy metadata
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine
        self.inspector = inspect(self.engine)

        # Create the schema if it doesn't exist.
        # Exclude the SQLite engine because there is only a default schemas for that engine.
        # And the SQLite dialect doesn't have a has_schema() method.
        if hasattr(self.engine.dialect, "has_schema") and callable(
            getattr(self.engine.dialect, "has_schema")
        ):
            with self.engine.connect() as conn:
                if not self.engine.dialect.has_schema(conn, self.schema):
                    conn.execute(CreateSchema(self.schema))

        # Describe table metadata
        self.indicator = self.describe_indicator_table(name="indicator")
        self.indicator_name = self.describe_indicator_name_table(name="indicator_name")
        self.tables = {
            "indicator": self.indicator,
            "indicator_name": self.indicator_name,
        }
        # Create tables if they don't exist
        for table in self.tables.values():
            self.create_if_not_existing(table)

    def describe_indicator_table(self, name):
        """Define the metadata of a table containing indicator data."""
        table = Table(
            name,
            self.metadata,
            Column("reporter_code", Text, index=True),
            Column("reporter", Text),
            Column("indicator_code", Text, index=True),
            Column("year", Integer),
            Column("unit", Text),
            Column("value", Float),
            # Unit not in the unique constraint due to some nan values
            UniqueConstraint(
                "reporter_code",
                "indicator_code",
                "year",
            ),
            schema=self.schema,
        )
        return table

    def describe_indicator_name_table(self, name):
        """Define the metadata of a table containing indicator name data."""
        table = Table(
            name,
            self.metadata,
            Column("indicator_code", Text, index=True),
            Column("indicator_name", Text),
            UniqueConstraint(
                "indicator_code",
            ),
            schema=self.schema,
        )
        return table

    def select(
        self,
        table,
        reporter=None,
        reporter_code=None,
        indicator_code=None,
        year_start=None,
        year_stop=None,
    ):
        """
        Select world bank data for the given arguments

        :param str table: name of the database table to select from
        :param list or str reporter: list of reporter names
        :param list or int reporter_code: list of reporter codes
        :param list or int indicator_code: list of indicator codes
        :param int year_start: year from which data are retrieved
        :param int year_stop: year end data
        :return: A data frame of world bank indicators

        For example select population data https://data.worldbank.org/indicator/SP.POP.TOTL

            >>> from biotrade.world_bank import world_bank
            >>> pop = world_bank.db.select("indicator", indicator_code="SP.POP.TOTL")

        """
        table = self.tables[table]
        # Change string arguments to lists suitable for a
        # column.in_() clause
        if isinstance(reporter, str):
            reporter = [reporter]
        if isinstance(reporter_code, str):
            reporter_code = [reporter_code]
        if isinstance(indicator_code, str):
            indicator_code = [indicator_code]
        # Build the select statement
        stmt = table.select()
        if reporter is not None:
            stmt = stmt.where(table.c.reporter.in_(reporter))
        if reporter_code is not None:
            stmt = stmt.where(table.c.reporter_code.in_(reporter_code))
        if indicator_code is not None:
            stmt = stmt.where(table.c.indicator_code.in_(indicator_code))
        if year_start is not None:
            stmt = stmt.where(table.c.year >= year_start)
        if year_stop is not None:
            stmt = stmt.where(table.c.year <= year_stop)
        # Query the database and return a data frame
        with self.engine.connect() as conn:
            df = pandas.read_sql_query(stmt, conn)
        return df


class DatabaseWorldBankPostgresql(DatabaseWorldBank):
    """Database using the PostgreSQL engine use the same database for all data sources
    a separate schema for each data source

    The connection URL is defined in an environment variable, for example by
    adding the following to  your .bash_aliases or .bash_rc:

        export BIOTRADE_DATABASE_URL="postgresql://rdb@localhost/biotrade"
    """

    database_url = database_url
    schema = "raw_world_bank"


class DatabaseWorldBankSqlite(DatabaseWorldBank):
    """Database using the SQLite engine"""

    database_url = f"sqlite:///{data_dir}/world_bank/world_bank.db"
    schema = "main"
