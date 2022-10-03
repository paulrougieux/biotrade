#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.
"""
# First party modules
import logging

# Third party modules
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
            if not self.engine.dialect.has_schema(self.engine, self.schema):
                self.engine.execute(CreateSchema(self.schema))

        # Describe table metadata
        self.indicator = self.describe_indicator_table(name="indicator")
        self.tables = {
            "indicator": self.indicator,
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
            Column("value", Float),
            UniqueConstraint(
                "reporter_code",
                "indicator_code",
                "year",
            ),
            schema=self.schema,
        )
        return table


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
