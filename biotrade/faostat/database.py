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
import pandas

# Internal modules
from biotrade import data_dir
from biotrade.common.database import Database


class DatabaseFaostat(Database):
    """
    Database to store UN Comtrade data.

    Select forestry production data for Italy

        >>> from biotrade.faostat import faostat
        >>> db = faostat.db_sqlite
        >>> fp_ita = db.select(table="forestry_production",
        >>>                    reporter=["Italy"])

    More examples are available under the documentation of the `select` method below.

    Select data for Italy using an SQL Alchemy select statement. Return results
    using a pandas data frame:

        >>> import pandas
        >>> db = faostat.db_sqlite
        >>> reporter = db.forestry_production.columns.get("reporter")
        >>> statement = db.forestry_production.select().where(reporter == "Italy")
        >>> fp_it = pandas.read_sql_query(statement, db.engine)

    Use a SQL select statement directly

        >>> query = "SELECT * FROM forestry_production WHERE reporter = 'Italy'"
        >>> fp_it = pandas.read_sql_query(query, db.engine)
        >>> query = "SELECT * FROM forestry_trade WHERE reporter = 'Italy'"
        >>> ft_it = pandas.read_sql_query(query, db.engine)

    Lower level method to store forestry production data in the database. This
    should be taken care of by faostat.pump:

        >>> fp = faostat.pump.forestry_production
        >>> faostat.db_sqlite.append(fp, "forestry_production")
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
        # Describe table metadata
        self.forestry_production = self.describe_production_table(
            name="forestry_production"
        )
        self.forestry_trade = self.describe_trade_table(name="forestry_trade")
        self.crop_production = self.describe_production_table(name="crop_production")
        self.crop_trade = self.describe_trade_table(name="crop_trade")
        self.table = {
            "forestry_production": self.forestry_production,
            "forestry_trade": self.forestry_trade,
            "crop_production": self.crop_production,
            "crop_trade": self.crop_trade,
        }
        # Create tables if they don't exist
        for table in self.table.values():
            self.create_if_not_existing(table)

    def describe_production_table(self, name):
        """Define the metadata of a table containing production data.

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

            cd ~/repos/biotrade_data/faostat/ && ls -l faostat.db
            -rw-r--r-- 1 paul paul 99246080 Nov  2 05:34 faostat.db
            -rw-r--r-- 1 paul paul 251060224 Nov  2 05:43 faostat.db

        As a result of this little experiment, it is probably not worth using
        product and country tables with joins on indexes, because the gain will
        not be very large.
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

    def describe_trade_table(self, name):
        """Define the metadata of a table containing production data."""
        table = Table(
            name,
            self.metadata,
            Column("reporter_code", Integer),
            Column("reporter", Text),
            Column("partner_code", Integer),
            Column("partner", Text),
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
                "partner_code",
                "product_code",
                "element_code",
                "unit",
                "flag",
            ),
            schema=self.schema,
        )
        return table

    def select(self, table, reporter=None, partner=None):
        """Select production data for the given arguments

        :param list or str reporter: List of reporter names
        :param list or str partner: List of partner names
        :return: A data frame of trade flows

        For example select crop production data for 2 countries

        >>> from biotrade.faostat import faostat
        >>> db = faostat.db_sqlite
        >>> cp2 = db.select(table="crop_production",
        >>>                 reporter=["Portugal", "Estonia"])

        Select forestry trade flows data reported by all countries, with
        Austria as a partner country:

        >>> ft_aut = db.select(table="forestry_trade",
        >>>                    partner=["Austria"])

        Select crop trade flows reported by the Netherlands where Brazil was a
        partner

        >>> ct_nel_bra = db.select(table="crop_trade",
        >>>                        reporter="Netherlands",
        >>>                        partner="Brazil")

        Select the mirror flows reported by Brazil, where the Netherlands was a partner

        >>> ct_bra_bel = db.select(table="crop_trade",
        >>>                        reporter="Brazil",
        >>>                        partner="Netherlands")

        """
        table = self.table[table]
        # Change character variables to a list suitable for a column.in_() clause
        if isinstance(reporter, str):
            reporter = [reporter]
        if isinstance(partner, str):
            partner = [partner]
        stmt = table.select()
        if reporter is not None:
            stmt = stmt.where(table.c.reporter.in_(reporter))
        if partner is not None:
            stmt = stmt.where(table.c.partner.in_(partner))
        df = pandas.read_sql_query(stmt, self.engine)
        return df


class DatabaseFaostatSqlite(DatabaseFaostat):
    """Database using the SQLite engine"""

    database_url = f"sqlite:///{data_dir}/faostat/faostat.db"
    schema = "main"
