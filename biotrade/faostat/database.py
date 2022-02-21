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
from sqlalchemy import Table, Column, MetaData, or_
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
from sqlalchemy_utils import database_exists, create_database
import pandas

# Internal modules
from biotrade import data_dir, database_url
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
        self.forestry_production = self.describe_production_table(
            name="forestry_production"
        )
        self.forestry_trade = self.describe_trade_table(name="forestry_trade")
        self.crop_production = self.describe_production_table(name="crop_production")
        self.crop_trade = self.describe_trade_table(name="crop_trade")
        self.land_use = self.describe_land_table(name="land_use")
        self.land_cover = self.describe_land_table(name="land_cover")
        self.forest_land = self.describe_land_table(name="forest_land")
        self.tables = {
            "forestry_production": self.forestry_production,
            "forestry_trade": self.forestry_trade,
            "crop_production": self.crop_production,
            "crop_trade": self.crop_trade,
            "land_cover": self.land_cover,
            "land_use": self.land_use,
            "forest_land": self.forest_land,
        }
        # Create tables if they don't exist
        for table in self.tables.values():
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
        """Define the metadata of a table containing trade data."""
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

    def describe_land_table(self, name):
        """Define the metadata of a table containing land use data."""
        table = Table(
            name,
            self.metadata,
            Column("reporter_code", Integer),
            Column("reporter", Text),
            Column("item_code", Integer),
            Column("item", Text),
            Column("element_code", Integer),
            Column("element", Text),
            Column("period", Integer),
            Column("year", Integer),
            Column("source_code", Integer),
            Column("source", Text),
            Column("unit", Text),
            Column("value", Float),
            Column("flag", Text),
            Column("note", Text),
            UniqueConstraint(
                "period",
                "reporter_code",
                "item_code",
                "element_code",
                "unit",
                "flag",
            ),
            schema=self.schema,
        )
        return table

    def read_sql_query(self, stmt):
        """A wrapper around pandas.read_sql_query"""
        return pandas.read_sql_query(stmt, self.engine)

    def select(
        self,
        table,
        reporter=None,
        partner=None,
        product=None,
        reporter_code=None,
        partner_code=None,
        product_code=None,
    ):
        """Select production data for the given arguments

        :param list or str reporter: List of reporter names
        :param list or str partner: List of partner names
        :param list or str product: List of product names
        :param list or str product_code: List of product codes
        :return: A data frame of trade flows

        Note that the search for reporter and partner will be based on perfect
        matches whereas product can be partial matches.

        For example select crop production data for 2 countries

            >>> from biotrade.faostat import faostat
            >>> db = faostat.db
            >>> cp1 = db.select(table="crop_production",
            >>>                 reporter=["Portugal", "Estonia"])

        Select the same data using the country codes instead of the country names

            >>> cp2 = db.select(table="crop_production",
            >>>                 reporter_code=[63, 174])

        Compare values

            >>> index = ["reporter", "product", "element", "year"]
            >>> cp1.sort_values(index, inplace=True, ignore_index=True)
            >>> cp2.sort_values(index, inplace=True, ignore_index=True)
            >>> cp1.equals(cp2)

        Select forestry trade flows data reported by Austria with all partner countries:

            >>> ft_aut = db.select(table="forestry_trade",
            >>>                    reporter=["Austria"])

        Select forestry trade flows data reported by all countries, with
        Austria as a partner country:

            >>> ft_aut_p = db.select(table="forestry_trade",
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

        Select crop production where products contain the word "soy"

            >>> from biotrade.faostat import faostat
            >>> db = faostat.db
            >>> soy_prod = db.select(table="crop_production",
            >>>                      product = "soy")

        Select soybeans and oil soybean production using their product codes

            >>> soy_prod_2 = db.select(table="crop_production",
            >>>                        product_code = [236, 237])

        Compare the two data frame. Sort them first because rows are in a
        different order.

            >>> index = ["reporter", "product", "element", "year"]
            >>> soy_prod_2.sort_values(index, inplace=True, ignore_index=True)
            >>> soy_prod.sort_values(index, inplace=True, ignore_index=True)
            >>> soy_prod.equals(soy_prod_2)

        Select crop trade where products contain the word "soy"

            >>> soy_trade = db.select(table="crop_trade", product = "soy")

        Select crop production where products contain the words in the list

            >>> products_of_interest = ["soy", "palm", "sun", "oil", "rapeseed"]
            >>> veg_oil = db.select(table="crop_production",
            >>>                      product = products_of_interest)
            >>> for e in veg_oil["element"].unique():
            >>>     print(e)
            >>>     print(veg_oil[veg_oil.element == e]["product"].unique())

        """
        table = self.tables[table]
        # Change character or integer arguments to lists suitable for a
        # column.in_() clause or for a list comprehension.
        if isinstance(reporter, str):
            reporter = [reporter]
        if isinstance(partner, str):
            partner = [partner]
        if isinstance(product, str):
            product = [product]
        if isinstance(reporter_code, (int, str)):
            reporter_code = [reporter_code]
        if isinstance(partner_code, (int, str)):
            partner_code = [partner_code]
        if isinstance(product_code, (int, str)):
            product_code = [product_code]
        # Build the select statement
        stmt = table.select()
        if reporter is not None:
            stmt = stmt.where(table.c.reporter.in_(reporter))
        if partner is not None:
            stmt = stmt.where(table.c.partner.in_(partner))
        if product is not None:
            stmt = stmt.where(or_(table.c.product.ilike(f"%{p}%") for p in product))
        if reporter_code is not None:
            stmt = stmt.where(table.c.reporter_code.in_(reporter_code))
        if partner_code is not None:
            stmt = stmt.where(table.c.partner_code.in_(partner_code))
        if product_code is not None:
            stmt = stmt.where(table.c.product_code.in_(product_code))
        # Query the database and return a data frame
        df = pandas.read_sql_query(stmt, self.engine)
        return df


class DatabaseFaostatPostgresql(DatabaseFaostat):
    """Database using the PostgreSQL engine use the same database for all data sources
    a separate schema for each data source

    The connection URL is defined in an environment variable, for example by
    adding the following to  your .bash_aliases or .bash_rc:

        export BIOTRADE_DATABASE_URL="postgresql://rdb@localhost/biotrade"
    """

    database_url = database_url
    schema = "raw_faostat"


class DatabaseFaostatSqlite(DatabaseFaostat):
    """Database using the SQLite engine"""

    database_url = f"sqlite:///{data_dir}/faostat/faostat.db"
    schema = "main"
