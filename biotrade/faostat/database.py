#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.
"""

# Third party modules
from sqlalchemy import Integer, Float, SmallInteger, Text, UniqueConstraint
from sqlalchemy import Table, Column, MetaData, or_
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.sql import func
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
        >>> db = faostat.db
        >>> fp_ita = db.select(table="forestry_production",
        >>>                    reporter=["Italy"])

    More examples are available under the documentation of the `select` method below.

    Select data for Italy using an SQL Alchemy select statement. Return results
    using a pandas data frame:

        >>> import pandas
        >>> from biotrade.faostat import faostat
        >>> db = faostat.db
        >>> reporter = db.forestry_production.columns.get("reporter")
        >>> statement = db.forestry_production.select().where(reporter == "Italy")
        >>> with faostat.db.engine.connect() as conn:
        >>>     fp_it = pandas.read_sql_query(statement, conn)

    Use a SQL select statement directly

        >>> from sqlalchemy import text
        >>> # In case you are using an SQLite database
        >>> schema = "main"
        >>> # In case you are using a PostGreSQL database
        >>> schema = "raw_faostat"
        >>> query = text(f"SELECT * FROM {schema}.forestry_production WHERE reporter = 'Italy'")
        >>> with faostat.db.engine.connect() as conn:
        >>>     fp_it = pandas.read_sql_query(query, conn)
        >>> query = text(f"SELECT * FROM {schema}.forestry_trade  WHERE reporter = 'Italy'")
        >>> with faostat.db.engine.connect() as conn:
        >>>     ft_it = pandas.read_sql_query(query, conn)

    Database update methods are in the faostat.pump object.

    """

    # To be overwritten by the children
    database_url = None
    schema = None

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        self.logger = self.parent.logger
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
        self.forestry_production = self.describe_production_table(
            name="forestry_production"
        )
        self.country = self.describe_country_table(name="country")
        self.crop_production = self.describe_production_table(name="crop_production")
        self.crop_trade = self.describe_trade_table(name="crop_trade")
        self.food_balance = self.describe_food_balance_table(name="food_balance")
        self.forest_land = self.describe_land_table(name="forest_land")
        self.forestry_trade = self.describe_trade_table(name="forestry_trade")
        self.land_cover = self.describe_land_table(name="land_cover")
        self.land_use = self.describe_land_table(name="land_use")
        self.tables = {
            "country": self.country,
            "crop_production": self.crop_production,
            "crop_trade": self.crop_trade,
            "food_balance": self.food_balance,
            "forest_land": self.forest_land,
            "forestry_production": self.forestry_production,
            "forestry_trade": self.forestry_trade,
            "land_cover": self.land_cover,
            "land_use": self.land_use,
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
            >>> fp = faostat.pump.read_df("forestry_production")
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

    def describe_food_balance_table(self, name):
        """Define the metadata for the Supply and Utilisation Accounts table
        containing data about food balance, production and trade
        """
        table = Table(
            name,
            self.metadata,
            Column("reporter_code", Integer),
            Column("reporter", Text),
            Column("product_code", Text),
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

    def describe_country_table(self, name):
        """Define the metadata of a table containing country data."""
        table = Table(
            name,
            self.metadata,
            Column("continent_code", Integer),
            Column("continent", Text),
            Column("sub_continent_code", Integer),
            Column("sub_continent", Text),
            Column("eu27", SmallInteger),
            Column("country_code", Integer),
            Column("country_name", Text),
            UniqueConstraint(
                "country_code",
            ),
            schema=self.schema,
        )
        return table

    def read_sql_query(self, stmt):
        """A wrapper around pandas.read_sql_query"""
        with self.engine.connect() as conn:
            df = pandas.read_sql_query(stmt, conn)
        return df

    def select(
        self,
        table,
        reporter=None,
        partner=None,
        product=None,
        element=None,
        reporter_code=None,
        partner_code=None,
        product_code=None,
        period_start=None,
        period_end=None,
    ):
        """Select faostat data for the given arguments

        :param str table: name of the database table to select from
        :param list or str reporter: list of reporter names
        :param list or str partner: list of partner names
        :param list or str product: list of product names
        :param list or str element: list of element names
        :param list or int or str reporter_code: list of reporter codes
        :param list or int or str partner_code: list of partner codes
        :param list or int or str product_code: list of product codes
        :param int period_start: integer for filtering data from start year
        :param int period_end: integer for filtering data up to end year
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

            >>> ft_aut = faostat.db.select(table="forestry_trade",
            >>>                            reporter=["Austria"])

        Select forestry trade flows data reported by all countries, with
        Austria as a partner country:

            >>> ft_aut_p = faostat.db.select(table="forestry_trade",
            >>>                              partner=["Austria"])

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

        Select crop trade where products contain the words in the list

            >>> veg_oil = db.select(table="crop_trade",
            >>>                     product = products_of_interest)

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
        if isinstance(element, str):
            element = [element]
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
        if element is not None:
            stmt = stmt.where(table.c.element.in_(element))
        if reporter_code is not None:
            stmt = stmt.where(table.c.reporter_code.in_(reporter_code))
        if partner_code is not None:
            stmt = stmt.where(table.c.partner_code.in_(partner_code))
        if product_code is not None:
            stmt = stmt.where(table.c.product_code.in_(product_code))
        if period_start is not None:
            stmt = stmt.where(table.c.period >= period_start)
        if period_end is not None:
            stmt = stmt.where(table.c.period <= period_end)
        # Query the database and return a data frame
        df = self.read_sql_query(stmt)
        return df

    def agg_reporter_partner_eu_row(
        self,
        table,
        product_code,
    ):
        """
        Aggregate EU27 and ROW both on the reporter and eventually partner side
        as regard faostat tables, using sqlalchemy statements for specific
        product code list

        :param (list) product_code, list of integer product codes
        :return (DataFrame) df, containing the aggregations for the product codes

        As example, query the crop trade table and aggregate EU27 and ROW data
        both reporter and partner side for product code equal to 236,
        corresponding to soybean commodity

            >>> from biotrade.faostat import faostat
            >>> db = faostat.db
            >>> df = db.agg_reporter_partner_eu_row("crop_trade", product_code=[236])

        """
        # Table to select of raw_faostat schema
        table = self.tables[table]
        # Select all columns where product code is specified by function argument product_code
        # If product code list is None return an error
        table_stmt = table.select()
        if product_code is None:
            raise ValueError("Specify product code list")
        else:
            table_stmt = table_stmt.where(table.c.product_code.in_(product_code))
        # Render the selection queryable again and rename it "table_selection"
        table_selection = table_stmt.subquery().alias("table_selection")
        # Select country table of raw_faostat_schema
        country_table = self.tables["country"]
        # Select two columns of country table and rename them for reporter selection
        reporter_stmt = select(
            [
                country_table.c.country_code.label("reporter_code"),
                country_table.c.eu27.label("reporter_eu27"),
            ]
        )
        # Render the selection queryable again and rename it "reporter_selection"
        reporter_selection = reporter_stmt.subquery().alias("reporter_selection")
        # Outer left join on reporter side
        join_stmt = table_selection.join(
            reporter_selection,
            table_selection.c.reporter_code == reporter_selection.c.reporter_code,
            isouter=True,
        )
        # If the selected table contains the partner column, the aggregation EU/ROW is performed also from this side
        if "partner" in table.c.keys():
            # Select two columns of country table and rename them for partner selection
            partner_stmt = select(
                [
                    country_table.c.country_code.label("partner_code"),
                    country_table.c.eu27.label("partner_eu27"),
                ]
            )
            # Render the selection queryable again and rename it "partner_selection"
            partner_selection = partner_stmt.subquery().alias("partner_selection")
            # Second outer left join on partner side
            join_stmt = join_stmt.join(
                partner_selection,
                table_selection.c.partner_code == partner_selection.c.partner_code,
                isouter=True,
            )
        # Select the join statement and rename it "join_selection"
        join_selection = select(join_stmt).alias("join_selection")
        # Columns to retain from the join selection
        join_columns = [
            join_selection.c.product_code,
            join_selection.c.product,
            join_selection.c.element,
            join_selection.c.unit,
            join_selection.c.year,
            join_selection.c.reporter_eu27,
            func.sum(join_selection.c.value).label("value"),
        ]
        # Column to retain from the join selection for partner side
        if "partner" in table.c.keys():
            join_columns.insert(-1, join_selection.c.partner_eu27)
        # Select column subset of join selection to return for dataframe
        stmt = select(join_columns)
        # Delete from column list the column not used for the groupby
        del join_columns[-1]
        # Aggregate by columns the join selection
        stmt = stmt.group_by(*join_columns)
        # Return the dataframe from the query to db
        df = self.read_sql_query(stmt)
        return df

    def extract_product_names_codes(self, table_list):
        """
        Extract product names and product codes from tables of
        Faostat db. Duplicates are dropped out.

        :param (table_list) tables, list of Faostat table names
        :return (DataFrame) faostat_products, containing product names and codes

        For example, obtain product codes and names from crop_trade and
        crop_production Faostat tables

        >>> from biotrade.faostat import faostat
        >>> table_list = ["crop_production", "crop_trade"]
        >>> faostat_products = faostat.db.extract_product_names_codes(table_list)

        """
        faostat_products = pandas.DataFrame()
        # Loop over tables
        for table in table_list:
            # Select faostat table
            faostat_table = self.tables[table]
            # Select product names and codes from Faostat table
            stmt = (
                faostat_table.select()
                .with_only_columns(
                    [faostat_table.c.product_code, faostat_table.c.product]
                )
                .distinct(faostat_table.c.product_code, faostat_table.c.product)
            )
            # Retrieve dataset
            table_products = self.read_sql_query(stmt)
            faostat_products = pandas.concat(
                [faostat_products, table_products], ignore_index=True
            )
        # Drop duplicates
        faostat_products.drop_duplicates(
            subset="product_code", ignore_index=True, inplace=True
        )
        return faostat_products


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
