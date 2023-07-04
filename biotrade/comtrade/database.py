#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Get yearly Comtrade data at the 2 digit level.
Load the complete table into a pandas data frame.

    >>> import pandas
    >>> from biotrade.comtrade import comtrade
    >>> db = comtrade.db_pgsql
    >>> df = pandas.read_sql_table("yearly_hs2", db.engine, schema="raw_comtrade")

Select monthly oak trade data using the select method defined here.

    >>> from biotrade.comtrade import comtrade
    >>> oak = comtrade.db.select("monthly", product_code="440791")

Select data for the year 2017 using an SQL Alchemy select statement (lower level). Return results
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

Note the main variables of interest are "net_weight" and "trade_value". A query
of the monthly data shows that "quantity" is always empty:

    select * from raw_comtrade.monthly where quantity is not null limit 4;
    # Returns 0 rows

"""
# First party modules
import logging
import pandas
import re

# Third party modules
import comtradeapicall
from sqlalchemy import Integer, Float, Text, UniqueConstraint
from sqlalchemy import Table, Column, MetaData, and_, or_
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema

# Internal modules
from biotrade import data_dir, database_url
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

        # Create the schema if it doesn't exist.
        # Exclude the SQLite engine because there is only a default schemas for that engine.
        # And the SQLite dialect doesn't have a has_schema() method.
        if hasattr(self.engine.dialect, "has_schema") and callable(
            getattr(self.engine.dialect, "has_schema")
        ):
            with self.engine.connect() as conn:
                if not self.engine.dialect.has_schema(conn, self.schema):
                    conn.execute(CreateSchema(self.schema))
                    conn.commit()

        # Describe table metadata and create them if they don't exist
        # Product table
        self.product = self.describe_product_table(name="product")
        # Trade tables with data at the HS 6 digit level
        self.monthly = self.describe_trade_table(name="monthly")
        self.yearly = self.describe_trade_table(name="yearly")
        # Trade table with data at the HS 2 digit level
        self.yearly_hs2 = self.describe_trade_table(name="yearly_hs2")
        self.tables = {
            "product": self.product,
            "monthly": self.monthly,
            "yearly": self.yearly,
            "yearly_hs2": self.yearly_hs2,
        }
        # Create tables if they don't exist
        for table in self.tables.values():
            self.create_if_not_existing(table)

    def describe_product_table(self, name):
        """Define the metadata of a table containing product codes

        :param str table: name of the table to create
        """
        table = Table(
            name,
            self.metadata,
            Column("product_code", Text),
            Column("product_description", Text),
            Column("parent", Text),
            Column("is_leaf", Integer),
            Column("aggregate_level", Integer),
            UniqueConstraint(
                "product_code",
            ),
            schema=self.schema,
        )
        return table

    def describe_trade_table(self, name):
        """Define the metadata of a table containing Comtrade data.

        :param str table: name of the table to create

        The unique constraint is a very important part of the table structure.
        It makes sure that there will be no duplicated flows.

        Alternatively a table metadata structure could be automatically loaded with:

            Table('yearly_hs2', self.metadata, autoload_with=self.engine)

        The python code below was originally generated with:

            sqlacodegen --schema raw_comtrade --tables yearly_hs2 postgresql://rdb@localhost/biotrade

        Note the "product" column is left empty, removed from the data frame
        before it is stored in the database because it would be too large. The
        text description of a product (commodity) is available in the product table.
        """
        table = Table(
            name,
            self.metadata,
            Column("dataset_code", Text),
            Column("classification_search_code", Text),
            Column("classification", Text),
            Column("is_original_classification", Integer),
            Column("frequency", Text),
            Column("ref_date", Integer),
            Column("year", Integer),
            Column("month", Integer),
            Column("period", Integer),
            Column("is_reported", Integer),
            Column("is_aggregated", Integer),
            Column("flow_code", Text),
            Column("reporter_code", Integer, index=True),
            Column("partner_code", Integer, index=True),
            Column("partner_2_code", Integer),
            Column("customs_proc_code", Text),
            Column("mode_of_supply_code", Text),
            Column("mode_of_transport_code", Integer),
            Column("product_code", Text, index=True),
            Column("product_type", Text),
            Column("unit_code", Integer),
            Column("quantity", Float),
            Column("is_quantity_estimated", Integer),
            Column("alt_qty_unit_code", Integer),
            Column("alt_qty", Float),
            Column("is_alt_quantity_estimated", Integer),
            Column("net_weight", Float),
            Column("is_net_weight_estimated", Integer),
            Column("gross_weight", Float),
            Column("is_gross_weight_estimated", Integer),
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
                "flag",
            ),
            schema=self.schema,
        )
        return table

    def read_sql_query(self, stmt):
        """A wrapper around pandas.read_sql_query"""
        with self.engine.connect() as conn:
            df = pandas.read_sql_query(stmt, conn)
        return df

    def check_data_presence(
        self,
        table,
        start_year,
        end_year,
        frequency,
    ):
        """
        Query db table to check if data are present or not for a certain
        time period.

        :param (string) table, name of table to check data
        :param (int) start_year, initial period of the query
        :param (int) end_year, final year of the query
        :param (frequency) frequency, frequency of data
        :return (bool) check_presence, True if data are already inside
            db otherwise False

        For example check if monthly data from 2016 to 2022 are present:
            >>> from biotrade.comtrade import comtrade
            >>> data_check = comtrade.db.check_data_presence(
                    table = "monthly"
                    start_year = 2016,
                    end_year = 2022,
                    frequency = "M",
                )
        """
        check_presence = False
        # Select table
        table = self.tables[table]
        # Initial statement
        stmt = table.select()
        # Reformulate period as monthly or yearly frequency
        if frequency == "M":
            # Start from January of the start year
            start_period = int(str(start_year) + "01")
            # End in December of the end year
            end_period = int(str(end_year) + "12")
        elif frequency == "A":
            start_period = start_year
            end_period = end_year
        # Select data inside time from start period to end period
        if start_period is not None:
            stmt = stmt.where(table.c.period >= start_period)
        if end_period is not None:
            stmt = stmt.where(table.c.period <= end_period)
        # Read data from db to data frame and select the first one
        df = self.read_sql_query(stmt.limit(1))
        # If length of df is not zero, it means that data are inside db
        if len(df):
            check_presence = True
        return check_presence

    def delete_data(
        self,
        table,
        start_period,
        end_period,
    ):
        """
        Database method to delete rows from Comtrade db.

        :param (str) table, name of db table
        :param(int) start_period, from which time to delete rows
        :param(int) end_period, until which time to delete rows

        For example delete data from table "monthly" for January 2016:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.db.delete_data(
                    table = "monthly"
                    start_period = 201601,
                    end_period = 201601,
                )

        """
        # Table of db
        table = self.tables[table]
        assert start_period is not None
        assert end_period is not None
        # Construct delete statement
        stmt = table.delete().where(
            and_(
                table.c.period >= start_period,
                table.c.period <= end_period,
            )
        )
        # Execute delete statement
        stmt.execute()
        self.logger.info(
            "Delete data from database table %s, from %s to %s",
            table,
            start_period,
            end_period,
        )

    def select(
        self,
        table,
        reporter=None,
        partner=None,
        # TODO: use Comtrade.products.hs to enable a query on a product partial mapping
        reporter_code=None,
        partner_code=None,
        product_code=None,
        product_code_start=None,
        flow=None,
        period_start=None,
        period_end=None,
    ):
        """
        Select comtrade trade data for the given arguments

        :param str table: name of the database table to select from
        :param list or str reporter: list of reporter names
        :param list or str partner: list of partner names
        :param list or int reporter_code: list of reporter codes
        :param list or int partner_code: list of partner codes
        :param list or int product_code: list of product codes
            Exact matches of product codes
        :param list or int product_code_start: list of product codes
            Partial matches of all products that start with this code
        :param list or str flow: list of flow, for example "import", "export"
        :param int period_start: filter on start period data
        :param int period_end: filter on end period data
        :return: A data frame of trade flows

        For example select monthly time series of Oak sawnwood trade

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.db.select("monthly", product_code="440791")

        """
        table = self.tables[table]
        # Change character or integer arguments to lists suitable for a
        # column.in_() clause or for a list comprehension.
        if isinstance(reporter, str):
            reporter = [reporter]
        if isinstance(partner, str):
            partner = [partner]
        if isinstance(reporter_code, int):
            reporter_code = [reporter_code]
        if isinstance(partner_code, int):
            partner_code = [partner_code]
        if isinstance(product_code, str):
            product_code = [product_code]
        if isinstance(product_code_start, str):
            product_code_start = [product_code_start]
        if isinstance(flow, str):
            flow = [flow]
        # Build the select statement
        stmt = table.select()
        # Define complementary data and columns to be selected and renamed
        column_dict = {
            "reporterCode": "reporter_code",
            "reporterDesc": "reporter",
            "reporterCodeIsoAlpha3": "reporter_iso",
            "PartnerCode": "partner_code",
            "PartnerDesc": "partner",
            "PartnerCodeIsoAlpha3": "partner_iso",
            "qtyCode": "unit_code",
            "qtyDescription": "unit",
        }
        # reporter data
        reporter_data = comtradeapicall.getReference("reporter")[
            ["reporterCode", "reporterDesc", "reporterCodeIsoAlpha3"]
        ].rename(columns=column_dict)
        # partner data
        partner_data = comtradeapicall.getReference("partner")[
            ["PartnerCode", "PartnerDesc", "PartnerCodeIsoAlpha3"]
        ].rename(columns=column_dict)
        # quantity data
        quantity_data = comtradeapicall.getReference("qtyunit")[
            ["qtyCode", "qtyDescription"]
        ].rename(columns=column_dict)
        # define a compiled regex
        regex_pat = re.compile(r"\W+")
        # flow data
        flow_data = comtradeapicall.getReference("flow")[["id", "text"]].rename(
            columns={"id": "flow_code", "text": "flow"}
        )
        # rename flow column content to snake case using the compiled regex
        flow_data["flow"] = (
            flow_data["flow"].str.replace(regex_pat, "_", regex=True).str.lower()
        )
        # mode of transport data
        mot_data = (
            comtradeapicall.getReference("mot")[["id", "text"]]
            .astype({"id": int})
            .rename(
                columns={
                    "id": "mode_of_transport_code",
                    "text": "mode_of_transport",
                }
            )
        )
        # custom procedure data
        customs_data = comtradeapicall.getReference("customs")[["id", "text"]].rename(
            columns={
                "id": "customs_proc_code",
                "text": "customs",
            }
        )
        if reporter is not None:
            # Obtain codes from reporter data
            reporter = reporter_data[
                reporter_data.reporter.isin(reporter)
            ].reporter_code.tolist()
            # Add already defined reporter codes
            if isinstance(reporter_code, list):
                reporter_code += reporter
            else:
                reporter_code = reporter
        if partner is not None:
            # Obtain codes from partner data
            partner = partner_data[
                partner_data.partner.isin(partner)
            ].partner_code.tolist()
            # Add already defined reporter codes
            if isinstance(partner_code, list):
                partner_code += partner
            else:
                partner_code = partner
        if reporter_code is not None:
            stmt = stmt.where(table.c.reporter_code.in_(reporter_code))
        if partner_code is not None:
            stmt = stmt.where(table.c.partner_code.in_(partner_code))
        if product_code is not None:
            stmt = stmt.where(table.c.product_code.in_(product_code))
        if product_code_start is not None:
            stmt = stmt.where(
                or_(table.c.product_code.ilike(f"{c}%") for c in product_code_start)
            )
        if flow is not None:
            # Rename flow list to snake case using the compiled regex
            flow = [regex_pat.sub("_", item).lower() for item in flow]
            # Obtain codes from flow data
            flow_code = flow_data[flow_data.flow.isin(flow)].flow_code.tolist()
            stmt = stmt.where(table.c.flow_code.in_(flow_code))
        if period_start is not None:
            stmt = stmt.where(table.c.period >= period_start)
        if period_end is not None:
            stmt = stmt.where(table.c.period <= period_end)
        # Query the database and return a data frame
        df = self.read_sql_query(stmt)
        # Merge with complementary data
        df = (
            df.merge(reporter_data, on="reporter_code", how="left")
            .merge(partner_data, on="partner_code", how="left")
            .merge(quantity_data, on="unit_code", how="left")
            .merge(flow_data, on="flow_code", how="left")
            .merge(mot_data, on="mode_of_transport_code", how="left")
            .merge(customs_data, on="customs_proc_code", how="left")
        )
        return df

    def select_long(self, *args, **kwargs):
        """Same as select but in long format with an element column and where
        the empty columns have been removed.

        Example:

            >>> from biotrade.comtrade import comtrade
            >>> swd = comtrade.db.select_long("monthly", product_code="440710")

        """
        df = self.select(*args, **kwargs)
        assert len(df) > 0
        # Remove empty columns
        not_empty = [col for col in df.columns if not all(df[col].isna())]
        df = df[not_empty]
        # This can be removed once this is dealt with in comtrade/pump.py
        # Rename column content to snake case using a compiled regex
        regex_pat = re.compile(r"\W+")
        if "flow" in df.columns:
            df["flow"] = df["flow"].str.replace(regex_pat, "_", regex=True).str.lower()
            # Remove the plural "s"
            df["flow"] = df["flow"].str.replace("s", "", regex=True)
        # TODO: Change period to a date time object
        # Reshape to long format
        value_cols = ["net_weight", "trade_value"]
        index = set(df.columns) - set(value_cols)
        df_long = df.melt(
            id_vars=list(index),
            value_vars=value_cols,
            var_name="element",
            value_name="value",
        )
        # Remove NA values for the weight
        df_long = df_long[~df_long.value.isna()]

        # Fix an issue with element appearing as a list in R
        # I don't understand why the element column is of sqlalchemy.sql.elements nature?
        # R> flatten(swd_con$element[1])
        # Error in `stop_bad_type()`:
        # ! Element 1 of `.x` must be a vector, not a `sqlalchemy.sql.elements.quoted_name
        # /sqlalchemy.util.langhelpers.MemoizedSlots/python.builtin.str/python.builtin.obj
        # ect` object
        df_long = df_long.reset_index(drop=True)
        df_long["element"] = df_long["element"].str.strip()
        return df_long


class DatabaseComtradePostgresql(DatabaseComtrade):
    """Database using the PostgreSQL engine use the same database for all data sources
    a separate schema for each data source

    The connection URL is defined in an environment variable, for example by
    adding the following to  your .bash_aliases or .bash_rc:

        export BIOTRADE_DATABASE_URL="postgresql://rdb@localhost/biotrade"
    """

    database_url = database_url
    schema = "raw_comtrade"


class DatabaseComtradeSqlite(DatabaseComtrade):
    """Database using the SQLite engine, use a separate database for each data source"""

    database_url = f"sqlite:///{data_dir}/comtrade/comtrade.db"
    schema = "main"
