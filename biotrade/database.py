#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Download and store in the database as used when updating the database

    >>> comtrade.pump.append_to_db(cc = "440799")

"""
# First party modules
import logging

# Third party modules
from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import create_engine
from sqlalchemy import Text, BigInteger, Float, UniqueConstraint


class Database:
    """
    Database to store UN comtrade data.
    """

    # Database configuration
    engine = create_engine("postgresql://rdb@localhost/biotrade")
    schema = "raw_comtrade"

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # SQL Alchemy metadata
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine

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

    @property
    def yearly_hs2(self):
        """Yearly Comtrade data at the 2 digit level
        of the Harmonized System product classification

        Alternatively the structure could be automatically loaded with:

            Table('yearly_hs2', self.metadata, autoload_with=self.engine)

        The python code below was automatically generated with

            sqlacodegen --schema raw_comtrade --tables yearly_hs2 postgresql://rdb@localhost/biotrade

        """
        t_yearly_hs2 = Table(
            "yearly_hs2",
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
            schema="raw_comtrade",
        )
        return t_yearly_hs2
