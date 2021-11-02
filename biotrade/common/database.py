#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Parent object to faostat.database.py and comtrade.database.py

"""


class Database:
    """
    Database parent class

    Keep methods and parameters commons to all databases in this file.
    """

    # To be overwritten by the children
    database_url = None
    schema = None
    engine = None
    inspector = None
    logger = None

    def write_df(self, df, table, if_exists="fail", drop_description=True):
        """Store a data frame inside a given database table"""
        # Drop the lengthy product description
        if drop_description and "product_description" in df.columns:
            df.drop(columns=["product_description"], inplace=True)
        df.to_sql(
            name=table,
            con=self.engine,
            schema=self.schema,
            if_exists=if_exists,
            index=False,
        )
        self.logger.info("Wrote %s rows to the database table %s", len(df), table)

    def append(self, *args, if_exists="append", **kwargs):
        """Store a data frame inside a given database table

        Legacy version for compatibilty.
        TODO: refactor this
        """
        self.write_df(if_exists=if_exists, *args, **kwargs)

    def create_if_not_existing(self, table):
        """Create a table in the database if it doesn't exist already

        table sqlalchemy.sql.schema.Table instance description of a table structure
        """
        #  Create the table if it doesn't exist
        if not self.inspector.has_table(table.name, schema=self.schema):
            table.create()
            self.logger.info("Created table %s in schema %s.", table.name, self.schema)
        return table
