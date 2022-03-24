#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.


"""
# Built-in modules

# Third party modules
import logging
import pandas

# Internal modules


class Dump:
    """
    Dump comtrade data to compressed PostgreSQL dump files.


    For example dump forest related products (on a server)

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.dump.store2d(table="monthly", 44)

    By default it will create a file name "monthly_44.dump.gz" in the biotrade_data/ repository
    The location of that repository is defined by the environment variable $BIOTRADE_DATA.

    Load the dump into a database (on a laptop)

        >>> comtrade.dump.load(file_name="")

    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        self.db = self.parent.db
        # Location of the data
        self.data_dir = self.parent.data_dir / "dump"
        if not self.data_dir.exists():
            self.data_dir.mkdir()

    def store_2d_csv(
        self,
        table,
        product_code=None,
    ):
        """Dump all rows where the product code starts with the given product code at the 2 digit level

        :param table_name, str defining the comtrade table
            ("monthly", "yearly", "yearly_hs2")
        :param int product_code 2 digit product code

        If a list of product codes is given, store one file for each 2 digit code.

        For example save monthly "Animal Or Vegetable Fats And Oil" data which starts with 15:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.dump.store_2d_csv("monthly", 15)

        """
        # If product_code is a single item, make it a list
        if isinstance(product_code, (int, str)):
            product_code = [product_code]
        # SQL Alchemy table object
        table = self.db.tables[table]
        # Loop on products
        for this_code in product_code:
            # Find all products that start with the given code
            stmt = table.select()
            stmt = stmt.where(table.c.product_code.ilike(f"{this_code}%"))
            df = pandas.read_sql_query(stmt, self.db.engine)
            # Store the data frame to a dump file
            file_name = f"{table}_{this_code}.csv".replace(".", "_", 1)
            file_path = self.data_dir / (file_name + ".gz")
            self.logger.info("Storing data to:\n %s", file_path)
            df.to_csv(file_path, index=False, compression="gzip")

    def load_2d_csv(self, table, file_path):
        """Load a dump into the given database table

        :param (str) table, name of the database table
        :param (str or file object) file_path, path to the file from which to save the data

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> file_path = comtrade.dump.data_dir / "raw_comtrade_monthly_15.csv.gz"
            >>> comtrade.dump.load_2d_csv("monthly", file_path)

        """
        df = pandas.read_csv(file_path)
        self.db.append(df, table)
