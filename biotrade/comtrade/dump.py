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

    def store2d(
        self,
        table,
        product_code=None,
    ):
        """Dump rows with product code starting with the given product code at the 2 digit level

        If a list of product codes is given, store one file for each 2 digit code.

        :param table_name, str defining the comtrade table
        ("monthly", "yearly", "yearly_hs2")
        :param int product_code

        """
        if isinstance(product_code, (int, str)):
            product_code = [product_code]

    def load(self, file_name):
        """Load a dump into the database"""
