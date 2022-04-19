#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the list of products as stored in the database

    >>> from biotrade.comtrade import comtrade
    >>> hs = comtrade.products.hs
    >>> # Paper related products
    >>> print(hs[hs.product_description.str.contains("paper")])

Get the module internal list of products at the HS 2 digit level, with an
indicator of what products are saved when updating data. Where the column
bioeconomy == 1 the product is saved, bioeconomy == 0 the product is discarded.

    >>> products2d = comtrade.products.hs2d
    >>> # Paper related products
    >>> print(products2d[products2d.product_description.str.contains("paper")])

"""
# Third party modules
import pandas


class Products(object):
    """
    Comtrade product list, with additional information.

    Update the list of product codes from Comtrade

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.pump.update_db_parameter()

    """

    def __init__(self, parent):
        self.parent = parent
        self.db = self.parent.db
        self.config_data_dir = self.parent.config_data_dir
        self.hs_csv_file = self.parent.data_dir / "classificationHS.csv"

    @property
    def hs(self, update=False):
        """List of products as stored in the database

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.products.hs

        Refresh the HS product list by downloading it again from the Comtrade API

            >>> comtrade.pump.update_db_parameter()

        The list is downloaded from the Comtrade API and stored into the database.
        """
        df = self.db.select("product")
        return df

    @property
    def short_names(self, update=False):
        """List of short product names as stored in the package config_data folder

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.products.short_names

        """
        file_name = self.config_data_dir / "comtrade_hs_product_short_names.csv"
        df = pandas.read_csv(file_name)
        return df

    @property
    def hs2d(self):
        """The module internal list of products at the 2 digits level.

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.products.hs2d

        For information the internal list is a filtered version of
        the list of Harmonized System (HS) commodities
        originally downloaded from Comtrade with the method:

            >>> comtrade.pump.get_parameter_list("classificationHS.json")

        """
        df = pandas.read_csv(
            self.config_data_dir / "comtrade_hs_2d.csv",
            # Force the product_code column to remain a character column,
            # otherwise str "01" becomes int 1.
            dtype={"product_code": str},
        )
        return df

    def under(self, product_code):
        """Give all products under the given product code"""
