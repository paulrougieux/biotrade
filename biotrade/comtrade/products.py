#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the list of products from the Comtrade API

    >>> from biotrade.comtrade import comtrade
    >>> hs = comtrade.pump.get_parameter_list("classificationHS.json")

Get the module internal list of products at the HS 2 digit level

    >>> products2d = comtrade.products.hs2d
    >>> # Paper related products
    >>> print(products2d[products2d.text.str.contains("paper")])

"""
# Third party modules
import pandas


class Products(object):
    """
    Comtrade product list, with additional information.

    Update the list of product codes from Comtrade

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.pump.update_product_hs()



    """

    def __init__(self, parent):
        self.parent = parent
        self.config_data_dir = self.parent.config_data_dir
        self.hs_csv_file = self.parent.data_dir / "classificationHS.csv"

    @property
    def hs(self, update=False):
        """List of products as stored in the database

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.products.hs

        Refresh the HS product list by downloading it again from the Comtrade API

            >>> comtrade.products.hs_csv_file.unlink()
            >>> comtrade.products.hs

        The list is downloaded only once from the Comtrade API and cached for
        the duration of the session.
        """
        # TODO: load the data frame from the database instead with
        # df = self.db.select("product")
        if not self.hs_csv_file.exists():
            df = self.parent.pump.get_parameter_list("classificationHS.json")
            df.to_csv(self.hs_csv_file, index=False)
        else:
            df = pandas.read_csv(
                self.hs_csv_file,
                # Force the id column to remain a character column,
                # otherwise str "01" becomes int 1.
                dtype={"id": str},
            )
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
            # Force the id column to remain a character column,
            # otherwise str "01" becomes int 1.
            dtype={"id": str},
        )
        return df

    def under(self, product_code):
        """Give all products under the given product code"""
