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
    """

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Directories #
        self.config_data_dir = self.parent.config_data_dir

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
