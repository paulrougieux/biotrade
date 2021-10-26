#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the module internal list of products groups

    >>> from biotrade.faostat import faostat
    >>> product_groups = faostat.products.groups

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
    def groups(self):
        """FAOSTAT product groups"""
        df = pandas.read_csv(
            self.config_data_dir / "faostat_forestry_product_groups.csv"
        )
        return df
