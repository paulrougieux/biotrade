#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Get the module internal list of products groups

    >>> from biotrade.faostat import faostat
    >>> print(faostat.products.forestry_production_groups)
    >>> print(faostat.products.forestry_trade_groups)

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
    def forestry_production_groups(self):
        """FAOSTAT forestry production groups"""
        df = pandas.read_csv(
            self.config_data_dir / "faostat_forestry_production_groups.csv"
        )
        return df

    @property
    def forestry_trade_groups(self):
        """FAOSTAT forestry trade groups"""
        df = pandas.read_csv(self.config_data_dir / "faostat_forestry_trade_groups.csv")
        return df
