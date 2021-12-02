#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC Biomass Project.
Unit D1 Bioeconomy.
"""

# Third party modules
import pandas

# Internal modules
from biotrade import module_dir

PRODUCTS = pandas.read_csv(
    module_dir / "config_data/faostat_forestry_production_short_names.csv"
)
SELECTED_PRODUCTS = [
    "ir_c",
    "ir_nc",
    "pp",
    "sw",
    "sw_c",
    "sw_nc",
    "wp",
    "fw_c",
    "fw_nc",
]
PRODUCTS = PRODUCTS[PRODUCTS.product_short.isin(SELECTED_PRODUCTS)].copy()


class HwpCountry:
    """
    This object gives access to the Harvested Wood Products data for one country.

        >>> from biotrade.common.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.hwp.production_wide

    """

    def __repr__(self):
        return '%s object name "%s"' % (self.__class__, self.country_name)

    def __init__(self, parent):
        """Get the country name from this object's parent class."""
        self.country_name = parent.country_name
        self.faostat = parent.faostat

    @property
    def production_wide(self):
        """Reshape wood production data to wide format

        >>> from biotrade.common.country import Country
        >>> ukr = Country("Ukraine")
        >>> ukr.hwp.production_wide

        """
        df = self.faostat.forestry_production

        return df

    def trade_wide(self):
        """Reshape wood trade data to wide format"""
