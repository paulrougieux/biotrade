#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Roberto Pilli

Usage:

    >>> from biotrade.hwp import compute_ipcc_tier_2
    >>> from biotrade.common.country import Country
    >>> aut = Country("Austria")
    >>> aut_fp = aut.faostat.forestry_production

    >>> compute_ipcc_tier_2(aut_fp)


TODO: attach these function to the country object so that they can be called using

aut.hwp.compute_ipcc_tier2

"""
# Third party modules
import pandas

# Internal modules
from biotrade import module_dir
from biotrade.faostat.country import FaostatCountry

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


def production_wide(df):
    """Reshape wood production data to wide format for computation

    >>> from biotrade.common.country import Country
    >>> from biotrade.hwp import compute_ipcc_tier_2
    >>> aut = Country("Austria")
    >>> aut_fp = aut.faostat.forestry_production
    >>> production_wide(aut_fp)

    """


def compute_ipcc_tier_2(df):
    """Compute ipcc tier 2 from the given data frame"""
    df

    @property
    def faostat(self):
        """FAOSTAT data for one country"""
        return FaostatCountry(self)
