#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the product mapping

Usage:
    >>> from biotrade.common.merge_comtrade_faostat import merge_comtrade_faostat

"""

from biotrade.faostat import faostat
from biotrade.comtrade import comtrade
from biotrade.common.products import comtrade_faostat_mapping


def merge_faostat_comtrade(faostat_table, comtrade_table, faostat_code):
    """ "Merge faostat and comtrade bilateral trade

    :param faostat_table str: name of the faostat table to select from
    :param comtrade_table str: name of the comtrade table to select from
    :param list faostat_code: list of faostat code to be loaded

    The function does the following:

        1. Load FAOSTAT bilateral trade data for the given codes
        2. Find the corresponding Comtrade codes using the mapping table
        3. Load Comtrade monthly data for the corresponding codes
        4. Aggregate Comtrade to yearly and for the last data point extrapolate
             to current year based on the last 12 months
        5. Merge faostat and comtrade data based on an index
           reporter, partner, year, index

    Usage:

        >>> from biotrade.common.merge_comtrade_faostat import merge_comtrade_faostat
        >>> merge_faostat_comtrade(faostat_table="forestry_trade",
        >>>                        comtrade_table="monthly",
        >>>                        faostat_code = [1632, 1633])

    """
    # 1. Load FAOSTAT bilateral trade data for the given codes
    df_faostat = faostat.db.select(faostat_table, product_code=faostat_code)
    # 2. Find the corresponding Comtrade codes using the mapping table
    product_mapping = comtrade_faostat_mapping.query("faostat_code in @faostat_code")
    # 3. Load Comtrade monthly data for the corresponding codes
    df_comtrade = comtrade.db.select(
        comtrade_table, product_code=product_mapping["comtrade_code"]
    )
    # Replace product codes by the FAOSTAT product codes
    product_dict = product_mapping.set_index("comtrade_code").to_dict()["faostat_code"]
    df_comtrade["product_code"] = df_comtrade["product_code"].replace(product_dict)
    # Replace country codes by the FAOSTAT country codes
    country_mapping = faostat.country_groups.df
    # country_dict =

    # 4. Aggregate Comtrade to yearly and using the faostat group

    # And for the last data point extrapolate to current year
    # based on the last 12 months present in the data
