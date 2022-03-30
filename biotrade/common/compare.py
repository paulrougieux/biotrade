#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

Get the product mapping

Usage:

    >>> from biotrade.common.compare import merge_comtrade_faostat

"""

import warnings
from biotrade.faostat import faostat
from biotrade.comtrade import comtrade
from biotrade.common.products import comtrade_faostat_mapping


def replace_exclusively(df, code_column, code_dict, na_value=-1):
    """Replace codes with the dict and all other values with na_value

    :param series pandas series of product or country codes to replace
    :param dict dict of key value replacement pairs sent to the
        pandas.core.series.replace method.

    Not available values (na_value) are represented as -1 by default, because it is
    easier to handle as grouping variable.

    :return a panda series with replaced values
    """
    selector = df[code_column].isin(code_dict.keys())
    # Extract the corresponding names to get a nicer warning when available
    name_column = code_column.replace("_code", "")
    if name_column in df.columns:
        missing = df.loc[~selector, [code_column, name_column]].drop_duplicates()
    else:
        missing = df.loc[~selector, [code_column]].drop_duplicates().drop_duplicates()
    warnings.warn(
        f"The following codes are present in {code_column} but missing "
        f"from the mapping dictionary:\n{missing}"
    )
    # Add missing keys to the dictionary and map them to the na_value
    code_dict = code_dict.copy()
    code_dict.update(dict(zip(missing[code_column], [na_value] * len(missing))))
    return df[code_column].replace(code_dict)


def transform_comtrade_using_faostat_codes(comtrade_table, faostat_code):
    """Load and transform a Comtrade data table to use FAOSTAT product and country codes

    :param comtrade_table str: name of the comtrade table to select from
    :param list faostat_code: list of faostat codes to be loaded

    The function makes Comtrade monthly data available with faostat codes. It
    should also work on Comtrade yearly data (untested).
    It does the following:

        1. Find the corresponding Comtrade codes using the mapping table
        2. Load comtrade data
        3. Load Comtrade monthly data for the corresponding codes
        4. Replace product codes and country codes by the FAOSTAT codes.
        5. Reshape the Comtrade data to a longer format similar
            to FAOSTAT.
        6. Aggregate Comtrade flows to the FAOSTAT product codes

    Example use:

        >>> from biotrade.common.compare import transform_comtrade_using_faostat_codes

    """
    # 1. Find the corresponding Comtrade codes using the mapping table
    product_mapping = comtrade_faostat_mapping.query("faostat_code in @faostat_code")
    # 2. Load Comtrade monthly data for the corresponding codes
    df_wide = comtrade.db.select(
        comtrade_table, product_code=product_mapping["comtrade_code"]
    )
    # Replace Comtrade product codes by the FAOSTAT product codes
    product_dict = product_mapping.set_index("comtrade_code").to_dict()["faostat_code"]
    df_wide["product_code"] = df_wide["product_code"].replace(product_dict)
    df_wide["product_code"] = replace_exclusively(df_wide, "product_code", product_dict)
    # Replace Comtrade country codes by the FAOSTAT country codes
    country_mapping = faostat.country_groups.df[["faost_code", "un_code"]]
    country_dict = country_mapping.set_index("un_code").to_dict()["faost_code"]
    df_wide["reporter_code"] = replace_exclusively(
        df_wide, "reporter_code", country_dict
    )
    df_wide["partner_code"] = replace_exclusively(df_wide, "partner_code", country_dict)
    # Reshape Comtrade to long format
    index = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code",
        "period",
        "year",
        "unit",
        "flag",
        "flow_code",
        "flow",
    ]
    df = df_wide.melt(
        id_vars=index,
        # We loose the quantity column, but it is not available
        # in the monthly data see comtrade/database.py
        value_vars=["net_weight", "trade_value"],
        var_name="element",
        value_name="value",
    )
    # Add units
    df["unit"] = df["element"].replace({"net_weight": "kg", "trade_value": "usd"})
    # A query of the Comtrade monthly data shows that the "quantity" column is always empty
    #     select * from raw_comtrade.monthly where quantity is not null limit 4;
    # Returns 0 rows
    # Rename element="net_weight" to "quantity" to be similar to FAOSTAT
    df["element"] = (
        df["flow"].str.lower().replace("-", "_", regex=True) + "_" + df["element"]
    )
    df["element"] = (
        df["element"]
        .replace("s_trade", "", regex=True)
        .replace("s_net_weight", "_quantity", regex=True)
    )
    # The "flag" column is kept out of the index so lines with different flags can be aggregated
    index = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code",
        "period",
        "year",
        "unit",
        "element",
    ]
    df_agg = df.groupby(index)["value"].agg(sum).reset_index()
    return df_agg


def merge_faostat_comtrade(faostat_table, comtrade_table, faostat_code):
    """ "Merge faostat and comtrade bilateral trade

    :param faostat_table str: name of the faostat table to select from
    :param comtrade_table str: name of the comtrade table to select from
    :param list faostat_code: list of faostat codes to be loaded

    The function does the following:

        1. Load FAOSTAT bilateral trade data for the given codes
        2. Load Comtrade with faostat codes using the method above
            transform_comtrade_using_faostat_codes
        3. Aggregate Comtrade to yearly
        4. For the last data point extrapolate
             to the current year based on values from the last 12 months
        5. Concatenate FAOSTAT and Comtrade data

    Usage:

        >>> from biotrade.common.compare import merge_comtrade_faostat
        >>> merge_faostat_comtrade(faostat_table="forestry_trade",
        >>>                        comtrade_table="monthly",
        >>>                        faostat_code = [1632, 1633])

    """
    # 1. Load FAOSTAT bilateral trade data for the given codes
    df_faostat = faostat.db.select(faostat_table, product_code=faostat_code)
    # 2. Load Comtrade biltaral trade data for the given codes
    df_comtrade = transform_comtrade_using_faostat_codes(
        comtrade_table=comtrade_table, faostat_code=faostat_code
    )
    # 4. Aggregate Comtrade to yearly using the faostat product groups

    # And for the last data point extrapolate to current year
    # based on the last 12 months present in the data
