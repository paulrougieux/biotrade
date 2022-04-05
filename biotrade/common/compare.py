#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

Get a data frame with both FAOSTAT and Comtrade data, where Comtrade product
and country codes have been converted to their equivalent FAOSTAT codes.

    >>> from biotrade.common.compare import merge_faostat_comtrade
    >>> swd = merge_faostat_comtrade(faostat_table="forestry_trade",
    >>>                              comtrade_table="monthly",
    >>>                              faostat_code = [1632, 1633])

"""

import warnings
import pandas
from biotrade.faostat import faostat
from biotrade.comtrade import comtrade
from biotrade.common.products import comtrade_faostat_mapping


def replace_exclusively(df, code_column, code_dict, na_value=-1):
    """Replace codes with the dict and all other values with na_value

    :param series pandas series of product or country codes to replace
    :param dict dict of key value replacement pairs sent to the
        pandas.core.series.replace method.
    :return a panda series with replaced values

    Not available values (na_value) are represented as -1 by default, because it is
    easier to handle as grouping variable.

    """
    selector = df[code_column].isin(code_dict.keys())
    # Extract the corresponding names to get a nicer warning when available
    name_column = code_column.replace("_code", "")
    if name_column in df.columns:
        missing = df.loc[~selector, [code_column, name_column]].drop_duplicates()
    else:
        missing = df.loc[~selector, [code_column]].drop_duplicates().drop_duplicates()
    # If there are missing values display a warning and set the code to na_value
    if not missing.empty:
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
        2. Load Comtrade monthly data for the corresponding codes
        3. Replace product codes and country codes by the FAOSTAT codes
        4. Reshape the Comtrade data to a longer format similar to FAOSTAT
        5. Aggregate Comtrade flows to the FAOSTAT product codes

    Example use:

        >>> from biotrade.common.compare import transform_comtrade_using_faostat_codes

    """
    # 1. Find the corresponding Comtrade codes using the mapping table
    selector = comtrade_faostat_mapping["faostat_code"].isin(faostat_code)
    product_mapping = comtrade_faostat_mapping[selector]
    # 2. Load Comtrade monthly data for the corresponding codes
    df_wide = comtrade.db.select(
        comtrade_table, product_code=product_mapping["comtrade_code"]
    )
    # Replace Comtrade product codes by the FAOSTAT product codes
    product_dict = product_mapping.set_index("comtrade_code").to_dict()["faostat_code"]
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
        2. Load the transformed version of the Comtrade data with faostat codes
            using the method `transform_comtrade_using_faostat_codes`
        3. Aggregate Comtrade from monthly to yearly. For the last data point
            extrapolate to the current year based on values from the last 12 months
        4. Concatenate FAOSTAT and Comtrade data

    Usage:

        >>> from biotrade.common.compare import merge_faostat_comtrade
        >>> merge_faostat_comtrade(faostat_table="forestry_trade",
        >>>                        comtrade_table="monthly",
        >>>                        faostat_code = [1632, 1633])

    To investigate the number of periods reported for each country in the most
    recent years, I used:

        >>> df_comtrade = transform_comtrade_using_faostat_codes(
        >>>     comtrade_table="monthly", faostat_code = [1632, 1633])
        >>> (df_comtrade.query("year >= year.max() -2")
        >>>  .groupby(["reporter", "period"])["value"].agg(sum)
        >>>  .reset_index()
        >>>  .value_counts(["reporter"])
        >>>  .reset_index().to_csv("/tmp/value_counts.csv")
        >>> )

    Max reporting period:

        >>> (df_comtrade.groupby("reporter")["period"].max()
        >>>  .to_csv("/tmp/max_period.csv"))

    """
    # 1. Load FAOSTAT bilateral trade data for the given codes
    df_faostat = faostat.db.select(faostat_table, product_code=faostat_code)
    product_names = df_faostat[["product_code", "product"]].drop_duplicates()
    # 2. Load Comtrade biltaral trade data for the given codes
    df_comtrade = transform_comtrade_using_faostat_codes(
        comtrade_table=comtrade_table, faostat_code=faostat_code
    )
    # 3. Aggregate Comtrade from monthly to yearly. For the last data point
    #    extrapolate to the current year based on values from the last 12 months
    # Group by year and compute the sum of values for the 12 month in each year
    index = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code",
        "year",
        "unit",
        "element",
    ]
    df_comtrade_agg = df_comtrade.groupby(index)["value"].agg(sum)
    # The last year is not necessarily complete and it might differ by
    # countries. For any country. Sum the values of the last 12 months instead.
    # We need to go back a bit further , because in March of 2022, there might
    # be advanced countries which reported January 2022, but other countries
    # which still have their last reporting period as June 2021, or even
    # further back in 2020.
    df_comtrade = df_comtrade.copy()  # .query("year >= year.max() - 3").copy()
    df_comtrade["max_period"] = df_comtrade.groupby("reporter")["period"].transform(max)
    df_comtrade["last_month"] = df_comtrade["max_period"] % 100
    df_comtrade["previous_year"] = df_comtrade["max_period"] // 100 - 1
    # For the special case of December, last year stays the same
    # last month is zero so that 0+1 becomes January
    is_december = df_comtrade["last_month"] == 12
    df_comtrade.loc[is_december, "previous_year"] = df_comtrade["max_period"] // 100
    df_comtrade.loc[is_december, "last_month"] = 0
    df_comtrade["max_minus_12"] = (
        df_comtrade["previous_year"] * 100 + df_comtrade["last_month"] + 1
    )
    df_recent = df_comtrade.query("period >= max_minus_12").copy()
    df_recent["year"] = df_recent["previous_year"] + 1
    df_recent_agg = df_recent.groupby(index)["value"].agg(value_est=sum)
    # Combine the aggregated yearly values with the estimate for the last year
    df = pandas.concat([df_comtrade_agg, df_recent_agg], axis=1).reset_index()
    # Replace value by the estimate "value_est" where it is defined
    selector = ~df.value_est.isna()
    df.loc[selector, "value"] = df.loc[selector, "value_est"]
    df.drop(columns="value_est", inplace=True)
    # Flag the estimates
    df.loc[selector, "flag"] = "estimate"
    df.loc[~selector, "flag"] = ""
    # Add FAOSTAT product names
    df = df.merge(product_names, on="product_code")

    # 4. Concatenate FAOSTAT and Comtrade data
    df_faostat.drop(columns="period", inplace=True)
    df_faostat["source"] = "faostat"
    df["source"] = "comtrade"
    df_concat = pandas.concat([df_faostat, df])
    df_concat = df_concat.reset_index(drop=True)
    # Place the "source" column first for readability
    cols = df_concat.columns.to_list()
    cols = [cols[-1]] + cols[:-1]
    df_concat = df_concat[cols]
    return df_concat
