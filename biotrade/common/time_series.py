"""
Written by Selene Patani.

Functions to return time series change detections.
"""

import pandas as pd
import numpy as np


def relative_change(df, years=5, last_value=True):
    """
    Calculate the relative change of a quantity with respect to
    values assumed by that quantity in the n previous years for
    production and trade data.
    Relative change of a value is calculated as:
        (value of the reference year - mean of values of previous n years) / mean of values of previous n years * 100

    :param (DataFrame) df, dataframe containing the quantity with respect to calculate the relative change in time
    :param (int) years, temporal window over calculating the relative change (default = 5)
    :param (boolean) last_value, if True returns only values related to the most recent year (default = True)

    :return (DataFrame) df_rel_change, dataframe which contains a new column, "relative_change" calculation

    As example, compute the relative change for soy trade products from Brazil as reporter to Europe and
    rest of the world as partners.
    Import dependencies
        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> from biotrade.common.time_series import relative_change
    Select soy trade products with Brazil as reporter
        >>> soy_trade = faostat.db.select(table="crop_trade",
                reporter="Brazil", product="soy")
    Aggregate partners as Europe (eu) and rest of the World (row)
        >>> soy_trade_agg = agg_trade_eu_row(soy_trade)
    Calculate the relative change of values in time (years = 5 as default) for the most recent year
        >>> soy_trade_relative_change = relative_change(soy_trade_agg, years= 5, last_value = True)
    """
    # Define group columns
    groupby_column_list = ["reporter_code", "product_code", "element", "unit"]
    # Trade data contain information also regarding partners
    if "partner" in df.columns:
        groupby_column_list.append("partner")
    # Define groups with respect to calculate the relative change of values in time
    df_groups = df.groupby(groupby_column_list)
    # Allocate df reshaped to return
    df_rel_change = pd.DataFrame()
    # Column list to perform calculations
    column_list_calc = []
    # Add columns with respect to the number of years to calculate the relative change
    for year in range(1, years + 1):
        column_list_calc.append(f"value_{year}_year_before")
    # Colum to perform the mean value of previous years
    column_list_calc.append("mean_previous_years")
    # Loop on each group
    for key in df_groups.groups.keys():
        # Select data related to the specific group
        df_key = df_groups.get_group(key)
        # Sort data from the most recent year
        df_key = df_key.sort_values(by="year", ascending=False)
        # Calculate values related to most recent year at disposal
        if last_value:
            df_key = df_key[0 : years + 1]
        # Add column to dataframe
        for idx, column in enumerate(column_list_calc):
            # The last column is the mean of the previous year values
            if column == "mean_previous_years":
                df_key[column] = df_key[column_list_calc[:-1]].mean(
                    axis=1, skipna=False
                )
                break
            # Shift value column up with respect to the number of years considered
            df_key[column] = df_key["value"].shift(-(idx) - 1)
        # Finally calculate the relative change column
        df_key["relative_change"] = (
            (df_key["value"] - df_key["mean_previous_years"])
            / df_key["mean_previous_years"]
            * 100
        )
        # 0/0 and float/0 treated as NaN
        df_key["relative_change"][np.isinf(df_key["relative_change"])] = np.nan
        # Drop columns added for the calculation
        df_key = df_key.drop(column_list_calc, axis=1)
        # Return values related to most recent year at disposal
        if last_value:
            df_key = df_key[0:1]
        # Add the group to the final dataframe to return
        df_rel_change = df_rel_change.append(df_key, ignore_index=True)
    # Final dataframe with the new column relative_change
    return df_rel_change
