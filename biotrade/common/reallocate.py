#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reallocate import quantities to the original country of production of a commodity.

For example if country A imports a commodity from country B and country B has
imported that commodity from country C. You don't want to attribute country A's
land footprint to country B. The purpose of this script is to reallocate A's
footprint to country C. It is done by first splitting the imported quantity
from B into a share produced in B and a share imported by B (from 0% to 100%).
Then the share imported by B is reallocated to all its import partners
according to the share of trade coming from those countries into B. Quantities
are expressed in weight equivalent of the primary commodity. The conversion to
a footprint in terms of area occurs at a later stage.

Example: see obs3df_methods trade_share_reallocation.py

TODO: hard code the conversion factors and value share for a small list of
      selected products and paste the example here.

"""

from typing import Tuple
import pandas


def compute_share_prod_imp(
    df_prod: pandas.DataFrame, df_trade: pandas.DataFrame
) -> pandas.Series:
    """Compute the share between production and import for the given list of products.
    This function works on input data frames in wide format.
    It performs the following steps:

        1. Aggregate the trade data frame by reporters, year and product
        2. Merge with the production data frame
        3. Compute the share

    Returns a copy of the df_prod data frame with import and share_prod_imp columns.
    """
    index = ["reporter", "product", "year"]
    for name, this_df in {"df_prod": df_prod, "df_trade": df_trade}.items():
        for col in index:
            if col not in this_df:
                msg = f"{col} column not found in DataFrame {name}: {this_df.columns}"
                raise KeyError(msg)
    # Add optional code columns only if they are present in both df
    optional_cols = ["reporter_code", "product_code"]
    index += [
        col
        for col in optional_cols
        if col in df_trade.columns and col in df_prod.columns
    ]
    df_trade_agg = df_trade.groupby(index)["import_quantity"].agg(sum).reset_index()
    df = df_prod.merge(df_trade_agg, on=index, how="outer")
    df.fillna({"import_quantity": 0, "production": 0}, inplace=True)
    df["share_prod_imp"] = df["production"] / (df["import_quantity"] + df["production"])
    return df


def split_prod_imp(df: pandas.DataFrame) -> Tuple[pandas.Series, pandas.Series]:
    """Split a quantity between what is produced domestically and what is imported
    The input data frame must contain the share used for splitting."""
    prod = df["primary_eq"] * df["share_prod_imp"]
    imp = df["primary_eq"] * (1 - df["share_prod_imp"])
    return prod, imp


def split_by_partners(
    df_prod: pandas.DataFrame,
    df_trade: pandas.DataFrame,
    allocation_step: int,
) -> pandas.DataFrame:
    """Reallocate a quantity, by splitting it between different trade partners"""
    index = ["reporter", "primary_product", "year"]
    # Add optional code columns only if they are present in df
    optional_cols = ["reporter_code", "primary_product_code"]
    index += [col for col in optional_cols if col in df_trade.columns]
    df = df_prod.merge(df_trade, on=index, how="left")
    # Compute the proportion among all trade partners
    index = ["primary_product", "year"]
    df["prop"] = df.groupby(index)["import_quantity"].transform(lambda x: x / x.sum())
    var_this_step = f"primary_eq_imp_{allocation_step}"
    var_previous_step = f"primary_eq_imp_{allocation_step - 1}"
    # Reallocate the import to partners according to the proportion
    df[var_this_step] = df[var_previous_step] * df["prop"]
    return df


def reallocate_one_step():
    """Perform one step of the reallocation"""


def reallocate():
    """Perform all steps of the reallocation"""
