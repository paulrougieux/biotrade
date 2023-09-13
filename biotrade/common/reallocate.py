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

Example use: see obs3df_methods trade_share_reallocation.py

TODO: hard code the conversion factors and value share for a small list of
      selected products and share a reproducible example here.

"""

from typing import Tuple
import pandas


def code_columns(cols: list):
    """Given a list of column names, provide corresponding code columns such as
    reporter_code, partner_code or product_code."""
    cols_to_check = ["reporter", "partner", "product", "primary_product"]
    cols_present = [col for col in cols_to_check if col in cols]
    cols_with_index = [col + "_code" for col in cols_present]
    return cols_with_index


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
    index += [
        col
        for col in code_columns(index)
        if col in df_trade.columns and col in df_prod.columns
    ]
    df_trade_agg = df_trade.groupby(index)["import_quantity"].agg("sum").reset_index()
    df = df_prod.merge(df_trade_agg, on=index, how="outer")
    df.fillna({"import_quantity": 0, "production": 0}, inplace=True)
    df["share_prod_imp"] = df["production"] / (df["import_quantity"] + df["production"])
    return df


def split_prod_imp(
    df: pandas.DataFrame,
    df_share: pandas.DataFrame,
    step: int,
) -> Tuple[pandas.Series, pandas.Series]:
    """Split a quantity between what is produced domestically and what is imported
    The input data frame must contain the share used for splitting."""
    index = ["reporter", "primary_product", "year"]
    index += [col for col in code_columns(index) if col in df.columns]
    df_output = df.merge(df_share[index + ["share_prod_imp"]], on=index, how="left")
    primary_eq_var = f"primary_eq_{step - 1}"
    prod = df_output[primary_eq_var] * df_output["share_prod_imp"]
    imp = df_output[primary_eq_var] * (1 - df_output["share_prod_imp"])
    return prod, imp


def compute_share_by_partners(
    df: pandas.DataFrame,
) -> pandas.DataFrame:
    """Compute the share of import between different trade partners"""
    index = ["reporter", "primary_product", "year"]
    index += [col for col in code_columns(index) if col in df.columns]
    # Compute the proportion among all trade partners
    return df.groupby(index)["import_quantity"].transform(lambda x: x / x.sum())


def allocate_by_partners(
    df_prod: pandas.DataFrame,
    df_trade: pandas.DataFrame,
    step: int,
) -> pandas.DataFrame:
    """Reallocate a quantity, by splitting it between different trade partners"""
    df_trade = df_trade.copy()  # We don't change the input
    # Merge index with optional code columns
    index = ["primary_product", "year"]
    if step == 1:
        index += ["reporter"]
    else:
        index += [f"partner_{step-1}"]
        df_trade.rename(columns={"reporter": f"partner_{step-1}"}, inplace=True)
    index += [col for col in code_columns(index) if col in df_trade.columns]
    df_trade.rename(columns={"partner": f"partner_{step}"}, inplace=True)
    df = df_prod.merge(df_trade, on=index, how="left")
    var_alloc = f"primary_eq_imp_alloc_{step}"
    var_agg = f"primary_eq_imp_{step}"
    # Reallocate import quantities to partners according to the proportion
    df[var_alloc] = df[var_agg] * df["imp_share_by_p"]
    return df


def aggregate_on_partners(df: pandas.DataFrame, step: int) -> pandas.DataFrame:
    """Aggregate on partners, followed by a rename of reporter to partner.
    Since we are interested in how the partner production is distributed
    Aggregate import and keep track of production related variables.
    Aggregates the previous step: step - 1, so that the step argument
    is consistent with the other functions in this time step.
    """
    index = ["partner", "product", "primary_product", "year"]
    index += [col for col in code_columns(index) if col in df.columns]
    df_agg = (
        df.groupby(index)[f"primary_eq_imp_alloc_{step - 1}"]
        .agg(sum)
        .reset_index()
        .rename(
            columns={
                "partner": "reporter",
                "partner_code": "reporter_code",
                f"primary_eq_imp_alloc_{step - 1}": f"primary_eq_{step - 1}",
            }
        )
    )
    return df_agg


def reallocate(
    df: pandas.DataFrame,
    prim_prod: pandas.DataFrame,
    prim_trade: pandas.DataFrame,
    n_steps: int,
):
    """Perform all steps of the reallocation of secondary product production in
    primary commodity equivalent

    The `df` argument contains the production or trade flows to be reallocated,
    values expressed in terms of primary crop equivalent. The arguments
    `prim_prod` and `prim_trade` argument are just used to compute the share of
    primary crop production and import and to build the trade reallocation
    matrix for the primary crop.

    param: prim_prod, crop production (for example sunflower seeds production)
    param: prim_trade, crop trade (for example sunflower seeds trade)
    param: df, production of processed products (sunflower oil production)
    param: n_steps number of steps

    Returns a dictionary with trade and production for each step.
    """
    product_to_primary_product = {
        "product_code": "primary_product_code",
        "product": "primary_product",
    }
    # Crop production with import and share of production and import
    prod_share = compute_share_prod_imp(prim_prod, prim_trade)
    prod_share.rename(columns=product_to_primary_product, inplace=True)
    trade = prim_trade.rename(columns=product_to_primary_product).copy()
    real = dict()
    trade["imp_share_by_p"] = compute_share_by_partners(trade)
    # Drop these columns to avoid having them added at each step
    trade.drop(
        columns=["import_quantity", "reporter_code", "partner_code"], inplace=True
    )
    # First step
    df = df.copy()
    df["primary_eq_prod_1"], df["primary_eq_imp_1"] = split_prod_imp(df, prod_share, 1)
    real[("trade", 1)] = allocate_by_partners(df_prod=df, df_trade=trade, step=1)
    real[("prod", 1)] = df
    nrows_trade = len(real[("trade", 1)])
    msg = f"Reallocation step: {1}, production rows:{len(df)}, "
    msg += f"trade rows: {nrows_trade}"
    print(msg)
    # Subsequent steps
    for step in range(2, n_steps + 1):
        df = real[("trade", step - 1)]
        # Keep only rows above threshold
        selector = df[f"primary_eq_imp_alloc_{step - 1}"] > 1
        df = df.loc[selector].copy()
        df[f"primary_eq_{step - 1 }"] = df[f"primary_eq_imp_{step - 1 }"]
        df.drop(columns="imp_share_by_p", inplace=True)
        df[f"primary_eq_prod_{step}"], df[f"primary_eq_imp_{step}"] = split_prod_imp(
            df, prod_share, step
        )
        real[("trade", step)] = allocate_by_partners(
            df_prod=df, df_trade=trade, step=step
        )
        real[("prod", step)] = df
        nrows_trade = len(real[("trade", step)])
        msg = f"Reallocation step: {step}, production rows:{len(df)}, "
        msg += f"trade rows: {nrows_trade}"
        print(msg)
    # Return
    return real


def merge_reallocated_production(real: dict):
    """Merge the reallocated production data frame
    To compare the primary_eq_n columns Check how primary_eq columns change
    with increasing step numbers.
    """
    index = ["reporter", "product", "primary_product", "year"]
    index += code_columns(index)
    real_steps = [x[1] for x in real if x[0] == "trade"]
    real_prod = real[("prod", 1)].copy()
    for step in real_steps[1:]:
        df = real[("prod", step)]
        new_cols = sorted(list(set(df.columns) - set(index)))
        print("Step", step, "rows:", len(df), "new columns:", new_cols)
        real_prod = real_prod.merge(df, on=index, how="outer")
    return real_prod


def merge_reallocated_trade(real: dict):
    """Merge the reallocated trade data frame"""
    index = ["reporter", "partner", "product", "primary_product", "year"]
    index += code_columns(index)
    real_steps = [x[1] for x in real if x[0] == "trade"]
    real_trade = real[("trade", 1)].copy()
    for step in real_steps[1:]:
        df = real[("trade", step)]
        cols_remove = ["imp_share_by_p", "import_quantity", "period"]
        new_cols = sorted(list(set(df.columns) - set(index) - set(cols_remove)))
        print("Step", step, "rows:", len(df), "new columns:", new_cols)
        real_trade = real_trade.merge(df[index + new_cols], on=index, how="outer")
        print("N rows real trade", len(real_trade))
    return real_trade
