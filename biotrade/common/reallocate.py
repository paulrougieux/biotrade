#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Warning: this reallocation script is deprecated and will be move to deforestfoot.


Reallocate import quantities to the original country of production of a commodity.

When attempting to attribute a land footprint of import to a particular
country, the country of import is not necessarily the country of production.
For example if country A imports a commodity from country B and country B has
itself imported that commodity from country C. You don't want to directly
attribute country A's land footprint to country B. The purpose of this script
is to reallocate A's footprint to country C. It is done by:

1. Splitting the imported quantity from B into a share produced in B and a
   share imported by B (from 0% to 100%).

2. Then the share imported by B is reallocated to all its trade partners
   according to the share of trade coming from those countries into B. Quantities
   are expressed in weight equivalent of the primary commodity. The conversion to
   a footprint in terms of area occurs at a later stage.

In particular, if there is no production of the crop in country B, then it
makes sense for that production to be fully reattributed to country C.

Example use: see obs3df_methods trade_share_reallocation.py

TODO: hard code the conversion factors and value share for a small list of
      selected products. Then add a reproducible example here.

"""

from typing import Tuple
import warnings
import pandas
import numpy as np

msg = "This script is deprecated and will be moved to the deforestfoot package.\n"
msg += "Update your import path as follows:\n"
msg += "from deforestfoot.common.reallocate import reallocate"
warnings.warn(msg)


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
    """Compute the share between production and import for the given list of
    products. This function works on input data frames in wide format.


    If df_trade contains the primary commodity equivalent of all products (i.e.
    I_primcrop + I_productseq), then this will compute  the following share:

                          P_primcrop
            ----------------------------------------
            (P_primcrop + I_primcrop + I_productseq)

    If df_trade contains only the trade in primary commodity (for example
    crops) then this will compute the following share:

                    P_primcrop
            --------------------------
            (P_primcrop + I_primcrop)

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
    # Depending on whether df contains production or trade data,
    # Join on reporter or partner
    if step == 1 and "partner" not in df.columns:
        rep_or_par = "reporter"
    else:
        rep_or_par = f"partner_{step-1}"
        df_share = df_share.copy()
        df_share.rename(columns={"reporter": rep_or_par}, inplace=True)
    index = [rep_or_par, "primary_product", "year"]
    index += [col for col in code_columns(index) if col in df.columns]
    df_output = df.merge(df_share[index + ["share_prod_imp"]], on=index, how="left")
    primary_eq_var = f"primary_eq_{step - 1}"

    # Debug start
    # if step == 2:
    #     breakpoint()
    #     df_output_fc  = df_output.query("reporter == 'France' and partner_1 == 'Chile' and year == 2018 and product == 'sunflower_seed_oil_crude'")
    #     df_output_fc[["primary_eq_1", "share_prod_imp"]]
    #     df.query("reporter == 'France' and partner_1 == 'Chile' and year == 2018 and product == 'sunflower_seed_oil_crude'")[["primary_eq_1", "prod", "imp"]]
    # # Debug end

    df_output[f"primary_eq_prod_{step}"] = (
        df_output[primary_eq_var] * df_output["share_prod_imp"]
    )
    df_output[f"primary_eq_imp_{step}"] = df_output[primary_eq_var] * (
        1 - df_output["share_prod_imp"]
    )
    # Remove the share
    df_output.drop(columns="share_prod_imp", inplace=True)
    return df_output


def compute_share_by_partners(
    df: pandas.DataFrame, value=None, threshold=None
) -> pandas.DataFrame:
    """Compute the share of import between different trade partners

    param threshold: Threshold in tons below which the rows will be eliminated from the computation

    """
    if value is None:
        value = "import_quantity"
    if threshold is None:
        threshold = 100
    index = ["reporter", "primary_product", "year"]
    index += [col for col in code_columns(index) if col in df.columns]
    # Compute the proportion among all trade partners
    locator = df[value] > threshold
    df_out = df.loc[locator].copy()
    df_out["imp_share_by_p"] = df_out.groupby(index)[value].transform(
        lambda x: x / x.sum()
    )
    return df_out


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
    selected_cols = index + [f"partner_{step}", "imp_share_by_p"]
    df = df_prod.merge(df_trade[selected_cols], on=index, how="left")
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
        .agg("sum")
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
    threshold: float = None,
):
    """Perform all steps of the reallocation of secondary product production in
    primary commodity equivalent

    TODO: define an object at the level of the primary crop (soybans and
    products, palm and related products, chocolate and related products) Which
    has methods to perform the reallocation.

    The `df` argument contains the production or trade flows to be reallocated,
    values expressed in terms of primary crop equivalent. The arguments
    `prim_prod` and `prim_trade` argument are just used to compute the share of
    primary crop production and import and to build the trade reallocation
    matrix for the primary crop.

    If we use comtrade data, it should be both for the df and prim_trade
    arguments. Note the prim_prod argument will necessarily be from FAOSTAT
    because it's the only source that provides this information.

    param: prim_prod, crop production (for example sunflower seeds production)
    param: prim_trade, crop trade (for example sunflower seeds trade)
    param: df, production of processed products (sunflower oil production)
    param: n_steps number of steps

    Returns a dictionary with trade and production for each step.
    """
    if threshold is None:
        threshold = 1
    # TODO: move this to a separate function that does the reallocation of
    # production only Keep a function that does the reallocation of trade only
    product_to_primary_product = {
        "product_code": "primary_product_code",
        "product": "primary_product",
    }
    # Crop production with import and share of production and import
    prod_share = compute_share_prod_imp(prim_prod, prim_trade)
    prod_share.rename(columns=product_to_primary_product, inplace=True)
    trade = prim_trade.rename(columns=product_to_primary_product).copy()
    trade = compute_share_by_partners(trade)
    real = dict()
    # Drop these columns to avoid having them added at each step
    trade.drop(
        columns=["import_quantity", "reporter_code", "partner_code"], inplace=True
    )
    # First step
    df = df.copy()
    # Drop code columns because renaming of partner to reporter is needed at some point
    # and we don't want to rename also partner_code to reporter_code.
    df.drop(columns=["reporter_code"], inplace=True, errors="ignore")
    # Depending on whether df contains production data or trade data
    # add a partner_0 column
    if "partner" not in df.columns:
        df["partner_0"] = np.nan
    else:
        df["partner_0"] = df["partner"]
    # Split production and import
    df = split_prod_imp(df, prod_share, 1)
    real[("prod", 1)] = df
    # Allocate by partners
    real[("trade", 1)] = allocate_by_partners(df_prod=df, df_trade=trade, step=1)
    nrows_trade = len(real[("trade", 1)])
    msg = f"Reallocation step: {1}, production rows:{len(df)}, "
    msg += f"trade rows: {nrows_trade}"
    print(msg)
    # Subsequent steps
    for step in range(2, n_steps + 1):
        df = real[("trade", step - 1)]
        # Keep only rows above threshold
        selector = df[f"primary_eq_imp_alloc_{step - 1}"] > threshold
        df = df.loc[selector].copy()
        # Temporary column needed by the split_prod_imp function
        df[f"primary_eq_{step - 1 }"] = df[f"primary_eq_imp_alloc_{step - 1 }"]
        df.drop(columns="imp_share_by_p", inplace=True)
        # Split production and import
        df = split_prod_imp(df, prod_share, step)
        # Remove temporary column
        df.drop(columns=f"primary_eq_{step - 1 }")
        real[("prod", step)] = df
        # Allocate by partners
        real[("trade", step)] = allocate_by_partners(
            df_prod=df, df_trade=trade, step=step
        )
        nrows_trade = len(real[("trade", step)])
        msg = f"Reallocation step: {step}, production rows:{len(df)}, "
        msg += f"trade rows: {nrows_trade}"
        print(msg)
    # Return
    return real


def merge_reallocated(
    real: dict, rtol: float = None, atol: float = None, check_ignores_na: bool = False
):
    """
    Merge reallocated imports produced in the importing country.

    At each step, look at the production import split and keep only the import
    that was actually produced in each partner country.

    In case of production data, step 1 returns only the production in country
    A. Step 2 returns the import from first level partners that are actually
    produced in those countries. Step 3 returns the imports from the second
    level partners which is the quantity imported from the first level partners
    partners but not produced in these countries, because it was reimported
    from the second level partners.
    """
    if rtol is None:
        rtol = 1e-3
    if atol is None:
        atol = 100
    # Add a partner_0 column for the first step
    # Prepare a grouping index for the consistency check
    index = ["reporter", "primary_product", "year"]
    if "partner" not in real[("prod", 1)].columns:
        real[("prod", 1)]["partner_0"] = np.nan
    else:
        real[("prod", 1)]["partner_0"] = real[("prod", 1)]["partner"]
        index += ["partner_0"]
    # Assemble the import that was actually produced in each partner country
    df = pandas.DataFrame()
    last_step = pandas.DataFrame(real.keys())[1].max()
    selected_columns = ["primary_product", "year", "reporter", "partner"]
    selected_columns += ["primary_eq", "step"]
    for i in range(1, last_step + 1):
        df_step = real[("prod", i)].copy()
        df_step["partner"] = df_step[f"partner_{i-1}"]
        df_step["primary_eq"] = df_step[f"primary_eq_prod_{i}"]
        intermediate_partners = df_step.columns[
            df_step.columns.str.contains("partner_")
        ]
        intermediate_partners = intermediate_partners.to_list()
        df_step["step"] = i
        df = pandas.concat([df, df_step[selected_columns + intermediate_partners]])
    # Check that the aggregation didn't loose data
    df_agg = df.groupby(index)["primary_eq"].agg("sum").reset_index()
    real_prod_agg = (
        real[("prod", 1)].groupby(index)["primary_eq_0"].agg("sum").reset_index()
    )
    df_check = df_agg.merge(real_prod_agg, on=index, how="outer")
    # Ignore NA values in the check, drop any rows that contain NA value in a column
    if check_ignores_na:
        df_check.dropna(inplace=True)
    selector = np.isclose(
        df_check["primary_eq"], df_check["primary_eq_0"], rtol=rtol, atol=atol
    )
    if not all(selector):
        msg = f"With a relative tolerance of {rtol} "
        msg += f"and an absolute tolerance of {atol}, "
        msg += "the primary equivalent sum doesn't match for the following rows\n"
        msg += f"{df_check.loc[~selector]}"
        msg += "\n The primary_eq_0 column comes from the input data,"
        msg += " primary_eq is after the reallocation."
        raise ValueError(msg)
    return df


def sum_last_partner(df):
    """Sum up flows for the last partner at each step, to obtain flows
    between a reporter country and its last partners"""
    cols = df.columns
    cols = cols[cols.str.contains("partner_")]
    df["final_partner"] = "---"
    for col in cols:
        selector = df[col].isna()
        df.loc[~selector, "final_partner"] = df.loc[~selector, col]
    index = ["primary_product", "year", "reporter", "final_partner"]
    df_agg = df.groupby(index)["primary_eq"].agg("sum").reset_index()
    return df_agg
