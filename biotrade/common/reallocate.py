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


def compute_prod_imp_share(df_prod, df_trade):
    """Compute the production and import share for the given list of products"""
    index = ["reporter", "product"]
    optional_cols = ["reporter_code", "product_code"]
    index = index + optional_cols
    # Code columns are optional
    for col in optional_cols:
        if col not in df_trade.columns:
            index.remove(col)
            print(f"{col} not in the column names")
    df_trade_agg = df_trade.groupby(index).agg(imp=("value", sum)).reset_index()
    df = df_prod.merge(df_trade_agg, on=index, how="left")
    df["share_prod_imp"] = df["primary_crop_eq"] / (df["imp"] + df["primary_crop_eq"])
    return df


def split_prod_imp():
    """Split a quantity between what is produced domestically and what is imported"""


def split_by_partners():
    """Reallocate a quantity, by splitting it between different trade partners"""


def reallocate_one_step():
    """Perform one step of the reallocation"""


def reallocate():
    """Perform all steps of the reallocation"""
