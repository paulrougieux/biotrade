#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

"""
import pandas

CONVERSION_FACTORS_LEVEL1 = pandas.DataFrame(
    {
        "product": [
            "industrial_roundwood",
            "wood_fuel",
            "sawnwood",
            "wood_based_panels",
            "paper_and_paperboard",
        ],
        "coef": [1, 1, 1.88, 1.5, 3.5],
        "unit_converted": [
            "m3 roundwood/m3",
            "m3 roundwood/m3",
            "m3 roundwood/m3",
            "m3 roundwood/m3",
            "m3 roundwood/t",
        ],
    }
)


def convert_to_eq_rwd_level_1(
    df, conv_factors=CONVERSION_FACTORS_LEVEL1, selected_units=None
):
    """Convert level one products to their volume equivalent roundwood
    All other products are ignored.

    Usage:

    >>> from biotrade.faostat.convert import convert_to_eq_rwd_level_1
    >>> from biotrade.faostat import faostat
    >>> fp_ita = faostat.db.select(table="forestry_production",
    >>>                                   reporter=["Italy"])
    >>> fp_ita_eqr = convert_to_eq_rwd_level_1(fp_ita)

    """
    if selected_units is None:
        selected_units = ["m3", "tonnes"]
    selected_products = conv_factors["product"].to_list()
    # Keep only products present in the conv_factors table
    # Compute their volume equivalent roundwood
    dfeqr = (
        df.query("product in @selected_products and unit in @selected_units")
        .merge(conv_factors, on="product")
        .assign(value_eqrwd=lambda x: x["value"] * x["coef"])
    )
    return dfeqr
