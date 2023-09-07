#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

"""

import numpy as np

# Internal modules
from biotrade.faostat import faostat

# Make agg_trade_eu_row available here for backward compatibility
# so that the following import statement continues to work:
# >>> from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.common.aggregate import (  # noqa # pylint: disable=unused-import
    agg_trade_eu_row,
)

# Import country table selecting continents and sub continents columns
CONTINENTS = faostat.country_groups.continents[
    ["faost_code", "continent", "sub_continent"]
]


def agg_by_country_groups(df, agg_reporter=None, agg_partner=None):
    """
    Aggregate country data to continent or subcontinent groups

    :param data frame df: data from faostat merged with continent/subcontinent
    table
    :param str agg_reporter: "continent" or "sub_continent" defines which level
    of data aggregation to perform on the reporter side
    :param str agg_partner: "continent" or "sub_continent" defines which level
    of data aggregation to perform on the partner side
    :return dataframe aggregated by continent/subcontinent instead of
    countries

    For example selecting soy trade data for all world countries, aggregate
    data by continents or subcontinents for both reporter and partner

    Import function agg_by_country_groups

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_by_country_groups

    Select bilateral trade of soy

        >>> db = faostat.db
        >>> soy = db.select(table="crop_trade", product = "soy")

    Aggregate data by continents

        >>> soy_agg_continent = agg_by_country_groups(soy,
            agg_reporter = 'continent', agg_partner = 'continent')

    Aggregate data by subcontinents

        >>> soy_agg_subcontinent = agg_by_country_groups(soy,
            agg_reporter = 'sub_continent', agg_partner = 'sub_continent')

    For example selecting Brazil and Indonesia as reporter countries.
    Aggregate data by reporter countries and continents for partners only

        >>> soy = db.select(table="crop_trade",
                reporter = ["Brazil", "Indonesia"], product = "soy")
        >>> soy_agg_continent_r_p = agg_by_country_groups(soy,
                agg_partner = "continent")

    """

    # Consider countries with reporter code < 300, above to this value reporters are aggregations
    df = df[df.reporter_code < 300]
    # Merge reporters with the corresponding continent/subcontinent data
    df = df.merge(
        CONTINENTS,
        how="left",
        left_on="reporter_code",
        right_on="faost_code",
    )

    # Merge partners with the corresponding continent/subcontinent data
    if "partner_code" in df.columns:
        # Consider countries with partner code < 300, above to this value partners are aggregations
        df = df[df.partner_code < 300]
        df = df.merge(
            CONTINENTS,
            how="left",
            left_on="partner_code",
            right_on="faost_code",
            suffixes=("_reporter", "_partner"),
        )

    # fixed aggregation column names
    columns = [
        "period",
        "product",
        "product_code",
        "element",
        "element_code",
        "unit",
    ]
    # Columns for the aggregation
    index = []
    for col in columns:
        if col in df.columns:
            index.append(col)

    if agg_reporter is None:
        # aggregate by reporter and reporter code (if columns exist) since no
        # continent/subcontinent reporter aggregation selected
        [
            index.append(column)
            for column in df.columns
            if column in ["reporter", "reporter_code"]
        ]
    else:
        # check if the are reporter continent/subcontinent columns into
        # dataframe to aggregate
        [
            index.append(column)
            for column in df.columns
            if column in [f"{agg_reporter}_reporter", f"{agg_reporter}"]
        ]
    if agg_partner is None:
        # aggregate by partner and partner code (if columns exist) since no
        # continent/subcontinet partner aggregation selected
        [
            index.append(column)
            for column in df.columns
            if column in ["partner", "partner_code"]
        ]
    else:
        # check if the are partner continent/subcontinent columns into
        # dataframe to aggregate
        [
            index.append(column)
            for column in df.columns
            if column == f"{agg_partner}_partner"
        ]
    # Aggregate
    df_agg = df.groupby(index, dropna=False).agg(value=("value", "sum")).reset_index()
    # Check that the total value isn't changed
    np.testing.assert_allclose(
        df_agg["value"].sum(),
        df["value"].sum(),
        err_msg=f"The total value sum of the aggregated data {df_agg['value'].sum()}"
        + f" doesn't match with the sum of the input data frame {df['value'].sum()}",
    )
    return df_agg
