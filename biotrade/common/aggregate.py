#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aggregation functions common to all data sources.

"""

import warnings
from pandas.api.types import is_numeric_dtype
import numpy as np

# Internal modules
from biotrade.faostat import faostat

EU_COUNTRY_NAMES_LIST = faostat.country_groups.eu_country_names


def agg_trade_eu_row(
    df,
    grouping_side="partner",
    drop_index_col=None,
    value_col=None,
    index_side=None,
):
    """
    Aggregate bilateral trade data to eu and row as partners

    :param data frame df: Bilateral trade flows from faostat
    :param str grouping_side: "reporter" or "partner" defines on which side of the
    aggregation index country will be grouped together between EU and rest of
    the world, defaults to partner.
    :param drop_index_col list or str: variables to be dropped from the grouping index
    defaults to ["flag"]
    value_col
    :param value_col list of str: variables to be aggregated, defaults to ["value"]
    :param index_side is deprecated; use grouping_side
    :return bilateral trade flows aggregated by eu and row

    Aggregate over many products in one country

        >>> from biotrade.faostat import faostat
        >>> from biotrade.common.aggregate import agg_trade_eu_row
        >>> ft_can = faostat.db.select(table="forestry_trade",
        >>>                                   reporter=["Canada"])
        >>> ft_can_agg = agg_trade_eu_row(ft_can)

    Aggregate the mirror flows

        >>> ft_can_mirror = faostat.db.select(table="forestry_trade",
        >>>                                          partner=["Canada"])
        >>> ft_can_mirror_agg = agg_trade_eu_row(ft_can_mirror,
                grouping_side="reporter")

    Aggregate two products in many countries on both the reporter and partner sides

        >>> swd = faostat.db.select(table="forestry_trade", product="sawnwood")
        >>> swdagg1 = agg_trade_eu_row(swd, grouping_side="partner")
        >>> swdagg2 = agg_trade_eu_row(swdagg1, grouping_side="reporter")
        >>> swdagg2[["product", "reporter", "partner"]].drop_duplicates()

    When aggregating over partner or reporter groups, the reporter takes the
    name of the reporter group, partner information is kept and can be later
    deleted, if only one partner is present for the purpose of concatenating
    with production data.

        In : ft_can_mirror_agg["reporter"].unique()
        Out: array(['eu', 'row'], dtype=object)

    Select crop trade where products contain the word "soy". Then Aggregate
    Brazil Soy Exports.

        >>> from biotrade.faostat import faostat
        >>> from biotrade.common.aggregate import agg_trade_eu_row
        >>> db = faostat.db
        >>> soy_trade = db.select(table="crop_trade", product = "soy", reporter="Brazil")
        >>> soy_trade_agg = agg_trade_eu_row(soy_trade)

    Select Yearly sawnwood oak trade from Comtrade. Then aggregate over EU and Rest of the World.

        >>> from biotrade.comtrade import comtrade
        >>> from biotrade.common.aggregate import agg_trade_eu_row
        >>> swdoak = comtrade.db.select(table="yearly", product_code="440791")
        >>> # Display interesting columns
        >>> swdoak[["reporter", "partner", "year", 'unit', 'quantity', 'net_weight','trade_value']]
        >>> # Aggregate to EU and ROW
        >>> swdoak_agg = agg_trade_eu_row(swdoak, drop_index_col=["flag"],
        >>>                               value_col=['quantity', 'net_weight','trade_value'])

    Select Yearly soy trade from Comtrade then aggregate over EU and Rest of the World

        >>> from biotrade.comtrade import comtrade
        >>> from biotrade.common.aggregate import agg_trade_eu_row
        >>> df = comtrade.db.select("yearly", product_code_start="1507", partner="Brazil")
        >>> cols_of_interest = ['classification', 'year',
        >>>        'flow_code', 'flow', 'reporter_code',
        >>>        'reporter', 'partner_code', 'partner', 'partner_iso',
        >>>        'product_code', 'unit_code', 'unit', 'quantity', 'net_weight', 'trade_value' ]
        >>> dfeurow = agg_trade_eu_row(df[cols_of_interest],
        >>>                         grouping_side = "reporter",
        >>>                         value_col=['quantity', 'net_weight','trade_value'])

    """
    # Make a copy of the data frame so that it will not be modified in place
    df = df.copy()

    # Default argument values
    if drop_index_col is None:
        drop_index_col = ["flag"]
    if value_col is None:
        value_col = ["value"]
    # Change string arguments to lists
    if isinstance(drop_index_col, str):
        drop_index_col = [drop_index_col]
    if isinstance(value_col, str):
        value_col = [value_col]
    # Deprecate the old name for the argument
    if index_side is not None:
        warnings.warn(
            "index_side is deprecated; use grouping_side", DeprecationWarning, 2
        )
        grouping_side = index_side
    # Restrict values taken by the grouping argument
    if grouping_side not in ["reporter", "partner"]:
        raise ValueError(
            "grouping_side can only take the values 'reporter' or 'partner'"
        )
    # Remove "Total FAO" and "World" rows if present
    selector = df["partner"] != "World"
    if "partner_code" in df.columns:
        if is_numeric_dtype(df["partner_code"]):
            selector = selector & (df["partner_code"] < 1000)
    if any(~selector):
        partner_removed = df.loc[~selector, "partner"].unique()
        warnings.warn(f"Removing {partner_removed} from df")
        df = df[selector].copy()
    # Add EU and ROW groups
    country_group = grouping_side + "_group"
    df[country_group] = "row"
    df[country_group] = df[country_group].where(
        ~df[grouping_side].isin(EU_COUNTRY_NAMES_LIST), "eu"
    )
    # Build the aggregation index based on all columns
    index = df.columns.to_list()
    reporter_and_partner_cols = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        country_group,
    ]
    # Remove the reporter, partner and grouping column from the index
    # Some grouping columns be added back
    for col in set(drop_index_col + reporter_and_partner_cols + value_col):
        if col in df.columns:
            index.remove(col)
    # The aggregation index depends on the grouping_side
    # Add back the columns that are not on the grouping side
    # Keep the code column only if available in df
    if grouping_side == "partner":
        index = ["reporter_code", "reporter", country_group] + index
        if "reporter_code" not in df.columns:
            index.remove("reporter_code")
    if grouping_side == "reporter":
        index = [country_group, "partner_code", "partner"] + index
        if "partner_code" not in df.columns:
            index.remove("partner_code")
    # Aggregate
    df_agg = df.groupby(index, dropna=False)[value_col].agg("sum").reset_index()
    # When aggregating over partner groups, rename country_group to partner
    if grouping_side == "partner":
        df_agg = df_agg.rename(columns={country_group: "partner"})
    # When aggregating over reporter groups, rename reporter_group to reporter
    if grouping_side == "reporter":
        df_agg = df_agg.rename(columns={country_group: "reporter"})
    # Check that the total value hasn't changed
    try:
        np.testing.assert_allclose(
            df_agg[value_col].sum(),
            df[value_col].sum(),
            err_msg=f"The total value sum of the aggregated data {df_agg[value_col].sum()}"
            + f" doesn't match with the sum of the input data frame {df[value_col].sum()}",
        )
    except AssertionError:
        err_msg = (
            f"The total value sum of the aggregated data {df_agg[value_col].sum()}"
            + f" doesn't match with the sum of the input data frame {df[value_col].sum()}"
        )
        warnings.warn(err_msg)
    return df_agg


def nlargest(df, value_vars, time_vars=None, agg_groups=None, slice_groups=None, n=10):
    """Return the n largest rows by group sorted by the first value column. The
    value is a yearly average by default, this can be changed by passing
    "period" as the time_vars argument for monthly data for example.

    The aggregation is done in two steps: (1) a sum over the grouping variables
    including time, (2) an average over the grouping variables.

    Rows are sorted by the first of the value_vars and slicing takes the
    first n rows in each slice group.

    :param data frame df
    :param value_vars list or str: name of the value columns to be aggregated
    :param time_vars list or str: name of the time columns, defaults to "year"
    :param agg_groups list or str: name of the grouping columns
    :param slice_groups list or str: name of the slicing columns
    :param n int: number of rows to keep in each group
    :return data frame

    For example the 5 largest sawnwood pine trade partners by flow globally
    over January to June 2021:

        >>> from biotrade.comtrade import comtrade
        >>> from biotrade.common.aggregate import nlargest
        >>> sp = comtrade.db.select("monthly", product_code="440711")
        >>> # Keep data for the first semmester of 2021
        >>> sp2021s1 = sp.query("202101<=period & period<=202106 &partner!='World'")
        >>> nlargest(sp2021s1,
        >>>          value_vars=["trade_value", "net_weight"],
        >>>          agg_groups=["reporter", "partner","flow"],
        >>>          slice_groups=["flow"],
        >>>          n=5)

    The 6 largest sawnwood pine world importers

        >>> spw = comtrade.db.select("monthly", product_code="440711", partner="World")
        >>> nlargest(spw,
        >>>          value_vars=["trade_value", "net_weight"],
        >>>          agg_groups=["reporter", "partner", "flow"],
        >>>          slice_groups="flow",
        >>>          n=6)

    The 6 largest wheat producers over 2010-2020, by element

        >>> from biotrade.faostat import faostat
        >>> from biotrade.common.aggregate import nlargest
        >>> wheat = faostat.db.select("crop_production", product_code=15)
        >>> # Select 10 years and remove aggregate regions
        >>> wheat10 = wheat.query("2010<=period & period<=2020 & reporter_code<1000")
        >>> nlargest(wheat10,
        >>>          value_vars="value",
        >>>          agg_groups="reporter",
        >>>          slice_groups="element")

    The 5 largest cocoa beans exporters to the EU and ROW

        >>> from biotrade.faostat import faostat
        >>> from biotrade.common.aggregate import nlargest
        >>> from biotrade.common.aggregate import agg_trade_eu_row
        >>> cocob_raw = faostat.db.select("crop_trade", product_code=661, period_start=2015)
        >>> cocob_raw = cocob_raw.query("element in ['import_quantity', 'import_value']")
        >>> cocob = agg_trade_eu_row(cocob_raw, grouping_side="reporter")
        >>> # Yearly average over the last 5 years
        >>> nlargest(cocob,
        >>>          value_vars="value",
        >>>          agg_groups=["reporter","partner"],
        >>>          slice_groups="element")
        >>> # Yearly values
        >>> nlargest(cocob,
        >>>          value_vars="value",
        >>>          agg_groups=["reporter","partner"],
        >>>          slice_groups=["element", "year"])

    The 12 largest Polish export destination over the past 5 years (notice the
    absence of the slice group)

        >>> from biotrade.common.aggregate import nlargest
        >>> from biotrade.comtrade import comtrade
        >>> pl = comtrade.db.select("yearly", reporter="Poland", product_code_start="44")
        >>> nlargest(pl.query("year>=2017 and flow=='export'"),
        >>>          value_vars=["net_weight", "trade_value"],
        >>>          agg_groups=["reporter", "partner", "flow"],
        >>>          n=12
        >>>         )

    """
    if time_vars is None:
        time_vars = ["year"]
    # Transform all input parameters that are strings to lists
    if isinstance(time_vars, str):
        time_vars = [time_vars]
    if isinstance(agg_groups, str):
        agg_groups = [agg_groups]
    if isinstance(slice_groups, str):
        slice_groups = [slice_groups]
    if isinstance(value_vars, str):
        value_vars = [value_vars]
    # Make sure slice columns are part of the agg_groups
    if agg_groups is None:
        agg_groups = slice_groups
    if agg_groups is not None and slice_groups is not None:
        agg_groups = list(set(agg_groups).union(set(slice_groups)))
    # Compute the sum within each group for each time period (such as each year)
    # Then compute the average for each group over the years
    if agg_groups is not None:
        df_agg = df.groupby(agg_groups + time_vars)[value_vars].agg("sum")
        df_agg = df_agg.groupby(agg_groups)[value_vars].agg("mean")
    else:
        df_agg = df.groupby(time_vars)[value_vars].agg("sum")
    # Sort rows by ascending slice_groups and descending first value column
    if slice_groups is not None:
        df_slice = (
            df_agg.sort_values(
                slice_groups + [value_vars[0]],
                ascending=[True] * len(slice_groups) + [False],
            )
            .groupby(slice_groups)
            .head(n)
        )
    else:
        df_slice = df_agg.sort_values([value_vars[0]], ascending=False).head(n)
    df_slice = df_slice.reset_index()
    return df_slice


def check_aggregate_sum(df, index):
    """Check that the World aggregate given in this data set corresponds to
    the sum of its constituents

    :param data frame df
    :param index list: list of index columns to be used in the group_by

    Example

        >>> from biotrade.comtrade import comtrade
        >>> from biotrade.common.aggregate import nlargest
        >>> sp = comtrade.db.select("monthly", product_code="440711")
        >>> check_aggregate_sum(sp, partner_side = "World")

    """
    # Extract world aggregate
