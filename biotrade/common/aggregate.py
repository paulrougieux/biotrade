#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aggregation functions common to all data sources.

"""


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
    agg_groups = list(set(agg_groups).union(set(slice_groups)))
    # Compute the sum within each group for each time period (such as each year)
    # Then compute the average for each group over the years
    if agg_groups is not None:
        df_agg = df.groupby(agg_groups + time_vars)[value_vars].agg(sum)
        df_agg = df_agg.groupby(agg_groups)[value_vars].agg("mean")
    else:
        df_agg = df.groupby(time_vars)[value_vars].agg(sum)
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
