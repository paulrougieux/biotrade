#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.


"""


def compare_aggregate_to_constituents(df, prod_aggregate, prod_constituents):
    """Compare production and trade of aggregate products to the sum of their constituents

    :param DataFrame df: FAOSTAT production data in long format
    :param str prod_aggregate: Name of the aggregate product
    :param list prod_constituents: Names of the product constituents

    For more information, see the FAOSTAT forest products classification and
    standards at https://www.fao.org/forestry/statistics/80572/en/ in
    particular the data structure
    https://www.fao.org/forestry/49962-0f43c0da7039a611aa884b3c6c642f4ac.pdf
    explains how the different products and aggregates relate to each other.

    Return a reshaped data frame in wider format with aggregates and their constituents in columns.

    For example compare roundwood to the sum of industrial roundwood and fuel wood.

    >>> from biotrade.faostat import faostat
    >>> from biotrade.faostat.quality import compare_aggregate_to_constituents
    >>> fp = faostat.forestry_production
    >>> rwd = compare_aggregate_to_constituents(
    >>>     fp, "roundwood", ["industrial_roundwood", "wood_fuel"]
    >>> )
    >>> assert all(rwd["diff"] == 0)

    """
    prod_together = [prod_aggregate] + prod_constituents
    index = ["reporter", "element", "unit", "year"]
    df = (
        df.query("product in @prod_together")
        .pivot(index=index, columns="product", values="value")
        .fillna(0)
    )
    df[prod_aggregate + "_agg"] = df.drop(columns=prod_aggregate).sum(axis=1)
    df["diff"] = df[prod_aggregate] - df[prod_aggregate + "_agg"]
    return df
