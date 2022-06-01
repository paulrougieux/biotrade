#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.


"""


def compare_prod_agg_to_parts(df, prod_aggregate, prod_constituents):
    """Compare aggregate products to the sum of their constituents

    :param DataFrame df: FAOSTAT production data in long format
    :param str prod_aggregate: Name of the aggregate product
    :param list prod_constituents: Names of the product constituents

    Return a reshaped data frame in wider format with aggregates and their constituents in columns.

    For more information, see the FAOSTAT forest products classification and
    standards at https://www.fao.org/forestry/statistics/80572/en/
    in particular the data structure chart
    https://www.fao.org/forestry/49962-0f43c0da7039a611aa884b3c6c642f4ac.pdf
    explains how the different products and aggregates relate to each other.

    For example compare roundwood to the sum of industrial roundwood and fuel
    wood. In the complete forestry production dataset (including production,
    import and export values and quantities).

    >>> from biotrade.faostat import faostat
    >>> from biotrade.faostat.quality import compare_prod_agg_to_parts
    >>> fp = faostat.pump.read_df("forestry_production")
    >>> rwd = compare_prod_agg_to_parts(
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


def check_prod_agg_and_parts(df, prod_aggregate, prod_constituents):
    """Check that the product aggregate corresponds to the sum of its constituents

    This function has the same arguments as compare_prod_agg_to_parts. In case
    of an error, it gives the function call necessary to show the comparison
    data frame for further investigation.
    """
    output_df = compare_prod_agg_to_parts(
        df, prod_aggregate=prod_aggregate, prod_constituents=prod_constituents
    )
    if all(output_df["diff"] == 0):
        msg = f"OK: the product aggregate {prod_aggregate} "
        msg += f"does correspond to the sum of its constituents:\n {prod_constituents}"
        print(msg)
    else:
        msg = f"The product aggregate {prod_aggregate} "
        msg += (
            f"does not correspond to the sum of its constituents:\n {prod_constituents}"
        )
        msg += "\nLoad the data frame with:\n"
        msg += "from biotrade.faostat.quality import compare_prod_agg_to_parts\n"
        msg += f"compare_prod_agg_to_parts(your_dataframe, '{prod_aggregate}', {prod_constituents})"
        raise ValueError(msg)
