#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

"""


def value_quantity_share(df, variable, df_relationship, index_list):
    """
    Function to calculate value/quantity share. The value share is necessary
    to compute the land footprint of EU's imports.
    Reference documents: technical report Cuypers 2013 "The impact
    of EU consumption on deforestation: Comprehensive analysis of the impact of
    EU consumption on deforestation"

    :param df, a data frame of production/trade flows
    :param variable, str whether it's the "value_share" or the "quantity_share"
    :param df_relationship, a data frame with a parent column and a child
    column
    :param index_list, a list of columns used as aggregation index, before
    computing the value/quantity share
    :return the aggregate dataframe with the quantity/value share values

    For example compute the quantity share coefficients of soy products
    "oil_soybean" and "cake_soybeans" resulting from "soybeans" primary
    commodity.

    Select bilateral trade of soy

        >>> from biotrade.faostat import faostat
        >>> db = faostat.db
        >>> soy = db.select(table="crop_trade", product = "soy")

    Define the variable quantity_share to compute

        >>> variable = "quantity_share"

    Define the index list to obtain aggregate data:

        >>> index = ["reporter", "partner", "product", "year"]

    Define relationship dataframe

        >>> import pandas
        >>> relationship = pandas.DataFrame({
            'parent': ['soybeans','soybeans'],
            'child': ['oil_soybean', 'cake_soybeans']})

    Compute the quantity share coefficients and return the dataframe
    containing the corresponding column

        >>> df_quantity_share = value_quantity_share(
                soy,
                variable,
                relationship,
                index,
            )
    """

    # select dataframe depending on variable choice
    if variable == "value_share":
        # select df rows where element is export value
        df = df[df["element"] == "export_value"]
    elif variable == "quantity_share":
        # select df rows where element is export quantity
        df = df[df["element"] == "export_quantity"]

    # df grouped by index list
    df_agg = df.groupby(index_list).agg(value=("value", sum)).reset_index()

    # right join to select only rows of df_agg which contains children products
    # of df_relationship
    df_share = df_agg.merge(
        df_relationship,
        how="right",
        left_on="product",
        right_on="child",
    )

    # define new index list based on parent column instead of product
    index_list.remove("product")
    index_list.append("parent")

    # new dataframe grouped by index_list with sum_children column
    # corresponding to the sum of all children products
    df_sum_parent = (
        df_share.groupby(index_list).agg(sum_children=("value", sum)).reset_index()
    )

    # merge the two dataframe based on index_list
    df_share = df_share.merge(
        df_sum_parent,
        how="left",
        left_on=index_list,
        right_on=index_list,
    )

    # add new column computing the value/quantity share as value column
    # divided by the sum children column
    df_share[variable] = (df_share.value / df_share.sum_children).fillna(0)

    return df_share
