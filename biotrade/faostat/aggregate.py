#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

"""

from biotrade.faostat.country_groups import EU_COUNTRY_NAMES
from biotrade.faostat import faostat

# Import country table selecting continents and sub continents columns
df_continents = faostat.country_groups.continents[
    ["faost_code", "continent", "sub_continent"]
]


def agg_trade_eu_row(df, index_side="partner"):
    """Aggregate bilateral trade data to eu and row as partners

    :param data frame df: Bilateral trade flows from faostat
    :param str index_side: "reporter" or "partner" defines on which side of the
    aggregation index country will be grouped together between EU and rest of
    the world, defaults to partner.
    :return bilateral trade flows aggregated by eu and row

    Usage:

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> ft_can = faostat.db_sqlite.select(table="forestry_trade",
        >>>                                   reporter=["Canada"])
        >>> ft_can_agg = agg_trade_eu_row(ft_can)

    Aggregate the mirror flows

        >>> ft_can_mirror = faostat.db_sqlite.select(table="forestry_trade",
        >>>                                          partner=["Canada"])
        >>> ft_can_mirror_agg = agg_trade_eu_row(ft_can_mirror,
                index_side="reporter")

    Notice how when aggregating over partner groups, the function creates new
    elements such as "eu_export_quantity", "row_export_quantity" etc. This
    table can be concatenated with a production table for further analysis.

        In : ft_can_agg["element"].unique()
        Out:
        array(['eu_export_quantity', 'eu_export_value', 'eu_import_quantity',
               'eu_import_value', 'row_export_quantity', 'row_export_value',
               'row_import_quantity', 'row_import_value'], dtype=object)

    When aggregating over reporter groups, the reporter takes the name of the
    reporter group, partner information is kept and can be later deleted, if
    only one partner is present for the purpose of concatenating with
    production data.

        In : ft_can_mirror_agg["reporter"].unique()
        Out: array(['eu', 'row'], dtype=object)
    """
    if index_side not in ["reporter", "partner"]:
        raise ValueError("index_side can only take the values 'reporter' or 'partner'")
    country_group = index_side + "_group"
    df[country_group] = "row"
    df[country_group] = df[country_group].where(
        ~df[index_side].isin(EU_COUNTRY_NAMES), "eu"
    )
    # The aggregation index depends on the flow reporting side
    index = ["product_code", "product", "element", "unit", "year"]
    if index_side == "partner":
        index = ["reporter_code", "reporter", country_group] + index
    if index_side == "reporter":
        index = [country_group, "partner_code", "partner"] + index
    # Aggregate
    df_agg = df.groupby(index).agg(value=("value", sum)).reset_index()
    # When aggregating over partner groups, prefix elements by the group name
    if index_side == "partner":
        df_agg = df_agg.assign(
            element=lambda x: x["partner_group"] + "_" + x["element"]
        ).drop(columns=country_group)
    # When aggregating over reporter groups, rename reporter_group to reporter
    if index_side == "reporter":
        df_agg = df_agg.rename(columns={country_group: "reporter"})
    # Check that the total value hasn't changed
    if not sum(df_agg["value"]) == sum(df["value"]):
        raise ValueError(
            f"The total value sum of the aggregated data {sum(df_agg['value'])}"
            + f"doesn't match with the sum of the input data frame{sum(df['value'])}"
        )
    return df_agg


def agg_by_country_groups(df, agg_level):
    """Aggregate country data to continent or subcontinent groups

    :param data frame df: data from faostat merged with continent/subcontinent
    table
    :param str agg_level: "continent" or "sub_continent" defines which level
    of data aggregation to perform
    :return dataframe aggregated by continent/subcontinent instead of
    countries

    For example select all bilateral flows for soy trade and select the matrix
    by continents/subcontinents

    Import function agg_by_country_groups

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_by_country_groups

    Select bilateral trade of soy

        >>> db = faostat.db
        >>> df_soy = db.select(table="crop_trade", product = "soy")

    Aggregate data by continents

        >>> df_soy_agg_continent = agg_by_country_groups(df_soy_merge,
                'continent')

    Aggregate data by subcontinents

        >>> df_soy_agg_subcontinent = agg_by_country_groups(df_soy_merge,
            'sub_continent')
    """

    # Merge reporters with the corresponding continent/subcontinent data
    df = df.merge(
        df_continents, how="left", left_on="reporter_code", right_on="faost_code"
    )

    # Merge partners with the corresponding continent/subcontinent data
    if "partern_code" in df.columns:
        df = df.merge(
            df_continents,
            how="left",
            left_on="partner_code",
            right_on="faost_code",
            suffixes=("_reporter", "_partner"),
        )

    # fixed aggregation column names
    index = ["period", "product", "product_code", "element", "element_code", "unit"]
    for column in df.columns:
        if agg_level == "continent":
            # check if the are reporter /partner continent columns into
            # dataframe to aggregate
            if column in ["continent_reporter", "continent_partner", "continent"]:
                index.append(column)
        elif agg_level == "sub_continent":
            # check if the are reporter /partner subcontinent columns into
            # dataframe to aggregate
            if column in [
                "sub_continent_reporter",
                "sub_continent_partner",
                "sub_continent",
            ]:
                index.append(column)
    # Aggregate
    df_agg = df.groupby(index).agg(value=("value", sum)).reset_index()
    return df_agg
