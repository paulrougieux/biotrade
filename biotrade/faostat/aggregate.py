#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

"""

from biotrade.faostat.countries import EU_COUNTRY_NAMES


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
        >>> ft_can_mirror_agg = agg_trade_eu_row(ft_can_mirror, index_side="reporter")

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
        ValueError("Can only be 'reporter' or 'partner'")
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
    dfeurow = df.groupby(index).agg(value=("value", sum)).reset_index()
    if index_side == "partner":
        dfeurow = dfeurow.assign(
            element=lambda x: x["partner_group"] + "_" + x["element"]
        ).drop(columns=country_group)
    # When aggregating
    if index_side == "reporter":
        dfeurow = dfeurow.rename(columns={country_group: "reporter"})

    # Check that the total value hasn't changed
    if not sum(dfeurow["value"]) == sum(df["value"]):
        raise ValueError(
            f"The total value sum of the aggregated data {sum(dfeurow['value'])}"
            + f"doesn't match with the sum of the input data frame {sum(df['value'])}"
        )
    return dfeurow
