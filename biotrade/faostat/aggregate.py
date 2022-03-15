#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

"""

import warnings
from biotrade.faostat import faostat

# Import country table selecting continents and sub continents columns
CONTINENTS = faostat.country_groups.continents[
    ["faost_code", "continent", "sub_continent"]
]
EU_COUNTRY_NAMES_LIST = faostat.country_groups.eu_country_names


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
        >>> ft_can = faostat.db.select(table="forestry_trade",
        >>>                                   reporter=["Canada"])
        >>> ft_can_agg = agg_trade_eu_row(ft_can)

    Aggregate the mirror flows

        >>> ft_can_mirror = faostat.db.select(table="forestry_trade",
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

    Aggregate Brazil Soy Exports

    Select crop trade where products contain the word "soy"

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> db = faostat.db
        >>> soy_trade = db.select(table="crop_trade", product = "soy", reporter="Brazil")
        >>> soy_trade_agg = agg_trade_eu_row(soy_trade)


    """
    if index_side not in ["reporter", "partner"]:
        raise ValueError("index_side can only take the values 'reporter' or 'partner'")
    # Remove "Total FAO" rows if present
    selector = df["partner_code"] < 1000
    if any(~selector):
        partner_removed = df.loc[~selector, "partner"].unique()
        warnings.warn(f"Removing {partner_removed} from df")
        df = df[selector].copy()
    # Add EU and ROW groups
    country_group = index_side + "_group"
    df[country_group] = "row"
    df[country_group] = df[country_group].where(
        ~df[index_side].isin(EU_COUNTRY_NAMES_LIST), "eu"
    )
    # The aggregation index depends on the flow reporting side
    index = ["product_code", "product", "element", "unit", "year"]
    if index_side == "partner":
        index = ["reporter_code", "reporter", country_group] + index
    if index_side == "reporter":
        index = [country_group, "partner_code", "partner"] + index
    # Aggregate
    df_agg = df.groupby(index).agg(value=("value", sum)).reset_index()
    # When aggregating over partner groups, rename country_group to partner
    if index_side == "partner":
        df_agg = df_agg.rename(columns={country_group: "partner"})
    # When aggregating over reporter groups, rename reporter_group to reporter
    if index_side == "reporter":
        df_agg = df_agg.rename(columns={country_group: "reporter"})
    # Check that the total value hasn't changed
    if not df_agg["value"].sum() == df["value"].sum():
        raise ValueError(
            f"The total value sum of the aggregated data {sum(df_agg['value'])}"
            + f"doesn't match with the sum of the input data frame{sum(df['value'])}"
        )
    return df_agg


def agg_by_country_groups(df, agg_reporter=None, agg_partner=None):
    """Aggregate country data to continent or subcontinent groups

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

    # Merge reporters with the corresponding continent/subcontinent data
    df = df.merge(
        CONTINENTS,
        how="left",
        left_on="reporter_code",
        right_on="faost_code",
    )

    # Merge partners with the corresponding continent/subcontinent data
    if "partner_code" in df.columns:
        df = df.merge(
            CONTINENTS,
            how="left",
            left_on="partner_code",
            right_on="faost_code",
            suffixes=("_reporter", "_partner"),
        )

    # fixed aggregation column names
    index = ["period", "product", "product_code", "element", "element_code", "unit"]

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
    df_agg = df.groupby(index).agg(value=("value", sum)).reset_index()
    return df_agg
