#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions to analyse mirror flows.
"""
import logging

logger = logging.getLogger("biotrade.faostat")


def put_mirror_beside(df, drop_index_col=None):
    """Merge mirror flows to obtain a value column and a value_mirror column
    for the corresponding mirror flow.

    :param data frame df: bilateral trade flows in faostat compatible format,
        with an element column and a value column.
    :param drop_index_col list or str: variables to be dropped from the grouping index
    :return data frame with the same number of rows a new column value_mirror.

    The input data frame should contain data for both the export flow reported
    by the exporting country and the corresponding import flow reported by the
    importing country. The method swaps the reporter and partner columns (as
    well as reporter code and partner code), renames the value and flag columns
    to value_mirror and flag_mirror. Then merges back with the input data
    frame.

    Usage:

        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.mirror import put_mirror_beside
        >>> swd = faostat.db.select(table="forestry_trade", product="sawnwood")
        >>> swd_mir = put_mirror_beside(swd)

    Show mirror values that are reported as exactly the same

        >>> selector = swd_mir["value"] == swd_mir["value_mirror"]
        >>> swd_mir[selector]

    Show mirror values that are different or that are empty

        >>> swd_mir[~selector & ~swd_mir["value_mirror"].isna()]
        >>> swd_mir[swd_mir["value_mirror"].isna()]

    Select crop trade where products contain the word "soy" and then add the
    value_mirror column.

        >>> soy = faostat.db.select(table="crop_trade", product = "soy")
        >>> soy_mir = put_mirror_beside(soy)

    Get mirror values for trade flows aggregated at EU and ROW level

        >>> import pandas
        >>> from biotrade.faostat import faostat
        >>> from biotrade.faostat.mirror import put_mirror_beside
        >>> from biotrade.faostat.aggregate import agg_trade_eu_row
        >>> country = "Ukraine"
        >>> # Load trade reported by the country and group EU-ROW on the partner side
        >>> dfr = faostat.db.select("crop_trade", reporter=country, product_code="268")
        >>> dfragg = agg_trade_eu_row(dfr, grouping_side="partner")
        >>> # Load trade reported by its partners and group EU-ROW on the reporter side
        >>> dfp = faostat.db.select("crop_trade", partner=country, product_code="268")
        >>> dfpagg = agg_trade_eu_row(dfp, grouping_side="reporter")
        >>> # Concatenate the 2 datasets and put mirror value beside the value
        >>> dfm = put_mirror_beside(pandas.concat([dfragg, dfpagg]))

    """
    # Give drop_index_col its default value and make it a list
    if drop_index_col is None:
        drop_index_col = ["flag"]
    if isinstance(drop_index_col, str):
        drop_index_col = [drop_index_col]
    # Swap reporter and partner columns
    df_m = df.copy()
    reporter_cols = ["reporter", "reporter_code"]
    reporter_cols = [x for x in reporter_cols if x in df.columns]
    partner_cols = ["partner", "partner_code"]
    partner_cols = [x for x in partner_cols if x in df.columns]
    df_m[reporter_cols] = df[partner_cols]
    df_m[partner_cols] = df[reporter_cols]
    # Swap element names
    df_m["element"] = df_m["element"].str.replace("import", "xxx")
    df_m["element"] = df_m["element"].str.replace("export", "import")
    df_m["element"] = df_m["element"].str.replace("xxx", "export")
    # Drop the element_code column
    if "element_code" in df.columns:
        df_m.drop(columns="element_code", inplace=True)
    # Build the aggregation index based on all columns in df_m
    # Removing the "value" and the drop_index_col columns
    index = df_m.columns.to_list()
    for col in drop_index_col + ["value"]:
        if col in df.columns:
            index.remove(col)
    logger.info("\nMerging mirror flows on the following index:\n %s", index)
    # Merge with the original data frame
    return df.merge(df_m, on=index, how="outer", suffixes=("", "_mirror"))
