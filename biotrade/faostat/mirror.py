#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions to analyse mirror flows.
"""
import warnings


def put_mirror_beside(df, drop_index_var=None):
    """Merge mirror flows to obtain a value column and a value_mirror column
    for the corresponding mirror flow.

    :param data frame df: bilateral trade flows in faostat compatible format,
        with an element column and a value column.
    :param drop_index_var list or str: variables to be dropped from the grouping index
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
        >>> # Load trade reported by Brazil and group on the partner side
        >>> brar = faostat.db.select("crop_trade", reporter="Brazil", product_code="268")
        >>> braragg = agg_trade_eu_row(brar, grouping_side="partner")
        >>> # Load trade reported by its partners and group on the reporter side
        >>> brap = faostat.db.select("crop_trade", partner="Brazil", product_code="268")
        >>> brapagg = agg_trade_eu_row(brap, grouping_side="reporter")
        >>> # Concatenate the 2 datasets and put mirror value beside the value
        >>> bram = put_mirror_beside(pandas.concat([braragg, brapagg]))

    """
    # Give drop_index_var its default value and make it a list
    if drop_index_var is None:
        drop_index_var = ["flag"]
    if isinstance(drop_index_var, str):
        drop_index_var = [drop_index_var]
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
    # Removing the "value" and the drop_index_var columns
    index = df_m.columns.to_list()
    for col in drop_index_var + ["value"]:
        if col in df.columns:
            index.remove(col)
    warnings.warn(f"\nMerging mirror flows on the following index:\n {index}")
    # Merge with the original data frame
    return df.merge(df_m, on=index, how="left", suffixes=("", "_mirror"))
