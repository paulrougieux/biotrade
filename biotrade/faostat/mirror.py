#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions to analyse mirror flows.
"""


def put_mirror_beside(df):
    """Merge mirror flows to obtain a value column and a value_mirror column
    for the corresponding mirror flow.

    :param data frame df: bilateral trade flows in faostat compatible format,
        with an element column and a value column.
    :output: data frame with the same number of rows a new column value_mirror.

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

    """
    # Swap reporter and partner columns
    df_m = df.copy()
    df_m[["reporter", "reporter_code"]] = df[["partner", "partner_code"]]
    df_m[["partner", "partner_code"]] = df[["reporter", "reporter_code"]]
    # Swap element names
    df_m["element"] = df_m["element"].str.replace("import", "xxx")
    df_m["element"] = df_m["element"].str.replace("export", "import")
    df_m["element"] = df_m["element"].str.replace("xxx", "export")
    # Rename value and flag
    df_m["value_mirror"] = df_m["value"]
    df_m["flag_mirror"] = df_m["flag"]
    df_m.drop(columns=["value", "flag"], inplace=True)
    # Drop the element_code column
    df_m.drop(columns="element_code", inplace=True)
    # Merge with original data frame
    # index contains every column except value and flag (because flags might differ)
    index = [
        "reporter_code",
        "reporter",
        "partner_code",
        "partner",
        "product_code",
        "product",
        "element",
        "period",
        "year",
        "unit",
    ]
    return df.merge(df_m, on=index, how="left")
