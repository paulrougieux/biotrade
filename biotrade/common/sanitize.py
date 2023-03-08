#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.


"""

import numpy as np
import re


def sanitize_variable_names(df, column_renaming, db, short_name):
    """Sanitize column names using a mapping table."""
    # Rename columns to snake case
    df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
    # Columns of the db table
    db_table_cols = db.tables[short_name].columns.keys()
    # Original column names
    cols_to_check = db.parent.column_names[
        db.parent.column_names["biotrade"].isin(db_table_cols)
    ][column_renaming].tolist()
    # Check columns which have changed in the input source
    cols_to_change = set(cols_to_check).difference(df.columns)
    # If column names have changed raise an error
    if cols_to_change:
        raise ValueError(
            f"The following columns \n{list(cols_to_change)}\nhave changed in the input source {column_renaming}.\nUpdate config_data/column_names.csv before updating table {short_name}"
        )
    # Map columns using the naming convention defined in self.column_names
    mapping = db.parent.column_names.set_index(column_renaming).to_dict()["biotrade"]
    # Discard nan keys of mapping dictionary
    mapping.pop(np.nan, None)
    # Filter df selecting only columns for db
    df = df[cols_to_check].copy()
    # Rename columns using the naming convention defined in self.column_names
    df.rename(columns=mapping, inplace=True)
    return df
