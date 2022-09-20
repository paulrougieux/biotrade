#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.


"""

import pandas

# import re
from biotrade import module_dir

# Location of module configuration data
config_data_dir = module_dir / "config_data"

# Load a mapping table used to rename columns
df = pandas.read_csv(config_data_dir / "column_names.csv")

non_na_values = (~df.filter(like="faostat").isna()).sum(axis=1)
column_names = df[non_na_values > 0]


# def sanitize_variable_names(df, column_renaming, short_name):
#     """Sanitize column names using a mapping table."""
#     # Rename columns to snake case
#     df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
#     # Columns of the db table
#     db_table_cols = self.db.tables[short_name].columns.keys()
#     # Original column names
#     cols_to_check = self.column_names[self.column_names["jrc"].isin(db_table_cols)][
#         column_renaming
#     ].tolist()
#     # Check columns which have changed in the input source
#     cols_to_change = set(cols_to_check).difference(df.columns)
#     # If column names have changed raise an error
#     if cols_to_change:
#         raise ValueError(
#             f"The following columns \n{list(cols_to_change)}\nhave changed in the input source {column_renaming}.\nUpdate config_data/column_names.csv before updating table {short_name}"
#         )
#     # Map columns using the naming convention defined in self.column_names
#     mapping = self.column_names.set_index(column_renaming).to_dict()["jrc"]
#     # Discard nan keys of mapping dictionary
#     mapping.pop(np.nan, None)
#     # Obtain columns for db upload
#     columns = list(df.columns.intersection(list(mapping.keys())))
#     # Filter df selecting only columns for db
#     df = df[columns].copy()
#     # Rename columns using the naming convention defined in self.column_names
#     df.rename(columns=mapping, inplace=True)
