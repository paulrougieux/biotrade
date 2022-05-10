"""
Written by Selene Patani.

Script made to export data related to all commodities and tree of the key products, for the web platform

"""

import numpy as np
import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat

# Name of product file to retrieve
faostat_key_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve dataset
product_tree = pd.read_csv(faostat_key_commodities_file)
# Retrieve parent and child codes
parent_codes = product_tree["parent_code"].unique().tolist()
child_codes = product_tree["child_code"].unique().tolist()
# Union of the codes without repetitions
key_product_codes = np.unique(parent_codes + child_codes).tolist()
# Put primary codes instead of keys
product_dict = dict(zip(product_tree.parent, product_tree.parent_code))
product_tree.replace({"primary_commodity": product_dict}, inplace=True)
# Replace product keys with names
product_tree["parent"] = product_tree.parent.str.replace("_", " ").str.capitalize()
product_tree["child"] = product_tree.child.str.replace("_", " ").str.capitalize()
# Rename columns
column_rename_dict = {
    "primary_commodity": "primary_product_code",
    "parent": "parent_product_name",
    "parent_code": "parent_product_code",
    "child": "child_product_name",
    "child_code": "child_product_code",
    "bp": "bp_flag",
}
product_tree.rename(columns=column_rename_dict, inplace=True)
# Select faostat table
faostat_table = faostat.db.tables["crop_trade"]
# Select product names and codes from Faostat db
stmt = (
    faostat_table.select()
    .with_only_columns([faostat_table.c.product, faostat_table.c.product_code])
    .distinct(faostat_table.c.product, faostat_table.c.product_code)
)
# Retrieve dataset and replace product keys with names
faostat_products = pd.read_sql_query(stmt, faostat.db.engine)
faostat_products["product"] = (
    faostat_products["product"].str.replace("_", " ").str.capitalize()
)
# Rename column
faostat_products.rename(columns={"product": "product_name"}, inplace=True)
# Add key product flag
faostat_products["key_product_flag"] = faostat_products.product_code.isin(
    key_product_codes
).astype(int)
# Save csv files
folder_path = data_dir / "front_end"
if not folder_path.exists():
    folder_path.mkdir()
product_tree.to_csv(folder_path / "key_product_tree_list.csv", index=False)
faostat_products.to_csv(folder_path / "product_list.csv", index=False)
