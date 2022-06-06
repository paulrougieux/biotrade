"""
Written by Selene Patani.

Script made to export data related to all commodities and tree of the key products, for the web platform

"""

import numpy as np
import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat
import os
from pathlib import Path

# Name of product file to retrieve
faostat_key_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve tree dataset
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
# Select product names and codes from Faostat tables
table_list = [
    "crop_production",
    "crop_trade",
    "forestry_production",
    "forestry_trade",
]
faostat_products = faostat.db.extract_product_names_codes(table_list)
# Replace product keys with names
faostat_products["product"] = (
    faostat_products["product"].str.replace("_", " ").str.capitalize()
)
# Rename column
faostat_products.rename(columns={"product": "product_name"}, inplace=True)
# Add -1 - Others as product code and product name for average data
faostat_products = pd.concat(
    [
        faostat_products,
        pd.DataFrame([[-1, "Others"]], columns=faostat_products.columns),
    ],
    ignore_index=True,
)
# Add key product flag
faostat_products["key_product_flag"] = faostat_products.product_code.isin(
    key_product_codes
).astype(int)
# Save csv files to env variable path or into biotrade data folder
if os.environ.get("FRONT_END_DATA"):
    folder_path = Path(os.environ["FRONT_END_DATA"])
else:
    folder_path = data_dir / "front_end"
folder_path.mkdir(exist_ok=True)
product_tree.to_csv(folder_path / "key_product_tree_list.csv", index=False)
faostat_products.to_csv(folder_path / "product_list.csv", index=False)
