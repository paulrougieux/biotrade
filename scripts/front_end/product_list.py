"""
Written by Selene Patani.

Script made to export data related to all commodities and tree of the key products, for the web platform

"""

import pandas as pd
from biotrade.faostat import faostat
from functions import save_file, main_product_list, comtrade_products

# Name of product file to retrieve
faostat_key_commodities_file = (
    faostat.config_data_dir / "faostat_commodity_tree.csv"
)
# Retrieve tree dataset
product_tree = pd.read_csv(faostat_key_commodities_file)
# Obtain the main product codes
main_product_list = main_product_list()
# Filter and rename columns
column_rename_dict = {
    "primary_commodity_code": "primary_product_code",
    "parent_code": "parent_product_code",
    "child_code": "child_product_code",
    "bp": "bp_flag",
}
product_tree = product_tree[column_rename_dict.keys()]
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
# Add key product flag (including maize, rubber and wood)
faostat_products["key_product_flag"] = faostat_products.product_code.isin(
    [*product_tree.primary_product_code.unique().tolist(), 56, 836, 1864, 1865]
).astype(int)
# Filter only for regulation products (final decision)
faostat_products = faostat_products[
    faostat_products.product_code.isin(main_product_list)
]
# Save csv files to env variable path or into biotrade data folder
save_file(product_tree, "key_product_tree_list.csv")
save_file(faostat_products, "product_list.csv")
# Retrieve regulation products and save the file
comtrade_list = comtrade_products()
columns = ["product_code", "product_name"]
comtrade_list = comtrade_list.drop_duplicates(subset=columns)[
    columns
].reset_index(drop=True)
save_file(comtrade_list, "comtrade_product_list.csv")
