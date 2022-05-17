"""
Written by Selene Patani.

Script made to export data related to harvested area/production of countries associated to the key commodities, for the web platform

"""

import numpy as np
import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat

# Name of product file to retrieve
faostat_key_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve dataset
df = pd.read_csv(faostat_key_commodities_file)
# Retrieve parent and child codes
parent_codes = df["parent_code"].unique().tolist()
child_codes = df["child_code"].unique().tolist()
# Union of the codes without repetitions
key_product_codes = np.unique(parent_codes + child_codes).tolist()
# Select quantities from Faostat db for crop data for all countries (code < 1000)
crop_data = faostat.db.select(
    table="crop_production",
    product_code=key_product_codes,
    element=["production", "area_harvested", "stocks"],
)
crop_data = crop_data[crop_data["reporter_code"] < 1000]
# Define China data with isocode CHN and Faostat code 357 from China mainland (41) + Taiwan (214) data
crop_china = crop_data[crop_data["reporter_code"].isin([41, 214])]
crop_china = (
    crop_china.groupby(["product_code", "element", "period", "unit"])
    .agg({"value": "sum"})
    .reset_index()
)
crop_china["reporter_code"] = 357
# Add China data to crop_data
crop_data = pd.concat([crop_data, crop_china], ignore_index=True)
# Reporter codes
reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
reporter = pd.read_csv(reporter_file)
# Obtain iso3 codes with the merge
crop_data = crop_data.merge(
    reporter[["faost_code", "iso3_code"]],
    how="left",
    left_on="reporter_code",
    right_on="faost_code",
)
crop_data["reporter_code"] = crop_data["iso3_code"]
# Faostat code 41 (China mainland) and 351 (China mainland + Hong Kong + Macao + Taiwan ) are not mapped into ISO 3 Codes
crop_data.dropna(subset="reporter_code", inplace=True)
# Columns to be retained
column_list = ["reporter_code", "product_code", "period", "value", "unit"]
# Harvested area data
harvested_area = crop_data[crop_data["element"] == "area_harvested"][column_list]
# Production data
production = crop_data[crop_data["element"].isin(["production", "stocks"])][column_list]
# Save csv files
folder_path = data_dir / "front_end"
folder_path.mkdir(exist_ok=True)
harvested_area.to_csv(folder_path / "harvested_area_annual_variation.csv", index=False)
production.to_csv(folder_path / "production_annual_variation.csv", index=False)
