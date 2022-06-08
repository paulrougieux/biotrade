"""
Written by Selene Patani.

Script made to export trends related to harvested area/production of countries associated to the key commodities, for the web platform

"""

import numpy as np
import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat
from biotrade.common.time_series import (
    relative_absolute_change,
    segmented_regression,
    merge_analysis,
)
import os
from pathlib import Path

# Name of product file to retrieve
faostat_key_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve dataset
df = pd.read_csv(faostat_key_commodities_file)
# Retrieve parent and child codes
parent_codes = df["parent_code"].unique().tolist()
child_codes = df["child_code"].unique().tolist()
# Union of the codes without repetitions
key_product_codes = np.unique(parent_codes + child_codes).tolist()
# Query db to obtain most recent year of db and data
most_recent_year = faostat.db.most_recent_year("crop_production")
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
crop_china["reporter"] = "China"
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
# Consider data after 1985 to calculate trends of last year
crop_data = crop_data[crop_data["year"] > 1985]
# Calculate the absolute and relative change
crop_data_change = relative_absolute_change(crop_data, last_value=True)
# Use as objective function the coefficient of determination (R2), significance level of 0.05 and at least 7 points for the linear regression
crop_data_regression = segmented_regression(
    crop_data, last_value=True, function="R2", alpha=0.05, min_data_points=7,
)
# Merge dataframes to compare results
df = merge_analysis(
    df_change=crop_data_change, df_segmented_regression=crop_data_regression
)
# Define the structure yyyy-yyyy for the period change and segmented regression columns
df["period_change"] = (
    df["year_range_lower_change"].astype("Int64").astype(str)
    + "-"
    + df["year_range_upper_change"].astype("Int64").astype(str)
).replace("<NA>-<NA>", np.nan)
df["period_regression"] = (
    df["year_range_lower_regression"].astype("Int64").astype(str)
    + "-"
    + df["year_range_upper_regression"].astype("Int64").astype(str)
).replace("<NA>-<NA>", np.nan)
# Transform from boolean to integers 0 or 1
df["mk_significance_flag"] = (
    df["mk_ha_test"].astype("boolean").astype("Int64")
).replace(pd.NA, np.nan)
# Drop nan values
df = df.dropna(subset=["relative_change", "absolute_change", "mk_slope"], how="all")
# Columns to be retained
column_list = [
    "reporter_code",
    "product_code",
    "period",
    "period_change",
    "period_regression",
    "relative_change",
    "absolute_change",
    "mk_slope",
    "mk_significance_flag",
    "unit",
]
# Harvested area data (only the most recent year of db)
harvested_area = df[
    (df["element"] == "area_harvested") & (df["year"] == most_recent_year)
][column_list]
# Production data (only the most recent year of db)
production = df[
    (df["element"].isin(["production", "stocks"])) & (df["year"] == most_recent_year)
][column_list]
# Save csv files to env variable path or into biotrade data folder
if os.environ.get("FRONT_END_DATA"):
    folder_path = Path(os.environ["FRONT_END_DATA"])
else:
    folder_path = data_dir / "front_end"
folder_path.mkdir(exist_ok=True)
harvested_area.to_csv(folder_path / "harvested_area_trends.csv", index=False)
production.to_csv(folder_path / "production_trends.csv", index=False)
