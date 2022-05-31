"""
Written by Selene Patani.

Script made to export data related to trade quantities of countries associated to the key commodities, for the web platform

"""

import numpy as np
import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat
from biotrade.common.compare import merge_faostat_comtrade

# Name of product file to retrieve
faostat_key_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve dataset
df = pd.read_csv(faostat_key_commodities_file)
# Retrieve parent and child codes
parent_codes = df["parent_code"].unique().tolist()
child_codes = df["child_code"].unique().tolist()
# Union of the codes without repetitions
key_product_codes = np.unique(parent_codes + child_codes).tolist()
# Select quantities from Faostat db and Comtrade for trade data for all countries (code < 1000)
trade_data_yearly = merge_faostat_comtrade("crop_trade", "yearly", key_product_codes)
trade_data_monthly = merge_faostat_comtrade("crop_trade", "monthly", key_product_codes)
# Merge both yearly and aggregated monthly data
merge_index_list = [
    "source",
    "reporter_code",
    "reporter",
    "partner_code",
    "partner",
    "product_code",
    "product",
    "element_code",
    "element",
    "year",
    "unit",
]
trade_data = trade_data_yearly.merge(
    trade_data_monthly,
    how="outer",
    on=merge_index_list,
    suffixes=("_yearly", "_monthly"),
)
# Remove nan reporters/partners (=-1)
trade_data = trade_data[
    ~(trade_data[["reporter_code", "partner_code"]] == -1).any(axis=1)
]
# Select only importers as repoter' quantities
trade_data = trade_data[trade_data["element"].isin(["import_value", "import_quantity"])]
# Consider monthly aggregations for most recent year of comtrade data and yearly data the the rest
selector = trade_data["flag_monthly"] == "estimate"
trade_data.loc[selector, "value"] = trade_data.loc[selector, "value_monthly"]
trade_data.loc[selector, "flag"] = trade_data.loc[selector, "flag_monthly"]
trade_data.loc[~selector, "value"] = trade_data.loc[~selector, "value_yearly"]
trade_data.loc[~selector, "flag"] = ""
trade_data["period"] = trade_data["year"]
trade_data.drop(
    columns=["value_monthly", "value_yearly", "flag_monthly", "flag_yearly"],
    inplace=True,
)
# Define China data with isocode CHN and Faostat code 357 from China mainland (41) + Taiwan (214) data both from reporter and partner
trade_china = trade_data[
    trade_data[["reporter_code", "partner_code"]].isin([41, 214]).any(axis=1)
]
# Aggregation on the reporter side
trade_china_1 = (
    trade_china[trade_china["reporter_code"].isin([41, 214])]
    .groupby(["source", "partner_code", "product_code", "element", "period", "unit"])
    .agg({"value": lambda x: x.sum(min_count=1)})
    .reset_index()
)
trade_china_1["reporter_code"] = 357
# Concat with reporter codes not 41 either 214
trade_china_1 = pd.concat(
    [trade_china[~trade_china["reporter_code"].isin([41, 214])], trade_china_1],
    ignore_index=True,
)
# Aggregation also on the partner side
trade_china_2 = (
    trade_china_1[trade_china_1["partner_code"].isin([41, 214])]
    .groupby(["source", "reporter_code", "product_code", "element", "period", "unit"])
    .agg({"value": lambda x: x.sum(min_count=1)})
    .reset_index()
)
trade_china_2["partner_code"] = 357
# Concat with partner codes not 41 and 214
trade_china_2 = pd.concat(
    [trade_china_1[~trade_china_1["partner_code"].isin([41, 214])], trade_china_2],
    ignore_index=True,
)
# Add China and Taiwan data to trade_data
trade_data = pd.concat([trade_data, trade_china_2], ignore_index=True,)
# Reporter codes
reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
reporter = pd.read_csv(reporter_file)
# Obtain iso3 codes with the merge on reporter and partner side
trade_data = trade_data.merge(
    reporter[["faost_code", "iso3_code"]],
    how="left",
    left_on=["reporter_code"],
    right_on=["faost_code"],
)
trade_data["reporter_code"] = trade_data["iso3_code"]
trade_data.drop(columns=["faost_code", "iso3_code"], inplace=True)
trade_data = trade_data.merge(
    reporter[["faost_code", "iso3_code"]],
    how="left",
    left_on=["partner_code"],
    right_on=["faost_code"],
)
trade_data["partner_code"] = trade_data["iso3_code"]
# Faostat code 41 (China mainland) and 351 (China mainland + Hong Kong + Macao + Taiwan ) are not mapped into ISO 3 Codes
trade_data.dropna(subset=["reporter_code", "partner_code"], inplace=True)
# Columns to be retained
column_list = [
    "source",
    "reporter_code",
    "partner_code",
    "product_code",
    "period",
    "value",
    "unit",
    "flag",
]
# Trade quantity import data
trade_quantity_import = trade_data[trade_data["element"] == "import_quantity"][
    column_list
]
# Trade value import data
trade_value_import = trade_data[trade_data["element"] == "import_value"][column_list]
# Save csv files
folder_path = data_dir / "front_end"
folder_path.mkdir(exist_ok=True)
trade_quantity_import.to_csv(
    folder_path / "trade_quantity_import_annual_variation.csv", index=False
)
trade_value_import.to_csv(
    folder_path / "trade_value_import_annual_variation.csv", index=False
)
