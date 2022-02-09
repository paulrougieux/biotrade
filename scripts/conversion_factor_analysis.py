"""
Written by Selene Patani

Script used to get statistics of extraction rates and waste of supplies
"""

import pandas as pd
import re
import numpy as np
from pathlib import Path

# Renaming compiler
regex_pat = re.compile(r"\W+")
file_name_fao = Path.cwd() / "scripts" / "TCF_Conversion_Factors_fao.xlsx"
# FAO coefficients
df = pd.read_excel(file_name_fao)
# Fuzzy match table
match1 = pd.read_csv(Path.cwd() / "scripts" / "fuzzy_match.csv", encoding="utf-8")
# Select meat columns and coefficients to store them into "meat_conversion_factor_coefficients.csv"
meat_coefficients = pd.concat([df[df.columns[0:4]], df[df.columns[7:17]]], axis=1)
meat_coefficients = meat_coefficients[meat_coefficients.isnull().sum(axis=1) < 10]
meat_coefficients.to_csv(
    Path.cwd() / "scripts" / "meat_conversion_factor_coefficients.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
# Select columns of interest for extraction rate and waste of supply
df = df[
    [
        "M49_code",
        "Region",
        "Country_name",
        "Item_description",
        "Extraction_rates_%",
        "Waste_of_supply_%",
        "Carcass_weight_as_%_of_live_weight",
        "Offals_content_of_live_weight_%",
        "Fats_content_of_live_weight_%",
        "Hides_content_of_live_weight_%",
    ]
]
# Renaming products description
df["Item_description"] = (
    df["Item_description"].str.replace(regex_pat, "_", regex=True).str.lower()
)
# Change match list: [product code old, product new, product code new]
manual_match_to_change = [
    [866, "meat_cattle", 867],
]
# For each match into change list, substitute values into match1 dataframe
for change in manual_match_to_change:
    # Find indices of df through product code which has to change
    indices = match1[match1["product_code"] == change[0]].index
    for index in indices:
        match1["match"][index] = np.nan
        match1["score"][index] = np.nan
        match1["product"][index] = change[1]
        match1["product_code"][index] = change[2]
# Add match list: [product original new, product new, product code new]
manual_match_to_add = [
    ["margarine_short", "margarine_short", 1242],
    ["margarine_liquid", "margarine_liquid", 1241],
    ["fat_nes_prepared", "fat_nes_prepared", 1243],
    ["oil_boiled_etc", "oil_boiled_etc", 1274],
    ["fatty_acids", "fatty_acids", 1276],
    ["oil_palm_fruit", "oil_palm_fruit", 254],
    ["palm_kernels", "palm_kernels", 256],
    ["offals_of_cattle_edible", "offals_edible_cattle", 868],
    ["cattle_hides_fresh", "hides_cattle_fresh", 919],
    ["meat_cattle_boneless_beef_veal", "meat_cattle_boneless_beef_veal", 870],
    ["meat_beef_and_veal_sausages", "meat_beef_and_veal_sausages", 874],
    ["meat_beef_preparations", "meat_beef_preparations", 875],
    ["meat_dried_nes", "meat_dried_nes", 1164],
    ["oils_fats_of_animal_nes", "oils_fats_of_animal_nes", 1168],
    ["tallow", "tallow", 1225],
    ["hides_cattle_wet_salted", "hides_cattle_wet_salted", 920],
    ["skins_calve_wet_salted", "skins_calve_wet_salted", 928],
    ["coffee_husks_and_skins", "coffee_husks_and_skins", 660],
    ["soya_curd", "soy_curd", 241],
    ["flour_oilseeds", "flour_oilseeds", 343],
    ["hydrogenate_fat_oil", "hydrogenate_fat_oil", 1275],
    ["cocoa_husks_shell", "cocoa_husks_shell", 663],
    ["cattle_butcher_fat", "cattle_butcher_fat", 871],
    ["beef_dry_smoked", "beef_dry_smoked", 872],
    ["meat_extracts", "meat_extracts", 873],
    ["beef_canned", "beef_canned", 876],
    ["homogenized_meat_prepared", "homogenized_meat_prepared", 877],
    ["meat_prepared", "meat_prepared", 1172],
    ["liver_prepared", "liver_prepared", 878],
    ["hides_nes_cattle", "hides_nes_cattle", 922],
    ["skins_nes_calves", "skins_nes_calves", 930],
    ["cattle_live_weight", "cattle", 866],
]
# For each match into add list, put new values into match1 dataframe
for match in manual_match_to_add:
    match1 = match1.append(
        {"original": match[0], "product": match[1], "product_code": match[2]},
        ignore_index=True,
    )
# Store final matches as a csv file "fuzzy_match_manual_gf.csv"
match1.to_csv(
    Path.cwd() / "scripts" / "fuzzy_match_manual_gf.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
# Merge the dataframe with matches
df = df.merge(match1, how="outer", left_on="Item_description", right_on="original")
del df["original"]
# Check matches for major products
major_products = ["palm", "soy", "sun", "rapeseed", "cocoa", "coffee", "cattle"]
df_main_products = (
    df[df["product"].str.contains("|".join(major_products), na=False)][
        ["Item_description", "match", "score", "product", "product_code"]
    ]
    .drop_duplicates()
    .reset_index(drop=True)
)
df_main_products = df_main_products.rename(columns={"Item_description": "original"})
# Store the main products matches as a csv file
df_main_products.to_csv(
    Path.cwd() / "scripts" / "main_products_match.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
# Consider as extraction rates also coefficients related to meat extraction
df["Extraction_rates_%"] = df.filter(
    [
        "Extraction_rates_%",
        "Carcass_weight_as_%_of_live_weight",
        "Offals_content_of_live_weight_%",
        "Fats_content_of_live_weight_%",
        "Hides_content_of_live_weight_%",
    ]
).max(1)
# Divide percentages by 100
df["Extraction_rates_%"] = df["Extraction_rates_%"] / 100
df["Waste_of_supply_%"] = df["Waste_of_supply_%"] / 100
# Flag extraction rates reported for countries
df["extraction_rate_country_specific_flag"] = (
    df["Extraction_rates_%"].notnull().astype(int)
)
# Rename columns
df = df.rename(
    columns={
        "M49_code": "fao_country_code",
        "Country_name": "fao_country_name",
        "Extraction_rates_%": "extraction_rate",
        "Waste_of_supply_%": "waste_of_supply",
        "product": "fao_product",
        "product_code": "fao_product_code",
    }
)
# Select columns of interest
df = df[
    [
        "fao_country_code",
        "fao_country_name",
        "extraction_rate",
        "waste_of_supply",
        "fao_product",
        "fao_product_code",
        "extraction_rate_country_specific_flag",
    ]
]
# Store data into faostat_agricultural_conversion_factors.csv
df["fao_product_code"] = df.fao_product_code.astype("Int64")
df["fao_country_code"] = df.fao_country_code.astype("Int64")
df = df.sort_values(by=["fao_country_code", "fao_product_code"])
df.to_csv(
    Path.cwd()
    / "biotrade"
    / "config_data"
    / "faostat_agricultural_conversion_factors.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
# Extract statistics (minimum, maximum, mean, standard deviation and sample size) of a commodity for extraction rate and waste of supply
statistics = df.groupby(["fao_product_code", "fao_product"], as_index=False).agg(
    {
        "extraction_rate": ["mean", "min", "max", "std", "count"],
        "waste_of_supply": ["mean", "min", "max", "std", "count"],
    }
)
# Rename column names of statistics dataframe
column_list = []
for column_name in statistics.columns.values:
    if column_name[1] == "":
        final_column_name = column_name[0]
    else:
        final_column_name = "_".join(column_name)
    column_list.append(final_column_name)
statistics.columns = column_list
# Add a column to define which product statistics are obtained by calculations from faostat_agricultural_conversion_factors.csv,
# in order to separate data obtained considering also the commodity tree
statistics["extraction_rate_flag"] = (
    statistics["extraction_rate_mean"].notnull().astype(int)
)
statistics["waste_of_supply_flag"] = (
    statistics["waste_of_supply_mean"].notnull().astype(int)
)
# Save into csv as global_extraction_rate_statics.csv into config_data folder
statistics.to_csv(
    Path.cwd() / "biotrade" / "config_data" / "global_extraction_rate_statistics.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
# Global average values of extraction rates missed for major commodities are taken from technical report. Still missed value for product codes =
# [1274,1164,1168,660,343,876,877,878,930].
# One of the value type reported below is used (in order of consideration) from pag. 722 of the technical report:
# 1) word average value
# 2) the mean of interval
# 3) the approximated value
# Add average commodity list: [product code, mean extraction rate]
commodity_tree_list = [
    [1242, 1.13],
    [1241, (1.1 + 1.5) / 2],
    [1243, 1.0],
    [1276, 0.98],
    [870, 0.71],
    [874, 0.8],
    [875, 0.6],
    [1225, 0.82],
    [920, 0.8],
    [928, 0.8],
    [1275, 1.07],
    [871, 0.12],
    [872, 0.46],
    [873, 0.2],
    [1172, 0.9],
    [922, 0.8],
]
# Substitute nan values of mean extraction rates with commodity tree values
for commodity in commodity_tree_list:
    index = statistics[statistics["fao_product_code"] == commodity[0]].index
    statistics["extraction_rate_mean"][index] = commodity[1]
# Sort by product codes
statistics = statistics.sort_values(by="fao_product_code")
# Save new df into "global_extraction_rate_statistics_manually_gf.csv"
statistics.to_csv(
    Path.cwd()
    / "biotrade"
    / "config_data"
    / "global_extraction_rate_statistics_manual_gf.csv",
    encoding="utf-8",
    index=False,
    na_rep="NA",
)
