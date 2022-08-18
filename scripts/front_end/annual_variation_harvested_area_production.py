"""
Written by Selene Patani.

Script made to export data related to harvested area/production of countries associated to the key commodities, for the web platform

"""

from functions import *

# Obtain the main product codes
main_product_list = main_product_list()
# Select quantities from Faostat db for crop data for all countries (code < 1000)
crop_data = faostat.db.select(
    table="crop_production",
    product_code=main_product_list,
    element=["production", "area_harvested", "stocks"],
)
crop_data = crop_data[crop_data["reporter_code"] < 1000]
# China Mainland + Taiwan data
df_china = consistency_check_china_data(crop_data)
# Add China data to crop_data (exclude Taiwan data)
crop_data = pd.concat(
    [crop_data[~(crop_data["reporter_code"] == 214)], df_china], ignore_index=True
)
# Substitute faostat codes with iso3 codes
crop_data = reporter_iso_codes(crop_data)
# Columns to be retained
column_list = ["reporter_code", "product_code", "period", "value", "unit"]
# Harvested area data
dropna_col = ["value"]
crop_data = replace_zero_with_nan_values(crop_data, dropna_col)
crop_data = crop_data.dropna(subset=dropna_col)
harvested_area = crop_data[crop_data["element"] == "area_harvested"][column_list]
# Production data
production = crop_data[crop_data["element"].isin(["production", "stocks"])][column_list]
# Save csv files to env variable path or into biotrade data folder
save_file(harvested_area, "harvested_area_annual_variation.csv")
save_file(production, "production_annual_variation.csv")
