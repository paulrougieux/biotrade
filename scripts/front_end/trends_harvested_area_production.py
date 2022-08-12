"""
Written by Selene Patani.

Script made to export trends related to harvested area/production of countries associated to the key commodities, for the web platform

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
# Add China data to crop_data
crop_data = pd.concat([crop_data, df_china], ignore_index=True)
# Substitute faostat codes with iso3 codes
crop_data = reporter_iso_codes(crop_data)
# Consider data after 1985 to calculate trends of last year
crop_data = crop_data[crop_data["year"] > 1985]
# Perform trend analysis
df = trend_analysis(crop_data)
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
# Drop nan values
dropna_col = ["relative_change", "absolute_change", "mk_slope"]
df = replace_zero_with_nan_values(df, dropna_col)
df = df.dropna(subset=dropna_col, how="all")
# Harvested area data (only the most recent year of db)
most_recent_year = sorted(df.year.unique(), reverse=True)[0]
harvested_area = df[
    (df["element"] == "area_harvested") & (df["year"] == most_recent_year)
][column_list]
# Production data (only the most recent year of db)
production = df[
    (df["element"].isin(["production", "stocks"])) & (df["year"] == most_recent_year)
][column_list]
save_file(harvested_area, "harvested_area_trends.csv")
save_file(production, "production_trends.csv")
