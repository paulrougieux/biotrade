"""
Written by Selene Patani.

Script made to export data of products and countries (averaging over periods of 5 years except last year of db) related to harvested area/production, for the web platform

"""

from functions import *

# Query db to obtain data
df = faostat.db.select(table="crop_production")
# Select only area harvested/production data and reporter with code lower
# than 1000
elements = ["area_harvested", "production", "stocks"]
df = df[(df["element"].isin(elements)) & (df["reporter_code"] < 1000)].reset_index(
    drop=True
)
# China Mainland + Taiwan data
df_china = consistency_check_china_data(df)
# Add China data to df (exclude Taiwan data, otherwise double counted into percentages and averages calculations with China + Taiwan)
df = pd.concat([df[~(df["reporter_code"] == 214)], df_china], ignore_index=True)
# Substitute faostat codes with iso3 codes
df = reporter_iso_codes(df)
# Define the columns and codes for the average calculations
dict_list = [
    {
        "average_col": "reporter_code",
        "percentage_col": "product_code",
        "threshold_code": -1,
        "index_list_add": [],
    },
    {
        "average_col": "product_code",
        "percentage_col": "reporter_code",
        "threshold_code": "OTH",
        "index_list_add": [],
    },
]
# Calculate the averages and percentages
df_final = average_results(df, 95, dict_list,)
# Columns to keep
drop_column = "element"
column_list = df_final.columns.tolist()
column_list.remove(drop_column)
# Define dropna columns
dropna_col = [col for col in df_final.columns if col.endswith(COLUMN_PERC_SUFFIX)]
# Save csv files to env variable path or into biotrade data folder
harvested_area = df_final[df_final["element"] == "area_harvested"][column_list].dropna(
    subset=dropna_col, how="all",
)
save_file(harvested_area, "harvested_area_average.csv")
production = df_final[df_final["element"].isin(["production", "stocks"])][
    column_list
].dropna(subset=dropna_col, how="all",)
save_file(production, "production_average.csv")
