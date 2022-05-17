"""
Written by Selene Patani.

Script made to export data of products and countries (averaging over periods of 5 years except last year of db) related to harvested area/production, for the web platform

"""

import numpy as np
import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat

# Value of the threshold for products/reporters
threshold = 95
# Connect to faostat db
db = faostat.db
# Query db to obtain most recent year of db and data
most_recent_year = db.most_recent_year("crop_production")
df = db.select(table="crop_production")
# Select only area harvested/production data and reporter with code lower
# than 1000
elements = ["area_harvested", "production", "stocks"]
df = df[(df["element"].isin(elements)) & (df["reporter_code"] < 1000)].reset_index(
    drop=True
)
# Produce China data with isocode CHN and Faostat code 357 from China mainland (41) + Taiwan (214) data
df_china = df[df["reporter_code"].isin([41, 214])]
df_china = (
    df_china.groupby(["product_code", "element", "year", "unit"])
    .agg({"value": "sum"})
    .reset_index()
)
df_china["reporter_code"] = 357
# Add China data to df
df = pd.concat([df, df_china], ignore_index=True)
# Reporter codes
reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
reporter = pd.read_csv(reporter_file)
# Obtain iso3 codes
df = df.merge(
    reporter[["faost_code", "iso3_code"]],
    how="left",
    left_on="reporter_code",
    right_on="faost_code",
)
df["reporter_code"] = df["iso3_code"]
# Faostat code 41 (China mainland) and 351 (China mainland + Hong Kong + Macao + Taiwan ) are not mapped into ISO 3 Codes
df.dropna(subset="reporter_code", inplace=True)
# Consider all the years of the table except the most recent (no aggregation for it)
years = sorted(df.year.unique(), reverse=True)[1:]
# Define aggregation of 5 years at a time, starting from the most recent year - 1
periods = np.array(range(0, len(years))) // 5 + 1
# Construct the df_periods containing the periods yyyy-yyyy and merge it with df
df_periods = pd.DataFrame(
    {"year": [most_recent_year, *years], "period_aggregation": [0, *periods]}
)
# Define the min year inside each period
df_periods_min = (
    df_periods.groupby(["period_aggregation"]).agg({"year": "min"}).reset_index()
)
# Define the max year inside each period
df_periods_max = (
    df_periods.groupby(["period_aggregation"]).agg({"year": "max"}).reset_index()
)
# Merge on the periods
df_periods = df_periods.merge(
    df_periods_min, how="left", on="period_aggregation", suffixes=("", "_min")
)
df_periods = df_periods.merge(
    df_periods_max, how="left", on="period_aggregation", suffixes=("", "_max")
)
# Define the structure yyyy-yyyy for the period aggregation column
df_periods["period_aggregation"] = (
    df_periods["year_min"].astype(str) + "-" + df_periods["year_max"].astype(str)
)
# Assign the associated period to each data
df = df.merge(df_periods[["year", "period_aggregation"]], on="year", how="left")
df["period"] = df["period_aggregation"]
# Default index list for aggregations
index_list = ["element", "period", "unit"]
df_final = pd.DataFrame()
for column in ["reporter_code", "product_code"]:
    # Define columns for the two cases
    index_list_upd = [column, *index_list]
    average_col_name = "average_value"
    total_col_name = "total_value"
    percentage_col_name = "value_percentage"
    if column == "reporter_code":
        average_col_name = average_col_name + "_reporter"
        total_col_name = total_col_name + "_reporter"
        aggregation_column = "product_code"
        percentage_col_name = percentage_col_name + "_product"
        other_code = -1
    else:
        average_col_name = average_col_name + "_product"
        total_col_name = total_col_name + "_product"
        aggregation_column = "reporter_code"
        percentage_col_name = percentage_col_name + "_reporter"
        other_code = "OTH"
    # For products and reporters calculate mean and sum of the values in the given period aggregation, related to a specific unit and element
    df_mean = (
        df.groupby([*index_list_upd, "year"])
        .agg({"value": "sum"})
        .reset_index()
        .groupby(index_list_upd)
        .agg({"value": "mean"})
        .reset_index()
        .rename(columns={"value": average_col_name})
    )
    df_total = (
        df.groupby(index_list_upd)
        .agg({"value": "sum"})
        .reset_index()
        .rename(columns={"value": total_col_name})
    )
    # Products/reporters aggregated with the column list
    df_new = (
        df.groupby([*index_list_upd, aggregation_column])
        .agg({"value": "sum"})
        .reset_index()
    )
    # Merge with mean and total values
    df_new = df_new.merge(df_mean, how="left", on=index_list_upd)
    df_new = df_new.merge(df_total, how="left", on=index_list_upd)
    # Percentage associated to the product/reporter on a given aggregated period
    df_new[percentage_col_name] = df_new["value"] / df_new[total_col_name] * 100
    # Sort by percentage, compute the cumulative sum and shift it by one
    df_new.sort_values(
        by=[*index_list_upd, percentage_col_name],
        ascending=False,
        inplace=True,
        ignore_index=True,
    )
    # Skip nan values is True for cumsum by default
    df_new["cumsum"] = df_new.groupby(index_list_upd)[percentage_col_name].cumsum()
    df_new["cumsum_lag"] = df_new.groupby(index_list_upd)["cumsum"].transform(
        "shift", fill_value=0
    )
    # Label above the threshold 'Others'
    # Create a grouping variable instead of product_code/reporter_code, which will be 'Others' for
    # values above the threshold
    df_new[aggregation_column] = df_new[aggregation_column].where(
        df_new["cumsum_lag"] < threshold, other_code
    )
    # Group products/reporters that are in the 'Others' category and calculate their percentage
    df_new = (
        df_new.groupby([*index_list_upd, aggregation_column])
        .agg({"value": "sum", average_col_name: "first", total_col_name: "first",})
        .reset_index()
    )
    df_new[percentage_col_name] = df_new["value"] / df_new[total_col_name] * 100
    if df_final.empty:
        df_final = df_new
    else:
        df_final = df_final.merge(
            df_new, how="outer", on=[*index_list_upd, aggregation_column]
        )
# Columns to keep
column_list = [
    "period",
    "reporter_code",
    "product_code",
    "average_value_reporter",
    "value_percentage_product",
    "average_value_product",
    "value_percentage_reporter",
    "unit",
]
# Save csv files
folder_path = data_dir / "front_end"
folder_path.mkdir(exist_ok=True)
harvested_area = df_final[df_final["element"] == "area_harvested"][column_list]
harvested_area.to_csv(folder_path / "harvested_area_average.csv", index=False)
production = df_final[df_final["element"].isin(["production", "stocks"])][column_list]
production.to_csv(folder_path / "production_average.csv", index=False)
