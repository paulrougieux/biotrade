"""
Written by Selene Patani.

Script made to export trends related to bilateral trades of countries associated to palm oil, soybeans, cocoa beans, coffee roasted, meat cattle, hides cattle fresh
"""

from functions import *
from biotrade.common.time_series import (
    relative_absolute_change,
    segmented_regression,
    merge_analysis,
)

# Selected product codes
product_list = [236, 257, 657, 661, 867, 919]
# Trade data related to product code list
trade_data = merge_faostat_comtrade_data(product_list)
# China Mainland + Taiwan data
df_china = consistency_check_china_data(trade_data)
# Add China data to trade_data
trade_data = pd.concat([trade_data, df_china,], ignore_index=True,)
# Substitute faostat codes with iso3 codes
trade_data = reporter_iso_codes(trade_data)
# Consider data after 1985 to calculate trends of last year (excluded estimated values)
trade_data = trade_data[
    (trade_data["year"] > 1985) & (trade_data["estimate_flag"] == 0)
]
# Calculate the absolute and relative change
trade_data_change = relative_absolute_change(trade_data, last_value=True)
# Use as objective function the coefficient of determination (R2), significance level of 0.05 and at least 7 points for the linear regression
trade_data_regression = segmented_regression(
    trade_data, last_value=True, function="R2", alpha=0.05, min_data_points=7,
)
# Merge dataframes to compare results
df = merge_analysis(
    df_change=trade_data_change, df_segmented_regression=trade_data_regression
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
    "partner_code",
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
# Define most recent year for faostat and comtrade data
trade_data_faostat = df[df["source"] == "faostat"]
most_recent_year_faostat = sorted(trade_data_faostat.year.unique(), reverse=True)[0]
trade_data_comtrade = df[df["source"] == "comtrade"]
most_recent_year_comtrade = sorted(trade_data_comtrade.year.unique(), reverse=True)[0]
# Consider selected columns of import quantities and save the file
trade_data_faostat_reporter = trade_data_faostat[
    (trade_data_faostat["year"] == most_recent_year_faostat)
    & (trade_data_faostat["element"] == "import_quantity")
][column_list]
save_file(trade_data_faostat_reporter, "faostat_trade_quantity_trends.csv")
# Consider selected columns of import quantities and save the file
trade_data_comtrade_reporter = trade_data_comtrade[
    (trade_data_comtrade["year"] == most_recent_year_comtrade)
    & (trade_data_comtrade["element"] == "import_quantity")
][column_list]
save_file(trade_data_comtrade_reporter, "comtrade_trade_quantity_trends.csv")
# Consider selected columns of export quantities and save the file --> mirror flows
trade_data_faostat_partner = trade_data_faostat[
    (trade_data_faostat["year"] == most_recent_year_faostat)
    & (trade_data_faostat["element"] == "export_quantity")
][column_list]
save_file(trade_data_faostat_partner, "faostat_trade_quantity_trends_mf.csv")
# Consider selected columns of export quantities and save the file --> mirror flows
trade_data_comtrade_partner = trade_data_comtrade[
    (trade_data_comtrade["year"] == most_recent_year_comtrade)
    & (trade_data_comtrade["element"] == "export_quantity")
][column_list]
save_file(trade_data_comtrade_partner, "comtrade_trade_quantity_trends_mf.csv")
