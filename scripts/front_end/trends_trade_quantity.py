"""
Written by Selene Patani.

Script made to export trends related to bilateral trades of countries associated to palm oil, soybeans, cocoa beans, coffee roasted, meat cattle, hides cattle fresh
"""

from functions import *

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
# Perform trend analysis
df = trend_analysis(trade_data)
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
save_file(trade_data_faostat_reporter, "faostat_trends.csv")
# Consider selected columns of import quantities and save the file
trade_data_comtrade_reporter = trade_data_comtrade[
    (trade_data_comtrade["year"] == most_recent_year_comtrade)
    & (trade_data_comtrade["element"] == "import_quantity")
][column_list]
save_file(trade_data_comtrade_reporter, "comtrade_trends.csv")
# Consider selected columns of export quantities and save the file --> mirror flows
trade_data_faostat_partner = trade_data_faostat[
    (trade_data_faostat["year"] == most_recent_year_faostat)
    & (trade_data_faostat["element"] == "export_quantity")
][column_list]
save_file(trade_data_faostat_partner, "faostat_trends_mf.csv")
# Consider selected columns of export quantities and save the file --> mirror flows
trade_data_comtrade_partner = trade_data_comtrade[
    (trade_data_comtrade["year"] == most_recent_year_comtrade)
    & (trade_data_comtrade["element"] == "export_quantity")
][column_list]
save_file(trade_data_comtrade_partner, "comtrade_trends_mf.csv")
