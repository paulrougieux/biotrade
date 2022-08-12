"""
Written by Selene Patani

Script used to compute averages and percentages of trade quantities related to the main commodities, europe and rest of the world imports

"""

from functions import *
from biotrade.faostat.aggregate import agg_trade_eu_row

# Obtain the main product codes
main_product_list = main_product_list()
# Trade data related to product code list
trade_data = merge_faostat_comtrade_data(main_product_list)
# China Mainland + Taiwan data
df_china = consistency_check_china_data(trade_data)
# Add China data to trade_data (exclude Taiwan data, otherwise double counted into percentages and averages calculations with China + Taiwan)
trade_data = pd.concat(
    [
        trade_data[
            ~((trade_data[["reporter_code", "partner_code"]] == 214).any(axis=1))
        ],
        df_china,
    ],
    ignore_index=True,
)
# Substitute faostat codes with iso3 codes
trade_data = reporter_iso_codes(trade_data)
# Define the columns and codes for the average calculations
dict_list = [
    {
        "average_col": "product_code",
        "percentage_col": "reporter_code",
        "threshold_code": "OTH",
        "index_list_add": [],
    },
    {
        "average_col": "product_code",
        "percentage_col": "partner_code",
        "threshold_code": "OTH",
        "index_list_add": [],
    },
]
# Consider faostat data
trade_data_faostat = trade_data[trade_data["source"] == "faostat"]
# Calculate the averages and percentages
trade_data_faostat_avg = average_results(trade_data_faostat, 95, dict_list,)
# Consider comtrade data (excluded estimated values)
trade_data_comtrade = trade_data[
    (trade_data["source"] == "comtrade") & (trade_data["estimate_flag"] == 0)
]
# Calculate the averages and percentages
trade_data_comtrade_avg = average_results(trade_data_comtrade, 95, dict_list,)
drop_column_list = [
    "element",
    dict_list[1]["percentage_col"],
    dict_list[1]["percentage_col"] + COLUMN_PERC_SUFFIX,
]
column_list = trade_data_faostat_avg.columns.tolist()
for drop_column in drop_column_list:
    column_list.remove(drop_column)
# Define dropna columns
dropna_col = [
    col for col in column_list if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
]
# Consider selected columns of import quantities and save the file (drop nan)
trade_data_faostat_avg_imp = trade_data_faostat_avg.copy()
trade_data_faostat_avg_imp = replace_zero_with_nan_values(
    trade_data_faostat_avg_imp, dropna_col
)
trade_data_faostat_avg_imp = trade_data_faostat_avg_imp.dropna(subset=dropna_col)
trade_data_faostat_reporter = trade_data_faostat_avg_imp[
    trade_data_faostat_avg_imp["element"] == "import_quantity"
][column_list]
# # Add column describing flow
# trade_data_faostat_reporter["flow"] = "import"
save_file(trade_data_faostat_reporter, "faostat_average.csv")
# Consider selected columns of import quantities and save the file (drop nan)
trade_data_comtrade_avg_imp = trade_data_comtrade_avg.copy()
trade_data_comtrade_avg_imp = replace_zero_with_nan_values(
    trade_data_comtrade_avg_imp, dropna_col
)
trade_data_comtrade_avg_imp = trade_data_comtrade_avg_imp.dropna(subset=dropna_col)
trade_data_comtrade_reporter = trade_data_comtrade_avg_imp[
    trade_data_comtrade_avg_imp["element"] == "import_quantity"
][column_list]
# # Add column describing flow
# trade_data_comtrade_reporter["flow"] = "import"
save_file(trade_data_comtrade_reporter, "comtrade_average.csv")
drop_column_list = [
    "element",
    dict_list[0]["percentage_col"],
    dict_list[0]["percentage_col"] + COLUMN_PERC_SUFFIX,
]
column_list = trade_data_faostat_avg.columns.tolist()
for drop_column in drop_column_list:
    column_list.remove(drop_column)
# Define dropna columns
dropna_col = [
    col for col in column_list if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
]
# Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
trade_data_faostat_avg_exp = trade_data_faostat_avg.copy()
trade_data_faostat_avg_exp = replace_zero_with_nan_values(
    trade_data_faostat_avg_exp, dropna_col
)
trade_data_faostat_avg_exp = trade_data_faostat_avg_exp.dropna(subset=dropna_col)
trade_data_faostat_partner = trade_data_faostat_avg_exp[
    trade_data_faostat_avg_exp["element"] == "export_quantity"
][column_list]
# # Add column describing flow
# trade_data_faostat_partner["flow"] = "export"
save_file(trade_data_faostat_partner, "faostat_average_mf.csv")
# Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
trade_data_comtrade_avg_exp = trade_data_comtrade_avg.copy()
trade_data_comtrade_avg_exp = replace_zero_with_nan_values(
    trade_data_comtrade_avg_exp, dropna_col
)
trade_data_comtrade_avg_exp = trade_data_comtrade_avg_exp.dropna(subset=dropna_col)
trade_data_comtrade_partner = trade_data_comtrade_avg_exp[
    trade_data_comtrade_avg_exp["element"] == "export_quantity"
][column_list]
# # Add column describing flow
# trade_data_comtrade_partner["flow"] = "export"
save_file(trade_data_comtrade_partner, "comtrade_average_mf.csv")
# Consider averages for EU and rest of the world partners
dict_list = [
    {
        "average_col": "product_code",
        "percentage_col": "partner_code",
        "threshold_code": "OTH",
        "index_list_add": ["reporter_code"],
    },
    {
        "average_col": "product_code",
        "percentage_col": "reporter_code",
        "threshold_code": "OTH",
        "index_list_add": ["partner_code"],
    },
]
# Aggregate data with reporters as eu and row
trade_data_group_reporter = agg_trade_eu_row(
    trade_data[trade_data["estimate_flag"] == 0],
    grouping_side="reporter",
    drop_index_col=["estimate_flag", "faost_code", "iso3_code", "period"],
)
# Aggregate data with partners as eu and row
trade_data_group_partner = agg_trade_eu_row(
    trade_data[trade_data["estimate_flag"] == 0],
    grouping_side="partner",
    drop_index_col=["estimate_flag", "faost_code", "iso3_code", "period"],
)
# Concatenate in a unique df
trade_data_group = pd.concat(
    [trade_data_group_reporter, trade_data_group_partner], ignore_index=True,
)
# Substitute with name and codes of the aggregations for the web platform
selector = trade_data_group["reporter"] == "eu"
trade_data_group.loc[selector, "reporter_code"] = "EU27"
selector = trade_data_group["reporter"] == "row"
trade_data_group.loc[selector, "reporter_code"] = "ROW"
selector = trade_data_group["partner"] == "eu"
trade_data_group.loc[selector, "partner_code"] = "EU27"
selector = trade_data_group["partner"] == "row"
trade_data_group.loc[selector, "partner_code"] = "ROW"
trade_data_group[["reporter", "partner"]] = trade_data_group[
    ["reporter", "partner"]
].replace(["eu", "row"], ["European Union", "Rest Of the World"])
# Define the period column as the year column
trade_data_group["period"] = trade_data_group["year"]
# Consider faostat data
trade_data_faostat = trade_data_group[trade_data_group["source"] == "faostat"]
# Calculate the averages and percentages
trade_data_faostat_avg = average_results(trade_data_faostat, 95, dict_list,)
# Consider comtrade data (excluded estimated values)
trade_data_comtrade = trade_data_group[trade_data_group["source"] == "comtrade"]
# Calculate the averages and percentages
trade_data_comtrade_avg = average_results(trade_data_comtrade, 95, dict_list,)
drop_column_list = [
    "element",
    dict_list[1]["percentage_col"] + COLUMN_PERC_SUFFIX,
]
column_list = trade_data_faostat_avg.columns.tolist()
for drop_column in drop_column_list:
    column_list.remove(drop_column)
# Define dropna columns
dropna_col = [
    col for col in column_list if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
]
# Consider selected columns of import quantities and save the file (drop nan)
trade_data_faostat_avg_imp = trade_data_faostat_avg.copy()
trade_data_faostat_avg_imp = replace_zero_with_nan_values(
    trade_data_faostat_avg_imp, dropna_col
)
trade_data_faostat_avg_imp = trade_data_faostat_avg_imp.dropna(subset=dropna_col)
trade_data_faostat_reporter = trade_data_faostat_avg_imp[
    (trade_data_faostat_avg_imp["element"] == "import_quantity")
    & (trade_data_faostat_avg_imp["reporter_code"].isin(["EU27", "ROW"]))
][column_list]
# # Add column describing flow
# trade_data_faostat_reporter["flow"] = "import"
save_file(trade_data_faostat_reporter, "faostat_average_eu_row.csv")
# Consider selected columns of import quantities and save the file (drop nan)
trade_data_comtrade_avg_imp = trade_data_comtrade_avg.copy()
trade_data_comtrade_avg_imp = replace_zero_with_nan_values(
    trade_data_comtrade_avg_imp, dropna_col
)
trade_data_comtrade_avg_imp = trade_data_comtrade_avg_imp.dropna(subset=dropna_col)
trade_data_comtrade_reporter = trade_data_comtrade_avg_imp[
    (trade_data_comtrade_avg_imp["element"] == "import_quantity")
    & (trade_data_comtrade_avg_imp["reporter_code"].isin(["EU27", "ROW"]))
][column_list]
# # Add column describing flow
# trade_data_comtrade_reporter["flow"] = "import"
save_file(trade_data_comtrade_reporter, "comtrade_average_eu_row.csv")
drop_column_list = [
    "element",
    dict_list[0]["percentage_col"] + COLUMN_PERC_SUFFIX,
]
column_list = trade_data_faostat_avg.columns.tolist()
for drop_column in drop_column_list:
    column_list.remove(drop_column)
# Define dropna columns
dropna_col = [
    col for col in column_list if col.endswith((COLUMN_AVG_SUFFIX, COLUMN_PERC_SUFFIX))
]
# Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
trade_data_faostat_avg_exp = trade_data_faostat_avg.copy()
trade_data_faostat_avg_exp = replace_zero_with_nan_values(
    trade_data_faostat_avg_exp, dropna_col
)
trade_data_faostat_avg_exp = trade_data_faostat_avg_exp.dropna(subset=dropna_col)
trade_data_faostat_partner = trade_data_faostat_avg_exp[
    (trade_data_faostat_avg_exp["element"] == "export_quantity")
    & (trade_data_faostat_avg_exp["partner_code"].isin(["EU27", "ROW"]))
][column_list]
# # Add column describing flow
# trade_data_faostat_partner["flow"] = "export"
save_file(trade_data_faostat_partner, "faostat_average_eu_row_mf.csv")
# Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
trade_data_comtrade_avg_exp = trade_data_comtrade_avg.copy()
trade_data_comtrade_avg_exp = replace_zero_with_nan_values(
    trade_data_comtrade_avg_exp, dropna_col
)
trade_data_comtrade_avg_exp = trade_data_comtrade_avg_exp.dropna(subset=dropna_col)
trade_data_comtrade_partner = trade_data_comtrade_avg_exp[
    (trade_data_comtrade_avg_exp["element"] == "export_quantity")
    & (trade_data_comtrade_avg_exp["partner_code"].isin(["EU27", "ROW"]))
][column_list]
# # Add column describing flow
# trade_data_comtrade_partner["flow"] = "export"
save_file(trade_data_comtrade_partner, "comtrade_average_eu_row_mf.csv")
