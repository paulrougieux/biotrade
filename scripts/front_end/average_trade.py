from functions import *

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
    },
    {
        "average_col": "product_code",
        "percentage_col": "partner_code",
        "threshold_code": "OTH",
    },
]
# Consider faostat data
trade_data_faostat = trade_data[trade_data["source"] == "faostat"]
# Calculate the averages and percentages
trade_data_faostat = average_results(trade_data_faostat, 95, dict_list,)
# Consider comtrade data (excluded estimated values)
trade_data_comtrade = trade_data[
    (trade_data["source"] == "comtrade") & (trade_data["estimate_flag"] == 0)
]
# Calculate the averages and percentages
trade_data_comtrade = average_results(trade_data_comtrade, 95, dict_list,)
column_list = [
    "period",
    "product_code",
    "average_value_product_code",
    "unit",
    "reporter_code",
    "value_percentage_reporter_code",
]
# Consider selected columns of import quantities and save the file (drop nan)
trade_data_faostat_reporter = trade_data_faostat[
    trade_data_faostat["element"] == "import_quantity"
][column_list].dropna(
    subset=["average_value_product_code", "value_percentage_reporter_code"]
)
save_file(trade_data_faostat_reporter, "faostat_trade_quantity_average.csv")
# Consider selected columns of import quantities and save the file (drop nan)
trade_data_comtrade_reporter = trade_data_comtrade[
    trade_data_comtrade["element"] == "import_quantity"
][column_list].dropna(
    subset=["average_value_product_code", "value_percentage_reporter_code"]
)
save_file(trade_data_comtrade_reporter, "comtrade_trade_quantity_average.csv")
# Substitute reporter_code column with partner_code
old_col = "reporter_code"
new_col = "partner_code"
column_list = (
    column_list[: column_list.index(old_col)]
    + [new_col]
    + column_list[column_list.index(old_col) + 1 :]
)
# Substitute value_percentage_reporter_code column with value_percentage_partner_code
old_col = "value_percentage_reporter_code"
new_col = "value_percentage_partner_code"
column_list = (
    column_list[: column_list.index(old_col)]
    + [new_col]
    + column_list[column_list.index(old_col) + 1 :]
)
# Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
trade_data_faostat_partner = trade_data_faostat[
    trade_data_faostat["element"] == "export_quantity"
][column_list].dropna(
    subset=["average_value_product_code", "value_percentage_partner_code"]
)
save_file(trade_data_faostat_partner, "faostat_trade_quantity_average_mf.csv")
# Consider selected columns of export quantities and save the file (drop nan) --> mirror flows
trade_data_comtrade_partner = trade_data_comtrade[
    trade_data_comtrade["element"] == "export_quantity"
][column_list].dropna(
    subset=["average_value_product_code", "value_percentage_partner_code"]
)
save_file(trade_data_comtrade_partner, "comtrade_trade_quantity_average_mf.csv")

