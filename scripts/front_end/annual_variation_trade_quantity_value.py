"""
Written by Selene Patani.

Script made to export data related to trade quantities of countries associated to the key commodities, for the web platform

"""

from functions import *

# Obtain the main product codes
main_product_list = main_product_list()
# Trade data related to product code list
trade_data = merge_faostat_comtrade_data(main_product_list)
# China Mainland + Taiwan data
df_china = consistency_check_china_data(trade_data)
# Add China and Taiwan data to trade_data
trade_data = pd.concat([trade_data, df_china], ignore_index=True,)
# Substitute faostat codes with iso3 codes
trade_data = reporter_iso_codes(trade_data)
# Transform flag column into int type
trade_data = trade_data.astype({"estimate_flag": int})
# Columns to be retained
column_list = [
    "reporter_code",
    "partner_code",
    "product_code",
    "period",
    "value",
    "unit",
]
# Consider selected columns of import quantities and values and save the files (drop nan)
trade_quantity_faostat = trade_data[
    (trade_data["source"] == "faostat") & (trade_data["element"] == "import_quantity")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_quantity_faostat["flow"] = "import"
save_file(trade_quantity_faostat, "faostat_annual_variation.csv")
trade_value_faostat = trade_data[
    (trade_data["source"] == "faostat") & (trade_data["element"] == "import_value")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_value_faostat["flow"] = "import"
save_file(trade_value_faostat, "faostat_value_annual_variation.csv")
# Consider selected columns of export quantities and values and save the files (drop nan)
trade_quantity_faostat = trade_data[
    (trade_data["source"] == "faostat") & (trade_data["element"] == "export_quantity")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_quantity_faostat["flow"] = "export"
save_file(trade_quantity_faostat, "faostat_annual_variation_mf.csv")
trade_value_faostat = trade_data[
    (trade_data["source"] == "faostat") & (trade_data["element"] == "export_value")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_value_faostat["flow"] = "export"
save_file(trade_value_faostat, "faostat_value_annual_variation_mf.csv")
# Add estimate flag for comtrade data
column_list.append("estimate_flag")
# Consider selected columns of import quantities and values and save the files (drop nan)
trade_quantity_comtrade = trade_data[
    (trade_data["source"] == "comtrade") & (trade_data["element"] == "import_quantity")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_quantity_comtrade["flow"] = "import"
save_file(trade_quantity_comtrade, "comtrade_annual_variation.csv")
trade_value_comtrade = trade_data[
    (trade_data["source"] == "comtrade") & (trade_data["element"] == "import_value")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_value_comtrade["flow"] = "import"
save_file(trade_value_comtrade, "comtrade_value_annual_variation.csv")
# Consider selected columns of export quantities and values and save the files (drop nan)
trade_quantity_comtrade = trade_data[
    (trade_data["source"] == "comtrade") & (trade_data["element"] == "export_quantity")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_quantity_comtrade["flow"] = "export"
save_file(trade_quantity_comtrade, "comtrade_annual_variation_mf.csv")
trade_value_comtrade = trade_data[
    (trade_data["source"] == "comtrade") & (trade_data["element"] == "export_value")
][column_list].dropna(subset=["value"])
# Add column describing flow
trade_value_comtrade["flow"] = "export"
save_file(trade_value_comtrade, "comtrade_value_annual_variation_mf.csv")
