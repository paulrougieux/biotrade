"""
Written by Selene Patani

Copyright (c) 2023 European Union
Licenced under the MIT licence

Script used to compute for each country the relative and absolute change values related to production and harvested area of the last reported year.
Deforestation products traded with EU and ROW (Faostat and Comtrade db) changes are computer also reporting mirror flows.
Csv files are stored into obs3df_methods / scripts folder
"""
import pandas as pd
import numpy as np
from pathlib import Path
from biotrade.faostat import faostat
from biotrade.faostat.mirror import put_mirror_beside
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.comtrade import comtrade
from biotrade.common.compare import merge_faostat_comtrade
from biotrade.common.time_series import relative_absolute_change

# Name of product file to retrieve
faostat_main_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve dataset
df = pd.read_csv(faostat_main_commodities_file)
# Retrieve parent and child codes
parent_codes = df["parent_code"].unique().tolist()
child_codes = df["child_code"].unique().tolist()
# Union of the codes without repetitions
product_codes = np.unique(parent_codes + child_codes).tolist()
# Select quantities from Faostat db for crop data
crop_data = faostat.db.select(
    table="crop_production",
    product_code=product_codes,
    element=["production", "area_harvested"],
)
# Compute the relative and absolute change in time of crop quantities and return the last value related to the average of 5 previous years
crop_data_change = relative_absolute_change(crop_data, years=5, last_value=True)
# Add a flag for values related to the most recent year of the table data
most_recent_year_crop_prod = faostat.db.most_recent_year("crop_production")
crop_data_change["flag_most_recent_year_db"] = (
    crop_data_change["year"] == most_recent_year_crop_prod
).astype(int)
# Reorder columns
crop_data_cols = [
    "reporter_code",
    "reporter",
    "product_code",
    "product",
    "element_code",
    "element",
    "year",
    "flag_most_recent_year_db",
    "unit",
    "value",
    "year_range_lower",
    "year_range_upper",
    "average_value",
    "relative_change",
    "absolute_change",
]
crop_data_change = crop_data_change[crop_data_cols]
# Save the production file in the biotrade script folder (ignored by git)
production_change_file = Path.cwd() / "scripts" / "commodity_change_production.csv"
crop_data_change[crop_data_change["element"] == "production"].to_csv(
    production_change_file, index=False, encoding="latin1", na_rep="NA",
)
# Save the harvested area file in the biotrade script folder (ignored by git)
harvested_area_change_file = (
    Path.cwd() / "scripts" / "commodity_change_harvested_area.csv"
)
crop_data_change[crop_data_change["element"] == "area_harvested"].to_csv(
    harvested_area_change_file, index=False, encoding="latin1", na_rep="NA",
)
# Select quantities from Faostat and Comtrade tables where product codes are the previously selected
trade_data = merge_faostat_comtrade(
    faostat_table="crop_trade", comtrade_table="yearly", faostat_code=product_codes
)
# Aggregate export values by EU and ROW as partners
trade_data_agg_partner = agg_trade_eu_row(
    trade_data[trade_data["element"].isin(["export_quantity", "export_net_weight"])],
    grouping_side="partner",
)
# Aggregate import values by EU and ROW as reporters
trade_data_agg_reporter = agg_trade_eu_row(
    trade_data[trade_data["element"].isin(["import_quantity", "import_net_weight"])],
    grouping_side="reporter",
)
# Compute the relative and absolute change in time of export quantities and return the last value related to the average of 5 previous years
trade_change_partner = relative_absolute_change(
    trade_data_agg_partner, years=5, last_value=True
)
# Compute the relative and absolute change in time of import quantities and return the last value related to the average of 5 previous years
trade_change_reporter = relative_absolute_change(
    trade_data_agg_reporter, years=5, last_value=True
)
# Report the mirror flows of values, average values of last 5 years, relative and absolute changes
trade_change_mirror = put_mirror_beside(
    pd.concat([trade_change_partner, trade_change_reporter]),
    drop_index_col=["average_value", "relative_change", "absolute_change",],
)
# Add a flag for values related to the most recent year of the table data: to distinguish between faostat and comtrade
most_recent_year_crop_trade = faostat.db.most_recent_year("crop_trade")
most_recent_year_yearly = comtrade.db.most_recent_year("yearly")
faostat_flag = (
    trade_change_mirror[trade_change_mirror["source"] == "faostat"]["year"]
    == most_recent_year_crop_trade
).astype(int)
comtrade_flag = (
    trade_change_mirror[trade_change_mirror["source"] == "comtrade"]["year"]
    == most_recent_year_yearly
).astype(int)
trade_change_mirror["flag_most_recent_year_db"] = pd.concat(
    [faostat_flag, comtrade_flag]
).sort_index()
# Reorder columns
trade_cols = [
    "reporter_code",
    "reporter",
    "partner_code",
    "partner",
    "source",
    "product_code",
    "product",
    "element_code",
    "element",
    "year",
    "flag_most_recent_year_db",
    "unit",
    "value",
    "value_mirror",
    "year_range_lower",
    "year_range_upper",
    "average_value",
    "average_value_mirror",
    "relative_change",
    "relative_change_mirror",
    "absolute_change",
    "absolute_change_mirror",
]
trade_change_mirror = trade_change_mirror[trade_cols]
# Save the file in the biotrade script folder (ignored by git)
commodity_change_file = Path.cwd() / "scripts" / "commodity_change_trade.csv"
trade_change_mirror.to_csv(
    commodity_change_file, index=False, encoding="latin1", na_rep="NA",
)

