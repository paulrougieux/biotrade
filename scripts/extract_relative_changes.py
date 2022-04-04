"""
Written by Selene Patani

Script used to compute for each exporting country the relative change value on deforestation products traded to EU and ROW.
Csv files are stored into obs3df_methods / scripts folder
"""
import pandas as pd
import numpy as np
from pathlib import Path
from biotrade.faostat import faostat
from biotrade.faostat.aggregate import agg_trade_eu_row
from biotrade.common.time_series import relative_change

# Name of product file to retrieve
faostat_main_commodities_file = faostat.config_data_dir / "faostat_commodity_tree.csv"
# Retrieve dataset
df = pd.read_csv(faostat_main_commodities_file)
# Retrieve parent and child codes
parent_codes = df["parent_code"].unique().tolist()
child_codes = df["child_code"].unique().tolist()
# Union of the codes without repetitions
product_codes = np.unique(parent_codes + child_codes).tolist()
# Select export quantities from crop trade table where product codes are the previously selected
trade_data = faostat.db.select(
    table="crop_trade", product_code=product_codes, element="export_quantity"
)
# Aggregate values by EU and ROW as partners
trade_data_agg = agg_trade_eu_row(trade_data)
# Compute the relative change in time and return the last value related to the average of 5 previous years
trade_relative_change = relative_change(trade_data_agg, years=5, last_value=True)
# Save the file in the biotrade script folder (ignored by git)
faostat_commodity_relative_change_file = (
    Path.cwd() / "scripts" / "faostat_commodity_relative_change_export.csv"
)
trade_relative_change.to_csv(
    faostat_commodity_relative_change_file, index=False, encoding="latin1", na_rep="NA",
)

