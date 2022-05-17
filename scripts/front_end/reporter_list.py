"""
Written by Selene Patani.

Script made to export data related to countries, for the web platform

"""

import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat

# Name of reporter file to retrieve
reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
# Retrieve dataset
reporter = pd.read_csv(reporter_file)
# Columns to be retained and to change name
column_dict = {"iso3_code": "reporter_code", "fao_table_name": "reporter_name"}
# Drop rows where iso3 codes are nan
reporter = reporter[column_dict.keys()].rename(columns=column_dict).dropna()
# Add OTH - Others as reporter code and reporter for average data
reporter = pd.concat(
    [reporter, pd.DataFrame([["OTH", "Others"]], columns=reporter.columns)],
    ignore_index=True,
)
# Save csv files
folder_path = data_dir / "front_end"
folder_path.mkdir(exist_ok=True)
reporter.to_csv(folder_path / "reporter_list.csv", index=False)
