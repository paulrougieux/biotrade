"""
Written by Selene Patani.

Script made to export data related to countries, for the web platform

"""

import pandas as pd
from biotrade import data_dir
from biotrade.faostat import faostat

# Name of product file to retrieve
reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
# Retrieve dataset
reporter = pd.read_csv(reporter_file)
# Columns to be retained and to change name
column_dict = {"iso3_code": "reporter_code", "fao_table_name": "reporter_name"}
reporter = reporter[column_dict.keys()].rename(columns=column_dict).dropna()
# Save csv files
folder_path = data_dir / "front_end"
if not folder_path.exists():
    folder_path.mkdir()
reporter.to_csv(folder_path / "reporter_list.csv", index=False)
