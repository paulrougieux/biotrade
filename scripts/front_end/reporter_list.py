"""
Written by Selene Patani.

Script made to export data related to countries, for the web platform

"""

import pandas as pd
from biotrade.faostat import faostat
from functions import *

# Name of reporter file to retrieve
reporter_file = faostat.config_data_dir / "faostat_country_groups.csv"
# Retrieve dataset
reporter = pd.read_csv(reporter_file)
# Columns to be retained and to change name
column_dict = {
    "iso3_code": "reporter_code",
    "fao_table_name": "reporter_name",
    "fao_status_info": "status",
}
# Drop rows where iso3 codes are nan
reporter = reporter[column_dict.keys()].rename(columns=column_dict).dropna()
# Define current countries and old ones
selector = reporter.status == "old"
reporter.loc[selector, "status"] = "iso3_old"
reporter.loc[~selector, "status"] = "iso3"
# Add aggregation reporter code and reporter for average data
reporter = pd.concat(
    [
        reporter,
        pd.DataFrame(
            [
                ["OTH", "Others", "agg"],
                ["EU27", "European Union", "agg"],
                ["ROW", "Rest Of the World", "agg"],
            ],
            columns=reporter.columns,
        ),
    ],
    ignore_index=True,
)
# Save csv files to env variable path or into biotrade data folder
save_file(reporter, "reporter_list.csv")
