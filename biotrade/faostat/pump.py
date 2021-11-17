#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.
"""

# Built-in modules
from pathlib import Path
from zipfile import ZipFile
import re
import shutil
import urllib.request
import tempfile

# Third party modules
import logging
import pandas

# Internal modules
from biotrade.common.url_request_header import HEADER


class Pump:
    """
    Download trade data from FAOSTAT and store it locally.
    Read the zipped csv files into pandas data frames.

    Update all FAOSTAT datasets by downloading bulk files,
    then storing them in the SQLite database:

        >>> from biotrade.faostat import faostat
        >>> faostat.pump.download_all_datasets()
        >>> faostat.pump.update_db()

    Read an entire table directly from a CSV file to a data frame
    (without going through the database):

        >>> fp = faostat.pump.forestry_production

    Those lower level function are not needed for normal use.
    Update a dataset by downloading it again from FAOSTAT:

        >>> faostat.pump.download_zip_csv("Forestry_E_All_Data_(Normalized).zip")
        >>> fp = faostat.pump.read_df("forestry_production")
        >>> faostat.self.db.write_df(fp, "forestry_production")

    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.faostat")
    # Define URL request headers
    header = HEADER
    # Base URL to load data from the website
    url_api_base = "http://fenixservices.fao.org/faostat/static/bulkdownloads/"
    # Dataset names on the FAOSTAT platform https://www.fao.org/faostat/en/#data
    datasets = {
        "forestry_production": "Forestry_E_All_Data_(Normalized).zip",
        "forestry_trade": "Forestry_Trade_Flows_E_All_Data_(Normalized).zip",
        "crop_production": "Production_Crops_Livestock_E_All_Data_(Normalized).zip",
        "crop_trade": "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",
    }

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        self.db = self.parent.db
        self.data_dir = self.parent.data_dir
        # Mapping table used to rename columns
        self.column_names = self.parent.column_names

    def download_zip_csv(self, zip_file_name):
        """Download a compressed csv file from the FAOSTAT website

         Example use:

         >>> from biotrade.faostat import faostat
         >>> faostat.pump.download_zip_csv("Forestry_E_All_Data_(Normalized).zip")
         >>> faostat.pump.download_zip_csv("Forestry_Trade_Flows_E_All_Data_(Normalized).zip")
         >>> faostat.pump.download_zip_csv("Production_Crops_Livestock_E_All_Data_(Normalized).zip")
         >>> faostat.pump.download_zip_csv("Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip")

        # Check the content of the destination folder for updates
        !ls -al ~/repos/biotrade_data/faostat/
        """
        url_api_call = self.url_api_base + zip_file_name
        output_file = self.data_dir / zip_file_name
        self.logger.info("Downloading data from:\n %s", url_api_call)
        req = urllib.request.Request(url=url_api_call, headers=self.header)
        with urllib.request.urlopen(req) as response, open(
            output_file, "wb"
        ) as out_file:
            print(f"HTTP response code: {response.code}")
            shutil.copyfileobj(response, out_file)

    def read_zip_csv_to_df(self, zip_file, column_renaming, encoding="latin1"):
        """Read a zip file downloaded from the FAOSTAT API rename columns and return a data frame

        The zip file contains 2 csv file, a large one with the data and a small one with flags.
        We want to open the large csv file which has the same name as the zip file.

        Columns are renamed using the mapping table defined in config_data/column_names.csv.
        The product and element columns are renamed to snake case.

        Example use:

        >>> from biotrade.faostat import faostat
        >>> zip_file = faostat.data_dir / "Forestry_E_All_Data_(Normalized).zip"
        >>> df = faostat.read_zip_csv_to_df(
        >>>     zip_file=zip_file,
        >>>     column_renaming="faostat_forestry_production")

        """
        # Extract the name of the CSV file
        zip_file_name = Path(zip_file).name
        csv_file_name = re.sub(".zip$", ".csv", zip_file_name)
        # Read to a pandas data frame
        with ZipFile(zip_file) as zipfile:
            with zipfile.open(csv_file_name) as csvfile:
                df = pandas.read_csv(csvfile, encoding=encoding)
        df = self.sanitize_variable_names(df, column_renaming=column_renaming)
        return df

    def sanitize_variable_names(self, df, column_renaming):
        """Sanitize column names using the mapping table.
        Use snake case in product and element names"""
        # Rename columns to snake case
        df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
        # Rename columns using the naming convention defined in self.column_names
        mapping = self.column_names.set_index(column_renaming).to_dict()["jrc"]
        df.rename(columns=mapping, inplace=True)
        # Rename products to snake case using a compiled regex
        regex_pat = re.compile(r"\W+")
        df["product"] = (
            df["product"].str.replace(regex_pat, "_", regex=True).str.lower()
        )
        # Rename elements to snake case
        df["element"] = (
            df["element"].str.replace(regex_pat, "_", regex=True).str.lower()
        )
        # Convert NaN flags to an empty character variable
        # so that the flag column doesn't get converted to a list column when sent to R
        # Here is how the flag was encoded before the change
        # ft.flag.unique()
        # array([None, '*', 'R', 'Cv', 'P', 'A'], dtype=object)
        # Because of the absence of na.character type in pandas
        # these appear as two different data types when sent to R and that column is then
        # converted to a list column
        # Here is how the flag is encoded after the change
        # ft.flag.unique()
        # array(['', '*', 'R', 'Cv', 'P', 'A'], dtype=object)
        df.flag.fillna("", inplace=True)
        return df

    def read_df(self, short_name):
        """Read an entire zip csv file to a data frame"""
        df = self.read_zip_csv_to_df(
            zip_file=self.data_dir / self.datasets[short_name],
            column_renaming="faostat_" + short_name,
        )
        return df

    def transfer_csv_to_db_in_chunks(self, short_name):
        """Transfer large CSV files to the database in chunks
        so that a data frame with 40 million rows doesn't overload the memory.
        """
        # Unzip the CSV and write it to a temporary file on disk
        zip_file = ZipFile(self.data_dir / self.datasets[short_name])
        temp_dir = Path(tempfile.TemporaryDirectory().name)
        zip_file.extractall(temp_dir)
        # Read in chunk and pass each chunk to the database
        csv_file_name = temp_dir / re.sub(".zip$", ".csv", self.datasets[short_name])
        for df_chunk in pandas.read_csv(
            csv_file_name, chunksize=10 ** 6, encoding="latin1"
        ):
            df_chunk = self.sanitize_variable_names(df_chunk, "faostat_" + short_name)
            print(df_chunk.head(1))
            self.db.append(df=df_chunk, table=short_name)

    @property
    def forestry_production(self):
        """Forestry production data"""
        return self.read_df("forestry_production")

    @property
    def forestry_trade(self):
        """Forestry bilateral trade flows (trade matrix)"""
        return self.read_df("forestry_trade")

    def download_all_datasets(self):
        """Download all files in the datasets dictionary"""
        for zip_file_name in self.datasets.values():
            self.download_zip_csv(zip_file_name)

    def update_db(self, skip_crop_trade=False):
        """Update the sqlite database replace table content with the content of
        the bulk zipped csv files.

        Usage:

        >>> from biotrade.faostat import faostat
        >>> faostat.pump.update_db()

        Skip the large crop trade dataset

        >>> faostat.pump.update_db(skip_crop_trade=True)
        """
        # Drop and recreate the tables
        self.db.forestry_production.drop()
        self.db.forestry_trade.drop()
        self.db.crop_production.drop()
        self.db.crop_trade.drop()
        # Create the tables
        self.db.create_if_not_existing(self.db.forestry_production)
        self.db.create_if_not_existing(self.db.forestry_trade)
        self.db.create_if_not_existing(self.db.crop_production)
        self.db.create_if_not_existing(self.db.crop_trade)
        # For smaller datasets, write entire data frames to the SQLite database
        datasets_that_fit_in_memory = [
            "forestry_production",
            "forestry_trade",
            "crop_production",
        ]
        for table_name in datasets_that_fit_in_memory:
            df = self.read_df(table_name)
            self.db.append(df=df, table=table_name)
        if skip_crop_trade:
            return
        # There is a memory error with the crop trade dataset.
        # Read the file in chunks so that the memory doesn't get too full
        self.transfer_csv_to_db_in_chunks("crop_trade")
