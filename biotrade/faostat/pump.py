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

# Third party modules
import logging
import pandas

# Internal modules
from biotrade.url_request_header import HEADER


class Pump:
    """
    Download trade data from FAOSTAT and store it locally.
    Read the zipped csv files into pandas data frames.

    For example load forestry production data:

        >>> from biotrade.faostat import faostat
        >>> fp = faostat.pump.forestry_production

    Update the data by downloading it again from FAOSTAT:

        >>> faostat.pump.download_zip_csv("Forestry_E_All_Data_(Normalized).zip")

    Store the data in to a database

        >>> faostat.db_sqlite.append(fp, "forestry_production")
    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.faostat")
    # Define URL request headers
    header = HEADER
    # Base URL to load data from the website
    url_api_base = "http://fenixservices.fao.org/faostat/static/bulkdownloads/"

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
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
        return df

    @property
    def forestry_production(self):
        """Forestry production data"""
        df = self.read_zip_csv_to_df(
            zip_file=self.data_dir / "Forestry_E_All_Data_(Normalized).zip",
            column_renaming="faostat_forestry_production",
        )
        return df

    @property
    def forestry_trade(self):
        """Forestry bilateral trade flows (trade matrix)"""
        df = self.read_zip_csv_to_df(
            zip_file=self.data_dir / "Forestry_Trade_Flows_E_All_Data_(Normalized).zip",
            column_renaming="faostat_forestry_trade",
        )
        return df

    def update_sqlite_db(self):
        """Update the sqlite database drop and recreate the databases"""
        self.parent.db_sqlite.append(self.forestry_production, "forestry_production")
