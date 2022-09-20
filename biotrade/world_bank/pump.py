#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.

Usage update the World Bank data :

    >>> from biotrade.world_bank import world_bank
    >>> world_bank.pump.update()

"""
import logging
import urllib.request
import shutil
import pandas

# Internal modules
from biotrade.common.url_request_header import HEADER


class Pump:
    """
    Download World Bank data and store it locally in a database

    The pump can perform the following tasks:
        1. Download compressed csv files from the World Bank.
        2. Read the compressed csv files into pandas data frames.
        3. Transfer the data frames to a database.

    An update performs all 3 tasks, it download files, reads them in chunks and
    stores them in the database

        >>> from biotrade.world_bank import world_bank
        >>> world_bank.pump.update()

    Each task individually

       >>> world_bank.pump.download_zip_csv()

    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.world_bank")
    # Define URL request headers
    header = HEADER
    # URL to load data from the website
    url_bulk = "http://databank.worldbank.org/data/download/WDI_csv.zip"
    # Destination file name
    zip_file_name = "WDI_csv.zip"

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # TODO: Uncomment when ready
        # self.db = self.parent.db
        self.data_dir = self.parent.data_dir

    def download_zip_csv(self):
        """download a compressed csv file containing all World Bank indicators"""
        # Load the file to a temporary directory
        output_file = self.data_dir / self.zip_file_name
        self.logger.info("Downloading data from:\n %s", self.url_bulk)
        req = urllib.request.Request(url=self.url_bulk, headers=self.header)
        with urllib.request.urlopen(req) as response, open(
            output_file, "wb"
        ) as out_file:
            print(f"HTTP response code: {response.code}")
            shutil.copyfileobj(response, out_file)
        # TODO: Check zip file integrity and retry if not complete

    def read_zip_csv(self):
        """Read the World Bank zip csv file"""
        # TODO: continue work on this function
        csv_file = "to_be_defined"
        df = pandas.read_csv(csv_file)
        # remove empty columns
        for col in df.columns:
            if all(df[col].isna()):
                df.drop(columns=col, inplace=True)
        id_columns = [
            "Country Name",
            "Country Code",
            "Indicator Name",
            "Indicator Code",
        ]
        df_long = df.melt(id_vars=id_columns, var_name="period", value_name="value")
        return df_long
