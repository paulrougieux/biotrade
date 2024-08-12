#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Usage update the World Bank data :

    >>> from biotrade.world_bank import world_bank
    >>> world_bank.pump.update(["indicator", "indicator_name"])

"""
from pathlib import Path
from zipfile import ZipFile
import tempfile
import logging

try:
    import requests
except Exception as e:
    msg = (
        "Failed to import requests, you will not be able to load data from World Bank,"
    )
    msg += "but you can still use other methods.\n"
    print(msg, str(e))
import shutil
import pandas as pd
import csv

# Internal modules
from biotrade.common.url_request_header import HEADER
from biotrade.common.sanitize import sanitize_variable_names


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
    url_bulk = "https://databank.worldbank.org/data/download/WDI_CSV.zip?_gl=1*d0bq8i*_gcl_au*MTQ3NTE1NTk2OC4xNzIxMjkwNDE1"
    # Destination zip and csv file names
    zip_file_name = "WDI_CSV.zip"
    datasets = {
        "indicator": "WDICSV.csv",
        "indicator_name": "WDICSV.csv",
    }

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        self.db = self.parent.db
        self.data_dir = self.parent.data_dir
        # Number of lines to read from csv files before transform years to long format
        self.chunk_size = 10**4

    def download_zip_csv(self):
        """download a compressed csv file containing all World Bank indicators"""
        # Load the zip file to the data directory
        output_file = self.data_dir / self.zip_file_name
        self.logger.info("Downloading data from:\n %s", self.url_bulk)
        response = requests.get(url=self.url_bulk, headers=self.header, stream=True)
        with open(output_file, "wb") as out_file:
            print(f"HTTP response code: {response.status_code}")
            shutil.copyfileobj(response.raw, out_file)

    def transfer_csv_to_db_in_chunks(self, short_name, chunk_size, reformatting):
        """Read the World Bank zip csv file and transfer large long format CSV
        file to the database in chunks so that a data frame with millions of
        rows doesn't overload the memory."""
        temp_dir = Path(tempfile.TemporaryDirectory().name)
        try:
            # Unzip the CSV and write it to a temporary file on disk
            zip_file = ZipFile(self.data_dir / self.zip_file_name)
            zip_file.extractall(temp_dir)
            # Obtain the name of csv file to read
            csv_file = temp_dir / self.datasets[short_name]
            # Name of csv file columns to use
            id_columns = [
                "Country Name",
                "Country Code",
                "Indicator Name",
                "Indicator Code",
            ]
            if reformatting:
                # Retrieve series file to append unit column for indicator table
                unit_file = temp_dir / "WDISeries.csv"
                df_unit = pd.read_csv(unit_file)
                # Rename column same as csv file
                df_unit.rename(columns={"Series Code": "Indicator Code"}, inplace=True)
                # Keep only columns needed for the merge
                df_unit = df_unit[["Indicator Code", "Unit of measure"]]
            else:
                # Do not split the dataframe into chunks (len(df) = 383572)
                chunk_size = 10**6
            # Test if the file is corrupted
            with open(csv_file, "r") as csvfile:
                # Detect the delimiter
                dialect = csv.Sniffer().sniff(csvfile.read(1024))
                # Place the reader at the beginning
                csvfile.seek(0)
                # Read the file
                reader = csv.reader(csvfile, dialect)
                header = next(reader)
                for row in reader:
                    pass
        # Zip file corrupted
        except Exception as e:
            self.db.logger.warning(
                f"File for {short_name} table is not available due to {e}.\n Unable to pump {short_name} data."
            )
            if temp_dir.exists():
                # Remove temporary directory
                shutil.rmtree(temp_dir)
            return
        # Drop and recreate the table
        table = self.db.tables[short_name]
        table.drop(self.db.engine)
        self.db.create_if_not_existing(table)
        # Read the csv file, transform the dataframe and upload data to the database
        for df_chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            # Remove unnamed columns
            df_chunk.drop(df_chunk.filter(regex="Unnamed"), axis=1, inplace=True)
            if reformatting:
                # Reformatting year columns into long format
                df_chunk = df_chunk.melt(
                    id_vars=id_columns, var_name="year", value_name="value"
                )
                # Merge with unit column
                df_chunk = df_chunk.merge(
                    df_unit,
                    on=["Indicator Code"],
                    how="left",
                )
            else:
                # Get unique indicator names and codes
                df_chunk = df_chunk.drop_duplicates(
                    subset=["Indicator Name", "Indicator Code"]
                )
            # Rename columns and keep only those needed for the db table
            df_chunk = sanitize_variable_names(
                df_chunk, "world_bank", self.db, short_name
            )
            print(df_chunk.head(1))
            # Append chunk to the db
            self.db.append(df=df_chunk, table=short_name)
        if temp_dir.exists():
            # Remove temporary directory
            shutil.rmtree(temp_dir)

    def transfer_to_db(self, datasets, skip_confirmation=False):
        """Transfer from a csv file to the database by replacing the table
        content with the content of the zipped CSV files. Database field types
        are determined in world_bank.db.

        :param list datasets: list of dataset names, whose keys should be in
            the world_bank.pump.datasets and world_bank.db.tables dictionaries
        :param boolean skip_confirmation: ask confirmation as default (False)
            before deleting tables
        :return: Nothing

        Update all world bank indicators from the bulk file into the "indicator" table:

            >>> from biotrade.world_bank import world_bank
            >>> world_bank.pump.transfer_to_db("indicator")

        Use a larger chunk size

            >>> world_bank.pump.chunk_size = 10 ** 5

        """
        # Make datasets a list
        if isinstance(datasets, str):
            datasets = [datasets]
        if not skip_confirmation:
            if not self.confirm_db_table_deletion(datasets):
                return
        for table_name in datasets:
            # Keep as default reformatting csv to long years format
            reformatting = True
            if table_name == "indicator_name":
                # Do not reformat the csv file
                reformatting = False
            # Transfer the compressed CSV file to the database
            self.transfer_csv_to_db_in_chunks(table_name, self.chunk_size, reformatting)

    def update(self, datasets, skip_confirmation=False):
        """Update the given datasets by downloading them from World Bank Data and
        transferring them to the database

        :param list or str datasets: list of dataset names, whose keys should
            be in the world_bank.pump.datasets and world_bank.db.tables dictionaries

        Usage:

            >>> from biotrade.world_bank import world_bank
            >>> world_bank.pump.update(["indicator"])

        """
        # Make datasets a list
        if isinstance(datasets, str):
            datasets = [datasets]
        # Confirmation message
        if not skip_confirmation:
            if not self.db.confirm_db_table_deletion(datasets):
                return
        # Download compressed zip bulk dataset from World Bank Data to data folder
        self.download_zip_csv()
        # Transfer to the database
        # Skip confirmation because we already confirmed above
        self.transfer_to_db(datasets, skip_confirmation=True)
