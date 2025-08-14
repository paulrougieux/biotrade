#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Download FAOSTAT datasets.

## Renaming variables

It is important to be able to use variables in a sound way. Meaningful variable
names in snake case makes comparing and merging data across different sources
easier. In practice renaming is performed on column names and also on the content
of some columns, such as the element or the product column.

- The function `choose_column_renaming` decides which columns will be renamed
  according to `config_data/column_names.csv`

- The method `faostat.sanitize_variable_names` renames columns according to the
  names defined in the mapping table.


## Download FAOSTAT data into a data frame

Simply download a compressed CSV file from FAOSTAT and read it into a data
frame:

    >>> zip_file_name = faostat.pump.datasets["forestry_production"]
    >>> faostat.pump.download_zip_csv(zip_file_name)
    >>> fp = faostat.pump.read_df("forestry_production")


## Update your database

Usage example: download FAOSTAT datasets and store them in the database defined in
`faostat.db`:

    >>> from biotrade.faostat import faostat
    >>> faostat.pump.update(["crop_production", "crop_trade"])
    >>> faostat.pump.update(["forestry_production", "forestry_trade"])
    >>> faostat.pump.update(["food_balance"])
    >>> faostat.pump.update(["land_use", "land_cover"])
    >>> # Skip the table deletion confirmation message
    >>> faostat.pump.update(["land_use", "land_cover"], skip_confirmation=True)

"""

# Built-in modules
from pathlib import Path
from zipfile import ZipFile
import re
import shutil
import tempfile
import csv

try:
    import requests
except Exception as e:
    msg = "Failed to import requests, you will not be able to load data from FAOSTAT,"
    msg += "but you can still use other methods.\n"
    print(msg, str(e))

# Third party modules
import pandas
import numpy as np

# Internal modules
from biotrade.common.url_request_header import HEADER


def choose_column_renaming(short_name):
    """Choose which column from config_data/column_names.csv to use for
    renaming."""
    output = None
    for keyword in ["production", "trade", "land", "food_balance", "country"]:
        if keyword in short_name:
            output = "faostat_" + keyword
    if output is None:
        raise ValueError("No column to use for renaming", short_name)
    return output


class Pump:
    """
    Download trade data from FAOSTAT and store it locally in a database.

    The pump can perform the following tasks:
        1. Download compressed csv files from FAOSTAT.
        2. Read the compressed csv files into pandas data frames.
        3. Transfer the data frames to a database.

    An update performs all 3 tasks, it download files, reads them in chunks and
    stores them in the database

        >>> from biotrade.faostat import faostat
        >>> faostat.pump.update(["crop_production", "crop_trade"])
        >>> faostat.pump.update(["food_balance"])
        >>> faostat.pump.update(["land_use", "land_cover"])

    List available datasets and metadata links:

        >>> faostat.pump.datasets
        >>> faostat.pump.metadata_link

    Update all FAOSTAT datasets:

        >>> faostat.pump.download_all_datasets()
        >>> faostat.pump.transfer_all_datasets()
        >>> # Optionally, you can skip some of the large datasets
        >>> faostat.pump.transfer_all_datasets(skip=["food_balance", "crop_trade"])

    The following examples use lower level function which are not needed for
    normal use.

    Read an entire table directly from a CSV file to a data frame
    without going through the database:

        >>> fp = faostat.pump.read_df("forestry_production")

    Note that reading the entire dataset into a data frames can take a large
    part of the memory. It is recommended to start an analysis with a smaller
    data frame for a specific country or a specific product, using the
    faostat.db.select method.

    Update a dataset by downloading it again from FAOSTAT,
    reading it, then writing it to the database. Note this uses
    low level function, all steps can be handled by the update
    method automatically:

        >>> zip_file_name = faostat.pump.datasets["forestry_production"]
        >>> faostat.pump.download_zip_csv(zip_file_name)
        >>> fp = faostat.pump.read_df("forestry_production")
        >>> faostat.self.db.write_df(fp, "forestry_production")

    Which methods calls which one?

    - `update` calls:
        - `download_zip_csv`
        - `transfer_to_db`
    - `transfer_to_db` calls `transfer_csv_to_db_in_chunks`
    - `transfer_csv_to_db_in_chunks` calls `sanitize_variable_names`

    Other methods:
    - `read_df` uses read_zip_csv_to_df.
    - `read_df` is not used by the `update` and `transfer_to_db` methods.
      `read_df` should be kept for small datasets that are read directly into a
      data frame in memory.

    """

    # Define URL request headers
    header = HEADER
    # Base URL to load data from the website
    url_api_base = "http://fenixservices.fao.org/faostat/static/bulkdownloads/"
    # Dataset names on the FAOSTAT platform https://www.fao.org/faostat/en/#data
    datasets = {
        "forestry_production": "Forestry_E_All_Data_(Normalized).zip",
        "forestry_trade": "Forestry_Trade_Flows_E_All_Data_(Normalized).zip",
        "forest_land": "Emissions_Land_Use_Forests_E_All_Data_(Normalized).zip",
        "crop_production": "Production_Crops_Livestock_E_All_Data_(Normalized).zip",
        "crop_trade": "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",
        "land_cover": "Environment_LandCover_E_All_Data_(Normalized).zip",
        "land_use": "Inputs_LandUse_E_All_Data_(Normalized).zip",
        "food_balance": "SUA_Crops_Livestock_E_All_Data_(Normalized).zip",
    }
    # Link to the metadata
    metadata_link = {
        "forestry_production": "https://www.fao.org/faostat/en/#data/FO/metadata",
        "forestry_trade": "https://www.fao.org/faostat/en/#data/FT/metadata",
        "forest_land": "https://www.fao.org/faostat/en/#data/GF/metadata",
        "crop_production": "https://www.fao.org/faostat/en/#data/QCL/metadata",
        "crop_trade": "https://www.fao.org/faostat/en/#data/TM/metadata",
        "land_cover": "https://www.fao.org/faostat/en/#data/LC/metadata",
        "land_use": "https://www.fao.org/faostat/en/#data/RL/metadata",
        "food_balance": "https://www.fao.org/faostat/en/#data/SCL/metadata",
    }

    def __init__(self, parent):
        self.parent = parent
        self.logger = self.parent.logger
        self.db = self.parent.db
        self.data_dir = self.parent.data_dir
        # Mapping table used to rename columns
        self.column_names = self.parent.column_names
        # Number of lines to transfer from csv files to the database at once
        self.chunk_size = 10**7

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
        response = requests.get(url=url_api_call, headers=self.header, stream=True)
        with open(output_file, "wb") as out_file:
            print(f"HTTP response code: {response.status_code}")
            shutil.copyfileobj(response.raw, out_file)

    def read_zip_csv_to_df(
        self, zip_file, column_renaming, short_name, encoding="latin1"
    ):
        """Read a zip file downloaded from the FAOSTAT API rename columns and return a data frame

        The zip file contains 2 csv file, a large one with the data and a small one with flags.
        We want to open the large csv file which has the same name as the zip file.

        Columns are renamed using the mapping table defined in config_data/column_names.csv.
        The product and element columns are renamed to snake case.

        Example use:

        >>> from biotrade.faostat import faostat
        >>> zip_file = faostat.data_dir / "Forestry_E_All_Data_(Normalized).zip"
        >>> df = faostat.pump.read_zip_csv_to_df(
        >>>     zip_file=zip_file,
        >>>     column_renaming="faostat_production",
        >>>     short_name = "forestry_production")

        """
        # Extract the name of the CSV file
        zip_file_name = Path(zip_file).name
        csv_file_name = re.sub(".zip$", ".csv", zip_file_name)
        # Read to a pandas data frame
        with ZipFile(zip_file) as zipfile:
            with zipfile.open(csv_file_name) as csvfile:
                df = pandas.read_csv(csvfile, encoding=encoding)
        df = self.sanitize_variable_names(df, column_renaming, short_name)
        return df

    def sanitize_variable_names(self, df, column_renaming, short_name):
        """Sanitize column names using the mapping table.
        Use snake case in product and element names
        # TODO: use the function sanitize_variable_names common/sanitise.py
        """
        # Rename columns to snake case
        df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
        # Columns of the db table
        db_table_cols = self.db.tables[short_name].columns.keys()
        # Original column names
        cols_to_check = self.column_names[
            self.column_names["biotrade"].isin(db_table_cols)
        ][column_renaming].tolist()
        # Check columns which have changed in the input source
        cols_to_change = set(cols_to_check).difference(df.columns)
        # If column names have changed raise an error
        if cols_to_change:
            raise ValueError(
                f"The following columns \n{list(cols_to_change)}\nhave changed in the input source {column_renaming}.\nUpdate config_data/column_names.csv before updating table {short_name}"
            )
        # Map columns using the naming convention defined in self.column_names
        mapping = self.column_names.set_index(column_renaming).to_dict()["biotrade"]
        # Discard nan keys of mapping dictionary
        mapping.pop(np.nan, None)
        # Obtain columns for db upload
        columns = list(df.columns.intersection(list(mapping.keys())))
        # Filter df selecting only columns for db
        df = df[columns].copy()
        # Rename columns using the naming convention defined in self.column_names
        df.rename(columns=mapping, inplace=True)
        # Rename column contents to snake case using a compiled regex
        regex_pat = re.compile(r"\W+")
        for column in ["product", "item", "element"]:
            if column in df.columns:
                df[column] = (
                    df[column].str.replace(regex_pat, "_", regex=True).str.lower()
                )
                # Remove the last underscore if it's at the end of the name
                df[column] = df[column].str.replace("_$", "", regex=True)
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
        if "flag" in df.columns:
            df["flag"] = df["flag"].fillna("")
        return df

    def read_df(self, short_name, reload=False):
        """Read an entire zip csv file to a data frame

        Not recommended for large datasets which don't fit into memory.

        Example use:

        >>> from biotrade.faostat import faostat
        >>> fp = faostat.pump.read_df("forestry_production")
        >>> lu = faostat.pump.read_df("land_use")
        >>> lc = faostat.pump.read_df("land_cover")
        >>> fl = faostat.pump.read_df("forest_land")

        """
        zip_file_name = self.datasets[short_name]
        zip_file = self.data_dir / zip_file_name
        # Download the zip file is if it is not already in biotrade_data
        if not zip_file.exists():
            msg = f"\nThe file '{zip_file_name}' is absent from '{self.data_dir}'."
            self.logger.info(msg)
        if reload or not zip_file.exists():
            self.logger.info(f"Downloading {zip_file_name} from FAOSTAT.")
            self.download_zip_csv(zip_file_name)
        # Read the file into a data frame
        df = self.read_zip_csv_to_df(
            zip_file=zip_file,
            column_renaming=choose_column_renaming(short_name),
            short_name=short_name,
        )
        return df

    def transfer_csv_to_db_in_chunks(self, short_name, chunk_size):
        """Transfer large CSV files to the database in chunks
        so that a data frame with 40 million rows doesn't overload the memory.
        """
        temp_dir = Path(tempfile.TemporaryDirectory().name)
        # Csv file inside biotrade package config data directory
        if short_name == "country":
            csv_file_name = self.parent.config_data_dir / "faostat_country_groups.csv"
            encoding_var = "utf-8"
        # Zip files for table data
        else:
            # Unzip the CSV and write it to a temporary file on disk
            try:
                zip_file = ZipFile(self.data_dir / self.datasets[short_name])
                zip_file.extractall(temp_dir)
                csv_file_name = temp_dir / re.sub(
                    ".zip$", ".csv", self.datasets[short_name]
                )
                if short_name in [
                    "forestry_trade",
                    "land_cover",
                    "land_use",
                ]:
                    encoding_var = "latin1"
                else:
                    encoding_var = "utf-8"
                # Test if the file is corrupted
                with open(csv_file_name, "r", encoding=encoding_var) as csvfile:
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
        # Read in chunk and pass each chunk to the database
        for df_chunk in pandas.read_csv(
            csv_file_name, chunksize=chunk_size, encoding=encoding_var
        ):
            df_chunk = self.sanitize_variable_names(
                df_chunk, choose_column_renaming(short_name), short_name
            )
            print(df_chunk.head(1))
            self.db.append(df=df_chunk, table=short_name)
        if temp_dir.exists():
            # Remove temporary directory
            shutil.rmtree(temp_dir)

    def confirm_db_table_deletion(self, datasets):
        """Confirm database table deletion

        Separate method, because it is reused at different places."""
        msg = f"\nIf the database {self.db.engine} exists already, "
        msg += "this command will erase the following tables "
        msg += "and replace them with new data:\n - "
        msg += "\n - ".join(datasets)
        if input(msg + "\nPlease confirm [y/n]:") != "y":
            print("Cancelled.")
            return False
        else:
            return True

    def transfer_to_db(self, datasets, skip_confirmation=False):
        """Transfer from a csv file to the database by replacing the table
        content with the content of the zipped CSV files. Database field types
        are determined in faostat.db.

        :param list datasets: list of dataset names, whose keys should be in
            the faostat.pump.datasets and faostat.db.tables dictionaries
        :return: Nothing

        Usage:

            >>> from biotrade.faostat import faostat
            >>> faostat.pump.transfer_to_db()

        Use a larger chunk size

            >>> faostat.pump.chunk_size = 10 ** 6

        Skip the large crop trade and food balance datasets

            >>> faostat.pump.transfer_to_db(skip_crop_trade=True)

        """
        # Make datasets a list
        if isinstance(datasets, str):
            datasets = [datasets]
        if not skip_confirmation:
            if not self.confirm_db_table_deletion(datasets):
                return
        for table_name in datasets:
            # Transfer the compressed CSV file to the database
            self.transfer_csv_to_db_in_chunks(table_name, self.chunk_size)

    def update(self, datasets, skip_confirmation=False):
        """Update the given datasets by downloading them from FAOSTAT and
        transferring them to the database

        :param list or str datasets: list of dataset names, whose keys should
            be in the faostat.pump.datasets and faostat.db.tables dictionaries

        Usage:

            >>> from biotrade.faostat import faostat
            >>> faostat.pump.update(["crop_production", "crop_trade"])
            >>> faostat.pump.update(["food_balance"])
            >>> faostat.pump.update(["land_use", "land_cover"])

        """
        # Make datasets a list
        if isinstance(datasets, str):
            datasets = [datasets]
        # Confirmation message
        if not skip_confirmation:
            if not self.confirm_db_table_deletion(datasets):
                return
        # Download datasets from FAOSTAT
        for this_dataset in datasets:
            if this_dataset == "country":
                continue
            zip_file_name = self.datasets[this_dataset]
            self.download_zip_csv(zip_file_name)
        # Transfer to the database
        # Skip confirmation because we already confirmed above
        self.transfer_to_db(datasets, skip_confirmation=True)

    def download_all_datasets(self):
        """Download all files in the datasets dictionary"""
        for zip_file_name in self.datasets.values():
            self.download_zip_csv(zip_file_name)

    def transfer_all_datasets(self, skip):
        """Transfer all datasets to the database
        :param list skip: skip datasets in this list"""
        datasets = list(self.datasets.keys())
        # Make skip a list and remove list items from datasets
        if isinstance(skip, str):
            skip = [skip]
        for this_dataset in skip:
            datasets.remove(this_dataset)
        self.transfer_to_db(datasets)

    def show_metadata_link(self, short_name):
        """Display the metadata link associated with a dataset

        >>> from biotrade.faostat import faostat
        >>> faostat.pump.show_metadata_link("forestry_production")
        >>> faostat.pump.show_metadata_link("land_use")
        """
        print(self.metadata_link[short_name])
