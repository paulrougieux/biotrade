#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Forestry production data from FAOSTAT

    >>> from biotrade.faostat import faostat
    >>> df = faostat.forestry_production

Display information on column names used for renaming
and dropping less important columns:

    >>> faostat.column_names

Download or update a zip file from FAOSTAT.



"""

from pathlib import Path
import re
from zipfile import ZipFile

# Third party modules
import pandas

# Internal modules
from biotrade import module_dir, data_dir

# Define a logging mechanism to keep track of errors and debug messages
from biotrade.logger import create_logger

create_logger()


class Faostat:
    """
    Parent to various objects dealing with FAOSTAT data
    """

    # Location of the data
    data_dir = data_dir / "faostat"

    # Location of module configuration data
    config_data_dir = module_dir / "config_data"

    # Load a mapping table used to rename columns
    df = pandas.read_csv(config_data_dir / "column_names.csv")
    df = df.filter(regex="jrc|faostat")
    non_na_values = (~df.filter(like="faostat").isna()).sum(axis=1)
    column_names = df[non_na_values > 0]

    def read_zip_csv_to_df(self, zip_file, column_renaming):
        """Read a zip file downloaded from the FAOSTAT API rename columns and return a data frame

        The zip file contains 2 csv file, a large one with the data and a small one with flags.
        We want to open the large csv file which has the same name as the zip file.

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
                df = pandas.read_csv(csvfile, encoding="latin1")
        # Rename to snake case
        df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
        # Rename using the naming convention defined in self.column_names
        mapping = self.column_names.set_index(column_renaming).to_dict()["jrc"]
        df.rename(columns=mapping, inplace=True)
        return df

    @property
    def forestry_production(self):
        """Forestry production data"""
        zip_file = self.data_dir / "Forestry_E_All_Data_(Normalized).zip"
        df = faostat.read_zip_csv_to_df(
            zip_file=zip_file, column_renaming="faostat_forestry_production"
        )
        return df


# Make a singleton #
faostat = Faostat()
