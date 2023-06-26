#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download data from the Eurostat bulk download facility.

Noemi Cazzaniga's package [eurostat](https://pypi.org/project/eurostat/) was
taken as an inspiration to load Eurostat data from the bulk download
repository.

"""
# Standard imports
import logging
import re

# Third party libraries
import numpy as np
import pandas


class Pump:
    """
    Download exchange rates from the Eurostat bulk download platform

        >>> from biotrade.eurostat import eurostat
        >>> exch_rates = eurostat.pump.download_bulk_df("ert_bil_eur_m")

    Download the table of content of all datasets and search for population change

        >>> from biotrade.eurostat import eurostat
        >>> toc = eurostat.pump.download_toc()
        >>> toc.loc[toc.title.str.contains("population change", case=False)]

    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.eurostat")
    # Base URL to load trade trade data from the API
    url_api_base = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/"
    url_bulk_file = url_api_base + "BulkDownloadListing?sort=1&file=data%2F"
    # URL of the table of content
    url_toc = url_api_base + "BulkDownloadListing?sort=1&file=table_of_contents_en.txt"

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Mapping table used to rename columns
        self.column_names = self.parent.column_names

    def sanitize_variable_names(self, df, renaming_from, renaming_to):
        """
        Sanitize column names using the mapping table column_names.csv
        Use snake case in column names and replace some symbols
        """
        # Rename columns to snake case
        df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
        # Map columns using the naming convention defined in self.column_names
        mapping = self.column_names.set_index(renaming_from).to_dict()[renaming_to]
        # Discard nan keys of mapping dictionary
        mapping.pop(np.nan, None)
        # Rename columns using the naming convention defined in self.column_names
        df.rename(columns=mapping, inplace=True)
        return df

    def download_bulk_df(self, code):
        """
        Method of Pump class to download data from the eurostat bulk facility.
        The output is reshaped to wide format.

        The `read_csv` uses many delimiters to deal with the fact that the
        first 3 columns of the csv are comma separated while the rest of the
        data frame is tab separated. In addition, the first tab separation is
        only a tab, while the subsequent separation are a tab and a space.

        :param (str) code, the dataset code
        :output (data frame) in long format with a period column

        Example:

            >>> from biotrade.eurostat import eurostat
            >>> exch_rates = eurostat.pump.download_bulk_df("ert_bil_eur_m")
            >>> demo_gind = eurostat.pump.download_bulk_df("demo_gind")

        """
        # Build the API bulk URL
        url = f"{self.url_bulk_file}{code}.tsv.gz"
        df = pandas.read_csv(url, sep=",|\t| \t", na_values=":", engine="python")
        # Rename columns to snake case
        df.rename(columns=lambda x: re.sub(r"\W+", "_", str(x)).lower(), inplace=True)
        # If there is a time column, reshape to long format
        if any(df.columns.str.contains("time")):
            time_column = df.columns[df.columns.str.contains("time")][-1]
            # Id columns are before the time columns
            id_columns = df.loc[:, :time_column].columns
            df = df.melt(id_vars=id_columns, var_name="period", value_name="value")
            # Remove "\time" from the rightmost column of the index
            df = df.rename(columns=lambda x: re.sub(r"\\time", "", x))
        return df

    def download_toc(self):
        """
        Download the table of content of the eurostat bulk download facility as a data frame

        :output (data frame)

        Example:

            >>> from biotrade.eurostat import eurostat
            >>> toc = eurostat.pump.download_toc()

        Find all datasets that contain the word "population change"

            >>> toc.loc[toc.title.str.contains("population change", case=False)]

        """
        df = pandas.read_csv(self.url_toc, sep="\t")
        # Remove the empty values column
        df.drop(columns="values", inplace=True)
        # Remove white spaces at the beginning and end of strings
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        return df
