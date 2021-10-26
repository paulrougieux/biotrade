#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

You can use this object at the ipython console with the following examples.

Download trade flows and return a data frame

    >>> from biotrade.comtrade import comtrade
    >>> # Other sawnwood
    >>> swd99 = comtrade.pump.download(cc = "440799")
    >>> # Soy
    >>> soy = comtrade.pump.download(cc = "120190")

Get the list of products from the Comtrade API

    >>> hs = comtrade.pump.get_parameter_list("classificationHS.json")

Potential server Error messages

- HTTP Error 409 seems to be the error when the download limit has been reached
and we don't use the authentication token.
- We also get HTTP Error 500: "Internal Server Error" when the max argument is
greater than 9999.

"""
# Built-in modules
from pathlib import Path
from warnings import warn
import datetime
import os
import re
import time
import urllib.request

# Third party modules
import json
import logging
import pandas

# Internal modules
from biotrade.url_request_header import HEADER


class Pump:
    """
    Download trade data from the Comtrade API and store it inside a database.
    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")
    # Define URL request headers
    header = HEADER
    # Base URL to load trade trade data from the API
    url_api_base = "http://comtrade.un.org/api/get?"
    # Base URL to load metadata
    url_metadata_base = "https://comtrade.un.org/Data/cache/"

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Mapping table used to rename columns
        self.column_names = self.parent.column_names
        # Get API authentication token from an environmental variable
        self.token = None
        if os.environ.get("COMTRADE_TOKEN"):
            self.token = Path(os.environ["COMTRADE_TOKEN"])

    def download(self, *args, **kwargs):
        """Deprecated download function, see download_df instead"""
        warn("Deprecated, please use the download_df method instead.")
        return self.download_df(*args, **kwargs)

    def download_df(
        self,
        max="500",
        type="C",
        freq="A",
        px="HS",
        ps="2020",
        r="381",
        p="all",
        rg="all",
        cc="440799",
        fmt="json",
        head="M",
    ):
        """Download a CSV file from the UN Comtrade API and return a pandas data frame.

        Argument names are the same as the Comtrade API argument names.

        :param str max : maximum number of rows returned by the API
         Note the server returns "HTTP Error 500: Internal Server Error" if
         max is greater than 9999.
        :param str type : type of trade (c = commodities, s = services)
        :param str freq : frequency A Annual, M Monthly
        :param str px : classification
        :param str ps : time period
        :param str r : reporter country
        :param str p : partner country
        :param str rg : trade flow
        :param str cc : product code
        :param str fmt : data format, defaults to "json",
                         usage of "csv" is discouraged as it can cause data type issues
        :param str head : human (H) or machine (M) readable column headers
        """
        if fmt not in ("csv", "json"):
            raise ValueError(f"The data format '{fmt}' is not supported")

        # Build the API call
        url_api_call = (
            f"{self.url_api_base}max={max}&type={type}&freq={freq}&px={px}"
            + f"&ps={ps}&r={r}&p={p}&rg={rg}&cc={cc}&fmt={fmt}&head={head}"
        )
        if self.token is not None:
            url_api_call += f"&token={self.token}"
        self.logger.info("Downloading a data frame from:\n %s", url_api_call)

        # Load the data in csv format
        if fmt == "csv":
            df = pandas.read_csv(
                url_api_call,
                # Force the id column to remain a character column,
                # otherwise str "01" becomes int 1.
                dtype={"Commodity Code": str, "cmdcode": str},
            )

        # Load the data in json format
        if fmt == "json":
            req = urllib.request.Request(url=url_api_call, headers=self.header)
            with urllib.request.urlopen(req) as response:
                print(f"HTTP response code: {response.code}")
                json_content = json.load(response)
                # If the data was downloaded incorrectly, raise an error with the validation status
                if json_content["validation"]["status"]["value"] != 0:
                    raise urllib.error.URLError(
                        f"API message\n{json_content['validation']}"
                    )
                df = pandas.json_normalize(json_content["dataset"])

        # Raise an error if the data frame has the maximum number of rows returned by the query
        if len(df) >= int(max) - 1:
            raise ValueError(
                f"The number of rows in the returned data ({len(df)}) "
                + f"is equal to the maximum number of rows ({max}). "
                + f"Increase the max argument for product {cc} and reporter {r} "
                + "in order to get all requested data from Comtrade."
            )
        # Rename columns to snake case
        df.rename(columns=lambda x: re.sub(r" ", "_", str(x)).lower(), inplace=True)
        # Remove parenthesis and dots, used only for human readable dataset
        df.rename(columns=lambda x: re.sub(r"[()\.]", "", x), inplace=True)
        # Replace $ sign by d, used only for human readable dataset
        df.rename(columns=lambda x: re.sub(r"\$", "d", x), inplace=True)
        # Rename columns based on the jrc naming convention
        mapping = self.column_names.set_index("comtrade_machine").to_dict()["jrc"]
        df.rename(columns=mapping, inplace=True)
        return df

    def get_parameter_list(self, file_name):
        """Get the list of valid parameters from the Comtrade API
        A list of parameter in json format are available and explained on
        https://comtrade.un.org/data/doc/api

        For example to get the list of reporters

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.pump.get_parameter_list("reporterAreas.json")

        Get the list of partners

        >>> comtrade.pump.get_parameter_list("partnerAreas.json")

        Get the list of products

        >>> comtrade.pump.get_parameter_list("classificationHS.json")
        """
        url = self.url_metadata_base + file_name
        req = urllib.request.Request(url=url, headers=self.header)
        with urllib.request.urlopen(req) as response:
            print(f"HTTP response code: {response.code}")
            json_content = json.load(response)
            df = pandas.json_normalize(json_content["results"])
        return df

    def download_to_db(self, table, *args, **kwargs):
        """Download Comtrade data and store it into the given database table"""
        # You might need to set the chunk size argument if there is an error.
        df = self.download(*args, **kwargs)
        # Remove the lengthy product description (to be stored in a dedicated table)
        df.drop(columns=["commodity"], inplace=True, errors="ignore")

    def loop_on_reporters(self, product_code, table_name):
        """Download data from the Comtrade API for all reporters for the given product code.

        Download the last 5 years for one product, all reporters and all partners
        Upon download failure, give a message, wait 10 seconds then try to download again.
        In case of new failure, double the wait time until the download works again
        Store the data in the PostGreSQL database.

        For example download data for wood products 44

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.pump.loop_on_reporters("44", "yearly_hs2")
        """
        records_downloaded = 0
        reporters = self.parent.countries.reporters
        # Download the last 5 years for one product, one reporter and all its partners
        year = datetime.datetime.today().year
        # Convert each element of the list to a string
        years = [str(year - i) for i in range(1, 6)]
        years = ",".join(years)
        for reporter_code in reporters.id:
            reporter_name = reporters.text[reporters.id == reporter_code].to_string(
                index=False
            )
            download_successful = False
            # https://comtrade.un.org/data/doc/api
            # "1 request every second (per IP address or authenticated user)."
            time.sleep(2)
            # Reset additional sleep time used in case of error
            sleep_time = 10
            # Try to download doubling sleep time until it succeeds.
            # Abort when sleep time increases to more than half an hour.
            while not download_successful and sleep_time < 1800:
                try:
                    df = self.download(
                        max="9000",
                        type="C",
                        freq="A",
                        px="HS",
                        ps=years,
                        r=reporter_code,
                        p="all",
                        rg="all",
                        cc=product_code,
                        fmt="json",
                        head="M",
                    )
                    download_successful = True
                except Exception as error:
                    sleep_time *= 2
                    self.logger.info(
                        "Failed to download %s \n %s. \nWaiting %s seconds...",
                        reporter_name,
                        error,
                        sleep_time,
                    )
                    time.sleep(sleep_time)
            # Remove the lengthy product description (to be stored in a dedicated table)
            df.drop(columns=["commodity"], inplace=True, errors="ignore")
            # Store in the database store the message if it fails
            try:
                self.parent.database.append(df, table_name)
            except Exception as error:
                self.logger.info(
                    "Failed to store %s in the database\n %s", reporter_name, error
                )
            # Keep track of the country name and length of the data in the log file
            records_downloaded += len(df)
            self.logger.info(
                "Downloaded %s records for %s (code %s).\n"
                + "%s records downloaded in total.",
                len(df),
                reporter_name,
                reporter_code,
                records_downloaded,
            )
