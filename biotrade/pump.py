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
"""


# First party modules
import urllib.request
import re

# Third party modules
import json
import pandas
import logging


class Pump:
    """
    Download trade data from the Comtrade API and store it inside a database.
    """
    # Log debug and error messages
    logger = logging.getLogger('biotrade.comtrade')

    # Base URL to load trade trade data from the API
    url_api_base = "http://comtrade.un.org/api/get?"
    # Base URL to load metadata
    url_metadata_base = "https://comtrade.un.org/Data/cache/"
    # Define headers
    # Based on https://stackoverflow.com/a/66591873/2641825
    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.11 (KHTML, like Gecko) "
        "Chrome/23.0.1271.64 Safari/537.11",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
        "Accept-Encoding": "none",
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "keep-alive",
    }

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Mapping table used to rename columns
        self.column_names = self.parent.column_names

    def download(
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
        :param str type : type of trade (c = commodities, s = services)
        :param str freq : frequency
        :param str px : classification
        :param str ps : time period
        :param str r : reporter country
        :param str p : partner country
        :param str rg : trade flow
        :param str cc : product code
        :param str fmt : data format, defaults to json, usage of csv is discouraged as it can cause data type issues
        :param str head : human (H) or machine (M) readable headers

        """
        if fmt not in ("csv", "json"):
            raise ValueError(f"The data format '{fmt}' is not supported")

        # Build the API call
        url_api_call = (
            f"{self.url_api_base}max={max}&type={type}&freq={freq}&px={px}"
            + f"&ps={ps}&r={r}&p={p}&rg={rg}&cc={cc}&fmt={fmt}&head={head}"
        )
        self.logger.info(f"Downloading a data frame from:\n {url_api_call}")

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
        mapping = self.column_names.set_index("comtrade_m").to_dict()["jrc"]
        df.rename(columns=mapping, inplace=True)
        return df

    def get_parameter_list(self, file_name):
        """Get the list of valid parameters from the Comtrade API
        A list of parameter in json format are available and explained on
        https://comtrade.un.org/data/doc/api

        For example to get the list of reporters

        >>> from biotrade.comtrade.pump import comtrade.pump
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
