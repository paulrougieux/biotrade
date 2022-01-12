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
import datetime
import pytz
import os
import re
import time
import urllib.request

# Third party modules
import json
import logging
import pandas

# Internal modules
from biotrade.common.url_request_header import HEADER


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
        self.db = self.parent.db
        # Mapping table used to rename columns
        self.column_names = self.parent.column_names
        # Get API authentication token from an environmental variable
        self.token = None
        if os.environ.get("COMTRADE_TOKEN"):
            self.token = Path(os.environ["COMTRADE_TOKEN"])
        # Path of CSV log file storing API parameters and download status
        self.csv_log_path = self.parent.data_dir / "pump_comtrade_api_args.csv"

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
        """Download a CSV file from the UN Comtrade data API and return a pandas data frame.

        The data API is documented at https://comtrade.un.org/data/doc/api/

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

    def write_log(
        self,
        timedate="",
        table="",
        max="",
        type="",
        freq="",
        px="",
        ps="",
        r="",
        p="",
        rg="",
        cc="",
        fmt="",
        head="",
        token="",
        status="",
        rows="",
    ):
        """
        Pump method to write API parameter values used to query comtrade repository
        and status of stored data into table "biotrade_data/comtrade/pump_comtrade_table.csv".
        If table does not exist, it is created
        """
        # create headers of data frame
        df = pandas.DataFrame(
            {
                "date": [],
                "table": [],
                "max": [],
                "type": [],
                "freq": [],
                "px": [],
                "ps": [],
                "r": [],
                "p": [],
                "rg": [],
                "cc": [],
                "fmt": [],
                "head": [],
                "token": [],
                "status": [],
                "rows": [],
            }
        )
        # check if table exists
        if self.csv_log_path.exists() is False:
            # create table if not exist
            df.to_csv(self.csv_log_path, mode="a", index=False)
        # create new row of df containing API parameter values
        df.loc[len(df)] = [
            timedate.strftime("%Y-%m-%d %H:%M %z"),
            str(table),
            str(max),
            str(type),
            str(freq),
            str(px),
            str(ps),
            str(r),
            str(p),
            str(rg),
            str(cc),
            str(fmt),
            str(head),
            str(token),
            str(status),
            str(rows),
        ]
        # write row to table
        df.to_csv(self.csv_log_path, mode="a", index=False, header=False)

    def download_to_db_reporter_loop(
        self, table_name, start_year, end_year, product_code, frequency
    ):
        """Download data from the Comtrade API for all reporters for the given
        list of product codes and years specified.

        :param table_name, str defining the comtrade table to upload data
        ("monthly", "yearly", "yearly_hs2")
        :param start_year, int, the initial year to download data
        :param end_year, int, the final year to download data
        :param product_code, list, product codes
        :param frequency, str, defines the frequency of data ("M" monthly,
        "A" annual)

        Download data for all product codes, all reporters and all partners
        choosing frequency (annual or monthly) and table to store data
        Upon download failure, give a message, wait 10 seconds then try to
        download again.
        In case of new failure, double the wait time until the download works again
        Store the data in the specified database.

        For example download data for palm oil data of HS code 151190, between 2016 and 2021
        storing them into the monthly table of database

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.pump.download_to_db_reporter_loop(
                table_name = 'monthly',
                start_year = 2016,
                end_year = 2021,
                product_code = '151190',
                frequency = 'M',
            )
        """
        records_downloaded = 0
        reporters = self.parent.country_groups.reporters
        # list of years
        years = [str(i) for i in range(start_year, end_year + 1)]
        for reporter_code in reporters.id:

            reporter_name = reporters.text[reporters.id == reporter_code].to_string(
                index=False
            )
            # https://comtrade.un.org/data/doc/api
            # "1 request every second (per IP address or authenticated user)."
            time.sleep(2)

            # reset counter for loop of products
            product_counter = 0
            # rest counter for loop of periods
            period_counter = 0
            # define the number of product codes for API depending on the frequency
            if frequency == "A":
                nr_product = 15
            elif frequency == "M":
                nr_product = 5
            # Select the first 5 products code at a time to avoid row limits to download
            product_block = product_code[
                product_counter * nr_product : (product_counter + 1) * nr_product
            ]
            # no more products to query
            while product_block != []:
                # differentiate selection of the period based on the frequency
                if frequency == "A":
                    # selection of 5 years
                    period = years[period_counter * 5 : (period_counter + 1) * 5]
                elif frequency == "M":
                    # selection of 1 year(12 months)
                    period = years[period_counter : period_counter + 1]
                # no more periods to query
                if period == []:
                    # continue with the next slot of products code
                    product_counter += 1
                    period_counter = 0
                    product_block = product_code[
                        product_counter
                        * nr_product : (product_counter + 1)
                        * nr_product
                    ]
                    continue
                else:
                    # conversion to a string
                    period = ",".join(period)

                download_successful = False
                # Reset additional sleep time used in case of error
                sleep_time = 10
                # Instantiate data frame
                df = pandas.DataFrame()
                # Try to download doubling sleep time until it succeeds.
                # Abort when sleep time increases to more than an hour.
                while not download_successful and sleep_time < 3600:
                    try:
                        df = self.download_df(
                            max=100000,
                            freq=frequency,
                            ps=period,
                            r=reporter_code,
                            cc=",".join(product_block),
                        )
                        download_successful = True
                    except Exception as error_http:
                        sleep_time *= 2
                        self.logger.info(
                            "Failed to download %s \n %s. \nWaiting %s seconds...",
                            reporter_name,
                            error_http,
                            sleep_time,
                        )
                        # Trace API parameters and error HTTP into table
                        self.write_log(
                            timedate=datetime.datetime.now(
                                pytz.timezone("Europe/Rome")
                            ),
                            table=table_name,
                            max=100000,
                            type="C",
                            freq=frequency,
                            px="HS",
                            ps=period,
                            r=reporter_code,
                            p="all",
                            rg="all",
                            cc=",".join(product_block),
                            fmt="json",
                            head="M",
                            token=self.token,
                            status=error_http,
                            rows="",
                        )
                        time.sleep(sleep_time)
                # Store in the database store the message if it fails
                try:
                    self.db.append(df, table_name)
                    # Define the status of stored data
                    if len(df) > 0:
                        status = "stored"
                    elif len(df) == 0:
                        status = "empty"
                except Exception as error_db:
                    self.logger.info(
                        "Failed to store %s in the database\n %s",
                        reporter_name,
                        error_db,
                    )
                    status = error_db
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
                # Trace API parameters and db status into table
                self.write_log(
                    timedate=datetime.datetime.now(pytz.timezone("Europe/Rome")),
                    table=table_name,
                    max=100000,
                    type="C",
                    freq=frequency,
                    px="HS",
                    ps=period,
                    r=reporter_code,
                    p="all",
                    rg="all",
                    cc=",".join(product_block),
                    fmt="json",
                    head="M",
                    token=self.token,
                    status=status,
                    rows=len(df),
                )
                # new period
                period_counter += 1

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
        reporters = self.parent.country_groups.reporters
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
                self.parent.db.append(df, table_name)
            # TODO check the error details,
            # check if data are the sames otherwise raise an error
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
