#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.


The database can be updated every 2 weeks with a cron job such as

To run comtrade.pump.update_db periodically as a cron job, edit crontab:

    sudo vim /etc/crontab

And enter:

    0 0 0 */2 * cronjob_update_db.py

Where the cronjob_update_db.py file contains

    from biotrade.comtrade import comtrade
    comtrade.pump.update_db(table_name = "monthly", frequency = "M")

"""
# Built-in modules
from pathlib import Path
import warnings
import tempfile
import shutil
from zipfile import ZipFile
import datetime
import pytz
import os
import re
import time
from collections import OrderedDict

try:
    import requests
except Exception as e:
    msg = "Failed to import requests, you will not be able to load data from Comtrade,"
    msg += "but you can still use other methods.\n"
    print(msg, str(e))

# Third party modules
import json
import logging
import pandas
import numpy as np

try:
    import comtradeapicall
except Exception as e:
    msg = "Failed to import comtradeapicall package, you will not be able to load data from there,"
    msg += "but you can still use other methods.\n"
    print(msg, str(e))


# Internal modules
from biotrade.common.url_request_header import HEADER
from biotrade.comtrade.pump_products import PumpProducts


class Pump:
    """
    Download trade data from the Comtrade API and store it inside a database.

    There are two different ways to download data from the Comtrade API:
        - the public API
        - a restricted bulk API accessible with a token

    The public API is accessible to every one and is suitable to download small
    amounts of data for a few products in a few countries. For example, the
    following calls download trade data for the selected product code `cc`,
    reporter `r` and time period `ps` from the Comtrade API and returns data
    frames that contain sawnwood other (HS code 440799) and soya beans (HS
    code 120190) trade reported by Italy in 2020:

        >>> from biotrade.comtrade import comtrade
        >>> # Other sawnwood
        >>> sawnwood99 = comtrade.pump.download_df(cc = "440799", r="381", ps="2020")
        >>> # Soy
        >>> soya_beans = comtrade.pump.download_df(cc = "120190", r="381", ps="2020")

    The public API can also be used to get the list of products and product
    codes:

        >>> hs = comtrade.pump.get_parameter_list("classificationHS.json")

    Lists of countries and country codes are also available:

        >>> comtrade.pump.get_parameter_list("reporterAreas.json")
        >>> comtrade.pump.get_parameter_list("partnerAreas.json")

    In case you have access to the API token. It's also possible to download
    data from the Comtrade bulk API and store it in the database with:

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.pump.update_db(table_name = "monthly", frequency = "M")

    Potential Error messages returned by the Comtrade API:

    - HTTP Error 401 "Unauthorized" means that the token authentication is
      required and missing.
    - HTTP Error 409 "Conflict" seems to be the error when the download limit has been reached
      and we don't use the authentication token.
    - HTTP Error 500 "Internal Server Error" appears when the max argument is
      greater than 9999.

    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")
    # Define URL request headers
    header = HEADER
    # Base URL to load trade trade data from the API
    url_api_base = "https://comtrade.un.org/api/get"
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
            self.token = os.environ["COMTRADE_TOKEN"]
        # Path of CSV log file storing API parameters and download status
        self.csv_log_path = self.parent.data_dir / "pump_comtrade_api_args.csv"
        # Maximum number of rows in the Comtrade Free API limit
        self.max_row_free_api_limit = 1e5

    @property
    def products(self):
        """Download, cached and read trade data grouped by products"""
        return PumpProducts(self)

    def sanitize_variable_names(self, df, renaming_from, renaming_to):
        """
        Sanitize column names using the mapping table column_names.csv
        Use snake case in column names and replace some symbols
        """
        # Rename columns to snake case
        df.rename(columns=lambda x: re.sub(r" ", "_", str(x)).lower(), inplace=True)
        # Remove parenthesis and dots, used only for human readable dataset
        df.rename(columns=lambda x: re.sub(r"[()\.]", "", x), inplace=True)
        # Replace $ sign by d, used only for human readable dataset
        df.rename(columns=lambda x: re.sub(r"\$", "d", x), inplace=True)
        # Rename columns using the naming convention defined in self.column_names
        mapping = self.column_names.set_index(renaming_from).to_dict()[renaming_to]
        # Discard nan keys of mapping dictionary
        mapping.pop(np.nan, None)
        df.rename(columns=mapping, inplace=True)
        # Rename column content to snake case using a compiled regex
        regex_pat = re.compile(r"\W+")
        if "flow" in df.columns:
            df["flow"] = df["flow"].str.replace(regex_pat, "_", regex=True).str.lower()
            # Remove the plural "s"
            df["flow"] = df["flow"].str.replace("s", "", regex=True)
        return df

    def download_bulk_gz(self, period, frequency):
        """
        Method of Pump class to download bulk data from comtradeapicall package, which returns country gz files
        stored into a temporary directory

        :param (int) period, as year or year+month
        :param (str) frequency, as "A" yearly or "M" monthly
        :output (path) temp_dir, directory path which contains the gz files
        :output (int) response_code, response status of the comtradeapicall

        For example monthly bilateral trade gz files for all products in November
        2021 can be downloaded into a temporary directory as:

            >>> from biotrade.comtrade import comtrade
            >>> temp_dir, resp_code = comtrade.pump.download_bulk_gz(202111, "M")
        """

        # Temporary folder creation
        temp_dir = Path(tempfile.mkdtemp())
        self.logger.info(
            "Downloading %s data for period %s from:\n %s.%s",
            "yearly" if frequency == "A" else "monthly",
            period,
            comtradeapicall.__name__,
            comtradeapicall.bulkDownloadFinalFile.__name__,
        )
        # Variable for checking successful gz download
        download_successful = False
        # Default time to download gz files
        time_start = time.time()
        sleep_time = 0
        # Send the request: if successful put the gz files into the temporary
        # folder else retry.
        # If the wait raises to more than 1 hour the loop stops.
        while not download_successful and sleep_time < 3600:
            try:
                # Create a request
                comtradeapicall.bulkDownloadFinalFile(
                    self.token,
                    temp_dir,
                    typeCode="C",
                    freqCode=frequency,
                    clCode="HS",
                    period=period,
                    decompress=False,
                )
                response_code = 200
                download_successful = True
                # No data available
                if len(os.listdir(temp_dir)) == 0:
                    response_code = 404
            except (Exception, BaseException) as error:
                response_code = error
            sleep_time = time.time() - time_start
            self.logger.info(f"HTTP response code: {response_code}")
        return temp_dir, response_code

    def download_bulk_csv(self, period, frequency):
        """
        Method of Pump class to download bulk data from API
        https://comtrade.un.org/data/doc/api/bulk/ which returns a zip file
        stored into a temporary directory

        :param (int) period, as year or year+month
        :param (str) frequency, as "A" yearly or "M" monthly
        :output (path) temp_dir, directory path which contains the zip file
        :output (int) response_code, response status of the API request

        For example monthly bilateral trade data for all products in November
        2021 can be downloaded into a temporary directory as:

            >>> from biotrade.comtrade import comtrade
            >>> temp_dir, resp_code = comtrade.pump.download_bulk_csv(202111, "M")
        """

        # Construnction of API bulk url
        url_api_call = (
            f"{self.url_api_base}/bulk/C/{frequency}/{period}/ALL/HS?"
            + f"token={self.token}"
        )
        # Temporary folder creation
        temp_dir = Path(tempfile.mkdtemp())
        # Temporary zip file path
        temp_file = temp_dir / "temp.zip"
        self.logger.info("Downloading data from:\n %s", url_api_call)
        # Variable for checking successfull zip dowload
        download_successful = False
        # Default sleep time before downloading zip file
        sleep_time = 5
        # Send the request: if successful put the zip file into the temporary
        # folder else retry and double the wait time at each try
        # If the wait raises to more than 1 hour the loop stops.
        while not download_successful and sleep_time < 3600:
            time.sleep(sleep_time)
            try:
                # Create a request
                response = requests.get(
                    url=url_api_call, headers=self.header, stream=True
                )
                # Raise in case of HTTP error
                response.raise_for_status()
                with open(temp_file, "wb") as out_file:
                    # Copy the zip file into the output_file
                    shutil.copyfileobj(response.raw, out_file)
                    download_successful = True
                    response_code = response.status_code
            except requests.exceptions.HTTPError as error:
                response_code = error.response.status_code
                # Error 404 means that no data are present, stop the while loop
                if response_code == 404:
                    download_successful = True
            except requests.exceptions.RequestException as error:
                response_code = error
            sleep_time *= 2
            self.logger.info(f"HTTP response code: {response_code}")
        return temp_dir, response_code

    def transfer_gz_files(
        self,
        temp_dir,
        table_name,
        bioeconomy_tuple,
        check_data_presence,
        api_period,
    ):
        """
        Pump method to transfer gz files to db using dataframe.
        500k data frame rows ~ 350 MB of memory usage
        4 millions data frame rows ~ 2.8 GB of memory usage

        :param (path) temp_dir, path of the folder containing the gz files
            to copy into db
        :param (str) table_name, table name of the db
        :param (tuple) bioeconomy_tuple, commodity codes to store data
        :param (bool) check_data_presence, to delete data in case of existing
            rows into db
        :param (int) api_period, period to delete existing data
        :return (int) records_transferred, rows uploaded to db
        """
        # List of chunk rows
        chunk_list = []
        # Read the gz files
        for temp_file in os.listdir(temp_dir):
            # Preallocate a dataframe
            df = pandas.DataFrame()
            nr_attempts = 0
            df_from_gz = False
            while not df_from_gz and nr_attempts < 10:
                # Loop on gz files and upload them
                try:
                    df = pandas.read_csv(
                        temp_dir / temp_file,
                        sep="\t",
                        dtype={
                            "datasetCode": str,
                            "cmdCode": str,
                            "mosCode": str,
                        },
                    )
                    df_from_gz = True
                # Continue until max number of attempts is reached
                except Exception as error_gz:
                    self.logger.info(
                        f"Unable to load data from {temp_file} for period {api_period} due to\n {error_gz}.\n Retrying to download it.."
                    )
                    try:
                        file_str = re.split(r"[-\[\]]", temp_file)[2]
                        frequency = file_str[1]
                        reporter_code = int(file_str[2:5])
                        period = int(file_str[5:-2])
                        comtradeapicall.bulkDownloadFinalFile(
                            self.token,
                            temp_dir,
                            typeCode="C",
                            freqCode=frequency,
                            clCode="HS",
                            period=period,
                            reporterCode=reporter_code,
                            decompress=False,
                        )
                        response_code = 200
                    except (Exception, BaseException) as error:
                        response_code = error
                    self.logger.info(f"Response: {response_code}")
                    nr_attempts += 1
            if nr_attempts == 10:
                self.logger.warning(
                    f"Reached the max number of attempts, {nr_attempts}. Unable to load data from {temp_file} for period {api_period}"
                )
            # If length is > 0 select rows
            if not df.empty:
                # Remove potential white spaces between tab delimiter in string columns
                strip_cols = df.select_dtypes(["object"]).columns
                df[strip_cols] = df[strip_cols].apply(lambda x: x.str.strip())
                nrow_before = len(df)
                memory_before = round(df.memory_usage(deep=True).sum() / (1024**2), 2)
                selector = (
                    (df["customsCode"] == "C00")
                    & (df["motCode"] == 0)
                    & (df["mosCode"] == "0")
                    & (df["partner2Code"] == 0)
                    & (df["flowCode"].isin(["M", "X", "RM", "RX"]))
                )
                if table_name in ("monthly", "yearly"):
                    # Store codes with bioeconomy label and 6 digits, not differentiating modality of transport (0), supply ("0"), 2nd partner (0)  and procedures ("C00")
                    # only (re)exported and (re)imported data
                    selector_6d = (
                        selector
                        & df["cmdCode"].str.startswith(bioeconomy_tuple)
                        & (df["cmdCode"].str.len() == 6)
                    )
                    df = df[selector_6d]
                elif table_name == "yearly_hs2":
                    # Store codes with bioeconomy label and 2 digits, not differentiating modality of transport (0), supply ("0"), 2nd partner (0)  and procedures ("C00")
                    # only (re)exported and (re)imported data
                    selector_2d = selector & df["cmdCode"].isin(bioeconomy_tuple)
                    df = df[selector_2d]
                msg = f"Before filtering {temp_file[:-3]} file, the dataframe occupies {memory_before} MB and number of rows are {nrow_before}, after {round(df.memory_usage(deep=True).sum() / (1024**2), 2)} MB with {len(df)} rows"
                print(msg)
                # Append rows
                chunk_list.append(df)
        # Construct the final df to upload to db
        if chunk_list:
            df = pandas.concat(chunk_list)
            self.logger.info(
                "Memory usage of the filtered dataframe corresponding to period %s:\n%s GB",
                api_period,
                round(df.memory_usage(deep=True).sum() / (1024**3), 2),
            )
        if not df.empty:
            # Call method to rename column names
            df = self.sanitize_variable_names(
                df, renaming_from="comtrade_apicall", renaming_to="biotrade"
            )
            # Delete already existing data
            if check_data_presence:
                self.db.delete_data(
                    table_name,
                    api_period,
                    api_period,
                )
            # Append data to db
            self.db.append(df, table_name)
        else:
            self.logger.warning(
                f"Dataframe for period {api_period} is empty. Previous data in the db are not deleted."
            )
        # Keep track of the length of the transferred data in the log file
        records_transferred = len(df)
        return records_transferred

    def transfer_csv_chunk(
        self,
        temp_file,
        table_name,
        bioeconomy_tuple,
        check_data_presence,
        api_period,
        chunk_size=10**6,
    ):
        """
        Pump method to transfer large csv file to db using dataframe.
        500k data frame rows ~ 500 MB of memory usage
        2 millions data frame rows ~ 2 GB of memory usage

        :param (path) temp_file, path of the zip file containing the csv file
            to copy into db
        :param (str) table_name, table name of the db
        :param (tuple) bioeconomy_tuple, commodity codes to store data
        :param (bool) check_data_presence, to delete data in case of existing
            rows into db
        :param (int) api_period, period to delete existing data
        :param (int) chunk_size, splitting csv to transfer to df default is 10**6
        """
        # Number of records transferred from csv file
        records_downloaded_csv = 0
        # Open the zip file
        with ZipFile(temp_file) as zipfile:
            with zipfile.open(zipfile.namelist()[0]) as csvfile:
                # List of chunk rows
                chunk_list = []
                # Loop on csv chunks and upload them
                for df_chunk in pandas.read_csv(
                    csvfile,
                    encoding="utf-8",
                    # Force the id column to remain a character column,
                    # otherwise str "01" becomes int 1.
                    dtype={"Commodity Code": str, "Reporter ISO": str},
                    chunksize=chunk_size,
                ):
                    # If length is > 0 select rows
                    if not df_chunk.empty:
                        if table_name == "monthly":
                            # Store codes with bioeconomy label and 6 digits
                            df_chunk = df_chunk[
                                (
                                    df_chunk["Commodity Code"].str.startswith(
                                        bioeconomy_tuple
                                    )
                                )
                                & (df_chunk["Commodity Code"].str.len() == 6)
                            ]
                        elif table_name == "yearly_hs2":
                            # Store codes with bioeconomy label and 2 digits
                            df_chunk = df_chunk[
                                df_chunk["Commodity Code"].isin(bioeconomy_tuple)
                            ]
                        elif table_name == "yearly":
                            # Store codes with bioeconomy label and 6 digits
                            df_chunk = df_chunk[
                                (
                                    df_chunk["Commodity Code"].str.startswith(
                                        bioeconomy_tuple
                                    )
                                )
                                & (df_chunk["Commodity Code"].str.len() == 6)
                            ]
                        # Append rows
                        chunk_list.append(df_chunk)
        # Construct the final df to upload to db
        df = pandas.concat(chunk_list)
        self.logger.info(
            "Memory usage:\n%s GB",
            round(df.memory_usage(deep=True).sum() / (1024**3), 2),
        )
        if not df.empty:
            # Call method to rename column names
            df = self.sanitize_variable_names(
                df, renaming_from="comtrade_human", renaming_to="biotrade"
            )
            # Delete already existing data
            if check_data_presence:
                self.db.delete_data(
                    table_name,
                    api_period,
                    api_period,
                )
            # Append data to db
            self.db.append(df, table_name)
        # Keep track of the the length of the data in the log file
        records_downloaded_csv += len(df)
        return records_downloaded_csv

    def transfer_bulk_csv(
        self,
        table_name,
        start_year,
        end_year,
        frequency,
        check_data_presence,
    ):
        """
        Pump method to transfer bulk files of Comtrade API requests/package to
        Comtrade db.
        Performance with Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz:
        ~ 1.5 hours to upload db monthly data for 1 year
        ~ 9 hours to upload db monthly data for 6 years

        :param (str) table_name, name of the db table to store data
        :param (int) start_year, year from the download should start
        :param (int) start_year, year from the download should end
        :param (str) frequency, as "A" yearly or "M" monthly
        :param (bool) check_data_presence, if data already exists into db

        Example for uploading data from 2016 to 2017 with monthly data into
        "monthly" comtrade table:

        First check if data already exists into db

            >>> from biotrade.comtrade import comtrade
            >>> data_check = comtrade.db.check_data_presence(
            >>>     table = "monthly",
            >>>     start_year = 2016,
            >>>     end_year = 2017,
            >>>     frequency = "M",
            >>> )

        Upload data to db

            >>> comtrade.pump.transfer_bulk_csv(
            >>>     table_name = "monthly",
            >>>     start_year = 2016,
            >>>     end_year = 2017,
            >>>     frequency = "M",
            >>>     check_data_presence = data_check,
            >>> )

        """
        # Adjust frequency parameter in case it is wrong provided
        if table_name in ("yearly", "yearly_hs2"):
            frequency = "A"
        elif table_name == "monthly":
            frequency = "M"
        # Period list of downloads failed
        period_list_failed = []
        # Total records transferred from zip files of API requests
        total_records = 0
        # Download of bioeconomy codes from table "comtrade_hs_2d.csv"
        path = self.parent.config_data_dir / "comtrade_hs_2d.csv"
        # Store table codes into a data frame
        prod = pandas.read_csv(path)
        # Selection of codes which have bioeconomy column = 1
        bioeconomy_tuple = tuple(
            prod[prod.bioeconomy == 1]["product_description"].str[:2]
        )
        # Range of period to download data
        period_block = range(start_year, end_year + 1)
        # Add months for monthly frequency
        if frequency == "A":
            month_list = [""]
        elif frequency == "M":
            month_list = [
                "01",
                "02",
                "03",
                "04",
                "05",
                "06",
                "07",
                "08",
                "09",
                "10",
                "11",
                "12",
            ]
        # Date object of today
        current_date = datetime.datetime.now(pytz.timezone("Europe/Rome")).date()
        # Loop on year and eventually month, depending on the frequency
        # parameter
        for period in period_block:
            # Data not available in the future
            if period > current_date.year:
                break
            for month in month_list:
                if frequency == "M":
                    # Data not available in the future
                    if datetime.datetime(period, int(month), 1).date() > current_date:
                        break
                # Construct the period to pass to transfer_csv_chunk method
                api_period = int(str(period) + month)
                # Use the new Comtrade API package
                # Store gz data into the temporary directory
                temp_dir, response_code = self.download_bulk_gz(
                    api_period,
                    frequency,
                )
                # If data are downloaded (response = 200) copy data into a pandas data frame and upload it to the db
                if response_code == 200:
                    # Store into the database
                    try:
                        # Transfer gz files to db
                        records_downloaded = self.transfer_gz_files(
                            temp_dir,
                            table_name,
                            bioeconomy_tuple,
                            check_data_presence,
                            api_period,
                        )
                        # Total number of records
                        total_records += records_downloaded
                        self.logger.info(
                            f"Updated database table {table_name} for period"
                            + f" {api_period}\n"
                            + f"{total_records} records uploaded in total."
                        )
                    # Failed to store data into db
                    except Exception as error_db:
                        self.logger.warning(
                            "Failed to store data into the database for table"
                            + f" {table_name} and period {api_period}\n"
                            + f"{error_db}"
                        )
                        period_list_failed.append(api_period)
                # Data not downloaded from API requests/package
                else:
                    self.logger.warning(
                        "Failed to download from API request for"
                        + f" period {api_period} due to HTTP/URL error"
                    )
                    period_list_failed.append(api_period)
                # Remove temporary directory
                shutil.rmtree(temp_dir)
        # Log info if some periods failed to be uploaded into db
        if len(period_list_failed):
            self.logger.warning(
                "List of failed download periods for table"
                + f" {table_name}: {period_list_failed}"
            )

    def update_db(self, table_name, frequency, start_year=None):
        """
        Pump method to update db. If data from 2016 are already present,
        it updates data of the last and current year, otherwise it uploads
        data from 2016.

        :param (string) table_name, name of Comtrade db table
        :param (string) frequency, "M" for monthly data or "A" for annual
        :param (int) start_year, year to start the download from, defaults to
        last year if not specified

        Return an error if some periods are not uploaded to the database.

        Example of updating db table "monthly" with monthly frequency data:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.pump.update_db(table_name = "monthly", frequency = "M")

        """
        # Adjust frequency parameter in case it is wrongly provided
        if table_name in ("yearly", "yearly_hs2"):
            frequency = "A"
        elif table_name == "monthly":
            frequency = "M"
        current_year = datetime.datetime.now(pytz.timezone("Europe/Rome")).date().year
        # If start year not specified, update from the last year only
        if start_year is None:
            start_year = current_year - 1
        # Check if data from the start year are present into the database
        data_present = self.db.check_data_presence(
            table_name,
            start_year,
            current_year,
            frequency,
        )
        # Transfer from api bulk requests to db
        self.transfer_bulk_csv(
            table_name,
            start_year,
            current_year,
            frequency,
            data_present,
        )

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

        TODO: make arguments optional.

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
            f"{self.url_api_base}?max={max}&type={type}&freq={freq}&px={px}"
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
            response = requests.get(url=url_api_call, headers=self.header, stream=True)
            print(f"HTTP response code: {response.status_code}")
            json_content = json.load(response.raw)
            # If the data was downloaded incorrectly, raise an error with the validation status
            if json_content["validation"]["status"]["name"] != "Ok":
                raise requests.exceptions.RequestException(
                    f"API message\n{json_content['validation']}"
                )
            else:
                df = pandas.json_normalize(json_content["dataset"])

        # Raise an error if the data frame has the maximum number of rows returned by the query
        if len(df) >= int(max) - 1:
            raise ValueError(
                f"The number of rows in the returned data ({len(df)}) "
                + f"is equal to the maximum number of rows ({max}). "
                + f"Increase the max argument for product {cc} and reporter {r} "
                + "in order to get all requested data from Comtrade."
            )
        df = self.sanitize_variable_names(
            df, renaming_from="comtrade_machine", renaming_to="biotrade"
        )
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
        response = requests.get(url=url, headers=self.header, stream=True)
        print(f"HTTP response code: {response.status_code}")
        json_content = json.load(response.raw)
        df = pandas.json_normalize(json_content["results"])
        return df

    def update_db_parameter(self):
        """Update all parameter lists in the database table such as the product
        tables and country tables.

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.pump.update_db_parameter()

        """
        # Dict for mapping table names and columns
        table_name_dict = {
            "cmd:HS": "product",
            "reporter": "reporter",
            "partner": "partner",
            "qtyunit": "quantity_unit",
            "flow": "flow",
            "mot": "modality_of_transport",
            "customs": "custom",
        }
        # First element of the ordered dict is the unique constraint column of the related table
        table_col_dict = {
            "cmd:HS": OrderedDict(
                {
                    "id": "product_code",
                    "text": "product_description",
                    "isLeaf": "is_leaf",
                    "aggrLevel": "aggregate_level",
                }
            ),
            "reporter": OrderedDict(
                {
                    "reporterCode": "reporter_code",
                    "reporterDesc": "reporter",
                    "reporterNote": "reporter_note",
                    "reporterCodeIsoAlpha2": "reporter_iso2",
                    "reporterCodeIsoAlpha3": "reporter_iso",
                    "entryEffectiveDate": "entry_effective_date",
                    "isGroup": "is_group",
                    "entryExpiredDate": "entry_expired_date",
                }
            ),
            "partner": OrderedDict(
                {
                    "PartnerCode": "partner_code",
                    "PartnerDesc": "partner",
                    "partnerNote": "partner_note",
                    "PartnerCodeIsoAlpha2": "partner_iso2",
                    "PartnerCodeIsoAlpha3": "partner_iso",
                    "entryEffectiveDate": "entry_effective_date",
                    "isGroup": "is_group",
                    "entryExpiredDate": "entry_expired_date",
                }
            ),
            "qtyunit": OrderedDict(
                {
                    "qtyCode": "unit_code",
                    "qtyAbbr": "unit_abbreviation",
                    "qtyDescription": "unit",
                }
            ),
            "flow": OrderedDict({"id": "flow_code", "text": "flow"}),
            "mot": OrderedDict(
                {
                    "id": "mode_of_transport_code",
                    "text": "mode_of_transport",
                }
            ),
            "customs": OrderedDict(
                {
                    "id": "custom_proc_code",
                    "text": "custom_proc",
                }
            ),
        }
        # Load data of complementary Comtrade tables
        for table in table_col_dict.keys():
            df = comtradeapicall.getReference(table)
            df = df.rename(columns=table_col_dict[table])
            if "flow" in df.columns:
                # Rename column content to snake case using a compiled regex
                regex_pat = re.compile(r"\W+")
                df["flow"] = (
                    df["flow"].str.replace(regex_pat, "_", regex=True).str.lower()
                )
            # Delete existing data in the database
            self.logger.info("Dropping existing %s table.", table_name_dict[table])
            # session.execute(delete_stmt)
            with self.db.engine.connect() as conn:
                conn.execute(self.db.tables[table_name_dict[table]].delete())
            # Remove potential duplicated id codes of the new data
            duplicated = df.duplicated(list(table_col_dict[table].items())[0][1])
            if any(duplicated):
                self.logger.info(
                    "Dropping the following duplicated rows from %s new data:\n %s",
                    table_name_dict[table],
                    df[duplicated],
                )
                df = df[~duplicated]
            # Keep only columns actually present in the database structure
            cols_in_db = self.db.tables[table_name_dict[table]].columns.keys()
            # Warn about missing columns
            missing_cols = set(df.columns) - set(cols_in_db)
            if missing_cols:
                msg = "The following columns are present in the Comtrade metadata "
                msg += f"{missing_cols} but they are not present in the database "
                msg += f"{table_name_dict[table]} table structure\n {cols_in_db} \n "
                msg += f"or in the {table_name_dict[table]} renaming dict\n"
                msg += f"{table_col_dict[table]}"
                warnings.warn(msg)
            # Store the data in the database
            self.db.append(
                df[cols_in_db], table_name_dict[table], drop_description=False
            )
            self.logger.info("The %s table has been updated.", table_name_dict[table])

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
        Write API parameter values used to query the comtrade
        repository and status of stored data into the table
        "biotrade_data/comtrade/pump_comtrade_table.csv". If the table does not
        exist, it is created
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
                    "Failed to store %s in the database\n %s",
                    reporter_name,
                    error,
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
