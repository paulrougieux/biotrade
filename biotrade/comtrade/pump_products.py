"""Download UN Comtrade bilateral trade data grouped by products

Download, cache to parquet files and read data by products. Save different
files for different download dates.
"""

from functools import cached_property
from typing import Union
import pathlib

import json
import pandas
import pyarrow.dataset

from biotrade.common.parquet import harmonize_parquet_schema

try:
    import pyarrow
except Exception as e:
    msg = "Failed to import the pyarrow package, "
    msg += "you can still use methods that don't depend on it.\n"
    print(msg, str(e))

try:
    import urllib
except Exception as e:
    msg = (
        "Failed to import urllib package, you will not be able to load data from there,"
    )
    msg += "but you can still use other methods.\n"
    print(msg, str(e))


def get_latest_download_date(
    product_code: str, base_dir: Union[str, pathlib.Path]
) -> str:
    """
    Returns the latest download_date for the given product_code using pyarrow's dataset API.

    Parameters
    ----------
    product_code : int
        The product code to search for.
    base_dir : str
        Top-level directory.

    Example

        get_latest_download_date("440791", "/home/paul/rp/forobs/biotrade_data/comtrade/products")

    Returns
    -------
    int
        Latest download date, or None.
    """
    p = pathlib.Path(base_dir)
    download_dates = []
    for d in p.glob("download_date=*"):
        if (d / f"product_code={product_code}").is_dir():
            # Extract date string
            dd = int(d.name.split("=")[1])
            download_dates.append(dd)
    return str(max(download_dates)) if download_dates else None


class PumpProducts:
    """Download bilateral trade data grouped by products

    Download and save commodities and products to sub directories of
    biotrade_data/comtrade using parquet partitions to create a tree of
    downloaded files by the partition variables, see `self.partition_cols`.

    Download and cache data for one year or for all years:

        >>> from biotrade.comtrade import comtrade
        >>> print("Product directory", comtrade.products_dir)
        >>> print("Subdirectories will be created in the order of partition columns.")
        >>> print("Partition cols", comtrade.pump.products.partition_cols)
        >>> swd_oak_2023 = comtrade.pump.products.download_df(440791, 2023)
        >>> swd_oak_2024 = comtrade.pump.products.download_df(440791, 2024)

    Read data for all years available:

        >>> swd_oak = comtrade.pump.products.read_product_df("440791")
        >>> comtrade.pump.products.read_product_df(product_code = "440791")

    Read data for a specific year:


    """

    def __init__(self, parent):
        self.parent = parent
        self.pump = self.parent
        self.token = self.parent.token
        self.logger = self.parent.logger
        self.products_dir = self.pump.parent.products_dir
        # Minimum year to start downloading from.
        # Note sawnwood oak data 440791 starts in 1996 for example
        self.comtrade_year_min = 1990

    @cached_property
    def partition_cols(self):
        """Partition columns

        Partition columns will create a tree of parquet files with branches
        in that order"""
        partition_cols = ["download_date", "product_code", "period"]
        if not partition_cols[0] == "download_date":
            raise ValueError("download_date should be the first partition column")
        return partition_cols

    def download_df(self, product_code, year):
        """Download bilateral trade flows for a given product code in a given year

        Notes:
            - this method should be merge with the download_df method once all
              arguments of that method have been made optional.
            - The year argument could be made optional or a list to load all
              years in a range. Not implemented.

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> swd_oak_2023 = comtrade.pump.products.download_df(440791, 2023)
            >>> swd_oak_2024 = comtrade.pump.products.download_df(440791, 2024)

        """
        # Construct URL
        url = f"https://comtradeapi.un.org/data/v1/get/C/A/HS?period={year}&cmdCode={product_code}&includeDesc=false"
        headers = {
            "Cache-Control": "no-cache",
            "Ocp-Apim-Subscription-Key": self.token,
        }
        req = urllib.request.Request(url, headers=headers)
        req.get_method = lambda: "GET"
        self.logger.info(
            "Downloading %s data for period %s from:\n %s", product_code, year, url
        )
        response = urllib.request.urlopen(req)
        self.logger.info("API response code: %s", response.getcode())
        data = response.read()
        result = json.loads(data)
        if "data" in result:
            # Create a data frame from the JSON data
            df = pandas.DataFrame(result["data"])
        else:
            msg = "No data found in the API response. "
            msg += f"API error message: {result['error']}"
            self.logger.error(msg)
            raise ValueError(msg)
        # There might be a data field but it might be empty
        if df.empty:
            self.logger.warning(
                "No data for product_code %s in year %s", product_code, year
            )
            return
        self.logger.info("Downloaded %s rows.", len(df))
        df = self.pump.sanitize_variable_names(
            df, renaming_from="comtrade_machine", renaming_to="biotrade"
        )
        if len(df) >= self.pump.max_row_free_api_limit - 1:
            msg = f"Number of rows {len(df)} equal to the max number for the free API. "
            msg += "Check that you are not missing data in additional unreceived rows."
            self.logger.warning(msg)
        df["download_date"] = int(pandas.Timestamp.now().strftime("%Y%m%d"))
        # If it's not empty, save the data frame to a file inside biotrade_data
        # with product_code, year and download_date as grouping variables.
        df.to_parquet(path=self.products_dir, partition_cols=self.partition_cols)

    def read_df(
        self, product_code: int, reload: bool = False, download_date: str = None
    ) -> pandas.DataFrame:
        """
        Read bilateral trade flows for a given product code for all available years.

        This method loads pre-processed bilateral trade flow data for the specified product code.
        Data is read from a local parquet cache, or downloaded from Comtrade if unavailable or if reload is requested.
        Supports versioning via download dates, similar to FAOSTAT zip file caches.

        Parameters
        ----------
        product_code : int
            The product code for which bilateral trade data should be retrieved.
        reload : bool, optional
            If True, data for all available years is re-downloaded from Comtrade
            and the local cache is updated. Default is False.
        download_date : str, optional
            Download date in format 'YYYYMMDD' identifying which cached version to load.
            If None, loads the latest available version for the product_code.

        Returns
        -------
        df : pandas.DataFrame
            Bilateral trade flow DataFrame for the provided product_code, with all categorical
            columns converted to object dtype.

        Notes
        -----
        - Comtrade may retroactively update bilateral trade data, so each
          version is uniquely identified by its download date.
        - If reload is True, the method downloads and caches all years between
        ` self.comtrade_year_min` and last available year.
        - Future improvement: identify cache versions using hash or download
          date to distinguish updates.

        Implementation errors due to different data types across years.

        Loading the data with pandas.read_parquet() returns an
        ArrowNotImplementedError: Unsupported cast from double to null using
        function cast_null

        >>> df = pandas.read_parquet(product_directory)

        Loading the dataset with pyarrow.dataset.dataset, also returns an error:
        ArrowNotImplementedError: Unsupported cast from double to null using function cast_null
        >>> dataset = pyarrow.dataset.dataset(product_directory, format="parquet")
        >>> table = dataset.to_table()
        >>> df = table.to_pandas()

        Examples
        --------
        >>> from biotrade.comtrade import comtrade
        >>> swd_oak = comtrade.pump.products.read_df(440791)
        >>> swd_oak_specific = comtrade.pump.products.read_df(440791, download_date='20250906')

        See Also
        --------
        - `download_df` : Downloads a single year for the given product_code.
        - `get_latest_download_date` : Finds the latest cache version by date.
        """
        product_directory = (
            self.products_dir
            / f"download_date={download_date}"
            / f"product_code={product_code}"
        )
        if download_date is not None and not product_directory.exists():
            msg = f"The following directory doesn't exist: {product_directory}\n"
            msg += f"for the specified download date: {download_date}.\n"
            msg += "To reload data now, leave the download date empty."
            raise ValueError(msg)
        # Update the product directory with the latest download date if unspecified
        if download_date is None:
            download_date = get_latest_download_date(
                product_code, base_dir=self.products_dir
            )
            product_directory = (
                self.products_dir
                / f"download_date={download_date}"
                / f"product_code={product_code}"
            )
        # Download data from the Comtrade API if required
        if (not product_directory.exists()) or reload:
            last_year = int(pandas.Timestamp.now().strftime("%Y")) - 1
            msg = "Downloading data from the Comtrade API for product code: %s\n"
            msg += f"For all years between {self.comtrade_year_min} and {last_year}."
            self.logger.info(msg, product_code)
            for year in range(self.comtrade_year_min, last_year):
                self.download_df(product_code, year)
        # Read data
        self.logger.info("Reading data from parquet files in %s", product_directory)
        schema = harmonize_parquet_schema(product_directory)
        dataset = pyarrow.dataset.dataset(
            product_directory, format="parquet", schema=schema
        )
        table = dataset.to_table()
        df = table.to_pandas()
        # Convert categories columns to strings
        category_cols = df.select_dtypes(include=["category"]).columns
        df[category_cols] = df[category_cols].astype("object")
        return df

    def read_latest_downloaded_df(self, product_code):
        """Get data for the latest download_date only

        Usage:

            df, latest_date = get_latest_download_data("product_code=440791")
            print(f"Loaded {len(df)} rows from {latest_date}")

        """
        # TODO: loop over all available sub directories for the download date?
        # Find data for the latest download date
        product_dir = self.products_dir / f"product_code={product_code}"
        dataset = pyarrow.dataset.dataset(product_dir, format="parquet")

        # Get all unique download_dates
        download_dates = (
            dataset.to_table(columns=["download_date"])
            .to_pandas()["download_date"]
            .unique()
        )
        print(f"Available download dates: {sorted(download_dates)}")

        # TODO:
        msg = (
            "Group by years, get the latest date and load only that date for that year"
        )
        raise ValueError(msg)
        latest_date = max(download_dates)
        print(f"Latest download date: {latest_date}")

        # Filter for latest date only
        df = pandas.read_parquet(
            product_dir, filters=[("download_date", "==", latest_date)]
        )

        return df, latest_date
