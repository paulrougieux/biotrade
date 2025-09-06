"""Download UN Comtrade bilateral trade data grouped by products

Download, cache to parquet files and read data by products. Save different
files for different download dates.
"""

from functools import cached_property
import json
import pandas

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
            >>> swd_oak_2023 = comtrade.pump.products.download_product_df(440791, 2023)
            >>> swd_oak_2024 = comtrade.pump.products.download_product_df(440791, 2024)

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

    def read_df(self, product_code, reload=False, download_date=None):
        """Read bilateral trade flows for a given product code in all years
        available. Download the data if no data is available or if reload=True.

        Try to follow the behaviour faostat.read_df  which reads the zip file.
        The FAOSTAT zip file is a form of cache. In a similar way, the Comtrade
        parquet file is a cache of the comtrade bilateral trade data (except
        that it is pre-processed already). If reload is True, it will reload
        the data.

        Notes:

            - Comtrade can retroactively update bilateral trade flow data.
              Which means that a trade flows between 2 coutnries in a given year
              can change value in the future. The download and read functions
              could have a mechanism to track this through a hash number or
              simply by storing the download date in the downloaded/cache file
              name to distinguish different versions of the same trade flows for
              a given product in a given year. Not implemented.

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> swd_oak = comtrade.pump.products.read_df(440791)

        """
        product_directory = self.products_dir / f"product_code={product_code}"
        if not product_directory.exists() or reload:
            self.logger.info("Reloading data for product code: %s", product_code)
            # For all years between year min and last year
            last_year = int(pandas.Timestamp.now().strftime("%Y")) - 1
            for year in range(self.comtrade_year_min, last_year):
                self.download_df(product_code, year)
        if download_date is None:
            print("TODO: load latest data")
        else:
            # TODO: Read data for the selected download date
            df = pandas.read_parquet(product_directory)
            print("TODO: load data for specified download_date")
        # Convert all categories to object column types
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
