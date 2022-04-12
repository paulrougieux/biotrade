#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

JRC biomass Project.
Unit D1 Bioeconomy.


"""
# Built-in modules

# Third party modules
import logging
import pandas

# Internal modules


class Dump:
    """
    Dump comtrade data to compressed PostgreSQL dump files.


    For example dump forest related products (on a server)

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.dump.store2d(table="monthly", 44)

    By default it will create a file name "monthly_44.dump.gz" in the biotrade_data/ repository
    The location of that repository is defined by the environment variable $BIOTRADE_DATA.

    Load the dump into a database (on a laptop)

        >>> file_path = comtrade.dump.data_dir / "raw_comtrade_monthly_15.csv.gz"
        >>> comtrade.dump.load_2d_csv("monthly", file_path)

    """

    # Log debug and error messages
    logger = logging.getLogger("biotrade.comtrade")

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        self.db = self.parent.db
        # Location of the data
        self.data_dir = self.parent.data_dir / "dump"
        if not self.data_dir.exists():
            self.data_dir.mkdir()

    def store_2d_csv(
        self,
        table,
        product_code=None,
        chunk_size=10 ** 6,
    ):
        """Dump all rows where the product code starts with the given product code at the 2 digit level

        :param table_name, str defining the comtrade table
            ("monthly", "yearly", "yearly_hs2")
        :param int product_code 2 digit product code
        :param int chunk_size number of rows to read from the database at a time, to avoid memory errors

        If a list of product codes is given, store one file for each 2 digit code.

        For example save monthly "Animal Or Vegetable Fats And Oil" data which starts with 15:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.dump.store_2d_csv("monthly", 15, chunk_size=10**4)

        Dump all bioeconomy related products

            >>> hs2d = comtrade.products.hs2d.query("bioeconomy == 1")
            >>> for this_code in hs2d["product_code"]:
            >>>     comtrade.dump.store_2d_csv("monthly", this_code)

        """
        # If product_code is a single item, make it a list
        if isinstance(product_code, (int, str)):
            product_code = [product_code]
        # SQL Alchemy table object
        table = self.db.tables[table]
        # Loop on products
        for this_code in product_code:
            # Find all products that start with the given code
            stmt = table.select()
            stmt = stmt.where(table.c.product_code.ilike(f"{this_code}%"))
            df_iter = pandas.read_sql_query(stmt, self.db.engine, chunksize=chunk_size)
            # Store the data frame to a dump file
            file_name = f"{table}_{this_code}.csv".replace(".", "_", 1)
            file_path = self.data_dir / (file_name + ".gz")
            # Remove the destination file if it exists
            if file_path.exists():
                file_path.unlink()
            self.logger.info("Dumping data in chunks")
            # Take a chunk of data from the database and write it to CSV, iterate
            for df in df_iter:
                self.logger.info(
                    "product %s, period %s ",
                    *df.loc[0, ["product_code", "period"]].to_list(),
                )
                df.to_csv(
                    file_path,
                    mode="a",
                    header=not file_path.exists(),
                    index=False,
                    compression="gzip",
                )
            self.logger.info("Stored data to:\n %s", file_path)

    def load_2d_csv(self, table, file_path):
        """Load a dump into the given database table

        :param (str) table, name of the database table
        :param (str or file object) file_path, path to the file from which to save the data

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> file_path = comtrade.dump.data_dir / "raw_comtrade_monthly_15.csv.gz"
            >>> comtrade.dump.load_2d_csv("monthly", file_path)

        """
        df = pandas.read_csv(
            file_path,
            # Force the id column to remain a character column,
            # otherwise str "01" becomes int 1.
            dtype={"product_code": str},
        )
        self.db.append(df, table)
