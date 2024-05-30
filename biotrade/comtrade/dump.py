#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.


"""
# Built-in modules

# Third party modules
import logging
import pandas
from sqlalchemy import distinct, select
import warnings

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
        chunk_size=10**6,
    ):
        """Dump rows where the product code starts with the given product code at the 2 digit level

        :param table_name, str defining the comtrade table
            ("monthly", "yearly", "yearly_hs2")
        :param int product_code 2 digit product code
        :param int chunk_size number of rows to read from the database at a time, to avoid memory errors

        If a list of product codes is given, store one file for each 2 digit code.

        For example save monthly or yearly "Animal Or Vegetable Fats And Oil"
        data which starts with 15:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.dump.store_2d_csv("monthly", 15)
            >>> comtrade.dump.store_2d_csv("yearly", 15)

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
            # Find products that start with the given code
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

    def store_all_2d_csv(self, table):
        """Dump all bioeconomy related products

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> comtrade.dump.store_all_2d_csv("monthly")
            >>> comtrade.dump.store_all_2d_csv("yearly")
        """
        # Select 2 digit codes present in the database table
        stmt = select(distinct(self.db.tables[table].c.product_code))
        df = pandas.read_sql_query(stmt, self.db.engine)
        codes = [x for x in df.product_code.str[:2]]
        # Create one dump file for each 2 d code
        for this_code in set(codes):
            self.store_2d_csv(table, this_code)

    def load_2d_csv(self, table, file_path):
        """Load a dump into the given database table

        :param (str) table, name of the database table
        :param (str or file object) file_path, path to the file from which to save the data

        TODO: current version requires the user to manually delete the data
        from the database before an update

                psql $BIOTRADE_DATABASE_URL
                DELETE FROM raw_comtrade.monthly WHERE product_code like '44%';
                DELETE FROM raw_comtrade.yearly WHERE product_code like '44%';

        If data is already present in the database for that product code, the
        user will be asked to confirm deletion. To avoid this confirmation
        message when processing many dump files automatically, delete all table
        content before calling this function.

        For information, in case the data is not deleted and if the new data causes
        duplications the database unique constraint will raise an error:

            IntegrityError: (psycopg2.errors.UniqueViolation) duplicate key value
            violates unique constraint
            "monthly_period_flow_code_reporter_code_partner_code_product_key"
            DETAIL:  Key (period, flow_code, reporter_code, partner_code,
            product_code, unit_code, flag)=(201605, 1, 372, 360, 442090, 0, 0)

        Usage:

            >>> from biotrade.comtrade import comtrade
            >>> # Load monthly data for one 2 digit level code and all child codes under it
            >>> file_path = comtrade.dump.data_dir / "raw_comtrade_monthly_15.csv.gz"
            >>> comtrade.dump.load_2d_csv("monthly", file_path)
            >>> # Load yearly data for many 2 digit level codes and all child codes under it
            >>> periodicity = "yearly"
            >>> for code in ["12", "15", "23"]:
            >>>     file_path = comtrade.dump.data_dir
            >>>     file_path = file_path / f"raw_comtrade_{periodicity}_{code}.csv.gz"
            >>>     comtrade.dump.load_2d_csv(periodicity, file_path)

        """
        # Read the csv file in memory
        df = pandas.read_csv(
            file_path,
            # Force the id column to remain a character column,
            # otherwise str "01" becomes int 1.
            dtype={"product_code": str},
        )
        # Temporary fix because of mismatch between the latest Comtrade data
        # and the database table structure
        db_cols = self.db.tables["yearly"].c.keys()
        msg = "Columns present in the dump file but not in the database:"
        msg += f"{set(df.columns) - set(db_cols)}"
        warnings.warn(msg)
        for col in df.columns:
            if col not in db_cols:
                df.drop(columns=col, inplace=True)
        # Check that the name of the file contains the unique product code in the data

        # Delete existing data for the corresponding product code
        # TODO: uptate the db.delete method so that it can deal with product_code.
        # This should be possible if the where statements can be concatenated
        # Or create the statement here to delete at the HS 2 to level.
        self.logger.info("Reading into a data frame:\n %s", file_path)

        # Write to the database
        self.db.append(df, table)
