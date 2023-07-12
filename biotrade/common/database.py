#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Parent object to faostat.database.py and comtrade.database.py

"""

from sqlalchemy import select
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint


class Database:
    """
    Database parent class

    Keep methods and parameters commons to all databases in this file.
    """

    # To be overwritten by the children
    logger = None
    database_url = None
    engine = None
    inspector = None
    schema = None
    tables = None

    def check_unique_constraints(self, df, table):
        """ "
        Method that checks if nan values are present for unique constraint columns and delete the related rows to avoid duplications before appending the dataframe. In fact
        PostgreSQL does not consider nan values as unique and allows more than 1 insertion, causing duplications without errors.

        :param df, Dataframe to be uploaded
        :param table, string name of the table to upload the dataframe
        :return df, Dataframe without nan values in constraint columns

        """
        # Loop on the constraints
        for const in self.tables[table].constraints:
            # Check unique constraints
            if isinstance(const, UniqueConstraint):
                # Get the unique constraints columns
                unique_list = const.columns.keys()
                # Check if some of unique columns contains nan values
                df_nan = df[df[unique_list].isnull().any(axis=1)]
                # If nan values are present, raise a warning in the log and delete those rows
                if len(df_nan):
                    self.logger.warning(
                        "The following "
                        + str(len(df_nan))
                        + " rows will not be uploaded to the database table "
                        + table
                        + " due to nan values of unique constraints "
                        + str(unique_list)
                        + ":\nColumn names\n"
                        + str(df_nan.columns.tolist())
                        + "\nRows\n"
                        + "\n".join(str(i) for i in df_nan.values.tolist())
                    )
                    df.dropna(subset=unique_list, inplace=True)
                break
        return df

    def append(self, df, table, drop_description=True):
        """Store a data frame inside a given database table

        Note: we can only use df.to_sql() with the argument if_exists="append".
        Using if_exists="replace" would erase the table structure defined as
        SQlAlchemy metadata and use a default table structure based on data
        frame column types. We don't want the automated structure for several
        reasons. In particular, we want the database engine to enforce unique
        constraints and to return errors if data frame field types are not
        compatible with table field types defined in the database."""
        # Drop the lengthy product description
        if drop_description and "product_description" in df.columns:
            df.drop(columns=["product_description"], inplace=True)
        # Raise an error if the table is not defined in the metadata
        if table not in self.tables:
            raise ValueError(f"The table '{table}' is not defined in: \n{self}.")
        df = self.check_unique_constraints(df, table)
        df.to_sql(
            name=table,
            con=self.engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            chunksize=10**6,
        )
        self.logger.info("Wrote %s rows to the database table %s", len(df), table)

    def create_if_not_existing(self, table):
        """Create a table in the database if it doesn't exist already

        table sqlalchemy.sql.schema.Table instance description of a table structure
        """
        #  Create the table if it doesn't exist
        if not self.inspector.has_table(table.name, schema=self.schema):
            table.create(bind=self.engine)
            self.logger.info("Created table %s in schema %s.", table.name, self.schema)
        return table

    def most_recent_year(
        self,
        table,
    ):
        """
        Query db table to check which is the most recent year of data.

        :param (string) table, name of table to check data

        :return (int) most_recent_year, the year of the most updated version of table data

        For example check which is the most recent year for yearly table of Comtrade db

            >>> from biotrade.comtrade import comtrade
            >>> most_recent_year = comtrade.db.most_recent_year("yearly")

        """
        # Select table
        table = self.tables[table]
        # Most recent year statement
        stmt = select(func.max(table.c.year))
        # Read data from db to data frame and select the first one
        most_recent_year = self.read_sql_query(stmt)
        most_recent_year = most_recent_year.values[0][0]
        return most_recent_year

    def confirm_db_table_deletion(self, datasets):
        """Confirm database table deletion

        Separate method, because it is reused at different places."""
        msg = f"\nIf the database {self.engine} exists already, "
        msg += "this command will erase the following tables "
        msg += "and replace them with new data:\n - "
        msg += "\n - ".join(datasets)
        if input(msg + "\nPlease confirm [y/n]:") != "y":
            print("Cancelled.")
            return False
        else:
            return True
