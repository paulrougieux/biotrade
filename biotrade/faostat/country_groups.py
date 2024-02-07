#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

"""

# Third party modules
import pandas


class CountryGroups(object):
    """
    Comtrade product list, with additional information.

    Usage:

        >>> from biotrade.faostat import faostat
        >>> print("List of EU countries:")
        >>> print(faostat.country_groups.eu_country_names)
        >>> print("\n\nData frame of country groups:")
        >>> print(faostat.country_groups.df)

    Find the country codes for a given country using string matching:

        >>> country_codes = faostat.country_groups.search_name(["Brazil", "Indonesia"])
        >>> country_codes[["faost_code", "iso3_code", "un_code", "short_name"]]

    """

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Directories #
        self.config_data_dir = self.parent.config_data_dir

    @property
    def df(self):
        """Country groupings

        :return: A data frame of table faostat_country_groups.csv

            >>> from biotrade.faostat import faostat
            >>> df = faostat.country_groups.df

        """
        path = self.config_data_dir / "faostat_country_groups.csv"
        df = pandas.read_csv(
            path,
            keep_default_na=False,
            na_values=[""],
        )
        # Force country codes to be integers instead of floats.
        # Note, we are not using the nullable integer data type Int64 with capital I.
        # https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        # Preferring to keep the standard integer type and -1 for missing codes.
        # TODO: remove this behaviour NA should be kept as NA
        df["faost_code"] = df["faost_code"].fillna(-1).astype("int")
        df["un_code"] = df["un_code"].fillna(-1).astype("int")
        return df

    @property
    def eu_country_names(self):
        """
        EU country name list in the FAOSTAT data
        :return list of eu country names

            >>> from biotrade.faostat import faostat
            >>> eu_country_name_list = faostat.country_groups.eu_country_names
        """
        df = self.df
        return df[df["eu27"] == 1]["fao_table_name"].tolist()

    @property
    def continents(self):
        """Country groupings by continents and subcontinents

        For example select from the crop trade table soy products and merge
        with the continents and subcontinents data, regarding both reporter
        and partner countries.

        >>> from biotrade.faostat import faostat
        >>> db = faostat.db
        >>> df_soy = db.select(table="crop_trade", product = "soy")
        >>> df_continents = faostat.country_groups.continents
        >>> df_soy_merge = df_soy.merge(df_continents, how='left',
        >>>     left_on = 'reporter_code', right_on = 'faost_code')
        >>> df_soy_merge = df_soy_merge.merge(df_continents, how='left',
        >>>     left_on = 'partner_code', right_on = 'faost_code',
        >>>     suffixes=('_reporter','_partner'))
        """
        return self.df[["faost_code", "continent", "sub_continent"]]

    def search_name(self, pattern: str) -> pandas.DataFrame:
        """Search for a country name in the country groups table"""
        if isinstance(pattern, str):
            pattern = [pattern]
        selector = self.df["short_name"].str.contains("|".join(pattern), case=False)
        return self.df.loc[selector.fillna(False)]
