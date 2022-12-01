#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comparison between FAOSTAT and Comtrade country codes

    >>> from biotrade.comtrade import comtrade
    >>> from biotrade.faostat import faostat
    >>> cgc = comtrade.country_groups.reporters
    >>> cgf = faostat.country_groups.df[["faost_code", "un_code", "fao_table_name"]]
    >>> compare = cgc.merge(cgf, left_on="id", right_on="un_code")
    >>> # Show only countries where names don't match
    >>> compare.query("text != fao_table_name")

TODO: Add a df method and inherit from common/country_groups, see issue #102

"""
# Third party modules
import pandas


class CountryGroups(object):
    """
    Comtrade country lists.
    """

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Directories #
        self.config_data_dir = self.parent.config_data_dir

    @property
    def reporters(self):
        """The module internal list of reporter countries
        Usage:

        >>> from biotrade.comtrade import comtrade
        >>> comtrade.country_groups.reporters

        For information the internal list is a filtered version of
        the list of reporters
        originally downloaded from Comtrade with the method:

        >>> comtrade.pump.get_parameter_list("reporterAreas.json")
        """
        df = pandas.read_csv(self.config_data_dir / "comtrade_reporters.csv")
        # Remove the special id "all"
        df = df[df["id"] != "all"]
        # Change id to a numerical variable
        df["id"] = pandas.to_numeric(df["id"], errors="coerce")
        return df
