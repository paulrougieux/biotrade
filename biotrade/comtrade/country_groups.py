#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

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
        df = df[df.id != "all"]
        return df
