#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Usage:

    >>> from biotrade.faostat import faostat
    >>> print(faostat.country_groups.eu_country_names)

"""
# List of EU countries names in the FAOSTAT data
EU_COUNTRY_NAMES = [
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czechia",
    "Denmark",
    "Estonia",
    "Finland",
    "Germany",
    "France",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
]


class CountryGroups(object):
    """
    Comtrade product list, with additional information.
    """

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        # Directories #
        self.config_data_dir = self.parent.config_data_dir

    @property
    def eu_country_names(self):
        """EU country names in the FAOSTAT data"""
        return EU_COUNTRY_NAMES
