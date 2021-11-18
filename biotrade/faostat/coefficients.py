#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.
"""


# Third party modules
import pandas


class Coefficients:
    """
    Conversion coefficients for industrial processing and land area.
    """

    def __init__(self, parent):
        # Default attributes #
        self.parent = parent
        self.config_data_dir = self.parent.config_data_dir

    @property
    def cuypers(self):
        """Coefficients from Cupyers technical report
        table "Extraction rates and value shares of major oil crops"

        >>> from biotrade.faostat import faostat
        >>> faostat.coefficients.cuypers
        """
        df = pandas.read_csv(self.config_data_dir / "cuypers2013_extraction_rates.csv")
        return df
