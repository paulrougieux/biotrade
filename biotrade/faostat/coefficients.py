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
    def cuypers2013(self):
        """Coefficients from Cupyers technical report
        table "Extraction rates and value shares of major oil crops"

        Source: Cuypers, Dieter, Theo Geerken, Leen Gorissen, Arnoud Lust, Glen
        Peters, Jonas Karstensen, Sylvia Prieler, G. Fischer, Eva Hizsnyik, and
        Harrij Van Velthuizen. "The impact of EU consumption on deforestation:
        Comprehensive analysis of the impact of EU consumption on
        deforestation." (2013).

        Load the dataset:

        >>> from biotrade.faostat import faostat
        >>> faostat.coefficients.cuypers2013
        """
        df = pandas.read_csv(self.config_data_dir / "cuypers2013_extraction_rates.csv")
        return df
