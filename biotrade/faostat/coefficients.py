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
        https://ec.europa.eu/environment/forests/pdf/1.%20Report%20analysis%20of%20impact.pdf

        Load the dataset:

        >>> from biotrade.faostat import faostat
        >>> faostat.coefficients.cuypers2013
        """
        df = pandas.read_csv(self.config_data_dir / "cuypers2013_extraction_rates.csv")
        return df

    def agricultural_conversion_factors(self, filled=True):
        """
        Extraction rates and waste of supply are taken from the FAO,
        technical conversion factors for agricultural commodities available at:
        https://www.fao.org/economic/the-statistics-division-ess/methodology/methodology-systems/technical-conversion-factors-for-agricultural-commodities/ar/
        Missing extraction rates of countries are processed with world average
        values when available (filled = True), column country_specific is 1 only when extraction rate is available
        for the country.
        Load the coefficients:

        >>> from biotrade.faostat import faostat
        With gap filling
        >>> coefficients = faostat.coefficients.agricultural_conversion_factors(filled = True)
        Without gap filling
        >>> coefficients = faostat.coefficients.agricultural_conversion_factors(filled = False)
        """
        if filled:
            df = pandas.read_csv(
                self.config_data_dir / "faostat_agricultural_conversion_factors_gf.csv"
            )
        else:
            df = pandas.read_csv(
                self.config_data_dir / "faostat_agricultural_conversion_factors.csv"
            )
        return df

    def extraction_rate_statistics(self, filled=True):
        """
        Global extraction rate statistics (mean, min, max, std) of products are computed from "faostat_agricultural_conversion_factors.csv"
        Missing extraction rate average values are filled with commodity tree world average
        values when available (filled = True), column statistic_calculation is 1 only when values are computed
        from "faostat_agricultural_conversion_factors.csv".
        Load statistics:

        >>> from biotrade.faostat import faostat
        With gap filling
        >>> coefficients = faostat.coefficients.extraction_rate_statistics(filled = True)
        Without gap filling
        >>> coefficients = faostat.coefficients.extraction_rate_statistics(filled = False)
        """
        if filled:
            df = pandas.read_csv(
                self.config_data_dir / "global_extraction_rate_statistics_gf.csv"
            )
        else:
            df = pandas.read_csv(
                self.config_data_dir / "global_extraction_rate_statistics.csv"
            )
        return df
