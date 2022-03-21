#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

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

    @property
    def agricultural_conversion_factors(self):
        """
        Extraction rates and waste of supply are taken from the FAO, technical
        conversion factors for agricultural commodities available at:
        https://www.fao.org/economic/the-statistics-division-ess/methodology/methodology-systems/technical-conversion-factors-for-agricultural-commodities/ar/

        Column df descriptions
        fao_country_code: country code related to faostat db
        fao_country_name: country name related to faostat db
        extraction_rate: coefficient related to a product associated to the specific country
        waste_of_supply: coefficient related to a product associated to the specific country
        fao_product: product name related to faostat db
        fao_product_code: product code related to faostat db
        extraction_rate_country_specific_flag: boolean flag. 1 when extraction_rate of a product
        is available for the specific country, otherwise 0

        Load the coefficients:
            >>> from biotrade.faostat import faostat
            >>> coefficients = faostat.coefficients.agricultural_conversion_factors
        """
        df = pandas.read_csv(
            self.config_data_dir / "faostat_agri_conversion_factors.csv"
        )
        return df

    def extraction_rates(self, filled=True):
        """
        Global extraction rate and waste of supply statistics (mean, min, max,
        std, sample size) of products are computed from
        "faostat_agri_conversion_factors.csv" Missing extraction rate
        average values are filled with commodity tree values when available
        (filled = True).

        Column df descriptions
        fao_product_code: product code related to faostat db
        fao_product: product name related to faostat db
        extraction_rate_mean: average of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv",
            otherwise (if included) taken from commodity trees of technical report available at
            https://www.fao.org/economic/the-statistics-division-ess/methodology/methodology-systems/technical-conversion-factors-for-agricultural-commodities/ar/
        extraction_rate_min: minimum of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        extraction_rate_max: maximum of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        extraction_rate_std: standard deviation of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        extraction_rate_count: sample size of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        waste_of_supply_mean: average of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        waste_of_supply_min: minimum of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        waste_of_supply_max: maximum of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        waste_of_supply_std: standard deviation of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        waste_of_supply_count: sample size of coefficient grouped by product computed from "faostat_agri_conversion_factors.csv"
        extraction_rate_flag: boolean flag. 1 when extraction_rate statistics are computed from "faostat_agri_conversion_factors.csv", otherwise 0
        waste_of_supply_flag: boolean flag. 1 when waste_of_supply statistics are computed from "faostat_agri_conversion_factors.csv", otherwise 0

        Load statistics:
            >>> from biotrade.faostat import faostat
        With gap filling
            >>> faostat.coefficients.extraction_rates(filled = True)
        Without gap filling
            >>> faostat.coefficients.extraction_rates(filled = False)
        """
        if filled:
            df = pandas.read_csv(
                self.config_data_dir
                / "faostat_agri_conversion_factors_summary_stats_manual_gf.csv"
            )
        else:
            df = pandas.read_csv(
                self.config_data_dir
                / "faostat_agri_conversion_factors_summary_stats.csv"
            )
        return df
