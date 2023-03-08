#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test data aggregation functions from:

    - common/aggregate.py
    - faostat/aggregate.py

Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence
"""

import pandas
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal
from biotrade.faostat.aggregate import agg_trade_eu_row, agg_by_country_groups


def test_agg_by_country_groups():
    df = pandas.DataFrame(
        {
            "reporter": [
                "Spain",
                "Italy",
                "Afghanistan",
                "Australia",
                "Sweden",
                "Italy",
            ],
            "reporter_code": [203, 106, 2, 10, 210, 106],
            "partner": [
                "Argentina",
                "Colombia",
                "Brazil",
                "Norway",
                "Mexico",
                "Mexico",
            ],
            "partner_code": [9, 44, 21, 162, 138, 138],
            "value": [1, 2, 3, 5, 7, 1],
        }
    )
    dfp_expected = pandas.DataFrame(
        {
            "continent_reporter": ["Asia", "Europe", "Oceania"],
            "continent_partner": ["Americas", "Americas", "Europe"],
            "value": [3, 11, 5],
        }
    )
    dfp_output = agg_by_country_groups(
        df, agg_reporter="continent", agg_partner="continent"
    )
    assert_series_equal(dfp_output["value"], dfp_expected["value"])
    assert_frame_equal(dfp_output, dfp_expected)
    dfp_expected = pandas.DataFrame(
        {
            "reporter": [
                "Afghanistan",
                "Australia",
                "Italy",
                "Italy",
                "Spain",
                "Sweden",
            ],
            "reporter_code": [2, 10, 106, 106, 203, 210],
            "sub_continent_partner": [
                "South America",
                "Northern Europe",
                "Central America",
                "South America",
                "South America",
                "Central America",
            ],
            "value": [3, 5, 1, 2, 1, 7],
        }
    )
    dfp_output = agg_by_country_groups(df, agg_partner="sub_continent")
    assert_series_equal(dfp_output["value"], dfp_expected["value"])
    assert_frame_equal(dfp_output, dfp_expected)


def test_agg_trade_eu_row_with_letters():
    df = pandas.DataFrame(
        {
            "reporter": ["A", "A", "B"],
            "partner": ["Y", "Z", "Z"],
            "value": [1, 2, 1],
        }
    )
    dfp_expected = pandas.DataFrame(
        {"reporter": ["A", "B"], "partner": ["row", "row"], "value": [3, 1]}
    )
    dfp_output = agg_trade_eu_row(df, grouping_side="partner")
    assert_series_equal(dfp_output["value"], dfp_expected["value"])
    assert_frame_equal(dfp_output, dfp_expected)
    dfr_expected = pandas.DataFrame(
        {"reporter": ["row", "row"], "partner": ["Y", "Z"], "value": [1, 3]}
    )
    dfr_output = agg_trade_eu_row(df, grouping_side="reporter")
    assert_series_equal(dfr_output["value"], dfr_expected["value"])
    assert_frame_equal(dfr_output, dfr_expected)


def test_agg_trade_eu_row_with_eu_countries():
    df = pandas.DataFrame(
        {
            "reporter": ["Italy", "Italy", "Italy", "A"],
            "partner": ["Y", "France", "France", "Y"],
            "value": [1, 2, 1, 2],
        }
    )
    dfp_expected = pandas.DataFrame(
        {
            "reporter": ["A", "Italy", "Italy"],
            "partner": ["row", "eu", "row"],
            "value": [2, 3, 1],
        }
    )
    dfp_output = agg_trade_eu_row(df, grouping_side="partner")
    assert_series_equal(dfp_output["value"], dfp_expected["value"])
    assert_frame_equal(dfp_output, dfp_expected)
