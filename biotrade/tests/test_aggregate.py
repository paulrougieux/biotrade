#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test data aggregation functions from:

    - common/aggregate.py
    - faostat/aggregate.py

Written by Paul Rougieux and Selene Patani.
"""

import pandas
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal
from biotrade.faostat.aggregate import agg_trade_eu_row


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
