#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test the import reallocation functions

"""

import pandas
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal
from biotrade.common.reallocate import compute_share_prod_imp
from biotrade.common.reallocate import split_prod_imp
from biotrade.common.reallocate import split_by_partners


def test_compute_share_prod_imp():
    df_prod = pandas.DataFrame(
        {
            "year": 1,
            "reporter": ["a", "b"],
            "reporter_code": [1, 2],
            "product": ["p", "p"],
            "production": [1, 2],
        }
    )
    df_trade = pandas.DataFrame(
        {
            "year": 1,
            "reporter": ["a", "a", "b", "b"],
            "reporter_code": [1, 1, 2, 2],
            "partner": ["x", "y", "x", "z"],
            "partner_code": [24, 25, 24, 26],
            "product": ["p", "p", "p", "p"],
            "import_quantity": [1, 2, 3, 4],
        }
    )
    output = compute_share_prod_imp(df_prod, df_trade)
    expected = pandas.Series([0.25, 2 / 9], name="share_prod_imp")
    assert_series_equal(output["share_prod_imp"], expected)


def test_split_prod_imp():
    df = pandas.DataFrame(
        {
            "reporter": ["a", "b", "c"],
            "reporter_code": [1, 2, 3],
            "primary_product": ["p", "p", "q"],
            "primary_eq": [1, 2, 4],
            "share_prod_imp": [0, 1, 0.5],
        }
    )
    output_prod, output_imp = split_prod_imp(df)
    # Use float in the expected series to avoid AssertionError: Attributes of
    # Series are different Attribute "dtype" are different [left]:  float64
    # [right]: int64
    expected_prod = pandas.Series([0, 2, 2.0])
    expected_imp = pandas.Series([1, 0, 2.0])
    assert_series_equal(output_prod, expected_prod)
    assert_series_equal(output_imp, expected_imp)


def test_split_by_partners():
    df_prod = pandas.DataFrame(
        {
            "year": 1,
            "reporter": ["a", "b"],
            "reporter_code": [1, 2],
            "primary_product": ["p", "p"],
            "primary_eq_imp_0": [6, 14],
        }
    )
    df_trade = pandas.DataFrame(
        {
            "year": 1,
            "reporter": ["a", "a", "b", "b"],
            "reporter_code": [1, 1, 2, 2],
            "partner": ["x", "y", "x", "z"],
            "partner_code": [24, 25, 24, 26],
            "primary_product": ["p", "p", "p", "p"],
            "import_quantity": [1, 2, 3, 4],
        }
    )
    output = split_by_partners(df_prod, df_trade, 1)
    expected = df_trade.copy()
    # Use float in the expected series
    expected["primary_eq_imp_1"] = [2, 4, 6, 8.0]
    assert_frame_equal(output[expected.columns], expected)


# In [31]: oil production columns
# Index(['reporter_code', 'reporter', 'product_code', 'product', 'element_code',
#        'element', 'period', 'year', 'unit', 'value', 'flag',
#        'extraction_rate_mean', 'extraction_rate', 'oil_value_share',
#        'primary_eq'],
#       dtype='object')
#
# In [32]:  trade columns
# Out[32]: Index(['reporter', 'partner', 'product', 'period', 'value'], dtype='object')
