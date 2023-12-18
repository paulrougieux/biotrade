#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test the import reallocation functions

"""

import pandas
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal

from biotrade.common.reallocate import allocate_by_partners
from biotrade.common.reallocate import compute_share_by_partners
from biotrade.common.reallocate import compute_share_prod_imp
from biotrade.common.reallocate import split_prod_imp


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
            "year": 1,
            "primary_eq_0": [1, 2, 4],
        }
    )
    df_share = pandas.DataFrame(
        {
            "reporter": ["a", "b", "c"],
            "reporter_code": [1, 2, 3],
            "primary_product": ["p", "p", "q"],
            "year": 1,
            "share_prod_imp": [0, 1, 0.5],
        }
    )
    df_output = split_prod_imp(df, df_share, 1)
    # Use float in the expected series to avoid AssertionError:
    # Attributes of Series are different Attribute "dtype" are different
    # [left]: float64 [right]: int64
    expected_prod = pandas.Series([0, 2, 2.0])
    expected_imp = pandas.Series([1, 0, 2.0])
    assert_series_equal(
        df_output["primary_eq_prod_1"], expected_prod, check_names=False
    )
    assert_series_equal(df_output["primary_eq_imp_1"], expected_imp, check_names=False)


def test_allocate_by_partners():
    df_prod = pandas.DataFrame(
        {
            "year": 1,
            "reporter": ["a", "b"],
            "reporter_code": [1, 2],
            "primary_product": ["p", "p"],
            "primary_eq_imp_1": [6, 14],
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
    expected = df_trade.copy()
    expected.rename(columns={"partner": "partner_1"}, inplace=True)
    expected = expected[["year", "reporter", "partner_1", "primary_product"]]
    # Use float in the expected series
    expected["primary_eq_imp_alloc_1"] = [2, 4, 6, 8.0]
    # Share by partners
    df_trade_2 = compute_share_by_partners(df_trade, threshold=0)
    output = allocate_by_partners(df_prod, df_trade_2, 1)
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
