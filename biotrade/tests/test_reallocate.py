#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test the import reallocation functions

"""

import pandas
from pandas.testing import assert_series_equal

from biotrade.common.reallocate import compute_prod_imp_share
from biotrade.common.reallocate import split_prod_imp


def test_compute_prod_imp_share():
    df_prod = pandas.DataFrame(
        {
            "reporter": ["a", "b"],
            "reporter_code": [1, 2],
            "product": ["p", "p"],
            "primary_eq": [1, 2],
        }
    )
    df_trade = pandas.DataFrame(
        {
            "reporter": ["a", "a", "b", "b"],
            "reporter_code": [1, 1, 2, 2],
            "partner": ["x", "y", "x", "z"],
            "partner_code": [24, 25, 24, 26],
            "product": ["p", "p", "p", "p"],
            "value": [1, 2, 3, 4],
        }
    )
    output = compute_prod_imp_share(df_prod, df_trade)
    expected = pandas.Series([0.25, 2 / 9], name="share_prod_imp")
    assert_series_equal(output, expected)


def test_split_prod_imp():
    df = pandas.DataFrame(
        {
            "reporter": ["a", "b", "c"],
            "reporter_code": [1, 2, 3],
            "product": ["p", "p", "q"],
            "primary_eq": [1, 2, 4],
            "share_prod_imp": [0, 1, 0.5],
        }
    )
    output_prod, output_imp = split_prod_imp(df)
    # Use float in the expected series to avoid AssertionError: Attributes of
    # Series are different Attribute "dtype" are different [left]:  float64
    # [right]: int64
    expected_pri_cro_prod = pandas.Series([0, 2, 2.0], name="primary_eq_prod")
    expected_pri_cro_imp = pandas.Series([1, 0, 2.0], name="primary_eq_imp")
    assert_series_equal(output_prod, expected_pri_cro_prod)
    assert_series_equal(output_imp, expected_pri_cro_imp)


# In [31]: oil production columns
# Index(['reporter_code', 'reporter', 'product_code', 'product', 'element_code',
#        'element', 'period', 'year', 'unit', 'value', 'flag',
#        'extraction_rate_mean', 'extraction_rate', 'oil_value_share',
#        'primary_eq'],
#       dtype='object')
#
# In [32]:  trade columns
# Out[32]: Index(['reporter', 'partner', 'product', 'period', 'value'], dtype='object')
