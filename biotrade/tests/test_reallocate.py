#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test the import reallocation functions

"""

import pandas
from pandas.testing import assert_series_equal

from biotrade.common.reallocate import compute_prod_imp_share


def test_compute_prod_imp_share():
    df_prod = pandas.DataFrame(
        {
            "reporter": ["a", "b"],
            "reporter_code": [1, 2],
            "product": ["p", "p"],
            "primary_crop_eq": [1, 2],
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
    prod_imp_share_expected = pandas.Series([0.25, 2 / 9], name="share_prod_imp")
    df_output = compute_prod_imp_share(df_prod, df_trade)
    assert_series_equal(df_output["share_prod_imp"], prod_imp_share_expected)


# In [31]: oil production columns
# Index(['reporter_code', 'reporter', 'product_code', 'product', 'element_code',
#        'element', 'period', 'year', 'unit', 'value', 'flag',
#        'extraction_rate_mean', 'extraction_rate', 'oil_value_share',
#        'primary_crop_eq'],
#       dtype='object')
#
# In [32]:  trade columns
# Out[32]: Index(['reporter', 'partner', 'product', 'period', 'value'], dtype='object')
