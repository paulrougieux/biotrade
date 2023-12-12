#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test functions from:

    - scripts/front_end

Written by Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence
"""

import pandas as pd
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal
from scripts.front_end.functions import aggregated_data, reporter_iso_codes


# def test_reporter_iso_codes():
#     # Create a sample DataFrame for testing
#     df = pd.DataFrame(
#         {
#             "reporter_code": [351, 41, 214, 2, 15, 3, 3, 3],
#             "partner_code": [4, 6, 7, 9, 10, 3, 351, 41],
#             "value": [100, 200, 300, 400, 500, 600, 700, 800],
#         }
#     )
#     # Perform the transformation using the function
#     transformed_df = reporter_iso_codes(df)
#     # Define the expected output DataFrame
#     expected_df = pd.DataFrame(
#         {
#             "reporter_code": ["CHN", "AFG", "ALB"],
#             "partner_code": ["AND", "ARG", "CHN"],
#             "value": [200, 400, 800],
#         }
#     )
#     # Assert that the transformed DataFrame is equal to the expected DataFrame
#     assert_series_equal(
#         transformed_df["reporter_code"], expected_df["reporter_code"]
#     )
#     assert_series_equal(
#         transformed_df["partner_code"], expected_df["partner_code"]
#     )
#     assert_frame_equal(transformed_df, expected_df)


def test_aggregated_data():
    # Sample input data
    df = pd.DataFrame(
        {
            "source": ["a", "a", "a", "a", "a", "a", "a", "a"],
            "reporter_code": [1, 2, 1, 2, 5, 5, 1, 3],
            "partner_code": [7, 7, 6, 6, 10, 1, 2, 7],
            "reporter": ["A", "B", "A", "B", "E", "E", "A", "C"],
            "partner": ["G", "G", "F", "F", "J", "A", "B", "G"],
            "product_code": [11, 11, 13, 14, 15, 15, 11, 11],
            "product": ["11", "11", "13", "14", "15", "15", "11", "11"],
            "element_code": [16, 16, 18, 18, 20, 20, 16, 16],
            "element": ["b", "b", "c", "c", "d", "d", "b", "b"],
            "year": [2020, 2020, 2021, 2021, 2022, 2022, 2020, 2020],
            "period": [2020, 2020, 2021, 2021, 2022, 2022, 2020, 2020],
            "unit": ["kg", "kg", "kg", "kg", "kg", "kg", "kg", "kg"],
            "value": [100, 200, 300, 400, 500, 600, 700, 800],
        }
    )
    code_list = [1, 2, 3]
    agg_country_code = 99
    agg_country_name = "Aggregated"
    # Expected output
    expected_output = pd.DataFrame(
        {
            "source": ["a", "a", "a", "a", "a"],
            "reporter_code": [5, 99, 99, 99, 5],
            "partner_code": [10, 6, 6, 7, 99],
            "reporter": ["E", "Aggregated", "Aggregated", "Aggregated", "E"],
            "partner": ["J", "F", "F", "G", "Aggregated"],
            "product_code": [15, 13, 14, 11, 15],
            "product": ["15", "13", "14", "11", "15"],
            "element_code": [20, 18, 18, 16, 20],
            "element": ["d", "c", "c", "b", "d"],
            "year": [2022, 2021, 2021, 2020, 2022],
            "period": [2022, 2021, 2021, 2020, 2022],
            "unit": ["kg", "kg", "kg", "kg", "kg"],
            "value": [500, 300, 400, 1100, 600],
        }
    )
    # Sort column values
    expected_output = expected_output.sort_values(
        expected_output.columns.tolist()
    ).reset_index(drop=True)
    # Call the function
    result = aggregated_data(df, code_list, agg_country_code, agg_country_name)
    # Sort column values
    result = result.sort_values(result.columns.tolist()).reset_index(drop=True)
    # Assert the result
    assert_series_equal(
        result["value"],
        expected_output["value"],
    )
    assert_frame_equal(result, expected_output)
