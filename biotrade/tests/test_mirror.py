#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test functions in :

    - faostat/mirror.py


"""

import numpy as np
import pandas
from biotrade.faostat.mirror import put_mirror_beside


def test_put_mirror_beside():
    df = pandas.DataFrame(
        {
            "reporter_code": [1, 2, 2],
            "partner_code": [2, 1, 3],
            "value": [10, 20, 30],
            "element": ["export", "import", "import"],
        }
    )
    output = put_mirror_beside(df)
    expected = pandas.DataFrame(
        {
            "reporter_code": [1, 2, 2, 3],
            "partner_code": [2, 1, 3, 2],
            "value": [10, 20, 30, np.nan],
            "element": ["export", "import", "import", "export"],
            "value_mirror": [20, 10, np.nan, 30],
        }
    )
    pandas.testing.assert_frame_equal(output, expected)
