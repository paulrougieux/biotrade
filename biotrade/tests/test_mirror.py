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
            "partner_code": [2, 3, 1],
            "value": [10, 20, 30],
            "element": ["export", "import", "import"],
        }
    )
    output = put_mirror_beside(df)
    expected = pandas.DataFrame(
        {
            "reporter_code": [1, 2, 2, 3],
            "partner_code": [2, 3, 1, 2],
            "value": [10, 20, 30, np.nan],
            "element": ["export", "import", "import", "export"],
            "value_mirror": [30, np.nan, 10, 20],
        }
    )
    pandas.testing.assert_frame_equal(output, expected)
