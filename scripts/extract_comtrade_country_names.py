#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

"""

# Third party modules
import pandas

# Internal modules
from biotrade.comtrade import comtrade

# Show more rows
pandas.options.display.max_rows = 300

# Download the list of reporters
reporters = comtrade.pump.get_parameter_list("reporterAreas.json")

reporters.to_csv("/tmp/comtrade_reporters.csv", index=False)
