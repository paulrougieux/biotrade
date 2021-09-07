#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux.

JRC biomass Project.
Unit D1 Bioeconomy.

Specify the module dir to make it possible to access the extra data.

Usage:

    >>> from biotrade import module_dir

"""

from pathlib import Path
import os

module_dir = Path(__file__).resolve().parent

# Where is the data, default case #
data_dir = Path.home() / "repos/biotrade_data/"

# But you can override that with an environment variable #
if os.environ.get("BIOTRADE_DATA"):
    gftmx_data_dir = Path(os.environ['BIOTRADE_DATA'])
