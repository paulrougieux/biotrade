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

module_dir = Path(__file__).resolve().parent
