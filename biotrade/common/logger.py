#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written by Paul Rougieux and Selene Patani.

Copyright (c) 2023 European Union
Licenced under the MIT licence

JRC biomass Project.
Unit D1 Bioeconomy.

Copied from https://docs.python.org/3/howto/logging-cookbook.html

Create a biotrade logger with file handler:

    >>> from biotrade.logger import create_logger
    >>> create_logger("biotrade")

This logger can be then used directly using only the logging module itself from
any sub module or script:

    >>> import logging
    >>> logger = logging.getLogger('biotrade.sub_module')
    >>> logger.info("Doing this and that")
"""

# Third party modules
import logging
import os
from pathlib import Path

# Internal modules
from biotrade import data_dir


def create_logger():
    """Create a logger to keep track of debug and error messages"""
    # create logger with 'biotrade'
    logger = logging.getLogger("biotrade")
    # If it exists already it will just be reused
    if logger.hasHandlers():
        return
    # Set level
    logger.setLevel(logging.DEBUG)
    if os.environ.get("BIOTRADE_LOG"):
        path = Path(os.environ["BIOTRADE_LOG"])
    else:
        path = data_dir
    # create file handler which logs even debug messages
    fh = logging.FileHandler(path / "biotrade.log")
    fh.setLevel(logging.DEBUG)
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(
        "Created a logger with file handler %s.", str(path / "biotrade.log")
    )
