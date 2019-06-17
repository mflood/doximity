"""
   test_loggingsetup.py

   unit tests for loggingsetup.py
"""
import logging
from frivenmeld.loggingsetup import init_logging

def test_global_init():
    """
        test static init_logging()
    """
    init_logging(logging.DEBUG)
