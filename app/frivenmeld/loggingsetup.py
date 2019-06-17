"""
    loggingsetup.py
    utility to set up logging
"""
import logging

APP_LOGNAME = "frivenmeld"

def init_logging(loglevel=logging.DEBUG):
    """
        Creates standard logging
        for APP_LOGNAME
    """
    logger = logging.getLogger(APP_LOGNAME)
    logger.setLevel(logging.DEBUG)
    channel = logging.StreamHandler()
    channel.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    channel.setFormatter(formatter)
    logger.addHandler(channel)
    logger.debug("Initialized logging for %s", APP_LOGNAME)

    return logger

# end
