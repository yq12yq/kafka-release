import logging
from test_constants import *

logger = logging.getLogger("namedLogger")


def log_status(pass_status, message):
    """
    Print a log line for the status.
    :rtype: bool
    :param pass_status: whether check has passed or failed
    :param message: message to print in log
    """
    if pass_status:
        logger.info("%s: %s", message, PASSED)
    else:
        logger.info("%s: %s", message, FAILED)
    return pass_status
