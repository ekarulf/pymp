import logging
import multiprocessing

__all__ = ['logger', 'trace_function', 'State', 'Dispatcher', 'Proxy']

DEBUG = True   # for now

def get_logger(level=None):
    logger = multiprocessing.get_logger()
    format = '[%(asctime)s][%(levelname)s/%(processName)s] %(message)s'
    formatter = logging.Formatter(format)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if level:
        logger.setLevel(level)
    return logger


if DEBUG:
    logger = get_logger(logging.DEBUG)
else:
    logger = get_logger()

def trace_function(f):
    if DEBUG:
        def run(*args, **kwargs):
            logger.debug("%s called" % repr(f))
            value = f(*args, **kwargs)
            logger.debug("%s returned" % repr(f))
            return value
        return run
    else:
        return f
    

from pymp.dispatcher import State, Dispatcher, Proxy
