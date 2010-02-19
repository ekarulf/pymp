import logging
import multiprocessing

__all__ = ['logger', 'trace_function', 'State', 'Dispatcher', 'Proxy']

DEBUG = False   # for now

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
        name = f.func_name
        def run(*args, **kwargs):
            logger.debug("CALL: %s %s %s" % (name, repr(args), repr(kwargs)))
            try:
                value = f(*args, **kwargs)
            except:
                logger.debug("RAISE:  %s" % name)
                raise
            else:
                logger.debug("RETURN: %s = %s" % (name, repr(value)))
                return value
        return run
    else:
        return f
    

from pymp.dispatcher import State, Dispatcher, Proxy
