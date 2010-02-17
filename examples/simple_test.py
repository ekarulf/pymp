from pymp import logger, trace_function
from pymp.dispatcher import *
from multiprocessing import Pipe, Process
import threading

# Pretend datastructure
class Foo(object):
    @trace_function
    def __init__(self):
        self.count = 0
        self.lock = threading.RLock()
    
    @trace_function
    def test(self):
        with self.lock:
            self.count += 1
            logger.info("Count: %d" % self.count)
            return self.count

@trace_function
def main():
    parent_conn, child_conn = Pipe()
    p = Process(target=child, args=(child_conn,))
    p.start()

    dispatch = Dispatcher(parent_conn)
    dispatch.register(Foo)
    dispatch.start()

    bar = dispatch.Foo()
    bar.test()
    del bar

    foo = dispatch.Foo()
    count = 0
    try:
        while count < 10:
            count = foo.test()
            logger.info("Count: %d" % count)
            print count
            time.sleep(0.25)
    finally:
        dispatch.shutdown()

@trace_function
def child(conn):
    dispatch = Dispatcher(conn)
    dispatch.register(Foo)
    dispatch.start()
    try:
        dispatch.join()
    finally:
        dispatch.shutdown()
    
if __name__ == '__main__':
    main()