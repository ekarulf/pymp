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
    dispatch.consume("Foo")
    dispatch.consume("StaticFoo")
    dispatch.start()

    from multiprocessing.util import Finalize
    Finalize(dispatch, dispatch.shutdown, exitpriority=10)
    
    baz = dispatch.StaticFoo()
    baz.test()
    bar = dispatch.Foo()
    try:
        bar.tester()
    except AttributeError:
        print "Attribute Error Thrown!"
    else:
        assert False, "Should have thrown an exception"
    
    try:
        bar.test(True)
    except TypeError:
        print "Type Error Thrown!"
    else:
        assert False, "Should have thrown an exception"
    
    foo = dispatch.StaticFoo()
    count = 0
    while count < 3:
        count = foo.test()
        logger.info("Count: %d" % count)
        print count
        time.sleep(0.25)

@trace_function
def child(conn):
    dispatch = Dispatcher(conn)
    foo = Foo()
    static_foo = lambda:foo
    dispatch.provide(Foo)
    dispatch.provide(Foo, static_foo, 'StaticFoo')
    dispatch.start()
    try:
        dispatch.join()
    finally:
        dispatch.shutdown()
    
if __name__ == '__main__':
    import pdb
    main()
