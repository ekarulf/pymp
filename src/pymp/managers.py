import time
from threading import Event, RLock, Thread, current_thread
from pymp import logger, trace_function
from pymp.messages import Request, Response, ProxyHandle, generate_id
from collections import deque

class State(object):
    INIT, STARTUP, RUNNING, SHUTDOWN, TERMINATED = range(5)

class Dispatcher(object):
    PREFIX = '#'
    SPIN_TIME = 0.01
    _dispatch_ = ['del_proxy', 'new_proxy', 'shutdown', 'start']
    
    @trace_function
    def __init__(self, conn):
        self.lock = RLock()             # Public Lock
        self.state = State.INIT
        self._lock = RLock()            # Private Lock
        self._queue = deque()
        self._pending = dict()          # id => Event or Response
        self._proxy_classes = dict()    # Class => Class
        self._objects = dict()
        self._thread = Thread(target=self._run, args=(conn,))
        self._thread.start()
    
    @trace_function
    def __del__(self):
        if not self.alive():
            self.shutdown()
        self._thread.join()
        self._objects.clear()
    
    @trace_function
    def register(self, proxy_class, proxy_client=None, name=None):
        if not name:
            name = proxy_class.__name__
        with self._lock:
            if hasattr(self, name):
                raise NameError("The name '%s' is already in use" % name)
            if proxy_client:
                self._proxy_classes[proxy_class] = proxy_client
            def create_instance(*args, **kwargs):
                new_proxy_args = (proxy_class, args, kwargs)
                return self.call('#new_proxy', new_proxy_args)
            setattr(self, name, create_instance)
    
    def alive(self):
        return self.state in (State.STARTUP, State.RUNNING, State.SHUTDOWN)
    
    @trace_function
    def call(self, function, args=[], kwargs={}, proxy_id=None, wait=True):
        # Step 1: Send Request
        request = Request(generate_id(), proxy_id, function, args, kwargs)
        if wait:
            event = Event()
            with self._lock:
                self._pending[request.id] = event
        if function.startswith(self.PREFIX):
            self._queue.append(request) # Head-of-line
        else:
            self._queue.appendleft(request)
        # Step 2: Wait for Response
        if wait:
            event.wait()
        else:
            return
        # Step 3: Process Response
        with self._lock:
            response = self._pending.pop(request.id, None)
        if not isinstance(response, Response):
            raise RuntimeError('Dispatcher stored invalid response')
        elif response.exception:
            raise response.exception
        elif isinstance(response.return_value, ProxyHandle):
            proxy_class = self._proxy_classes.get(response.return_value.obj_type, Proxy)
            return proxy_class(self, response.return_value.id)
        else:
            return response.return_value
    
    @trace_function
    def start(self):
        if current_thread() == self._thread:
            self.state = State.RUNNING
            return
        with self.lock:
            if self.state is State.INIT:
                self.state = State.STARTUP
                self.call('#start') # blocking call
            elif self.state is State.STARTUP:
                raise RuntimeError("Start called on an object not fully constructed")
            else:
                raise RuntimeError("Error starting dispatcher, invalid state")
    
    @trace_function
    def _run(self, conn):
        while self.state is State.INIT:
            time.sleep(self.SPIN_TIME) # wait for the constructor to catch up
        
        while self.alive():
            if not self._write_once(conn):
                break
            if not self._read_once(conn):
                break
            if not (conn.poll(0) or len(self._queue) > 0):
                time.sleep(self.SPIN_TIME)
        
        self.state = State.TERMINATED
        conn.close()
    
    @trace_function
    def join(self):
        self._thread.join()
    
    @trace_function
    def new_proxy(self, source_class, args, kwargs):
        obj = source_class(*args, **kwargs)
        obj_id = id(obj)
        with self._lock:
            self._objects[obj_id] = obj
        return ProxyHandle(obj_id, source_class)
    
    @trace_function
    def del_proxy(self, proxy_id):
        """
        Called by clients to signify when they no longer need a proxy
        See: DefaultProxy.__del__
        """
        with self._lock:
            try:
                del self._objects[proxy_id]
            except KeyError:
                logger.warn("Error destructing object %s, not found" % str(proxy_id))
    
    def _write_once(self, conn):
        try:
            msg = self._queue.pop()
        except IndexError:
            return True
        try:
            conn.send(msg)
        except IOError:
            return False
        except Exception as exception:
            # Most likely a PicklingError
            response = Response(request.id, exception, None)
            self._process_response(response)
        return True
    
    def _read_once(self, conn):
        if not conn.poll(0):
            return True
        try:
            msg = conn.recv()
        except EOFError:
            return False
        if isinstance(msg, Request):
            self._process_request(msg)
        elif isinstance(msg, Response):
            self._process_response(msg)
        else:
            pass # Ignore
        return True
    
    @trace_function
    def _process_request(self, request):
        fname = request.function
        if fname.startswith(self.PREFIX):
            obj = self
            fname = fname[1:]
        else:
            with self._lock:
                obj = self._objects.get(request.proxy_id, None)
        if obj is None:
            raise RuntimeError("No object found")
        elif hasattr(obj, '_dispatch_') and fname in obj._dispatch_:
            function = getattr(obj, fname, None)
        elif not hasattr(obj, '_dispatch_') and not fname.startswith('_'):
            function = getattr(obj, fname, None)
        else:
            function = None
        if not callable(function):
            raise RuntimeError("No exposed method %s found on object %s" % (fname, repr(type(obj))))
        try:
            value = function(*request.args, **request.kwargs)
        except Exception as exception:
            response = Response(request.id, exception, None)
        else:
            response = Response(request.id, None, value)
        finally:
            self._queue.appendleft(response)
    
    @trace_function
    def _process_response(self, response):
        with self._lock:
            event = self._pending.pop(response.id, None)
            if hasattr(event, 'set'):
                self._pending[response.id] = response
                event.set()
    
    @trace_function
    def shutdown(self):
        if current_thread() == self._thread:
            self.state = State.TERMINATED
            return
        with self.lock:
            if self.state is State.RUNNING:
                self.state = State.STARTUP
                self.call('#shutdown') # blocking call
            elif self.state is State.TERMINATED:
                pass
            else:
                raise RuntimeError("Error starting dispatcher, invalid state")

class Proxy(object):
    @trace_function
    def __init__(self, dispatcher, proxy_id):
        self._dispatcher = dispatcher
        self._proxy_id = proxy_id
    
    @trace_function
    def __del__(self):
        self._dispatcher.call('#del_proxy', (self._proxy_id,), wait=False)
    
    @trace_function
    def __getattr__(self, name):
        def remote_call(*args, **kwargs):
            return self._dispatcher.call(name, args, kwargs, proxy_id=self._proxy_id)
        return remote_call

