import time
from threading import Event, RLock, Thread, current_thread
from pymp import logger, trace_function
from pymp.messages import DispatcherState, Request, Response, ProxyHandle, generate_id
from collections import deque

class State(object):
    INIT, STARTUP, RUNNING, SHUTDOWN, TERMINATED = range(5)

class Dispatcher(object):
    PREFIX = '#'
    SPIN_TIME = 0.005
    _dispatch_ = ['del_proxy', 'new_proxy']
    
    @trace_function
    def __init__(self, conn):
        self._state = State.INIT
        self._lock = RLock()            # protects internal methods
        self._queue = deque()
        self._pending = dict()          # id => Event or Response
        self._proxy_classes = dict()    # Class => Class
        self._objects = dict()
        self._thread = Thread(target=self._run, args=(conn,))
        self._thread.start()
        
    def get_state(self):
        return self._state
    
    def set_state(self, state):
        with self._lock:
            if state > self.state:
                self._state = state
                self._queue.append(DispatcherState(state))  # head of line
                logger.info("Changing state to %d" % state)
            elif state == self.state:
                pass
            else:
                raise ValueError('Invalid state progression')
    state = property(get_state, set_state)
    
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
        if self.state is State.INIT:
            self.state = State.STARTUP
        if self.state is State.STARTUP:
            while self.state is State.STARTUP:
                time.sleep(self.SPIN_TIME)
    
    @trace_function
    def shutdown(self):
        if self.state in (State.INIT, State.STARTUP, State.RUNNING):
            self.state = State.SHUTDOWN
    
    @trace_function
    def _run(self, conn):
        while self.state is State.INIT:
            time.sleep(self.SPIN_TIME) # wait for the constructor to catch up
        
        while self.state in (State.STARTUP, State.RUNNING):
            self._write_once(conn)
            self._read_once(conn)
            if not (conn.poll(0) or len(self._queue) > 0):
                time.sleep(self.SPIN_TIME)
        
        while len(self._queue) > 0:
            self._write_once(conn)  # send shutdown message if needed
        
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
        if not self.alive():
            return
        try:
            msg = self._queue.pop()
        except IndexError:
            return
        try:
            if isinstance(msg, DispatcherState) or self.state is State.RUNNING:
                conn.send(msg)
            else:
                logger.info("Skipping outgoing message %s" % repr(msg))
        except IOError:
            self.state = State.TERMINATED
        except Exception as exception:
            # Most likely a PicklingError
            response = Response(request.id, exception, None)
            self._process_response(response)
    
    def _read_once(self, conn):
        if not self.alive() or not conn.poll(0):
            return
        try:
            msg = conn.recv()
        except EOFError:
            self.state = State.TERMINATED
        if isinstance(msg, Request) and self.state is State.RUNNING:
            self._process_request(msg)
        elif isinstance(msg, Response) and self.state is State.RUNNING:
            self._process_response(msg)
        elif isinstance(msg, DispatcherState):
            if self.state is State.STARTUP and msg.state is State.STARTUP:
                self.state = State.RUNNING
            elif msg.state is State.SHUTDOWN:
                self.state = msg.state
        else:
            logger.info("Skipping incoming message %s" % repr(msg))
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

