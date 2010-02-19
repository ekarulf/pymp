import functools
import time
from multiprocessing.util import Finalize
from threading import Event, RLock, Thread, current_thread
from pymp import logger, trace_function
from pymp.messages import DispatcherState, Request, Response, ProxyHandle, generate_id
from collections import deque

class State(object):
    INIT, STARTUP, RUNNING, SHUTDOWN, TERMINATED = range(5)

class Dispatcher(object):
    PREFIX = '#'
    EXPOSED = '_dispatch_'
    SPIN_TIME = 0.005
    _dispatch_ = ['del_proxy', 'new_proxy']
    
    @trace_function
    def __init__(self, conn):
        self._state = State.INIT
        self._lock = RLock()            # protects internal methods
        self._queue = deque()
        self._pending = dict()          # id => Event or Response
        self._provided_classes = dict() # string => (Class, callable)
        self._consumed_classes = dict() # Class => Class
        self._objects = dict()
        self._thread = Thread(target=self._run, args=(conn,))
        self._thread.daemon = True
        self._thread.start()
        # We register with multiprocessing to prevent bugs related to the
        # order of execution of atexit functions & multiprocessing's join's
        Finalize(self, self._atexit, exitpriority=100)
        
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
    def _atexit(self):
        self.shutdown()
    
    @trace_function
    def __del__(self):
        if self.alive():
            self.shutdown()
        self._thread.join()
        self._objects.clear()
    
    @trace_function
    def provide(self, proxy_class, generator=None, name=None):
        """
        Registers a class that will be provided by this dispatcher
        If present, a generator will be used in lieu of using a the provided
        class's default constructor.
        """
        if not name:
            name = proxy_class.__name__
        with self._lock:
            if name in self._provided_classes:
                raise NameError("The name '%s' is already in use" % name)
            self._provided_classes[name] = (proxy_class, generator)
    
    @trace_function
    def consume(self, name, proxy_client=None):
        if hasattr(name, '__name__'):
            name = name.__name__
        with self._lock:
            if hasattr(self, name):
                raise NameError("The name '%s' is already in use" % name)
            self._consumed_classes[name] = proxy_client or Proxy
            def create_instance(*args, **kwargs):
                new_proxy_args = (name, args, kwargs)
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
            proxy_handle = response.return_value
            try:
                proxy_class = self._consumed_classes[proxy_handle.obj_type]
            except KeyError:
                logger.info("Recieved proxy_class for unexpected type %s" % proxy_handle.obj_type)
            else:
                return proxy_class(self, proxy_handle.id, proxy_handle.exposed)
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
            self._read_once(conn, timeout=self.SPIN_TIME)
        
        while len(self._queue) > 0:
            self._write_once(conn)  # send shutdown message if needed
        
        self.state = State.TERMINATED
        conn.close()
    
    @trace_function
    def join(self):
        self._thread.join()
    
    @trace_function
    def new_proxy(self, name, args, kwargs):
        with self._lock:
            if name not in self._provided_classes:
                raise NameError("%s does not name a provided class" % name)
            source_class, generator = self._provided_classes[name]
            if not generator:
                generator = source_class
            obj = generator(*args, **kwargs)
            obj_id = id(obj)
            
            obj_store, refcount = self._objects.get(obj_id, (obj, 0))
            assert obj is obj_store, "Different objects returned for the same key"
            self._objects[obj_id] = (obj, refcount + 1)
        # Generate the list of exposed methods
        exposed = getattr(source_class, self.EXPOSED, None)
        if exposed is None:
            exposed = []
            for attr_name, attribute in source_class.__dict__.items():
                if not attr_name.startswith('_') and callable(attribute):
                    exposed.append(attr_name)
        return ProxyHandle(obj_id, name, exposed)
    
    @trace_function
    def del_proxy(self, proxy_id):
        """
        Called by clients to signify when they no longer need a proxy
        See: DefaultProxy.__del__
        """
        with self._lock:
            obj, refcount = self._objects.get(proxy_id, (None, 0))
            if refcount <= 0:
                logger.warn("Error destructing object %s, not found" % str(proxy_id))
            elif refcount == 1:
                del self._objects[proxy_id]
            else:
                self._objects[proxy_id] = (obj, refcount - 1)
    
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
            if hasattr(msg, 'id'):
                response = Response(msg.id, exception, None)
                self._process_response(response)
    
    def _read_once(self, conn, timeout=0):
        if not self.alive() or not conn.poll(timeout):
            return
        try:
            msg = conn.recv()
        except EOFError:
            self.state = State.TERMINATED
        if isinstance(msg, Request) and self.state is State.RUNNING:
            response = self._process_request(msg)
            self._queue.appendleft(response)
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
    def _callmethod(self, obj, fname, args, kwargs):
        exposed = getattr(obj, self.EXPOSED, None)
        if exposed and fname in exposed or not exposed and not fname.startswith('_'):
            function = getattr(obj, fname, None)
        else:
            raise AttributeError("%s does not have an exposed method %s" % (repr(obj), fname))
        return function(*args, **kwargs)
    
    @trace_function
    def _process_request(self, request):
        exception = None
        fname = request.function
        if fname.startswith(self.PREFIX):
            obj = self          # invoke methods on dispatcher
            fname = fname[1:]   # strip prefix
        else:
            with self._lock:
                try:
                    obj, refcount = self._objects[request.proxy_id]
                except KeyError:
                    exception = RuntimeError("No object found")
                    return Response(request.id, exception, None)
        try:
            value = self._callmethod(obj, fname, request.args, request.kwargs)
        except Exception as exception:
            return Response(request.id, exception, None)
        else:
            return Response(request.id, None, value)
    
    @trace_function
    def _process_response(self, response):
        with self._lock:
            event = self._pending.pop(response.id, None)
            if hasattr(event, 'set'):
                self._pending[response.id] = response
                event.set()

class Proxy(object):
    @trace_function
    def __init__(self, dispatcher, proxy_id, exposed):
        self._dispatcher = dispatcher
        self._proxy_id = proxy_id
        self._exposed = exposed
        for name in exposed:
            func = functools.partial(self._callmethod, name)
            func.__name__ = name
            setattr(self, name, func)
    
    @trace_function
    def _callmethod(self, name, *args, **kwargs):
        return self._dispatcher.call(name, args, kwargs, proxy_id=self._proxy_id)
    
    @trace_function
    def __del__(self):
        self._dispatcher.call('#del_proxy', (self._proxy_id,), wait=False)
    
