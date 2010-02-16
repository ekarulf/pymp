from threading import Event, RLock, Thread, current_thread
from pymp.messages import Request, Response, ProxyHandle, generate_id
from collections import deque

class State(object):
    INIT, STARTUP, RUNNING, SHUTDOWN, TERMINATED = range(5)

class Dispatcher(object):
    PREFIX = '#'
    SPIN_TIME = 0.01
    _dispatch_ = ['destruct', 'shutdown', 'start']
    
    def __init__(self, conn, default_object=None):
        self.lock = RLock()             # Public Lock
        self.state = State.INIT
        self._lock = RLock()            # Private Lock
        self._queue = deque()
        self._pending = dict()          # id => Event or Response
        self._proxy_classes = dict()    # Class => Class
        self._objects = { None: default_object }
        self._thread = Thread(target=_run, args=(conn,))
        self._thread.start()
        self.start()
    
    def __del__(self):
        if not self.alive():
            self.shutdown()
        self._objects.clear()
    
    def alive(self):
        return self.state in (State.STARTUP, State.RUNNING, State.SHUTDOWN)
    
    def call(function, args=[], kwargs={}, object_id=None, wait=True):
        # Step 1: Send Request
        request = Request(generate_id(), object_id, function, args, kwargs)
        if wait:
            event = Event()
            with self._lock:
                self._pending[request.id] = event
        if function.beginswith(self.PREFIX):
            self._queue.append(request) # Head-of-line
        else:
            self._queue.append_left(request)
        # Step 2: Wait for Response
        if wait:
            event.wait()
        else:
            return
        # Step 3: Process Response
        with self._lock:
            response = self._pending.pop(request.id, None)
        if not instanceof(Response, response):
            raise RuntimeError('Dispatcher stored invalid response')
        elif response.exception:
            raise response.exception
        elif instanceof(ProxyHandle, response.return_value):
            proxy_class = self._proxy_classes.get(response.return_value.obj_type, Proxy)
            return proxy_class(response.return_value.id)
        else:
            return response.return_value
    
    def start(self):
        if current_thread() == self._thread:
            self.state = State.RUNNING
            return
        with self.lock:
            if self.state is State.CLOSED:
                self.state = State.STARTUP
                self.call('#start') # blocking call
            elif self.state is State.STARTUP:
                raise RuntimeError("Start called on an object not fully constructed")
            else:
                raise RuntimeError("Error starting dispatcher, invalid state")
    
    def _run(self):
        while self.state is not State.STARTUP:
            time.sleep(self.SPIN_TIME) # wait for the constructor to catch up
        
        while self.alive():
            if not self._write_once(conn):
                break
            if not self._read_once(conn):
                break
            time.sleep(self.SPIN_TIME)
        
        self.state = State.TERMINATED
        conn.close()
    
    def destruct(self, proxy_id):
        """
        Called by clients to signify when they no longer need a proxy
        See: DefaultProxy.__del__
        """
        with self._lock:
            try:
                del self._objects[proxy_id]
            except KeyError:
                # double destruction
                pass
    
    def _write_once(self, conn):
        try:
            msg = self._queue.pop()
        except IndexError:
            return True
        try:
            conn.send(msg)
        except IOError:
            return False
        else:
            return True
    
    def _read_once(self, conn):
        if not conn.poll(0):
            return True
        try:
            msg = conn.recv()
        except EOFError:
            return False
        if instanceof(Request, msg):
            self._process_request(msg)
        elif instanceof(Response, msg):
            self._process_response(msg)
        else:
            pass # Ignore
        return True
    
    def _process_request(self, request):
        fname = request.function
        if fname.beginswith(self.PREFIX):
            obj = self
            fname = fname[1:]
        else:
            with self._lock:
                obj = self._objects.get(request.proxy_id, None)
        if obj is None:
            raise RuntimeError("No object found")
        elif hasattr(obj, '_exposed_') and fname in obj._exposed_:
            function = getattr(obj, fname, None)
        elif not hasattr(obj, '_exposed_') and not fname.beginswith('_'):
            function = getattr(obj, fname, None)
        else:
            function = None
        if not callable(function):
            raise RuntimeError("No exposed method %s found on object %s" % (fname, repr(type(obj))))
        try:
            value = function(*request.args, **request.kwargs)
        except e:
            response = Response(request.id, e, None)
        else:
            response = Response(request.id, None, value)
        finally:
            self._queue.appendleft(response)
    
    def _process_response(self, response):
        with self._lock:
            event = self._pending.pop(response.id, None)
            if instanceof(Event, event):
                self._pending[response.id] = response
                event.set()
            else:
                pass # Ignore
    
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
    def __init__(self, dispatcher, proxy_id):
        self._dispatcher = dispatcher
        self._proxy_id = proxy_id
    
    def __del__(self):
        self._dispatcher.call('#destruct', (self.proxy_id,), wait=False)
    
    def __getattr__(self, name):
        def remote_call(*args, **kwargs):
            return self._dispatcher.call(name, args, kwargs, proxy_id=self._proxy_id)
        return remote_call

class Manager(object):
    pass