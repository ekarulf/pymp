from threading import Event, RLock, Thread, current_thread
from pymp.messages import Request, Response, generate_id
from collections import deque

class State(object):
    INIT, STARTUP, RUNNING, SHUTDOWN, TERMINATED = range(1,5)

class Dispatcher(object):
    PREFIX = '#'
    SPIN_TIME = 0.01
    _dispatch_ = ['start', 'shutdown']
    
    def __init__(self, conn):
        self.lock = RLock()
        self.state = State.CLOSED
        self._queue = deque()
        self._pending = dict() # id => Event or Response
        self._pending_lock = RLock()
        self._thread = Thread(target=_run, args=(conn,))
        self._thread.start()
        self.start()
    
    def __del__(self):
        if not self.alive():
            self.shutdown()
    
    def alive(self):
        return self.state in (State.STARTUP, State.RUNNING, State.SHUTDOWN)
    
    def call(function, args, kwargs, wait=True):
        # Step 1: Send Request
        request = Request(generate_id(), function, args, kwargs)
        if wait:
            event = Event()
        else:
            event = None
        with self._pending_lock:
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
        with self._pending_lock:
            response = self._pending.pop(request.id, None)
        if not instanceof(Response, response):
            raise RuntimeError('Dispatcher stored invalid response')
        elif response.exception:
            raise response.exception
        else:
            return response.return_value
    
    def start(self):
        if current_thread() == self._thread:
            self.state = State.RUNNING
            return
        with self.lock:
            if self.state is State.CLOSED:
                self.state = State.STARTUP
                self.call(self._dispatcher_name('start')) # blocking call
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
        raise NotImplemented('TODO: Monday Morning')
    
    def _process_response(self, response):
        with self._pending_lock:
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
                self.call(self._dispatcher_name('shutdown')) # blocking call
            elif self.state is State.TERMINATED:
                pass
            else:
                raise RuntimeError("Error starting dispatcher, invalid state")
    
    @classmethod
    def _dispatcher_name(self, name):
        return self.PREFIX + name

class Client(object):
    pass

class BaseManager(object):
    def __init__(self, parent=None):
        self.parent = parent
        if 
    
    def _callmethod(self, name, *args, **kwargs):
        