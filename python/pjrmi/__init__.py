import atexit
import collections.abc
import itertools
import io
import keyword
import logging
import numpy
import os
import pickle
import random
import re
import signal
import ssl
import snappy
import socket
import struct
import sys
import tempfile
import time
import weakref

from   builtins         import ascii
from   inspect          import getfullargspec
from   threading        import (Condition, Lock, RLock, Thread,
                                current_thread, local as ThreadLocal)
from   traceback        import format_tb
from   types            import (BuiltinMethodType, FunctionType, MethodType)

from   ._config         import (PJRMI_VERSION)
from   ._util           import (ImpreciseRepresentationError,
                                strict_array, strict_bool, strict_number)

# The C extension might not be present, import it on a best-effort basis
try:
    import pjrmi.extension
except ImportError:
    pass

# MethodWrapperType is not present in earlier Python3 versions
try:
    from types import MethodWrapperType
except ImportError:
    class MethodWrapperType():
        pass

LOG = logging.getLogger(__name__)
_DEEP_DEBUG = 5

# A jar containing all the runtime dependencies is stored in the module dir.
_PJRMI_FATJAR = "{}/lib/pjrmi.jar".format(os.path.dirname(__file__))
# Required shared libraries from the module dir.
_PJRMI_SHAREDLIBS_PATH = os.path.dirname(__file__) + '/lib'

class PJRmi:
    """
    Client code for connecting to the Python-Java-RMI infrastructure.

    This class connects to a PJRmi server. Once connected it can interact with
    the running Java process directly; local shims of remote objects are created
    locally in the Python instance and may be treated roughly like the Java
    objects that they really are.

    A word about threading. PJRmi has two different threading models. The simple
    one supports the basic client/server model but it is not reentrant; i.e. you
    can not use callbacks from one side into the other. Callbacks can be needed
    when, say, you pass a Python function to a Java method to invoke. Callbacks
    may be enabled on on the Java side by having a positive number of worker
    threads specified for the Java server. The two modes of operation use
    different threading models; when workers are enabled then a thread-handoff
    must happen on the Python side, thus reducing its call throughput.

    PJRmi supports multiple threads on the Python side. However, it is possible
    for the actions of one thread to block another in the case where callbacks
    are not enabled. As such, if you want to employ heavy use of Python threads
    then it is recommended that you enable callbacks by having the Java server
    use worker threads.
    """

    # Hello string, used upon connection. This should have the same value as the
    # one in the Java code. The major and minor version numbers should match the
    # `pjrmiVersion` values in `gradle.properties`. Typically the minor version
    # number should change whenever the wire format changes.
    _HELLO = b"PJRMI_1.13"

    # Flags denoting server info
    _FLAG_USE_WORKERS = 1

    # Message types
    _NONE                  = b'\0'
    _INSTANCE_REQUEST      = b'A' # Client to server
    _ADD_REFERENCE         = b'B' # Client to server & Server to client
    _DROP_REFERENCES       = b'C' # Client to server & Server to client
    _TYPE_REQUEST          = b'D' # Client to server
    _METHOD_CALL           = b'E' # Client to server
    _TO_STRING             = b'F' # Client to server
    _GET_FIELD             = b'G' # Client to server
    _SET_FIELD             = b'H' # Client to server
    _GET_ARRAY_LENGTH      = b'I' # Client to server
    _NEW_ARRAY_INSTANCE    = b'J' # Client to server
    _OBJECT_CAST           = b'K' # Client to server
    _LOCK                  = b'L' # Client to server
    _UNLOCK                = b'M' # Client to server
    _INJECT_CLASS          = b'N' # Client to server
    _GET_VALUE_OF          = b'O' # Client to server
    _GET_CALLBACK_HANDLE   = b'P' # Client to server
    _CALLBACK_RESPONSE     = b'Q' # Client to server
    _GET_PROXY             = b'R' # Client to server
    _INVOKE_AND_GET_OBJECT = b'S' # Client to server
    _INJECT_SOURCE         = b'T' # Client to server
    _REPLACE_CLASS         = b'U' # Client to server
    _OBJECT_REFERENCE      = b'a' # Server to client
    _TYPE_DESCRIPTION      = b'b' # Server to client
    _ARBITRARY_ITEM        = b'c' # Server to client
    _EXCEPTION             = b'd' # Server to client
    _ASCII_VALUE           = b'e' # Server to client
    _EMPTY_ACK             = b'f' # Server to client
    _ARRAY_LENGTH          = b'g' # Server to client
    _UTF16_VALUE           = b'h' # Server to client
    _PICKLE_BYTES          = b'i' # Server to client
    _CALLBACK              = b'j' # Server to client
    _PYTHON_EVAL_OR_EXEC   = b'k' # Server to client
    _PYTHON_INVOKE         = b'l' # Server to client
    _OBJECT_CALLBACK       = b'm' # Server to client
    _GET_OBJECT            = b'n' # Server to client
    _PYTHON_REFERENCE      = b'o' # Server to client
    _GETATTR               = b'p' # Server to client
    _SET_GLOBAL_VARIABLE   = b'q' # Server to client
    _SHMDATA_BYTES         = b'r' # Server to client

    # The Python request ID used by Java for unsolicited callbacks
    _CALLBACK_REQUEST_ID = -1

    # Some special values
    _NULL_HANDLE = 0

    # Method argument types
    _ARGUMENT_VALUE     = b'V'
    _ARGUMENT_REFERENCE = b'R'
    _ARGUMENT_SHMDATA   = b'S'
    _ARGUMENT_METHOD    = b'M'
    _ARGUMENT_LAMBDA    = b'L'

    # The wire format for passing objects from Java to python in things like
    # method calls
    _VALUE_FORMAT_REFERENCE                = b'A'
    _VALUE_FORMAT_RAW_PICKLE               = b'B'
    _VALUE_FORMAT_SNAPPY_PICKLE            = b'C'
    _VALUE_FORMAT_PYTHON_REFERENCE         = b'D'
    _VALUE_FORMAT_SHMDATA                  = b'E'
    _VALUE_FORMAT_BESTEFFORT_PICKLE        = b'F'
    _VALUE_FORMAT_BESTEFFORT_SNAPPY_PICKLE = b'G'
    # Expose some of these for the public user
    VALUE_FORMAT_PICKLE            = _VALUE_FORMAT_SNAPPY_PICKLE
    VALUE_FORMAT_BESTEFFORT_PICKLE = _VALUE_FORMAT_BESTEFFORT_SNAPPY_PICKLE
    VALUE_FORMAT_SHMDATA           = _VALUE_FORMAT_SHMDATA

    # TypeFlags (corresponds to values in PJRmi.TypeFlags)
    _TYPE_FLAGS_IS_PRIMITIVE            = 1 << 0
    _TYPE_FLAGS_IS_THROWABLE            = 1 << 1
    _TYPE_FLAGS_IS_INTERFACE            = 1 << 2
    _TYPE_FLAGS_IS_ENUM                 = 1 << 3
    _TYPE_FLAGS_IS_ARRAY                = 1 << 4
    _TYPE_FLAGS_IS_FUNCTIONAL_INTERFACE = 1 << 5

    # MethodFlags (corresponds to values in PJRmi.MethodFlags)
    _METHOD_FLAGS_IS_STATIC     = 1 << 0
    _METHOD_FLAGS_IS_DEPRECATED = 1 << 1
    _METHOD_FLAGS_IS_EXPLICIT   = 1 << 2
    _METHOD_FLAGS_HAS_KWARGS    = 1 << 3
    _METHOD_FLAGS_IS_DEFAULT    = 1 << 8

    # Types of method invocation
    SYNC_MODE_SYNCHRONOUS = b'S'
    SYNC_MODE_JAVA_THREAD = b'J'

    # Accepted values for various kwargs (for faster access)
    _ACCEPTED_VALUE_FORMATS = frozenset((
        _VALUE_FORMAT_REFERENCE,
        _VALUE_FORMAT_RAW_PICKLE,
        _VALUE_FORMAT_SNAPPY_PICKLE,
        _VALUE_FORMAT_PYTHON_REFERENCE,
        _VALUE_FORMAT_SHMDATA,
        _VALUE_FORMAT_BESTEFFORT_PICKLE,
        _VALUE_FORMAT_BESTEFFORT_SNAPPY_PICKLE,
    ))
    _ACCEPTED_SYNC_MODES = frozenset((
        SYNC_MODE_SYNCHRONOUS,
        SYNC_MODE_JAVA_THREAD
    ))

    # Java can't represent arrays which are any larger than this value
    # (inclusive). 2147483647 is 2^31-1, also known as Integer.MAX_VALUE.
    _MAX_JAVA_ARRAY_SIZE = 2147483647

    # All the instance, keyed by id()
    _INSTANCES = weakref.WeakValueDictionary()


    def __init__(self, transport, use_shm_arg_passing=False):
        """
        PJRmi constructor method.

        :param transport: The underlying data transport to connect over.
        """

        # First we register ourselves with the global dictionary
        self._INSTANCES[id(self)] = self

        # Create the _JavaObject meta-class for this instance, we use this to
        # look up class members etc. Each meta-class is specific to a connection.
        class _JavaClass(type):
            def __getattr__(self_, k):
                """
                Look for a inner classes of a Java class.
                """
                try:
                    # Look for it as an inner class, these are separated by '$'
                    # not '.' in Java Land
                    subclass = self.class_for_name('%s$%s' % (self_._classname, k))

                    # If we got here then we found it, save is as an attr so we
                    # don't need to perform the lookup next time
                    setattr(self_, k, subclass)
                    return subclass

                except Exception as e:
                    raise AttributeError(
                        f"Java class '{self_._classname}' has no such attribute '{k}': "
                        f"{e.__class__.__name__}: {e}"
                    ) from e

        # Init members
        self._ready              = False  # set at connect
        self._flags              = 0      # read at connect
        self._receiver           = None   # set at connect
        self._transport          = transport
        self._send_request_id    = itertools.count().__next__
        self._send_lock          = Lock() # protects _send_request_id and _send() calls
        self._recv_lock          = Lock() # protects _recv() calls
        self._recv_thread        = None   # set lazily
        self._recvd              = dict()
        self._class_getter       = None
        self._classes_by_id      = dict()
        self._classes_by_name    = dict()
        self._pending_drops      = list()
        self._callback_nextid    = itertools.count().__next__
        self._callback_func2id   = dict()
        self._callback_id2func   = dict()
        self._callback_func2wrap = dict()
        self._callback_obj2id    = dict()
        self._callback_obj2wrap  = dict()
        self._callback_id2obj    = dict()
        self._callback_id2ref    = dict()
        self._callback_lock      = RLock() # protects _callback_foo
        self._workers            = list()
        self._service_name       = None
        self._connected          = False
        self._eof                = False
        self._JavaClass          = _JavaClass
        self._use_shmdata        = use_shm_arg_passing
        self._shmdata_files      = list()
        self._shmdata_tidylists  = list()
        self._thread_id_xor      = random.randint(0, 0x7fffffffffffffff)

        # The handlers for different message types. We use these like a type of
        # switch statement. It also makes profiling easier since we can see what
        # method is being called and how long it's taking.
        self._handlers = {
            self._OBJECT_REFERENCE : self._handle_object_reference,
            self._TYPE_DESCRIPTION : self._handle_type_description,
            self._ARBITRARY_ITEM   : self._handle_arbitrary_item,
            self._EXCEPTION        : self._handle_exception,
            self._ASCII_VALUE      : self._handle_ascii_value,
            self._UTF16_VALUE      : self._handle_utf16_value,
            self._PICKLE_BYTES     : self._handle_pickle_bytes,
            self._EMPTY_ACK        : self._handle_empty_ack,
            self._ARRAY_LENGTH     : self._handle_array_length,
            self._PYTHON_REFERENCE : self._handle_python_reference,
            self._SHMDATA_BYTES    : self._handle_shmdata_bytes,
        }


    def __del__(self):
        # Drop the connection upon deletion and remove ourselves from the global
        # table of instances
        self.disconnect()
        try:
            # Race conditions with the GC mean that this might fail
            del self._INSTANCES[id(self)]
        except Exception:
            pass


    def __enter__(self):
        # Attempt to connect, this might fail if we're already connected so
        # we'll ignore errors. If it fails for another reason then we'll see
        # that in other ways later on when we try to use it.
        LOG.debug("Entering 'with' context")
        try:
            self.connect()
        except Exception:
            pass

        return self


    def __exit__(self, typ, value, traceback):
        # If we exited via a Java exception then, as we're about to disconnect,
        # PJRmi won't be available if and when it comes time to render it as a
        # string. As such we need to force rendering before we disconnect.
        if isinstance(value, JavaException):
            # Force the exception to render, thus caching the result. This
            # relies on the __str__() implementation in JavaExceptions.
            str(value)

        # Now we can disconnect
        LOG.debug("Exiting 'with' context, disconnecting")
        self.disconnect()


    def connect(self):
        """
        Connect to the server. If this fails it will leave the instance in an
        unusable state.
        """

        # Nop?
        if self._connected:
            raise ValueError('Already connected')

        self._transport.connect()

        # We assume that we will be using this thread for all our connections if
        # we don't have a separate Receiver thread. This is checked in
        # _read_result().
        self._recv_thread = current_thread()

        # The HELLO handshake
        self._transport.send(self._HELLO)

        # And some connection information about this client
        self._transport.send(self._format_utf16(' '.join(sys.argv)))
        self._transport.send(self._format_int32(os.getpid()))
        self._transport.send(self._format_int64(id(self)))

        # Now look for the ACK back
        ack = b''
        while len(ack) < len(self._HELLO):
            chunk = self._transport.recv(len(self._HELLO) - len(ack))
            if chunk == b'':
                self._eof = True
                raise IOError('EOF while reading handshake')
            ack += chunk

        # See if we got back what we expected. Typically the only error we
        # should see is that where the client is using a different version to
        # the server, and that should be pretty obvious from context.
        if ack != self._HELLO:
            self._transport.disconnect()
            raise IOError(
                "Didn't get expected handshake back; "
                "got %s but expected %s; "
                "this likely indicates a version mismatch between "
                "the client and server libraries."
                % (ack, self._HELLO)
            )

        # We are about to get a short string back. Read in its size, if it's
        # negative then it means we encountered an error and the connection will
        # be closed, else it's actually the service name from the other side.
        sz = struct.unpack('!b', self._transport.recv(1))[0]
        string = b''
        while len(string) < abs(sz):
            chunk = self._transport.recv(abs(sz) - len(string))
            if chunk == b'':
                self._eof = True
                raise IOError('EOF while reading connection string')
            string += chunk

        # Interpret the string in different ways depending on the sign of size
        if sz < 0:
            raise ValueError(string.decode())
        else:
            self._service_name = string

        # Now, the flags
        self._flags = struct.unpack('!b', self._transport.recv(1))[0]

        # We're now connected so it's correct to mark ourselves as so
        self._connected = True

        # Make sure that we gracefuly disconnect at process exit; this is
        # required to avoid race conditions in multi-threaded instances where
        # the Receiver thread will be hard-terminated and won't perform its
        # shutdown operations.
        parent_pid = os.getpid()
        def atexit_disconnect(selfref=weakref.ref(self)):
            # We use a weakref so that the GC can collect this PJRmi instance
            # when it is dead. If we are called in a forked child process, we do
            # not want to have this hook do anything since the child doesn't own
            # the connection.
            self_ = selfref()
            if os.getpid() == parent_pid and self_ is not None:
                self_.disconnect()
        atexit.register(atexit_disconnect)

        # Now instantiate a bunch classes we will reference a lot. Order is
        # important here since getting the class information of one object might
        # require having set up another (e.g. Object).
        #
        # If you add to these then ensure that you update the default passlist
        # in the Java code.
        self._java_lang_Object            = self.class_for_name('java.lang.Object')
        self._java_lang_Iterable          = self.class_for_name('java.lang.Iterable')
        self._java_lang_Comparable        = self.class_for_name('java.lang.Comparable')
        self._java_lang_Boolean           = self.class_for_name('java.lang.Boolean')
        self._java_lang_Character         = self.class_for_name('java.lang.Character')
        self._java_lang_String            = self.class_for_name('java.lang.String')
        self._java_lang_Number            = self.class_for_name('java.lang.Number')
        self._java_lang_Byte              = self.class_for_name('java.lang.Byte')
        self._java_lang_Short             = self.class_for_name('java.lang.Short')
        self._java_lang_Integer           = self.class_for_name('java.lang.Integer')
        self._java_lang_Long              = self.class_for_name('java.lang.Long')
        self._java_lang_Float             = self.class_for_name('java.lang.Float')
        self._java_lang_Double            = self.class_for_name('java.lang.Double')
        self._java_lang_void              = self.class_for_name('void')
        self._java_lang_boolean           = self.class_for_name('boolean')
        self._java_lang_char              = self.class_for_name('char')
        self._java_lang_byte              = self.class_for_name('byte')
        self._java_lang_short             = self.class_for_name('short')
        self._java_lang_int               = self.class_for_name('int')
        self._java_lang_long              = self.class_for_name('long')
        self._java_lang_float             = self.class_for_name('float')
        self._java_lang_double            = self.class_for_name('double')
        self._java_lang_Exception         = self.class_for_name('java.lang.Exception')
        self._java_util_Collection        = self.class_for_name('java.util.Collection')
        self._java_util_Iterator          = self.class_for_name('java.util.Iterator')
        self._java_util_List              = self.class_for_name('java.util.List')
        self._java_util_Map               = self.class_for_name('java.util.Map')
        self._java_util_Map_Entry         = self.class_for_name('java.util.Map$Entry')
        self._java_util_Set               = self.class_for_name('java.util.Set')
        self._java_util_concurrent_Future = self.class_for_name('java.util.concurrent.Future')

        # Add hooks for boxed types
        self._boxes_by_type_id = {
            self._java_lang_String. _type_id : _JavaString,
            self._java_lang_Byte.   _type_id : _JavaByte,
            self._java_lang_Short.  _type_id : _JavaShort,
            self._java_lang_Integer._type_id : _JavaInt,
            self._java_lang_Long.   _type_id : _JavaLong,
            self._java_lang_Float.  _type_id : _JavaFloat,
            self._java_lang_Double. _type_id : _JavaDouble
        }

        # We can now map the handlers for _handle_arbitrary_item
        self._handle_arbitrary_item_handlers = {
            self._java_lang_boolean._type_id : self._read_boolean,
            self._java_lang_byte._type_id    : self._read_int8,
            self._java_lang_char._type_id    : self._read_char,
            self._java_lang_double._type_id  : self._read_double,
            self._java_lang_float._type_id   : self._read_float,
            self._java_lang_int._type_id     : self._read_int32,
            self._java_lang_long._type_id    : self._read_int64,
            self._java_lang_short._type_id   : self._read_int16,
        }

        # Other utility classes. Defined after we have set up the boxes since
        # their deserialisation might depend on those boxes.
        self._java_lang_AutoCloseable               = self.class_for_name('java.lang.AutoCloseable')
        self._java_lang_NoSuchFieldException        = self.class_for_name('java.lang.NoSuchFieldException')
        self._java_lang_NoSuchMethodException       = self.class_for_name('java.lang.NoSuchMethodException')
        self._java_util_NoSuchElementException      = self.class_for_name('java.util.NoSuchElementException')
        self._java_util_function_Function           = self.class_for_name('java.util.function.Function')
        self._java_util_function_BiFunction         = self.class_for_name('java.util.function.BiFunction')
        self._com_deshaw_hypercube_Hypercube        = self.class_for_name('com.deshaw.hypercube.Hypercube')
        self._com_deshaw_pjrmi_JavaProxyBase        = self.class_for_name('com.deshaw.pjrmi.JavaProxyBase')
        self._com_deshaw_pjrmi_PythonObject         = self.class_for_name('com.deshaw.pjrmi.PythonObject')
        self._com_deshaw_pjrmi_PythonFunction       = self.class_for_name('com.deshaw.pjrmi.PythonFunction')
        self._com_deshaw_pjrmi_PythonKwargsFunction = self.class_for_name('com.deshaw.pjrmi.PythonKwargsFunction')
        self._com_deshaw_pjrmi_PythonSlice          = self.class_for_name('com.deshaw.pjrmi.PythonSlice')
        self._L_java_lang_boolean                   = self.class_for_name('[Z')
        self._L_java_lang_char                      = self.class_for_name('[C')
        self._L_java_lang_byte                      = self.class_for_name('[B')
        self._L_java_lang_short                     = self.class_for_name('[S')
        self._L_java_lang_int                       = self.class_for_name('[I')
        self._L_java_lang_long                      = self.class_for_name('[J')
        self._L_java_lang_float                     = self.class_for_name('[F')
        self._L_java_lang_double                    = self.class_for_name('[D')
        self._L_java_lang_Object                    = self.class_for_name('[Ljava.lang.Object;')
        self._L_java_lang_String                    = self.class_for_name('[Ljava.lang.String;')

        # Spawn a receiver thread, if any
        if (self._flags & self._FLAG_USE_WORKERS != 0):
            # Create a simple thread to handle pulling data in off the wire
            class Receiver(Thread):
                def __init__(self_):
                    super().__init__()
                    self_.daemon = True

                def run(self_):
                    while self._connected and not self._eof:
                        try:
                            # Read any message and then put it into the message-
                            # passing hash so that threads which want it can get
                            # it from there.
                            (msg_type, request_id, payload) = self._recv()
                            with self._recv_lock:
                                self._recvd[request_id] = (msg_type, request_id, payload)
                        except EOFError as e:
                            self._eof = True

                            # Only let the user know if we still think that we
                            # should be connected. If we're disconnected then
                            # this is just noise.
                            if self._connected:
                                LOG.info('Receiver thread %s exits with EOF: %s',
                                         self, e)
                        except Exception:
                            # We never want this thread to die owing to an
                            # exception; it should only ever die if the
                            # application exits As such we catch _all_
                            # exceptions here.
                            pass

            # Start it rolling
            self._receiver = Receiver()
            self._receiver.start()
            LOG.debug("Started a receiver thread")

        # Okay, we're now ready to handle any stuff we need to do
        self._ready = True

        # Spawn a thread to punt reference drops to the Java side
        class ReferenceDropper(Thread):
            def __init__(self_):
                super().__init__()
                self_.daemon = True

            def run(self_):
                # Do this periodically, as long as we are connected
                while self._connected and not self._eof:
                    last = 0
                    try:
                        # Handle this every second, but only sleep for 100ms so
                        # that we don't hold up the shutdown operations upon
                        # disconnect.
                        time.sleep(0.1)
                        now = time.time()
                        if now - last > 1.0:
                            self._handle_pending_drops()
                            last = now
                    except Exception as e:
                        # We never want this thread to die owing to an
                        # exception; it should only ever die if the application
                        # exits. As such we catch _all_ exceptions here. This
                        # can be noisy when the server is disconnecting so we
                        # only make it a debugging message.
                        LOG.debug("Failed to handle pending drops: %s", e)

        # Start it rolling
        dropper = ReferenceDropper()
        dropper.start()
        LOG.debug("Started a reference dropper thread")

        # If we happen to be on the same host, we need to start another thread
        # to keep track of files we are creating when trying to natively copy
        # files into /dev/shm. This is so that we do not leave any files we
        # created on disk, should the program exit in a not-so-nice manner.
        #
        # Here, we discard files that have been around for more than 5s, since
        # they are most likely no longer needed.
        if self._transport.is_localhost():
            class ShmdataCleaner(Thread):
                def __init__(self_):
                    super().__init__()
                    self_.daemon = True

                    # When we previously checked the files
                    self_._last_time = time.time()

                def run(self_):
                    while self._connected and not self._eof:
                        # Wait for 100us before we check. This also allows
                        # potential race conditions with _shmdata_files to
                        # rattle out (hopefully!). This also lessens the
                        # likelihood that our main thread dies while we're
                        # sleeping.
                        time.sleep(1e-4)

                        # We'll determine if we need to check our files based
                        # on this. We also use it as a baseline for removing
                        # old files.
                        curr_time = time.time()

                        # We're only removing files older than 5s,
                        # so don't busy-spin the thread.
                        if curr_time - self_._last_time < 1:
                            continue

                        # We'll replace self._shmdata_tidylists with this when
                        # we're done checking.
                        new_tidylists = list()

                        # Check the files in each list in _shmdata_tidylists
                        for tidylist in self._shmdata_tidylists:
                            # If we don't find any files in this list to exist
                            # then this will be false and we can throw the list
                            # away.
                            has_any = False
                            for filename in tidylist:
                                try:
                                    file_time = os.path.getmtime(filename)
                                except OSError:
                                    # If the file doesn't exist, we're okay--
                                    # we just need to remove it from our list
                                    # and we want to avoid throwing exceptions
                                    # at all costs.
                                    # If this thread throws an exception, the
                                    # main thread will die, building up all of
                                    # its cruft.
                                    continue

                                # Unlink all files that are > 5s old. These
                                # files are most likely no longer needed, as we
                                # are creating them on the fly per array call.
                                # Previously, we found 60s to be too long (the
                                # files persisted and may have filled the
                                # available space in /dev/shm/). This is more of
                                # an art than a science...
                                if curr_time - file_time > 5:
                                    try:
                                        os.unlink(filename)
                                        LOG.debug("Unlinking file %s manually", filename)
                                    except OSError:
                                        pass

                            # If any file existed then save this list to check again
                            if has_any:
                                new_tidylists.append(tidylist)

                        # Only keep non-empty lists
                        self._shmdata_tidylists = new_tidylists

                        # Are there any files in the main list to check?
                        if len(self._shmdata_files) > 0:
                            # Take ownership of the list of files and put a new
                            # list into place for next time.
                            self._shmdata_tidylists.append(self._shmdata_files)
                            self._shmdata_files = list()

                    # We're probably disconnected, delete everything
                    self._shmdata_tidylists.append(self._shmdata_files)
                    while len(self._shmdata_tidylists) > 0:
                        tidylist = self._shmdata_tidylists.pop()
                        for filename in tidylist:
                            try:
                                os.unlink(filename)
                                LOG.debug("Unlinking file %s manually", filename)
                            except OSError:
                                pass

            # Spawn the thread
            ShmdataCleaner().start()
            LOG.debug("Started a shmdata cleaner thread")


    def disconnect(self):
        """
        Disconnect from the server. Once this is called then the instance is no
        longer usable.
        """

        if self._connected:
            # Remove any files that may have been written in the process of
            # handling arrays natively, which can only occur if we're talking to
            # a localhost-based peer.
            if self._transport.is_localhost():
                # Append any straggling files:
                self._shmdata_tidylists.append(self._shmdata_files)
                while self._shmdata_tidylists:
                    tidylist = self._shmdata_tidylists.pop()
                    for filename in tidylist:
                        try:
                            os.unlink(filename)
                            LOG.debug("Unlinking file %s manually", filename)
                        except OSError:
                            pass

            self._connected = False
            self._transport.disconnect()


    def close(self):
        """
        Synonym for disconnect().
        """
        self.disconnect()


    def object_for_name(self, object_name):
        """
        Get a reference to an object in the server, exposed under a given name.

        :param object_name: The name of the object. This is defined by the
                            subclass of the PJRmi instance on the Java side.
        """

        # Sanity
        if object_name is None:
            return None

        # Send the request
        payload = self._format_utf16(str(object_name))
        req_id = self._send(self._INSTANCE_REQUEST, payload)

        # Read the result and give it back
        return self._read_result(req_id)


    def class_for_name(self, name):
        """
        Get the Java Class instance for a given name.

        :param name: The fully-qualified name of the Java class,
                     e.g. ``java.lang.String`` or ``java.util.Map$Entry``.
        """

        if not isinstance(name, str):
            raise TypeError("Given value was not a string: %s" + str(name))

        # Check if we have a cached class
        try:
            return self._classes_by_name[name]
        except KeyError:
            pass

        # Create the class
        klass = self._request_class(name)
        self._classes_by_id  [klass._type_id]   = klass
        self._classes_by_name[klass._classname] = klass
        return klass


    def lock_for_name(self, lock_name):
        """
        Get the exclusive Java lock associated with a given string, creating it if
        need be. This allows Python clients to avoid tripping over one-another.

        :param lock_name: The ASCII name of the lock to find.
        """

        if lock_name is None:
            raise ValueError("Given a null lock_name")

        # We only allow strings
        if not isinstance(lock_name, str):
            raise ValueError("Given lock_name was a <str> instance: %s <%s>" %
                             (lock_name, type(lock_name)))

        # Now we know it's safe to create
        return _JavaLock(self, lock_name)


    @property
    def javaclass(self):
        if self._class_getter is None:
            # It's okay for this not to be thread-safe. We don't mind multiple
            # instances being created.
            self._class_getter = _ClassGetter(self, ())
        return self._class_getter


    def is_instance_of(self, subclass_or_object, superclass):
        """
        Whether the given Java subclass or object is an instance of the given Java
        superclass.
        """

        # If the argument was a JavaObject then unbox it and recurse
        if isinstance(subclass_or_object, _JavaBox):
            return self.is_instance_of(subclass_or_object.java_object, superclass)
        if isinstance(subclass_or_object, _JavaObject):
            return self.is_instance_of(type(subclass_or_object), superclass)

        # Non-JavaObjects can never be subclasses of one-another
        try:
            if (not issubclass(subclass_or_object, _JavaObject) or
                not issubclass(superclass,         _JavaObject)):
                return False
        except TypeError:
            return False

        # Hand off
        return subclass_or_object._instance_of(superclass)


    def cast_to(self, obj, klass):
        """
        Cast the given Java object to the given Java type.

        :param obj:   The object to cast.
        :param klass: The class to cast to; a subclass of JavaObject.
        """

        # Null breeds null
        if obj is None:
            return None

        # We can only cast Java objects
        if not issubclass(obj.__class__, _JavaObject):
            raise TypeError("Can't cast non-JavaObject %s (%s)" %
                            (str(obj), str(obj.__class__)))

        # See what we're casting to. Note that a secured PJRmi instance might
        # not allow access to java.lang.Class so we have to do a bit of a
        # roundabout test for that.
        if klass is None:
            raise TypeError("Can't cast to class None")
        elif (getattr(klass, '_classname', None) == 'java.lang.Class' and
              hasattr(klass, 'getName')):
            klass = self.class_for_name(str(klass.getName()))
        if not isinstance(klass, self._JavaClass):
            raise TypeError("Can't cast to non Java class, had %s" % type(klass))

        # Send the request
        payload = (self._format_int32(klass._type_id) +
                   self._format_int64(obj._pjrmi_handle))
        req_id = self._send(self._OBJECT_CAST, payload)

        # Read the result and give it back
        return self._read_result(req_id)


    def value_of(self,
                 obj,
                 compress   =True,
                 best_effort=False):
        """
        Get a Python copy of the given Java object of the given object, if
        supported.

        This method is a mechanism to do a bulk data transfer from the Java side
        to the Python side, thus making purely pythonic operations to be done
        efficiently. For example, copying a Java ``double[]`` into a Numpy
        ``ndarray`` makes for vastly more efficient array access patterns on the
        Python side.

        By default the Java object must be wholly converted to its Python
        equivalent, otherwise an ``UnsupportedOperationException`` will be
        thrown. However, setting the ``best_effort`` kwarg to ``True`` will mean
        that unconvertable Java objects will remain untouched. This can be
        useful if, say, one has a ``List<Object>`` on the Java side with
        thousands of elements; iterating over this on the Python side will
        require a callback for each iteration step. Turning it into a Python
        ``list`` of ``Object``s will mean that the iteration will be purely
        local on the Python side.

        Health Warning: When using the ``best_effort`` option, what defines a
        "convertable" Java object is loosely defined as being anything which is
        supported by the Java ``PythonPickle`` code and is therefore subject to
        change over time.

        :param obj:         The Java object to render to Python
        :param compress:    Boolean to indicate whether to use compression when
                            transmitting back the value from the Java side, via
                            pickling.
        :param best_effort: Boolean to indicate whether to do a "best-effort"
                            attempt, meaning some Java objects might not be
                            fully converted.
        """

        # Null breeds null
        if obj is None:
            return None

        # Sanity
        if not issubclass(obj.__class__, _JavaObject):
            raise TypeError("Given a non-JavaObject %s (%s)" %
                            (str(obj), str(obj.__class__)))

        if compress:
            if best_effort:
                value_format = self._VALUE_FORMAT_BESTEFFORT_SNAPPY_PICKLE
            else:
                value_format = self._VALUE_FORMAT_SNAPPY_PICKLE
        else:
            if best_effort:
                value_format = self._VALUE_FORMAT_BESTEFFORT_PICKLE
            else:
                value_format = self._VALUE_FORMAT_RAW_PICKLE

        # For receiving arrays on the same host
        if (self._transport.is_localhost() and
            self._use_shmdata              and
            (obj.__class__ == self._L_java_lang_boolean or
             obj.__class__ == self._L_java_lang_byte    or
             obj.__class__ == self._L_java_lang_short   or
             obj.__class__ == self._L_java_lang_int     or
             obj.__class__ == self._L_java_lang_long    or
             obj.__class__ == self._L_java_lang_float   or
             obj.__class__ == self._L_java_lang_double)):
            value_format = self._VALUE_FORMAT_SHMDATA

        # Send the request
        payload = self._format_int64(obj._pjrmi_handle) + value_format

        req_id = self._send(self._GET_VALUE_OF, payload)

        # Read the result and give it back
        return self._read_result(req_id)


    def collect(self, value, timeout_secs=2**31, as_value=False):
        """
        Collect the results of all the Java ``Future``s found in the given value.

        Any Python object may be passed in for ``value``. If it is an understood
        Python container object, like a tuple or dict, then a new instance of
        that container will be created with its contents replaced by any
        collected values.

        The value resulting from an asynchronous method call may only be
        collected once; this is so that it may be garbage collected on the Java
        side. Passing in ``True`` for ``as_value`` will allow this collection to
        happen since you'll get back a Python object and the reference to the
        Java one will no longer be held by the PJRmi framework.

        :param value:        The Python object to collect the ``Future``s from.
        :param timeout_secs: How long to wait before giving up.
        :param as_value:     Whether to collect the results from the
                             ``Future``s as Python values.

        :return: A deep copy of any containers given, with the collected values,
                 or a collected value.
        """
        TimeUnit = self.class_for_name('java.util.concurrent.TimeUnit')

        now = time.time()
        if self.is_instance_of(value, self._java_util_concurrent_Future):
            return_format = \
                self._VALUE_FORMAT_SNAPPY_PICKLE if as_value else \
                self._VALUE_FORMAT_REFERENCE
            return value.get(int(timeout_secs),
                             TimeUnit.SECONDS,
                             __pjrmi_return_format__=return_format)

        elif isinstance(value, tuple):
            return tuple(self.collect(v,
                                      timeout_secs - (time.time() - now),
                                      as_value=as_value)
                         for v in value)

        elif isinstance(value, list):
            return [self.collect(v,
                                 timeout_secs - (time.time() - now),
                                 as_value=as_value)
                    for v in value]

        elif isinstance(value, dict):
            return {k : self.collect(value[k],
                                     timeout_secs - (time.time() - now),
                                     as_value=as_value)
                    for k in value}

        elif isinstance(value, set):
            return {self.collect(v,
                                 timeout_secs - (time.time() - now),
                                 as_value=as_value)
                    for v in value}

        else:
            return value


    def inject_class(self, filename):
        """
        Read some Java bytecode from a file and inject it into the running Java
        instance as a ``Class``.

        :param filename: The path of the Java class file containing the class
                         bytecode.

        :return: The Python class shim of the injected Java class.
        """

        if filename is None:
            return ValueError('Given a null filename')

        # Read in and send over the bytecode from the file
        with open(filename, 'rb') as fh:
            req_id = self._send(self._INJECT_CLASS, fh.read())

        # Read the result
        type_dict = self._read_result(req_id)
        if type_dict is None:
            raise TypeError("Could not read type information back from server")
        else:
            klass = self._create_class(type_dict)
            self._classes_by_id  [klass._type_id]   = klass
            self._classes_by_name[klass._classname] = klass

            return klass


    def inject_source(self, class_name, source):
        """
        Compile the Java class source-code with the given class name.

        :param class_name: The name of the class in the source given.

        :param source: The source code for the class to compile and inject,
                           e.g.:
public class TestInjectSource {
    public static int foo(int i) {
        return i+1;
    }
}

        :raises: ValueError if ``class_name`` or ``source`` is ``None``.

        :return: The Python class shim of the injected Java class.
        """
        if class_name is None:
            return ValueError('class_name was None')
        if source     is None:
            return ValueError('source was None')

        # Send the request by concatenating the class_name and source
        payload  = self._format_string(class_name)
        payload += self._format_string(source)
        req_id   = self._send(self._INJECT_SOURCE, payload)

        # Read the result
        type_dict = self._read_result(req_id)
        if type_dict is None:
            raise TypeError("Could not read type information back from server")
        else:
            klass = self._create_class(type_dict)
            self._classes_by_id  [klass._type_id]   = klass
            self._classes_by_name[klass._classname] = klass

            return klass


    def replace_class(self, klass, filename):
        """
        Read some Java bytecode from a file and use it to replace the implementation
        of an existing class in the JVM.

        For this to function the ``PJRmiAgent`` must be loaded into the JVM.

        :param klass:    The Python shim class of the Java class which we are
                         replacing.
        :param filename: The path of the Java class file containing the new
                         bytecode.

        :return: The Python class shim of the replaced Java class.
        """

        if filename is None:
            return ValueError('Given a null filename')

        # Read in and send over the bytecode from the file
        with open(filename, 'rb') as fh:
            bytecode = fh.read()

        # And send it off
        payload  = self._format_int32(klass._type_id)
        payload += self._format_int32(len(bytecode))
        payload += bytecode
        req_id   = self._send(self._REPLACE_CLASS, payload)

        # Read the result
        type_dict = self._read_result(req_id)
        if type_dict is None:
            raise TypeError("Could not read type information back from server")
        else:
            new_klass = self._create_class(type_dict)
            self._classes_by_id  [new_klass._type_id]   = new_klass
            self._classes_by_name[new_klass._classname] = new_klass

            return new_klass


    def get_constructor(self, klass, arg_types=None):
        """
        Get a handle on the Java version of the constructor for the given class,
        potentially using any given argument types to disambiguate overloaded
        instances.

        Once obtained this can be used as an argument to Java methods which use
        functional interfaces.

        :param klass:     The class to get the constructor for.
        :param arg_types: The Java classes of the argument types to be used for
                          disambiguation between different overloaded versions
                          of the constructor.
        """
        try:
            # Match the method in the class
            match = self._match_method("Constructor",
                                       klass,
                                       klass._constructors,
                                       arg_types)
            if match is None:
                if arg_types:
                    arg_str = " with argument types [%s]" % (
                                  ', '.join(getattr(a, '__name__', str(a))
                                            for a in arg_types)
                              )
                else:
                    arg_str = ""
                raise ValueError("Could not find match for constructor in %s%s" %
                                 (klass, arg_str))
            else:
                # We matched, so we can give back the representation
                return JavaMethod(self, True, match, klass, None)

        except (AttributeError,KeyError) as e:
            raise ValueError(
                "%s does not appear to be a Java class: %s" %
                (klass, e)
            )


    def get_bound_method(self, method, arg_types=None):
        """
        Get a handle on the Java version of the given method, potentially using any
        given argument types to disambiguate overloaded instances.

        Once obtained this can be used as an argument to Java methods which use
        functional interfaces.

        :param method:    The method we are unbinding, either as a attribute of
                          a Java class or of an instance of a Java class.
        :param arg_types: The Java classes of the argument types to be used for
                          disambiguation between different overloaded versions
                          of the method.
        """
        # Handle people passing in the special 'new' method, which is really the
        # constructor
        if method.__name__ == 'new':
            return self.get_constructor(method.__klass__, arg_types=arg_types)

        # Otherwise it's a regular method
        try:
            # Match the method in the class
            klass = method.__klass__
            match = self._match_method("Method '%s'" % (method.__name__,),
                                       klass,
                                       klass._methods.get(method.__name__),
                                       arg_types)
            if match is None:
                if arg_types:
                    arg_str = " with argument types [%s]" % (
                                  ', '.join(getattr(a, '__name__', str(a))
                                            for a in arg_types)
                              )
                else:
                    arg_str = ""
                raise ValueError("Could not find match for '%s' in %s%s" %
                                 (method, klass, arg_str))
            else:
                # We matched, so we can give back the representation
                return JavaMethod(self, False, match, klass, method.__this__)

        except (AttributeError,KeyError) as e:
            raise ValueError(
                "%s does not appear to be a method of a Java class: %s" %
                (method, e)
            )


    def get_java_logger(self, logger_name_or_class):
        """
        Helper method which looks up the Java Logger instance for a given name
        or class.

        :param logger_name_or_class: Either the Java class instance (obtained by
                                     calling class_for_name() or similar) or the
                                     Java "binary string" representing the class
                                     name (e.g. 'foo.bar.baz.Tick$Tock').
        """

        Logger = self.class_for_name('java.util.logging.Logger')

        # Figure out how we're looking it up
        if logger_name_or_class is None:
            raise ValueError("Given a null logger_name_or_class value")
        elif isinstance(logger_name_or_class, str):
            handle = str(logger_name_or_class)
        elif issubclass(logger_name_or_class, _JavaObject):
            handle = logger_name_or_class.getClass()
        else:
            raise TypeError("Don't know how to look up a logger using a %s" %
                            logger_name_or_class.__class__)

        # Now simply call the method
        return Logger.getLogger(handle)


    def set_java_log_level(self, logger_name_or_class, level_name_or_value, recursive=False):
        """
        Helper method which looks up the Java Logger instance for a given name
        or class and sets its logging level to the given level.

        :param logger_name_or_class: Either the Java class instance (obtained by
                                     calling class_for_name() or similar) or the
                                     Java "binary string" representing the class
                                     name (e.g. 'foo.bar.baz.Tick$Tock').
        :param level_name_or_value:  Either an instance of java.util.logging.Level
                                     or a string representing a static member of
                                     that class (e.g. 'INFO' or 'FINEST').
        :param recursive:            Whether to set all loggers found in the
                                     logger_name_or_class namespace to the given
                                     level. Here, a "namespace" what the logger's
                                     name starts with (e.g. "com.foo.bar" is in
                                     the "com" and "com.foo" namespaces).
        """

        Level      = self.class_for_name('java.util.logging.Level')
        LogManager = self.class_for_name('java.util.logging.LogManager')

        if level_name_or_value is None:
            raise ValueError("Given a null level_name_or_value value")
        elif isinstance(level_name_or_value, str):
            level = getattr(Level, level_name_or_value)
        elif self.is_instance_of(level_name_or_value, Level):
            level = level_name_or_value
        else:
            raise TypeError("Don't know how to set a log level using a %s" %
                            level_name_or_value.__class__)

        # Explicitly get the logger and set with the given level and set it to
        # the desired value before we look to see if any other loggers have
        # names matching it. (It's possible that the Java side hasn't
        # instantiated it yet and so it won't be in the list we get back below.)
        logger = self.get_java_logger(logger_name_or_class)
        logger.setLevel(level)

        # Now see if we're doing this recursively
        if recursive:
            # Look for all the loggers which start with the given name-or-
            # classname. We'll grab the logger of this name and figure out what
            # it's called to do this.
            basename = logger.getName()

            # Look at all the logger names
            names = LogManager.getLogManager().getLoggerNames()
            while names.hasMoreElements():
                # See if this name is a super-string of basename
                name = names.nextElement()
                if name.startswith(basename):
                    # Yes, so get and set
                    logger = self.get_java_logger(name)
                    logger.setLevel(level)


    def _match_method(self, description, klass, candidates, arg_types):
        """
        Attempt to match a method/constructor from a list of candidates, using any
        given argument types as criteria.
        """

        # Check for the degenerate case and early-out
        if candidates is None or len(candidates) == 0:
            return

        # If we have multiple candidates but no hinting then we can't make a match
        if arg_types is None:
            if len(candidates) == 1:
                # No hinting but only one choice, match on it
                return candidates[0]
            else:
                # No disambiguation can be made, so we can't match
                raise ValueError(
                    "%s has multiple candidates in %s "
                    "but no disambiguating argument types given" %
                    (description, klass)
                )

        # Pull out the type IDs from the given argument types
        type_ids = []
        for arg_type in arg_types:
            if isinstance(arg_type, str):
                arg_type = self.class_for_name(arg_type)
            if issubclass(arg_type, _JavaObject):
                type_ids.append(arg_type._type_id)
            else:
                raise ValueError(
                    "Given argument type %s "
                    "does not appear to be Java class" %
                    (arg_type,)
                )

        # Now attempt match the the arguments in out list of candidates
        for c in candidates:
            if c['argument_type_ids'] == type_ids:
                return c

        # If we got to here then we failed to match
        return None


    def _send(self, msg_type, payload):
        """
        Send a message to the other side.

        You can send from any thread but, if you are planning to use threads
        really heavily then you are likely better off enabling callbacks in the
        server and, thius, using a slightly different threading model
        internally.
        """

        assert(payload is not None)

        # We can't currently send messages larger than 2GB in size. This will
        # require a number of changes on both sides, partly since Java arrays
        # are limited by this value and we use byte[]s on the other side to
        # receive the message.
        payload_size = len(payload)
        if payload_size > self._MAX_JAVA_ARRAY_SIZE:
            # Note that this kinda sucks if any exception printing code attempts
            # to capture the local variables up the stack via a __repr__; it
            # will get unhappy since we have a bunch of multi-gigabyte arrays.
            raise IOError(
                "Can't send a message of size %d bytes" % payload_size
            )

        # Determine the thread ID. This is mildly expensive so we only do it if
        # we are re-entrant.
        if self._has_receiver:
            # Get the ID of the current thread. If the thread has an attribute
            # called 'thread_id' then we use that, else we use the native ident
            # value. We XOR in a constant random value to make the ID unique
            # across multiple processes.
            thread    = current_thread()
            tid       = getattr(thread, 'thread_id', thread.ident)
            thread_id = tid ^ self._thread_id_xor
        else:
            # Don't need this if we're not reentrant
            thread_id = -1

        # We always do this under the send lock so that any single message is
        # sent atomically. We don't want the contents of two messges to be
        # comingled on the stream if they happen to be sent from different
        # threads.
        with self._send_lock:
            # Pack everything by hand to avoid the overhead of multiple function
            # calls
            request_id = self._send_request_id()
            self._transport.send(b"%c%s%s" % (msg_type,
                                              struct.pack('!qii',
                                                          thread_id,
                                                          request_id,
                                                          payload_size),
                                              payload))

        return request_id


    def _recv(self):
        """
        Pull data in off the wire.

        You can receive from any thread but, if you are planning to use threads
        really heavily then you are likely better off enabling callbacks in the
        server and, thius, using a slightly different threading model internally.
        """

        # This either happens locally, if we don't have a Receiver thread, or in
        # the Receiver thread if we do.

        # Keep trying until we get something to give back
        while True:
            # Need to read something off the wire. We want 17 bytes in the
            # header, which we'll unpack below.
            result = b''
            while len(result) < 17:
                # Read in the data on the connection; this will block
                # until it's read something
                chunk = self._transport.recv(17 - len(result))

                # If the result is empty that's Python telling us the
                # we've hit the EOF and the connection is dead
                if len(chunk) == 0:
                    self._eof = True
                    raise EOFError("Connection to Java is closed")

                # Add on the bit we read
                result += chunk

            # See what we got back. Unpack this all in one go so as to avoid the
            # overhead of calling _read_foo() multiple times.
            (msg_type, thread_id, request_id, payload_size) = struct.unpack('!cqii', result)

            # Read the payload
            payload = b''
            while len(payload) < payload_size:
                payload += self._transport.recv(payload_size - len(payload))
            assert(len(payload) == payload_size)

            # See if it happened to be a callback
            if request_id == self._CALLBACK_REQUEST_ID:
                # This was an unsolicited message coming from Java which we just
                # happened to catch; hand it off and go around again for another try
                self._handle_incoming_request(msg_type, thread_id, payload)
            else:
                # This was a message which we were expecting, give it back to
                # the caller
                return (msg_type, request_id, payload)


    def _read_result(self, want_request_id):
        """
        Read the result of a send() call.
        """

        # Get back the result associated with the given request ID. We keep
        # trying until we get something.
        log_debug = LOG.isEnabledFor(logging.DEBUG)
        if log_debug:
            LOG.debug("Waiting for response to request %d", want_request_id)
        incoming = None
        while incoming is None:
            # If we stop being connected while this is happening then we bail
            if not self._connected or self._eof:
                raise EOFError("Connection to Java has closed")

            # This has to happen under the recv_lock so that we can either read
            # or pop messages correctly. This prevents multiple Python threads
            # from garbling the input stream or tripping over one-another.
            with self._recv_lock:
                # These initial two parts of the if/else bodies could be folded
                # out but we keep them separate because the semantics are quite
                # different.
                if self._has_receiver:
                    # Look in our dict of responses since that's where the
                    # receiver thread will put it
                    incoming = self._recvd.pop(want_request_id, None)
                else:
                    # The first thing we do here is to check to see if we have
                    # the result in the received map. This can happen if there
                    # is another thread somehow working behind our back and has
                    # put it there (see below). While there might not be
                    # reentrant working threads in play here we could have other
                    # threads doing things in Python. Examples of these are the
                    # ReferenceDropper and the ipython helper threads.
                    #
                    # The invariants here are guaranteed by the lock's
                    # protection. Provided that everyone is correctly pairing up
                    # requests and replies this should be fine.
                    incoming = self._recvd.pop(want_request_id, None)

                    # If it wasn't in there (likely) then just wait for it. This
                    # is a blocking call so, since we hold the _recv_lock, it
                    # will prevent any other thread from checking the _recvd map
                    # for the duration. That should be fine since we should
                    # never block forever; either the message is still in flight
                    # or it was picked up by another thread and put in the
                    # _recvd map already. As such, we should only ever hold up
                    # another thread by the time it takes our response to come
                    # back. If users really want to do heavyweight multi-
                    # threading then then should enable callbacks in the server.
                    # (We don't enable them by default since it requires a
                    # thread hand-off which slows down the non-callback use-
                    # case.)
                    if incoming is None:
                        incoming = self._recv()

                    # Ensure that it's the message which we expected. As noted
                    # above, some other threads might be at work without us
                    # realising it.
                    (msg_type, request_id, payload) = incoming
                    if request_id != want_request_id:
                        # We didn't expect this message so save it for another
                        # thread to pick up from the map
                        if log_debug:
                            LOG.debug(
                                "Got an unexpected response of %d of type '%s'; "
                                "deferring it",
                                request_id, msg_type
                            )
                        self._recvd[request_id] = incoming

                        # That wasn't for us so we null out what we got and go
                        # around again for another try
                        incoming = None

            # If we got nothing back then sleep for a nanosecond. This
            # effectively forces the thread to yield and allows the lock to
            # taken by the thread which is actually receiving the result. This
            # is needed because Python3 does mumble mumble mumble...
            if incoming is None:
                time.sleep(1e-9)

        # We had the result read by another thread, use that
        (msg_type, request_id, payload) = incoming
        if log_debug:
            LOG.debug("Got response of type <%s> to request %d", msg_type, request_id)

        # Handle the message
        handler = self._handlers.get(msg_type, self._unhandled_message_type)
        return handler(msg_type, payload)


    # --------------------------------------------------------------------------
    #
    # MESSAGE HANDLERS START
    #
    # These all have the prototype of func(self, msg_type, payload)


    def _handle_object_reference(self, msg_type, payload):
        # This is a reference to an object
        #  int32   : Type ID
        #  int64   : Handle
        #  int32   : Raw data len
        #  byte[]  : Any raw data
        (type_id, idx) = self._read_int32(payload, 0)
        (handle,  idx) = self._read_int64(payload, idx)
        (raw_len, idx) = self._read_int32(payload, idx)
        if raw_len >= 0:
            raw  = payload[idx : idx + raw_len]
            idx += raw_len
        else:
            raw = None
        return self._create_object(type_id, handle, raw)


    def _handle_type_description(self, msg_type, payload):
        # This is a type description:
        #  int32    : Type ID, or -1 if the type was not found
        #  int16    : Name length
        #  byte[]   : Name
        #  int32    : TypeFlags as int
        #  int32    : The type ID of the array element type, or -1
        #  int32    : Number of super-type IDs
        #  types[]  :
        #    int32    : Type ID
        #  int32    : Number of fields
        #  Field[]   :
        #    int16    : Field name length
        #    byte[]   : Field name
        #    int32    : Type ID
        #    byte[]   : isStatic flag
        #  int32    : Number of constructors
        #  CTOR[]   :
        #    int16    : Flags
        #    int16    : Number of arguments
        #    Param[]  :
        #      int32    : Argument type ID
        #      int32    : Parameter name length
        #      byte[]   : Parameter name
        #    int16    : Number of accepted kwargs
        #    Kwarg[]  :
        #      int32    : Kwarg name length
        #      byte[]   : Kwarg name
        #    byte[]   : Relative specificities
        #  int32    : Number of methods
        #  Method[] :
        #    int16    : Method name length
        #    byte[]   : Method name
        #    int16    : Flags
        #    int32    : Return type ID
        #    int16    : Number of arguments
        #    Param[]  :
        #      int32    : Argument type ID
        #      int32    : Parameter name length
        #      byte[]   : Parameter name
        #    int16    : Number of accepted kwargs
        #    Kwarg[]  :
        #      int32    : Kwarg name length
        #      byte[]   : Kwarg name
        #    byte[]   : Relative specificities (len is num-methods)
        (type_id, idx) = self._read_int32(payload, 0)
        if type_id >= 0:
            (name,                  idx) = self._read_ascii(payload, idx)
            (type_flags,            idx) = self._read_int32(payload, idx)
            (array_element_type_id, idx) = self._read_int32(payload, idx)

            is_primitive  = ((type_flags & self._TYPE_FLAGS_IS_PRIMITIVE           ) != 0)
            is_throwable  = ((type_flags & self._TYPE_FLAGS_IS_THROWABLE           ) != 0)
            is_interface  = ((type_flags & self._TYPE_FLAGS_IS_INTERFACE           ) != 0)
            is_functional = ((type_flags & self._TYPE_FLAGS_IS_FUNCTIONAL_INTERFACE) != 0)

            # Read all the super-type IDs
            (num_supertypes, idx) = self._read_int32(payload, idx)
            supertype_ids = list()
            for supertype_idx in range(num_supertypes):
                (supertype_id, idx) = self._read_int32(payload, idx)
                supertype_ids.append(supertype_id)

            # Read all the fields
            (num_fields, idx) = self._read_int32(payload, idx)
            fields = list()
            for field_idx in range(num_fields):
                (field_name, idx) = self._read_ascii  (payload, idx)
                (field_type, idx) = self._read_int32  (payload, idx)
                (is_static,  idx) = self._read_boolean(payload, idx)

                # Handle field names which happen to be reserved words in
                # Python by appending a "_". Hopefully that won't cause a
                # clash with a real Java field.
                if keyword.iskeyword(field_name):
                    field_name += '_'

                # Now add the argument info
                fields.append({'index'     : field_idx,
                               'name'      : field_name,
                               'type_id'   : field_type,
                               'is_static' : is_static})

            # Read all the constructors
            (num_ctors, idx) = self._read_int32(payload, idx)
            ctors = list()
            for ctor_idx in range(num_ctors):
                (ctor_flags,    idx) = self._read_int16(payload, idx)
                (num_arguments, idx) = self._read_int16(payload, idx)
                argument_types       = list()
                parameter_names      = list()
                for _ in range(num_arguments):
                    (argument_type,  idx) = self._read_int32(payload, idx)
                    (parameter_name, idx) = self._read_ascii(payload, idx)
                    argument_types .append(argument_type)
                    parameter_names.append(parameter_name)
                (num_kwargs, idx) = self._read_int16(payload, idx)
                kwarg_names       = set()
                for _ in range(num_kwargs):
                    (kwarg_name, idx) = self._read_ascii(payload, idx)
                    kwarg_names.add(kwarg_name)
                (specificities, idx) = self._read_int8_array(payload, idx)

                # Don't need to check static or default flags, CTORs can be
                # neither of these
                is_deprecated = ((ctor_flags & self._METHOD_FLAGS_IS_DEPRECATED) != 0)
                is_explicit   = ((ctor_flags & self._METHOD_FLAGS_IS_EXPLICIT  ) != 0)
                has_kwargs    = ((ctor_flags & self._METHOD_FLAGS_HAS_KWARGS   ) != 0)

                # Now add the info
                ctors.append({'name'                   : 'new',
                              'is_default'             : False,
                              'is_deprecated'          : is_deprecated,
                              'is_explicit'            : is_explicit,
                              'is_static'              : False,
                              'has_kwargs'             : has_kwargs,
                              'index'                  : ctor_idx,
                              'argument_type_ids'      : argument_types,
                              'parameter_names'        : parameter_names,
                              'kwarg_names'            : kwarg_names,
                              'relative_specificities' : specificities})

            # Now read all the methods, we store this in a by-name dict.
            # Since methods may be overloaded in Java the values in this
            # dict are lists of method details.
            (num_methods, idx) = self._read_int32(payload, idx)
            methods_by_name = dict()
            for method_idx in range(num_methods):
                (method_name,        idx) = self._read_ascii(payload, idx)
                (method_flags,       idx) = self._read_int16(payload, idx)
                (method_return_type, idx) = self._read_int32(payload, idx)
                (num_arguments,      idx) = self._read_int16(payload, idx)
                argument_types            = list()
                parameter_names           = list()
                for _ in range(num_arguments):
                    (argument_type,  idx) = self._read_int32(payload, idx)
                    (parameter_name, idx) = self._read_ascii(payload, idx)
                    argument_types .append(argument_type)
                    parameter_names.append(parameter_name)
                (num_kwargs, idx) = self._read_int16(payload, idx)
                kwarg_names       = set()
                for _ in range(num_kwargs):
                    (kwarg_name, idx) = self._read_ascii(payload, idx)
                    kwarg_names.add(kwarg_name)
                (specificities, idx) = self._read_int8_array(payload, idx)

                is_static     = ((method_flags & self._METHOD_FLAGS_IS_STATIC    ) != 0)
                is_deprecated = ((method_flags & self._METHOD_FLAGS_IS_DEPRECATED) != 0)
                is_default    = ((method_flags & self._METHOD_FLAGS_IS_DEFAULT   ) != 0)
                is_explicit   = ((method_flags & self._METHOD_FLAGS_IS_EXPLICIT  ) != 0)
                has_kwargs    = ((method_flags & self._METHOD_FLAGS_HAS_KWARGS   ) != 0)

                # Similar to fields, handle method names which happen to be
                # reserved words in Python by appending a "_". Hopefully that
                # won't cause a clash with a real Java method (it would have to
                # match both name and signature so it's "unlikely").
                if keyword.iskeyword(method_name):
                    method_name += '_'

                if method_name in methods_by_name:
                    methods = methods_by_name[method_name]
                else:
                    methods = list()
                    methods_by_name[method_name] = methods

                # Now add the method info
                methods.append({'name'                   : method_name,
                                'is_default'             : is_default,
                                'is_deprecated'          : is_deprecated,
                                'is_explicit'            : is_explicit,
                                'is_static'              : is_static,
                                'has_kwargs'             : has_kwargs,
                                'index'                  : method_idx,
                                'return_type_id'         : method_return_type,
                                'argument_type_ids'      : argument_types,
                                'parameter_names'        : parameter_names,
                                'kwarg_names'            : kwarg_names,
                                'relative_specificities' : specificities})

            # Finally we can create the object details
            return { 'name'                  : name,
                     'type_id'               : type_id,
                     'is_primitive'          : is_primitive,
                     'is_throwable'          : is_throwable,
                     'is_interface'          : is_interface,
                     'is_functional'         : is_functional,
                     'array_element_type_id' : array_element_type_id,
                     'supertype_ids'         : supertype_ids,
                     'fields'                : fields,
                     'constructors'          : ctors,
                     'methods'               : methods_by_name }


    def _handle_arbitrary_item(self, msg_type, payload):
        # This is an object of an unknown type being sent over the wire
        #  int32   : Type ID
        #  bytes[] : Result (if any)
        (type_id, idx) = self._read_int32(payload, 0)

        # Handle all the types we know about, primitives are sent back
        # as raw bits which we can convert
        handler = self._handle_arbitrary_item_handlers.get(type_id)
        if handler is not None:
            value = handler(payload, idx)[0]
        else:
            # For Objects we simply read the handle (and any raw value) and
            # create them
            (handle, raw_len) = struct.unpack('!qi', payload[idx:idx+12])
            idx += 12
            if raw_len >= 0:
                raw  = payload[idx : idx + raw_len]
                idx += raw_len
            else:
                raw = None
            value = self._create_object(type_id, handle, raw)

        return value


    def _handle_exception(self, msg_type, payload):
        # This is the result of calling a method on an object
        #  int32  : Type ID
        #  int32  : Handle
        #  int32  : Raw data len
        #  byte[] : Any raw data
        (type_id, idx) = self._read_int32(payload, 0)
        (handle,  idx) = self._read_int64(payload, idx)
        (raw_len, idx) = self._read_int32(payload, idx)
        if raw_len >= 0:
            raw  = payload[idx : idx + raw_len]
            idx += raw_len
        else:
            raw = None

        # Create the object (having this handle makes debugging easier)
        ex = self._create_object(type_id, handle, raw)

        # And raise the exception with this object
        raise ex


    def _handle_ascii_value(self, msg_type, payload):
        # This is a string
        (value, idx) = self._read_ascii(payload, 0)
        return value


    def _handle_utf16_value(self, msg_type, payload):
        # This is a unicode string
        (value, idx) = self._read_utf16(payload, 0)
        return value


    def _handle_pickle_bytes(self, msg_type, payload):
        # Read in the array of bytes (as a string)
        (value, idx) = self._read_byte_array(payload, 0)

        # Get the value encding format, and the data. For the format we use a
        # slice of length 1, instead of indexing with [0], because direct
        # indexing of bytes returns an int not a bytes object.
        value_format = value[0:1]
        data         = value[1: ]

        # See if we need to decompress it
        if value_format in (self._VALUE_FORMAT_SNAPPY_PICKLE,
                            self._VALUE_FORMAT_BESTEFFORT_SNAPPY_PICKLE):
            data = snappy.decompress(data)

        # And actually do the unpickle
        return pickle.loads(data, encoding='bytes')


    def _handle_shmdata_bytes(self, msg_type, payload):
        # We are receiving the components of a JniPJRmi$ArrayHandle
        # which we will use to read an array, in the following format:
        #  byte    : Return format
        #  int32   : Length of filename
        #  byte[]  : Filename
        #  int32   : Number of elements in the array
        #  int16   : Type of the array

        # Read in the return format
        (value_format, idx) = self._read_byte(payload, 0)
        assert (value_format == self._VALUE_FORMAT_SHMDATA), \
               ('unrecognized value format: %s' % value_format)

        # Read each component of the JniPJRmi$ArrayHandle
        (filename,   idx) = self._read_utf16(payload, idx)
        (num_elems,  idx) = self._read_int32(payload, idx)
        (arr_type,   idx) = self._read_char( payload, idx)

        # Read the array
        return pjrmi.extension.read_array(filename,
                                          num_elems,
                                          arr_type.encode('utf-8'))


    def _handle_empty_ack(self, msg_type, payload):
        # Nothing in this
        return None


    def _handle_array_length(self, msg_type, payload):
        # This is an int
        (value, idx) = self._read_int32(payload, 0)
        return value


    def _handle_python_reference(self, msg_type, payload):
        (object_id, idx) = self._read_int32(payload, 0)
        return self._get_callback_object(object_id)


    def _unhandled_message_type(self, msg_type, payload):
        raise ValueError("Received unhandled message type: %s" % msg_type)

    #
    # MESSAGE HANDLERS END
    #
    # --------------------------------------------------------------------------

    def _handle_incoming_request(self, msg_type, thread_id, payload):
        """
        Handle an unsolicited message coming from Java to us.
        """

        # We need to do this in another thread to prevent deadlocks if we
        # become reentrant
        class Worker(Thread):
            def __init__(self_):
                super().__init__()
                self_.daemon = True
                self_._condition = Condition()
                self_._task = None

            def run(self_):
                """
                Entry point.
                """

                # We wait until the PJRmi instance denotes that it is ready to
                # handle requests. This is to avoid a race-condition where we
                # are started as a minion by Java and it starts sending us stuff
                # to do right away.
                while not self._ready:
                    time.sleep(0.001)

                # Work forever...
                while True:
                    # Wait for the task to appear
                    self_._condition.acquire()
                    while self_._task is None:
                        self_._condition.wait()
                    self_._condition.release()

                    # And do it
                    self_._work(*self_._task)

                    # Reset and put ourselves back on the worker queue
                    self_._task = None
                    self._workers.append(self_)


            def work(self_, msg_type, thread_id, payload):
                """
                Get told to do the work by another thread.
                """

                # Set the task, the worker thread will pick this up
                self_._condition.acquire()
                self_._task = (msg_type, thread_id, payload)
                self_._condition.notify()
                self_._condition.release()


            def _work(self_, msg_type, thread_id, payload):
                """
                Actually do the work.
                """

                # Associate the thread
                self_.thread_id = thread_id

                # Any request ID we pick up along the way
                req_id = None

                # All done inside a big try block so that we catch exceptions
                try:
                    # See what we have to handle. No response/ACK is expected to
                    # these calls going to Java (since we are Java's response).
                    if msg_type == self._CALLBACK:
                        # This is a callback request
                        #  int32   : Java request ID
                        #  int32   : Python function ID
                        #  int32   : Num args
                        #  ...     : args
                        #  int32   : Num kwargs
                        #  ...     : kwargs as (str,value) pairs
                        (req_id,      idx) = self._read_int32(payload, 0)
                        (function_id, idx) = self._read_int32(payload, idx)
                        (num_args,    idx) = self._read_int32(payload, idx)

                        # And figure out its arguments
                        args = list()
                        for i in range(num_args):
                            (arg, idx) = self._read_argument(payload, idx)
                            args.append(arg)

                        # And the kwargs
                        (num_kwargs, idx) = self._read_int32(payload, idx)
                        kwargs = dict()
                        for i in range(num_kwargs):
                            (name, idx) = self._read_utf16   (payload, idx)
                            (arg,  idx) = self._read_argument(payload, idx)
                            kwargs[name] = arg

                        # Grab the function
                        function = self._get_callback_function(function_id)

                        # What we'll give back to Java. If this raises an exception
                        # then we throw that back to the Java side and let it deal
                        # with it.
                        if function is None:
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception("No function for ID %d" %
                                                          function_id)
                            )
                            is_exception = True

                        else:
                            try:
                                result = self._format_by_class(self._java_lang_Object,
                                                               function(*args))
                                is_exception = False

                            except JavaException as e:
                                result = self._format_by_class(
                                    self._get_class(e._type_id),
                                    e
                                )
                                is_exception = True

                            except Exception as e:
                                (_, _, tb) = sys.exc_info()
                                result = self._format_by_class(
                                    self._java_lang_Exception,
                                    self._java_lang_Exception(
                                        "Calling Python function %s raised exception: %s\n%s" %
                                        (function, e, _tb2jexstr(tb))
                                    )
                                )
                                is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._OBJECT_CALLBACK:
                        # This is a proxy callback request
                        #  int32   : Java request ID
                        #  int32   : Python object ID
                        #  int32   : Return type ID
                        #  string  : Method name
                        #  int32   : Num args
                        #  ...     : Args
                        (req_id,      idx) = self._read_int32(payload, 0)
                        (object_id,   idx) = self._read_int32(payload, idx)
                        (type_id,     idx) = self._read_int32(payload, idx)
                        (method_name, idx) = self._read_utf16(payload, idx)

                        # Grab the return type and the object
                        klass = self._get_class(type_id)
                        obj   = self._get_callback_object(object_id)

                        # And figure out its arguments
                        (num_args, idx) = self._read_int32(payload, idx)
                        args = list()
                        for i in range(num_args):
                            (arg, idx) = self._read_argument(payload, idx)
                            args.append(arg)

                        # And the kwargs
                        (num_kwargs, idx) = self._read_int32(payload, idx)
                        kwargs = dict()
                        for i in range(num_kwargs):
                            (name, idx) = self._read_utf16   (payload, idx)
                            (arg,  idx) = self._read_argument(payload, idx)
                            kwargs[name] = arg

                        # Handle name mangling for keywords again
                        if keyword.iskeyword(method_name):
                            method_name += '_'

                        # What we'll give back to Java. If this raises an exception
                        # then we throw that back to the Java side and let it deal
                        # with it. All the formatting of the result is done under
                        # the try so that we also see exceptions for that on the
                        # Java side.
                        try:
                            # Handle the case of not finding the method with the
                            # right exception. The Java side relies on this to
                            # determine what happened (and call any default method).
                            function = getattr(obj, method_name, None)
                            if function is None:
                                raise self._java_lang_NoSuchMethodException(
                                    "Could not find method %s in object %s" %
                                    (method_name, obj)
                                )

                            # Call the function, and render the result into the
                            # appropriate payload
                            result = self._format_by_class(klass, function(*args, **kwargs))
                            is_exception = False

                        except JavaException as e:
                            result = self._format_by_class(self._get_class(e._type_id), e)
                            is_exception = True

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "Calling Python method %s on object %s raised exception: %s\n%s" %
                                    (method_name, obj, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._PYTHON_EVAL_OR_EXEC:
                        # This is a callback request
                        #  int32   : Java request ID
                        #  bool    : Whether it's an eval (or, else, exec)
                        #  int32   : Return type ID
                        #  string  : What to eval...
                        (req_id,  idx) = self._read_int32  (payload, 0)
                        (is_eval, idx) = self._read_boolean(payload, idx)
                        (type_id, idx) = self._read_int32  (payload, idx)
                        (string,  idx) = self._read_utf16  (payload, idx)

                        # The expected return type
                        klass = self._get_class(type_id)

                        # What we'll give back to Java. If this raises an exception
                        # then we throw that back to the Java side and let it deal
                        # with it. We format the result in this call too so that the
                        # Java side will hear about any type-casting problems.
                        result = None
                        is_exception = False
                        try:
                            if is_eval:
                                # Do the eval
                                result = eval(string)

                                # If the other side expects a string back then we
                                # can stringify here
                                if (type_id == self._java_lang_String._type_id and
                                    result is not None):
                                    result = str(result)
                            else:
                                # Do the exec in the global context, we will return
                                # nothing
                                exec((string), globals())
                                result = None

                            # Render it for transmission back down the wire
                            result = self._format_by_class(klass, result)

                        except JavaException as e:
                            result       = self._format_by_class(self._get_class(e._type_id), e)
                            is_exception = True

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "Eval on '%s' raised exception: %s\n%s" %
                                    (string, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._PYTHON_INVOKE:
                        # This is Java attempting to call a function inside Python
                        #  int32   : Java request ID
                        #  int32   : Return type ID
                        #  string  : Python function name
                        #  int32   : Num args
                        #  ...     : Args
                        (req_id,    idx) = self._read_int32(payload, 0)
                        (type_id,   idx) = self._read_int32(payload, idx)
                        (func_name, idx) = self._read_utf16(payload, idx)
                        (num_args,  idx) = self._read_int32(payload, idx)

                        # The expected return type
                        klass = self._get_class(type_id)

                        # And figure out its arguments
                        args = list()
                        for i in range(num_args):
                            (arg, idx) = self._read_argument(payload, idx)
                            args.append(arg)

                        # What we'll give back to Java. If this raises an exception
                        # then we throw that back to the Java side and let it deal
                        # with it. All the formatting of the result is done under
                        # the try so that we also see exceptions for that on the
                        # Java side.
                        try:
                            # Pull the function into our local context, call it,
                            # and render the result into the appropriate payload
                            function = str2obj(func_name)
                            result = self._format_by_class(klass, function(*args))
                            is_exception = False

                        except JavaException as e:
                            result = self._format_by_class(self._get_class(e._type_id), e)
                            is_exception = True

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "Calling Python function %s raised exception: %s\n%s" %
                                    (func_name, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._GET_OBJECT:
                        # This is a request for a python object
                        #  int32   : Java request ID
                        #  string  : What to eval to get the object
                        (req_id, idx) = self._read_int32(payload, 0)
                        (string, idx) = self._read_utf16(payload, idx)

                        # What we'll give back to Java. If this raises an exception
                        # then we throw that back to the Java side and let it deal
                        # with it. We format the result in this call too so that the
                        # Java side will hear about any type-casting problems.
                        try:
                            # Do the eval to get the object
                            obj = str2obj(string)

                            # And get the ID associated with it
                            object_id = self._get_object_id(obj)

                            # Render it for transmission back down the wire
                            result = self._format_by_class(self._java_lang_Integer,
                                                           object_id)
                            is_exception = False

                        except JavaException as e:
                            result = self._format_by_class(self._get_class(e._type_id), e)
                            is_exception = True

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "Eval on '%s' raised exception: %s\n%s" %
                                    (string, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._INVOKE_AND_GET_OBJECT:
                        # This is a request to call a function and return a
                        # reference to the resulting object.
                        #  int32   : Java request ID
                        #  string  : Function to call
                        #  int32   : Number of args
                        #  ...     : Args
                        (req_id, idx)        = self._read_int32(payload, 0)
                        (function_name, idx) = self._read_utf16(payload, idx)
                        (num_args,  idx)     = self._read_int32(payload, idx)

                        # And figure out its arguments
                        args = list()
                        for i in range(num_args):
                            (arg, idx) = self._read_argument(payload, idx)
                            args.append(arg)

                        # What we'll give back to Java. If this raises an
                        # exception then we throw that back to the Java side and
                        # let it deal with it. We format the result in this call
                        # too so that the Java side will hear about any
                        # type-casting problems.
                        try:
                            # Pull the function into our local context and call
                            # it
                            function = str2obj(function_name)
                            obj = function(*args)

                            # And get the ID associated with it
                            object_id = self._get_object_id(obj)

                            # Render it for transmission back down the wire
                            result = self._format_by_class(self._java_lang_Integer,
                                                           object_id)
                            is_exception = False

                        except JavaException as e:
                            result = self._format_by_class(self._get_class(e._type_id), e)
                            is_exception = True

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "Eval on '%s' raised exception: %s\n%s" %
                                    (string, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._GETATTR:
                        # This is a proxy callback request
                        #  int32   : Java request ID
                        #  int32   : Python object ID
                        #  int32   : Field type ID
                        #  string  : Field name
                        (req_id,     idx) = self._read_int32(payload, 0)
                        (object_id,  idx) = self._read_int32(payload, idx)
                        (type_id,    idx) = self._read_int32(payload, idx)
                        (field_name, idx) = self._read_utf16(payload, idx)

                        # Grab the return type and the object
                        klass = self._get_class(type_id)
                        obj   = self._get_callback_object(object_id)

                        # What we'll give back to Java. If this raises an
                        # exception then we throw that back to the Java side and
                        # let it deal with it. All the formatting of the result
                        # is done under the try so that we also see exceptions
                        # for that on the Java side.
                        try:
                            result = self._format_by_class(klass, getattr(obj, field_name))
                            is_exception = False

                        except AttributeError as e:
                            result = self._format_by_class(
                                self._java_lang_NoSuchFieldException,
                                self._java_lang_NoSuchFieldException(str(e))
                            )
                            is_exception = True

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "getattr(%s, '%s') raised exception: %s\n%s" %
                                    (obj, field_name, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # And call back with whatever we got
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._SET_GLOBAL_VARIABLE:
                        # This is a callback request
                        #  int32   : Java request ID
                        #  string  : Variable name
                        #  arg     : Value (as an "argument")
                        (req_id, idx) = self._read_int32   (payload, 0)
                        (name,   idx) = self._read_utf16   (payload, idx)
                        (value,  idx) = self._read_argument(payload, idx)

                        # We'll give back nothing, unless we raise an exception
                        try:
                            # Just set it
                            globals()[name] = value
                            result = self._format_by_class(self._java_lang_Object, None)
                            is_exception = False

                        except Exception as e:
                            (_, _, tb) = sys.exc_info()
                            result = self._format_by_class(
                                self._java_lang_Exception,
                                self._java_lang_Exception(
                                    "'%s = %s' in globals raised exception: %s\n%s" %
                                    (name, value, e, _tb2jexstr(tb))
                                )
                            )
                            is_exception = True

                        # Return with any result, possibly an exception
                        payload_ = (self._format_int32(req_id) +
                                    self._format_boolean(is_exception) +
                                    result)
                        self._send(self._CALLBACK_RESPONSE, payload_)

                    elif msg_type == self._ADD_REFERENCE:
                        # Java telling us to add a reference to a Python object
                        #  int64 : Object ID (as a long)
                        (object_id, idx) = self._read_int64(payload, 0)
                        try:
                            self._add_object_reference(object_id)
                            # No response
                        except Exception:
                            pass # best effort

                    elif msg_type == self._DROP_REFERENCES:
                        # Java telling us to drop references to Python objects
                        #  int64 : Object ID (as a long)
                        (count, idx) = self._read_int32(payload, 0)
                        for i in range(count):
                            (object_id, idx) = self._read_int64(payload, 0)
                            try:
                                self._drop_object_reference(object_id)
                                # No response
                            except Exception:
                                pass # best effort

                    elif msg_type == self._EXCEPTION:
                        # We don't expect these to come in an unsolicted fashion
                        # so just log it. There's nothing else we can do here.
                        # Typically we might see these coming in when the Java
                        # side is shutting down for some (unexpected) reason and
                        # emits an exception as part of that.
                        (type_id, idx) = self._read_int32(payload, 0)
                        (handle,  idx) = self._read_int64(payload, idx)
                        (raw_len, idx) = self._read_int32(payload, idx)
                        if raw_len >= 0:
                            raw  = payload[idx : idx + raw_len]
                            idx += raw_len
                        else:
                            raw = None

                        # Create the object and just log it. This could fail if
                        # the Java side is shutting down, so more quietly log a
                        # raw version of things if that's the case. (Being quiet
                        # avoids breaking unit tests which don't expect output
                        # on shutdown.)
                        try:
                            LOG.error(
                                "Unable to handle unsolicited exception "
                                "from Java: %s",
                                self._create_object(type_id, handle, raw)
                            )
                        except Exception as e:
                            message = (
                                "Unable to handle unsolicited exception "
                                "from Java: type_id=%d, handle=%d, raw=%r, "
                                "create_object() error='%s')",
                                type_id, handle, raw, e
                            )
                            if (not self._connected or
                                self._eof           or
                                str(e) == "I/O operation on closed file"):
                                LOG.debug(*message)
                            else:
                                LOG.error(*message)

                    else:
                        # This isn't something which we can handle in an unsolicited
                        # fashion. Rather raising an exception and freaking out the
                        # current thread we'll just log an error. There's not a lot
                        # else we can do at this point unfortunately. (Since we
                        # don't know what Java was trying to do we can't send back
                        # any useful response.)
                        LOG.error(
                            "Unable to handle unsolicited request from Java of type %r",
                            msg_type
                        )

                except Exception as e:
                    # For the traceback
                    (_, _, tb) = sys.exc_info()

                    # Make sure we send back any exceptions if we can
                    if req_id is None:
                        raise e
                    else:
                        # Send back the exception
                        self._send(
                            self._CALLBACK_RESPONSE,
                            (
                                self._format_int32(req_id) +
                                self._format_boolean(True) +
                                self._format_by_class(
                                    self._java_lang_Exception,
                                    self._java_lang_Exception(
                                        "%s\n%s" %
                                        (e, _tb2jexstr(tb))
                                    )
                                )
                            )
                        )

        # Grab a worker for the job, creating if needbe
        try:
            worker = self._workers.pop()
        except IndexError:
            worker = Worker()
            worker.start()

        # And tell it to work on what we give it
        worker.work(msg_type, thread_id, payload)


    def _format_string(self, string):
        """
        Format a string as [int32:size][bytes[]:string], to send down the wire
        """

        if type(string) != bytes:
            string = string.encode('ascii')
        return self._format_int32(len(string)) + string


    def _format_utf16(self, string):
        """
        Format a string as [int32:size][bytes[]:utf16], to send down the wire.

        See http://www.ietf.org/rfc/rfc2781.txt for more information.
        """

        # Encode the string as UTF, this will include the byte-order-marker
        payload = string.encode('utf_16')
        return self._format_int32(len(payload)) + payload


    def _format_float(self, value):
        """
        Format a float as 4 raw bytes.
        """

        return struct.pack('!f', value)


    def _format_double(self, value):
        """
        Format a double as 8 raw bytes.
        """

        return struct.pack('!d', value)


    def _format_int64(self, value):
        """
        Format a 64-bit int as raw bytes.
        """

        return struct.pack('!q', value)


    def _format_int32(self, value):
        """
        Format a 32-bit int as raw bytes.
        """

        return struct.pack('!i', value)


    def _format_int16(self, value):
        """
        Format a 16-bit int as raw bytes.
        """

        return struct.pack('!h', value)


    def _format_int8(self, value):
        """
        Format an 8-bit int as raw bytes.
        """

        return (b"%c" % (value & 0xff))


    def _format_boolean(self, value):
        """
        Format a boolean as a byte.
        """

        return (b"%c" % (1 if value else 0))


    def _format_array(self, value, dtype):
        """
        Serializes the data of a one-dimensional array, or an iterable, according to
        the given dtype.
        """

        dtype = numpy.dtype(dtype)

        if not isinstance(value, (numpy.ndarray, list, tuple)):
            value = list(value)

        arr = numpy.asarray(value)

        if arr.ndim != 1:
            raise TypeError('The provided array must be one dimensional')

        if len(arr) > self._MAX_JAVA_ARRAY_SIZE:
            raise TypeError('The given array is larger than Java can represent')

        return bytes(numpy.array(strict_array(dtype.type, arr),
                                 dtype=dtype,
                                 copy=False,
                                 order='C').data)


    def _format_method_as(self, value, klass):
        """
        Format a Python Java method handle, targeting the given Java class.
        """
        # This needs to be a JavaMethod instance
        if not isinstance(value, JavaMethod):
            raise ValueError("Not a JavaMethod instance: %s" % value)

        # We should be able to actually format it as the given class
        if not value._can_format_as(klass):
            raise ValueError("Can't format '%s' as a %s" % (value, klass))

        # If the method has a handle on an instance then include that, else it's
        # the null reference
        if value._this is None:
            handle = self._NULL_HANDLE
        else:
            handle = value._this._pjrmi_handle

        # And build the result. Here we use a boolean to indicate the is_ctor
        # value. At some point we might want to use flags instead but, since
        # it's a byte with is_ctor in the LSB, we can probably just use an 8bit
        # flagset without changing the wire format.
        return (self._ARGUMENT_METHOD +
                self._format_boolean(value._is_ctor) +
                self._format_int32  (klass._type_id) +
                self._format_int32  (value._klass._type_id) +
                self._format_int32  (value._details['index']) +
                self._format_int64  (handle))


    def _format_as_lambda(self,
                          method, *args,
                          strict_types=True, allow_format_shmdata=True):
        """
        Format a method and its arguments as something which can be invoked on the
        Java side.

        :param method:  The `JavaMethod` instance which we want to invoke.
        :param args:    The arguments to call the Java method with (as Python
                        or Java objects).
        :param kwargs:  See `_format_by_class`.
        """
        # This needs to be a JavaMethod instance
        if not isinstance(method, JavaMethod):
            raise ValueError("Not a JavaMethod instance: %s" % method)

        # See if we should use the captured 'this' pointer or the one from the
        # argument list, or none at all
        if not method._is_static:
            if (len(args) == len(method._argument_type_ids) + 1 and
                type(args[0]) == method._klass):
                this = args[0]
                args = args[1:]
            else:
                this = method._this
        else:
            this = None

        # If we have no "this" pointer then we can only call a static method,
        # unless the caller gives us an instance pointer
        if this is None:
            if not method._is_static:
                raise ValueError(
                    "Attempt to marshall instance method in a static context"
                )
            handle = self._NULL_HANDLE
        else:
            handle = this._pjrmi_handle

        # Check the number of arguments which we were given, this should match
        # the number of type IDs in the argument list of the method
        if len(method._argument_type_ids) != len(args):
            raise ValueError(
                "Wrong number of arguments given, expected %d but had %d" %
                (len(self._argument_type_ids), len(args))
            )

        # And build the result
        result = (self._ARGUMENT_LAMBDA +
                  self._format_boolean(method._is_ctor) +
                  self._format_int32  (method._klass._type_id) +
                  self._format_int32  (method._details['index']) +
                  self._format_int64  (handle))

        # And now all the arguments
        result += self._format_int16(len(method._argument_type_ids))
        for (type_id, arg) in zip(method._argument_type_ids, args):
            result += self._format_by_class(self._get_class(type_id),
                                            arg,
                                            strict_types=strict_types,
                                            allow_format_shmdata=allow_format_shmdata)
        # And give it all back
        return result


    def _can_format_shmdata(self, value, klass):
        """
        Returns whether we can marshall the given value using SHM methods.
        As of now, this means the process is on the same host, the value
        is handled by the SHM marshalling code, and the feature is enabled.
        """
        # Enabled?
        if not (self._use_shmdata and self._transport.is_localhost()):
            return False

        # Has the extension?
        try:
            pjrmi.extension.write_array
        except NameError:
            return False

        # This only works for 1D numpy arrays as of 20220906. More complex
        # datatypes (e.g. multi-dimensional arrays or container objects) will
        # each require bespoke code to be written to handle them. It's not
        # _hard_ but it's not entirely trivial either. We can handle these on an
        # as-needed basis.
        if not isinstance(value, numpy.ndarray) or len(value.shape) != 1:
            return False

        # It must be less than 2^31 in size, the max Java array size.
        if len(value) > self._MAX_JAVA_ARRAY_SIZE:
            return False

        # Check what the Java type is since we must match. For Objects we'll use
        # the Python type to guide us.
        if klass._type_id in (self._L_java_lang_Object._type_id,
                              self._java_lang_Object  ._type_id):
            if value.dtype.name not in ('bool',
                                        'int8', 'int16', 'int32', 'int64',
                                        'float32', 'float64'):
                return False
        elif klass._type_id == self._L_java_lang_boolean._type_id:
            if value.dtype.name != 'bool':
                return False
        elif klass._type_id == self._L_java_lang_byte._type_id:
            if value.dtype.name != 'int8':
                return False
        elif klass._type_id == self._L_java_lang_short._type_id:
            if value.dtype.name != 'int16':
                return False
        elif klass._type_id == self._L_java_lang_int._type_id:
            if value.dtype.name != 'int32':
                return False
        elif klass._type_id == self._L_java_lang_long._type_id:
            if value.dtype.name != 'int64':
                return False
        elif klass._type_id == self._L_java_lang_float._type_id:
            if value.dtype.name != 'float32':
                return False
        elif klass._type_id == self._L_java_lang_double._type_id:
            if value.dtype.name != 'float64':
                return False
        else:
            # Unhandled type
            return False

        # Okay, it can work
        return True


    def _validate_format_array(self, obj):
        """
        Raises a `ValueError` exception if the given object is an array-like but
        can't be formatted as to represent an array on the Java side (e.g.
        because it's too large).

        This method is intended to be called from within ``_format_by_class``
        so, if you call it elsewhere, please update this comment and think about
        the implications of changing any semantics.
        """
        # Check the array length. We throw a ValueError here, not a TypeError,
        # since this is called inside _format_by_class and we want to abort and
        # give a meaningful error to the user. (As opposed to failing to method
        # bind.)
        if hasattr(obj, '__len__') and len(obj) > self._MAX_JAVA_ARRAY_SIZE:
            raise ValueError(
                "Argument length, %d, is greater than Java's maximum array size" %
                len(obj)
            )


    def _format_shmdata(self, klass, value, strict_types):
        """
        Returns the byte string for the result of a SHM write.
        """
        try:
            native_info = pjrmi.extension.write_array(value)
        except Exception as e:
            LOG.debug("Couldn't natively write the array: %s", e)

            # We will fall back on non-native formatting
            return self._format_by_class(klass,
                                         value,
                                         strict_types=strict_types,
                                         allow_format_shmdata=False)

        native_filename = native_info[0]
        native_length   = native_info[1]
        native_type     = native_info[2].decode()

        # Keep track of the files we may need to clean up
        self._shmdata_files.append(native_filename)

        return (self._ARGUMENT_SHMDATA +
                self._format_utf16(native_filename) +
                self._format_int32(native_length) +
                self._format_utf16(str(native_type)))


    def _format_by_class(self, klass, value,
                         strict_types=True, allow_format_shmdata=True):
        """
        Format a value according to the given class.
        """

        # Easy test for the void type. Unlike Java, Python's "void" methods are
        # simply ones which return None. As such we expect 'value' to be None
        # here. This code will still be called if we happen to, for example, be
        # trying to marshall the results of a callback from Java to Python.
        if klass._type_id == self._java_lang_void._type_id:
            # We should not be rendering a non-None value here, if we do then
            # the caller has done something wrong
            if value is not None:
                raise ValueError("Attempt to render non-None value as <void>: %s" % value)

            # We simply return a VALUE here, which happens to have no "value"
            # associated with it
            return (self._ARGUMENT_VALUE +
                    self._format_int32(self._java_lang_void._type_id))

        # Handle specially boxed types. We do hasattr() here since its twice as
        # fast as calling 'isinstance(value, _JavaBox)'. This might be marginal
        # but, since _format_by_class() is so heavily used every little helps.
        if hasattr(value, '_java_object'):
            # Handle regular Java unboxing (Integer to int etc.)
            if (isinstance(value, _JavaByte) and
                klass._type_id == self._java_lang_byte._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_Byte._type_id) +
                        self._format_int8(strict_number(numpy.int8, value.python_object)))

            elif (isinstance(value, _JavaShort) and
                  klass._type_id == self._java_lang_short._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_Short._type_id) +
                        self._format_int16(strict_number(numpy.int16, value.python_object)))

            elif (isinstance(value, _JavaInt) and
                  klass._type_id == self._java_lang_int._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_Integer._type_id) +
                        self._format_int32(strict_number(numpy.int32, value.python_object)))

            elif (isinstance(value, _JavaLong) and
                  klass._type_id == self._java_lang_long._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_Long._type_id) +
                        self._format_int64(strict_number(numpy.int64, value.python_object)))

            elif (isinstance(value, _JavaFloat) and
                  klass._type_id == self._java_lang_float._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_Float._type_id) +
                        self._format_float(strict_number(numpy.float32, value.python_object)))

            elif (isinstance(value, _JavaDouble) and
                  klass._type_id == self._java_lang_double._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_Double._type_id) +
                        self._format_double(strict_number(numpy.float64, value.python_object)))

            else:
                # Just unbox to whatever the actual Java object was and drop
                # into the code block below. This handles things like Strings.
                value = value._java_object

        # Inspect the value we've been given
        if isinstance(value, _JavaObject):
            # We need to check to see if this object is an instance of the given
            # argument class
            if value is not None and not value.__class__._instance_of(klass):
                # Catch the case where we are mixing PJRmi connections
                if value._pjrmi_inst is not self:
                    raise KeyError(
                        "Attempt to use Java object (%s: %s) from connection %s with %s" %
                        (str(value.__class__), str(value),
                         value._pjrmi_inst._transport, self._transport)
                    )
                else:
                    raise _LazyTypeError(klass, value)

            # Objects are represented by their handles.
            return self._ARGUMENT_REFERENCE + self._format_int64(value._pjrmi_handle)

        else:
            # Handle the case where this a proxy, but allow us to fall through
            # to the below should this fail so that we may attempt to bind in
            # other ways
            if isinstance(value, JavaProxyBase):
                # A proxy represents some form of interface. As such we must
                # marshall it as one. If the other side is expecting a specific
                # interface then we will wrap it as that. If the other side
                # wants an Object then we just wrap it as an empty interface.
                # This is needed when Java does something like passing the proxy
                # to an equals() method, which takes an Object.
                if klass._is_interface:
                    wire_type = klass
                elif klass._type_id == self._java_lang_Object._type_id:
                    wire_type = self._com_deshaw_pjrmi_JavaProxyBase
                else:
                    wire_type = None

                if wire_type is not None:
                    try:
                        # This is a proxy subclass so attempt to turn it into
                        # the desired argument
                        return self._format_by_class(
                            klass,
                            self._get_object_wrapper(value, wire_type),
                            strict_types=strict_types
                        )
                    except Exception as e:
                        LOG.debug("Can't format proxy %s as a %s: %s",
                                  value, klass, e)

            # We've been given a native value to marshall. Format it as a raw
            # bag of bytes.
            if klass._type_id == self._java_lang_Object._type_id:
                # This is basically a typeless object. We infer what sort of
                # thing we want to send by looking at the Python type.
                #
                # For numeric values we first try to match on exact numpy types
                # then we whittle it down to the most restrictive type which can
                # represent the value. This is because Java's Method.invoke()
                # will work if you give, say, "foo(int i)" a Short then it works
                # but if you give it a Long then it doesn't. (I.e. it will work
                # if the object can be converted to the required one without
                # loss of precision.) However, this is not true of methods which
                # take objects since these use explicit casting, i.e.
                # "foo(Integer i)" cannot be given a Short.
                if value is None:
                    return (self._ARGUMENT_REFERENCE +
                            self._format_int64(self._NULL_HANDLE))

                elif isinstance(value, _JavaObject):
                    return (self._ARGUMENT_REFERENCE +
                            self._format_int64(value._pjrmi_handle))

                elif isinstance(value, (bool, numpy.bool_)):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Boolean._type_id) +
                            self._format_boolean(True if value else False))

                elif isinstance(value, numpy.int8):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Byte._type_id) +
                            self._format_int8(strict_number(numpy.int8, value)))

                elif isinstance(value, numpy.int16):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Short._type_id) +
                            self._format_int16(strict_number(numpy.int16, value)))

                elif isinstance(value, numpy.int32):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Integer._type_id) +
                            self._format_int32(strict_number(numpy.int32, value)))

                elif isinstance(value, numpy.int64):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Long._type_id) +
                            self._format_int64(strict_number(numpy.int64, value)))

                elif isinstance(value, numpy.float32):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Float._type_id) +
                            self._format_float(strict_number(numpy.float32, value)))

                elif isinstance(value, numpy.float64):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Double._type_id) +
                            self._format_double(strict_number(numpy.float64, value)))

                elif allow_format_shmdata and self._can_format_shmdata(value, klass):
                    # This is an one-dimensional numpy array that we know we can handle natively
                    return self._format_shmdata(klass, value, strict_types)

                elif isinstance(value, bytes):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_byte._type_id) +
                            self._format_int32(len(value)) +
                            value)

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'int8':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_byte._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(strict_array(numpy.int8, numpy.ascontiguousarray(value)).astype(">i1")).tobytes())

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'int16':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_short._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(strict_array(numpy.int16, numpy.ascontiguousarray(value)).astype(">i2")).tobytes())

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'int32':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_int._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(strict_array(numpy.int32, numpy.ascontiguousarray(value)).astype(">i4")).tobytes())

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'int64':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_long._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(strict_array(numpy.int64, numpy.ascontiguousarray(value)).astype(">i8")).tobytes())

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'float32':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_float._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(strict_array(numpy.float32, numpy.ascontiguousarray(value)).astype(">f4")).tobytes())

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'float64':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_double._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(strict_array(numpy.float64, numpy.ascontiguousarray(value)).astype(">f8")).tobytes())

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name == 'bool':
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_boolean._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_boolean(True if el else False)
                                         for el in value))

                elif isinstance(value, numpy.ndarray) and len(value.shape) == 1 and value.dtype.name.startswith('str'):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_String._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_by_class(self._java_lang_String,
                                                           el,
                                                           strict_types=strict_types)
                                          for el in value))

                elif isinstance(value, int):
                    if numpy.can_cast(value, numpy.int8):
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Byte._type_id) +
                                self._format_int8(strict_number(numpy.int8, value)))

                    elif numpy.can_cast(value, numpy.int16):
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Short._type_id) +
                                self._format_int16(strict_number(numpy.int16, value)))

                    elif numpy.can_cast(value, numpy.int32):
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Integer._type_id) +
                                self._format_int32(strict_number(numpy.int32, value)))

                    else:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Long._type_id) +
                                self._format_int64(strict_number(numpy.int64, value)))

                elif isinstance(value, float):
                    if numpy.float32(value) == value:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Float._type_id) +
                                self._format_float(strict_number(numpy.float32, value)))
                    else:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Double._type_id) +
                                self._format_double(strict_number(numpy.float64, value)))

                elif isinstance(value, char):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_lang_Character._type_id) +
                            self._format_utf16(str(value)))

                elif isinstance(value, str):
                    # Catch strings which are really our special _JavaString shim
                    if (isinstance(value, _JavaString) and
                        value._java_string is not None):
                        return (self._ARGUMENT_REFERENCE +
                                self._format_int64(value._java_string._pjrmi_handle))
                    else:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_String._type_id) +
                                self._format_utf16(str(value)))

                elif hasattr(value, 'items'):
                    it = value.iteritems() if hasattr(value, 'iteritems') else value.items()
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_util_Map._type_id) +
                            self._format_int32(len(value)) +
                            b''.join((self._format_by_class(self._java_lang_Object,
                                                            k,
                                                            strict_types=strict_types) +
                                      self._format_by_class(self._java_lang_Object,
                                                            v,
                                                            strict_types=strict_types))
                                           for (k, v) in it))

                elif isinstance(value, collections.abc.Set):
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._java_util_Set._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_by_class(self._java_lang_Object,
                                                           el,
                                                           strict_types=strict_types)
                                          for el in value))

                elif isinstance(value, slice):
                    # This is a slice object which should have integer offsets
                    # in it, though they may be null. We use longs rather than
                    # ints since we might not be slicing arrays; some container
                    # objects might have more than 2^32 elements.
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._com_deshaw_pjrmi_PythonSlice._type_id) +
                            b''.join(self._format_by_class(
                                         self._java_lang_Object,
                                         strict_number(numpy.int32, el) if el is not None else None,
                                         strict_types=strict_types
                                     ) for el in (value.start, value.stop, value.step)))

                elif (hasattr(value, '__iter__') and
                      not isinstance(value, str)):
                    # Iterable, turned into an array of Objects
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(self._L_java_lang_Object._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_by_class(self._java_lang_Object,
                                                           el,
                                                           strict_types=strict_types)
                                          for el in value))

                elif isinstance(value, (FunctionType, MethodType)):
                    return self._format_by_class(klass,
                                                 self._get_callback_wrapper(value),
                                                 strict_types=strict_types)

                elif isinstance(value, JavaMethod) and value._can_format_as(klass):
                    return self._format_method_as(value, klass)

                else:
                    # Don't know how to marshall this
                    raise ValueError(
                        "Don't know how to turn '%s' %s into a <%s>" %
                        (str(value), str(value.__class__), klass._classname)
                    )

            elif value is None and not klass._is_primitive:
                # Allow null to match anything, though this could lead to ambiguity...
                return self._ARGUMENT_REFERENCE + self._format_int64(self._NULL_HANDLE)

            # Marshalling to a Number type?
            elif klass._type_id == self._java_lang_Number._type_id:
                if isinstance(value, int):
                    if numpy.int8(value) == value:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Byte._type_id) +
                                self._format_int8(strict_number(numpy.int8, value)))

                    elif numpy.int16(value) == value:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Short._type_id) +
                                self._format_int16(strict_number(numpy.int16, value)))

                    elif numpy.int32(value) == value:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Integer._type_id) +
                                self._format_int32(strict_number(numpy.int32, value)))

                    else:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Long._type_id) +
                                self._format_int64(strict_number(numpy.int64, value)))

                elif isinstance(value, float):
                    if numpy.float32(value) == value:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Float._type_id) +
                                self._format_float(strict_number(numpy.float32, value)))
                    else:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(self._java_lang_Double._type_id) +
                                self._format_double(strict_number(numpy.float64, value)))

                else:
                    # Don't know how to marshall this
                    raise ValueError(
                        "Don't know how to turn '%s' %s into a <%s>" %
                        (str(value), str(value.__class__), klass._classname)
                    )

            elif klass._type_id in (self._java_lang_boolean._type_id,
                                    self._java_lang_Boolean._type_id):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_boolean(strict_bool(value)))

            elif klass._type_id in (self._java_lang_char.     _type_id,
                                    self._java_lang_Character._type_id):
                if not isinstance(value, str):
                    raise TypeError("Expected a string but had %s: %s" %
                                    (str(value.__class__), ascii(value)))
                elif len(value) != 1:
                    raise TypeError("Expected a single char but had %s: %s" %
                                    (str(value.__class__), ascii(value)))
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_utf16(str(value)))

            elif klass._type_id in (self._java_lang_float._type_id,
                                    self._java_lang_Float._type_id) and \
                not hasattr(value, '__iter__'): # <-- "Not collections.Iterable"
                # If the user has given us a numpy type then we assume that they
                # know what they are doing when it comes to types, and we
                # disallow a lossy conversion.
                if type(value) == numpy.float64:
                    raise ValueError("%s is not assignable to %s" %
                                     (type(value), klass._classname))
                else:
                    # Otherwise we'll use strict against a Python float here to
                    # allow truncation to float32 happen silently. This is
                    # intentional since it will probably always be what the user
                    # wants to happen.
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_float(strict_number(numpy.float64, value)))

            elif klass._type_id in (self._java_lang_double._type_id,
                                    self._java_lang_Double._type_id) and \
                not hasattr(value, '__iter__'):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_double(strict_number(numpy.float64, value)))

            elif klass._type_id in (self._java_lang_byte._type_id,
                                    self._java_lang_Byte._type_id) and \
                not hasattr(value, '__iter__'):
                if strict_types and \
                   type(value) in (numpy.int16, numpy.int32, numpy.int64,
                                   numpy.uint8, numpy.uint16, numpy.uint32, numpy.uint64,
                                   numpy.float32, numpy.float64, float):
                    raise ValueError("%s is not assignable to %s" %
                                     (type(value), klass._classname))
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int8(strict_number(numpy.int8, value)))

            elif klass._type_id in (self._java_lang_short._type_id,
                                    self._java_lang_Short._type_id) and \
                not hasattr(value, '__iter__'):
                if strict_types and \
                   type(value) in (numpy.int32, numpy.int64,
                                   numpy.uint16, numpy.uint32, numpy.uint64,
                                   numpy.float32, numpy.float64, float):
                    raise ValueError("%s is not assignable to %s" %
                                     (type(value), klass._classname))
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int16(strict_number(numpy.int16, value)))

            elif klass._type_id in (self._java_lang_int.    _type_id,
                                    self._java_lang_Integer._type_id) and \
                not hasattr(value, '__iter__'):
                if strict_types and \
                   type(value) in (numpy.int64,
                                   numpy.uint32, numpy.uint64,
                                   numpy.float32, numpy.float64, float):
                    raise ValueError("%s is not assignable to %s" %
                                     (type(value), klass._classname))
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(strict_number(numpy.int32, value)))

            elif klass._type_id in (self._java_lang_long._type_id,
                                    self._java_lang_Long._type_id) and \
                not hasattr(value, '__iter__'):
                if strict_types and \
                   type(value) in (numpy.uint64,
                                   numpy.float32, numpy.float64, float):
                    raise ValueError("%s is not assignable to %s" %
                                     (type(value), klass._classname))
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int64(strict_number(numpy.int64, value)))

            elif klass._type_id == self._L_java_lang_char._type_id:
                self._validate_format_array(value)
                if not isinstance(value, str):
                    raise TypeError("Expected a string but had %s: %s" %
                                    (str(value.__class__), str(value)))
                else:
                    # We render these as UTF-16 and let the Java side deal with
                    # turning the resultant String into its underlying char[]
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_utf16(str(value)))

            elif klass._type_id == self._L_java_lang_boolean._type_id:
                self._validate_format_array(value)
                if allow_format_shmdata and self._can_format_shmdata(value, klass):
                    return self._format_shmdata(klass, value, strict_types)
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_boolean(strict_bool(el))
                                        for el in value))

            elif klass._type_id == self._L_java_lang_float._type_id:
                self._validate_format_array(value)
                if allow_format_shmdata and self._can_format_shmdata(value, klass):
                    return self._format_shmdata(klass, value, strict_types)
                else:
                    # Use strict against a Python float here to allow truncation
                    # to float32 happen silently. This is intentional since it
                    # will probably always be what the user wants to happen.
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            memoryview(
                                strict_array(
                                    numpy.float64,
                                    numpy.ascontiguousarray(value)
                                ).astype(">f4")
                            ).tobytes())

            elif klass._type_id == self._L_java_lang_byte._type_id:
                self._validate_format_array(value)
                if isinstance(value, bytes):
                    # A bytes object we can just send raw
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            value)
                elif isinstance(value, str):
                    # Allow a string to be treated as a list of bytes, if it's
                    # ASCII. If it's not ASCII then we probably don't want to be
                    # doing this and an error will be thrown.
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_int8(el)
                                     for el in value.encode('ASCII')))
                else:
                    # Anything else we attempt to convert into a list of 8bit
                    # integers (bytes)
                    if allow_format_shmdata and self._can_format_shmdata(value, klass):
                        return self._format_shmdata(klass, value, strict_types)
                    else:
                        return (self._ARGUMENT_VALUE +
                                self._format_int32(klass._type_id) +
                                self._format_int32(len(value)) +
                                self._format_array(value, 'int8'))

            elif klass._type_id == self._L_java_lang_short._type_id:
                self._validate_format_array(value)
                if allow_format_shmdata and self._can_format_shmdata(value, klass):
                    return self._format_shmdata(klass, value, strict_types)
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            self._format_array(value, '>i2'))

            elif klass._type_id == self._L_java_lang_int._type_id:
                self._validate_format_array(value)
                if allow_format_shmdata and self._can_format_shmdata(value, klass):
                    return self._format_shmdata(klass, value, strict_types)
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            self._format_array(value, '>i4'))

            elif klass._type_id == self._L_java_lang_long._type_id:
                self._validate_format_array(value)
                if allow_format_shmdata and self._can_format_shmdata(value, klass):
                    return self._format_shmdata(klass, value, strict_types)
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            self._format_array(value, '>i8'))

            elif klass._type_id == self._L_java_lang_double._type_id:
                self._validate_format_array(value)
                if allow_format_shmdata and self._can_format_shmdata(value, klass):
                    return self._format_shmdata(klass, value, strict_types)
                else:
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            self._format_array(value, '>f8'))

            elif klass._classname.startswith("["):
                self._validate_format_array(value)

                # An array of something, figure out what...
                array_klass = self._get_class(klass._array_element_type_id)
                if (hasattr(value, '__iter__') and
                    not isinstance(value, str)):
                    # Iterable, turned into an array
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(len(value)) +
                            b''.join(self._format_by_class(array_klass,
                                                           el,
                                                           strict_types=strict_types)
                                         for el in value))
                else:
                    # Single element, wrapped in an array
                    return (self._ARGUMENT_VALUE +
                            self._format_int32(klass._type_id) +
                            self._format_int32(1) + # length
                            self._format_by_class(array_klass,
                                                  value,
                                                  strict_types=strict_types))

            elif (isinstance(value, _JavaString) and
                  value._java_string is not None):
                return (self._ARGUMENT_REFERENCE +
                        self._format_int64(value._java_string._pjrmi_handle))

            elif (isinstance(value, str) and
                  self._java_lang_String._instance_of(klass)):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._java_lang_String._type_id) +
                        self._format_utf16(str(value)))

            elif (hasattr(value, 'items') and
                  klass._type_id == self._java_util_Map._type_id):
                ok = self._java_lang_Object
                it = value.iteritems() if hasattr(value, 'iteritems') else value.items()
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_int32(len(value)) +
                        b''.join((self._format_by_class(ok,
                                                        k,
                                                        strict_types=strict_types) +
                                  self._format_by_class(ok,
                                                        v,
                                                        strict_types=strict_types))
                                      for (k, v) in it))

            elif (isinstance(value, collections.abc.Set) and
                  klass._type_id == self._java_util_Set._type_id):
                ok = self._java_lang_Object
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_int32(len(value)) +
                        b''.join(self._format_by_class(ok,
                                                       el,
                                                       strict_types=strict_types)
                                     for el in value))

            elif (hasattr(value, '__iter__') and
                  not isinstance(value, str) and
                  klass._type_id in (self._java_lang_Iterable.  _type_id,
                                     self._java_util_Collection._type_id,
                                     self._java_util_List.      _type_id)):
                ok = self._java_lang_Object
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_int32(len(value)) +
                        b''.join(self._format_by_class(ok,
                                                       el,
                                                       strict_types=strict_types)
                                     for el in value))

            elif (klass._type_id == self._com_deshaw_pjrmi_PythonSlice._type_id and
                  (isinstance(value, slice) or
                   hasattr(value, '__len__') and (len(value) == 2 or len(value) == 3))):
                # We want to handle this as a slice; convert it as such
                if isinstance(value, slice):
                    parts = (value.start, value.stop, value.step)
                elif len(value) == 2:
                    parts = (value[0], value[1], None)
                elif len(value) == 3:
                    parts = value
                return (self._ARGUMENT_VALUE +
                        self._format_int32(self._com_deshaw_pjrmi_PythonSlice._type_id) +
                        b''.join(self._format_by_class(
                                     self._java_lang_Object,
                                     strict_number(numpy.int32, el) if el is not None else None,
                                     strict_types=strict_types
                                 ) for el in parts))

            elif (isinstance(value, (FunctionType, MethodType)) and
                  klass._is_functional):
                return self._format_by_class(klass,
                                             self._get_callback_wrapper(value, klass),
                                             strict_types=strict_types)

            elif klass._type_id == self._com_deshaw_pjrmi_PythonObject._type_id:
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_int32(self._get_object_id(value)))

            elif (klass._type_id == self._com_deshaw_hypercube_Hypercube._type_id and
                  type(value) == numpy.ndarray):
                return (self._ARGUMENT_VALUE +
                        self._format_int32(klass._type_id) +
                        self._format_by_class(self._L_java_lang_long, value.shape) +
                        self._format_int32(1) + # num arrays to follow
                        self._format_by_class(self._java_lang_Object, value.flatten()))

            elif isinstance(value, JavaMethod) and value._can_format_as(klass):
                return self._format_method_as(value, klass)

            else:
                if klass._is_interface:
                    # We can attempt to turn this value into a proxy of some
                    # form. We'll try to treat it as a lambda first and then
                    # fall back to trying as a looks-like instance. If all that
                    # fails we just throw the exception at the bottom.
                    try:
                        return self._format_by_class(klass,
                                                     self._get_callback_wrapper(value, klass),
                                                     strict_types=strict_types)
                    except Exception:
                        pass
                    try:
                        return self._format_by_class(klass,
                                                     self._get_object_wrapper(value, klass),
                                                     strict_types=strict_types)
                    except Exception:
                        pass

                # Don't know how to marshall this
                raise ValueError("Don't know how to turn '%s' %s into a <%s>" %
                                 (str(value), str(value.__class__), klass._classname))


    def _read_ascii(self, bytes, index):
        """
        Read a string from some data looking like [int32:size][bytes[]:string]
        from a byte buffer.

        :return: The string, the new offset into the byte buffer.
        """

        if len(bytes) < 4 + index:
            return (None, 0)

        (length, index) = self._read_int32(bytes, index)
        if length < 0:
            return (None, index)
        elif length == 0:
            return ("", index)
        else:
            # We want to return a native string
            val = bytes[index:index+length]
            return (val.decode('ascii'), index+length)


    def _read_utf16(self, bytes, index):
        """
        Read a unicode string from some data looking like [int32:size][bytes[]:utf16]
        from a byte buffer.

        :return: The string, the new offset into the byte buffer.
        """

        if len(bytes) < 4 + index:
            return (None, 0)

        (length, index) = self._read_int32(bytes, index)
        if length < 0:
            return (None, index)
        elif length == 0:
            return ("", index)
        else:
            return (str(bytes[index:index+length], encoding='utf_16'), index+length)


    def _read_float(self, bytes, index):
        """
        Read a float from the wire as 32 raw bits.

        :return: The value, the new offset into the byte buffer.
        """

        return (struct.unpack('!f', bytes[index:index+4])[0], index+4)


    def _read_double(self, bytes, index):
        """
        Read a double from the wire as 64 raw bits.

        :return: The value, the new offset into the byte buffer.
        """

        return (struct.unpack('!d', bytes[index:index+8])[0], index+8)


    def _read_int64(self, bytes, index):
        """
        Read a 64 bit int from raw bytes.

        :return: The value, the new offset into the byte buffer.
        """

        return (struct.unpack('!q', bytes[index:index+8])[0], index+8)


    def _read_int32(self, bytes, index):
        """
        Read a 32 bit int from raw bytes.

        :return: The value, the new offset into the byte buffer
        """

        return (struct.unpack('!i', bytes[index:index+4])[0], index+4)


    def _read_int16(self, bytes, index):
        """
        Read a 16 bit int from raw bytes.

        :return: The value, the new offset into the byte buffer.
        """

        return (struct.unpack('!h', bytes[index:index+2])[0], index+2)


    def _read_int8(self, bytes, index):
        """
        Read an 8 bit int from raw bytes.

        :return: The value, the new offset into the byte buffer.
        """

        return (struct.unpack('!b', bytes[index:index+1])[0], index+1)


    def _read_byte(self, bytes, index):
        """
         Read a byte from raw bytes.

         :return: The value, the new offset into the byte buffer.
         """

        return (bytes[index:index+1], index+1)


    def _read_char(self, bytes, index):
        """
        Read a 16 bit Java char from raw bytes, encoded as UTF-16 (high byte
        first).

        :return: The value, the new offset into the byte buffer.
        """

        # These are sent as specified by DataOutput#writeChar() which dictates
        # that the high-byte is written first (i.e. big endian).
        return (bytes[index:index+2].decode("utf_16_be"), index+2)


    def _read_boolean(self, bytes, index):
        """
        Read a boolean from raw bytes.

        :return: The value, the new offset into the byte buffer.
        """

        return (bytes[index] != 0, index+1)


    def _read_byte_array(self, bytes, index):
        """
        Reads a byte array from some data looking like [int32:size][byte[]:data]
        from a byte buffer.

        :return: A string representing the bytes, the new offset into the byte
                 buffer.
        """

        if len(bytes) < 4 + index:
            return (None, 0)

        (length, index) = self._read_int32(bytes, index)
        if length < 0:
            return (None, index)
        elif length == 0:
            return ("", index)
        else:
            return (bytes[index:index+length], index+length)


    def _read_int8_array(self, bytes, index):
        """
        Reads an array of 8 bit integers array from some data looking like
        [int32:size][byte[]:data] from a byte buffer.

        :return: An array-like of int8s, the new offset into the byte buffer.
        """

        # Hand off to the version which reads the values as a string
        (byte_array, index) = self._read_byte_array(bytes, index)

        # Convert to an array of int8s
        int8_array = numpy.array(bytearray(byte_array)).astype(numpy.int8)

        return (int8_array, index)


    def _read_argument(self, bytes, idx):
        """
        Reads a function call argument from the stream. This might be by reference
        or by (compressed) value.

        :return: The argument, the new offset into the byte buffer.
        """

        (arg_type, idx) = self._read_byte(bytes, idx)
        if arg_type == self._VALUE_FORMAT_REFERENCE:
            (type_id, idx) = self._read_int32(bytes, idx)
            (handle,  idx) = self._read_int64(bytes, idx)
            (raw_len, idx) = self._read_int32(bytes, idx)
            if raw_len >= 0:
                raw  = bytes[idx : idx + raw_len]
                idx += raw_len
            else:
                raw = None
            arg = self._create_object(type_id, handle, raw)

        elif arg_type == self._VALUE_FORMAT_RAW_PICKLE:
            (data, idx) = self._read_byte_array(bytes, idx)
            arg = pickle.loads(data)

        elif arg_type == self._VALUE_FORMAT_SNAPPY_PICKLE:
            (data, idx) = self._read_byte_array(bytes, idx)
            arg = pickle.loads(snappy.decompress(data), encoding="bytes")

        elif arg_type == self._VALUE_FORMAT_PYTHON_REFERENCE:
            (object_id, idx) = self._read_int32(bytes, idx)
            arg = self._get_callback_object(object_id)

        elif arg_type == self._VALUE_FORMAT_SHMDATA:
            (filename,  idx) = self._read_utf16(bytes, idx)
            (num_elems, idx) = self._read_int32(bytes, idx)
            (arr_type,  idx) = self._read_char (bytes, idx)

            # Read the data
            arg = pjrmi.extension.read_array(filename,
                                             num_elems,
                                             arr_type.encode('utf-8'))

        else:
            raise ValueError("Unknown argument marshall type '%s' %d" %
                             (arg_type, ord(arg_type)))

        # Give back what we got and the new index
        return (arg, idx)


    def _get_class(self, type_id):
        """
        Get the Class instance for a given type ID.
        """

        try:
            klass = self._classes_by_id[type_id]
        except KeyError:
            klass = self._request_class(type_id)
            self._classes_by_id  [klass._type_id]   = klass
            self._classes_by_name[klass._classname] = klass

        return klass


    def _request_class(self, type_id_or_name):
        """
        Request and create the Class instance for a given type ID.
        """

        # Send the request, this is by ID
        if type_id_or_name.__class__ == str:
            payload = (b'\x00' + self._format_string(type_id_or_name))
            req_id = self._send(self._TYPE_REQUEST, payload)
        else:
            payload = (b'\x01' + self._format_int32(type_id_or_name))
            req_id = self._send(self._TYPE_REQUEST, payload)

        # Read the result
        type_dict = self._read_result(req_id)
        if type_dict is None:
            raise TypeError(
                "Could not read type information from server for type %s" %
                type_id_or_name
            )
        else:
            return self._create_class(type_dict)


    def _create_class(self, type_dict):
        """
        Create the Python shim of a Java Class instance, from the given
        ``type_dict`` instance.
        """

        # If this is a Java Throwable then it should also be a Python Exception
        if type_dict['is_throwable']:
            bases = (_JavaObject, JavaException)
        else:
            bases = (_JavaObject,)

        # Get different versions of the classname. The classname is what Java
        # understands when you try to look up a class by name. The prettyname is
        # something a little more human-readable. And the simplename is the
        # unqualified prettyname.
        classname  = sys.intern(type_dict['name'])
        prettyname = classname.replace('$', '.')
        if prettyname[0] == '[':
            squares = '[]' * prettyname.count('[')
            if prettyname.startswith('[L') and prettyname.endswith(';'):
                prettyname = '%s%s' % (classname[2:-1], squares)
            elif prettyname.endswith('[Z'):
                prettyname = 'boolean%s' % squares
            elif prettyname.endswith('[C'):
                prettyname = 'char%s' % squares
            elif prettyname.endswith('[B'):
                prettyname = 'byte%s' % squares
            elif prettyname.endswith('[S'):
                prettyname = 'short%s' % squares
            elif prettyname.endswith('[I'):
                prettyname = 'int%s' % squares
            elif prettyname.endswith('[J'):
                prettyname = 'long%s' % squares
            elif prettyname.endswith('[F'):
                prettyname = 'float%s' % squares
            elif prettyname.endswith('[D'):
                prettyname = 'double%s' % squares
        simplename = prettyname.split('.')[-1]

        # Create the class instance using our meta-class type
        klass = self._JavaClass(classname,
                                bases,
                                { '_classname'     : classname,
                                  '_prettyname'    : prettyname,
                                  '_simplename'    : simplename,
                                  '_type_id'       : type_dict['type_id'],
                                  '_is_primitive'  : type_dict['is_primitive'],
                                  '_is_interface'  : type_dict['is_interface'],
                                  '_is_functional' : type_dict['is_functional'],
                                  '_is_immutable'  : False,
                                  '_constructors'  : type_dict['constructors'],
                                  '_methods'       : type_dict['methods'],
                                  '_hash_code'     : None })

        # Different handling for arrays or not
        array_element_type_id = type_dict['array_element_type_id']
        if array_element_type_id < 0:
            setattr(klass, '_is_array', False)
        else:
            setattr(klass, '_is_array', True)
            setattr(klass, '_array_element_type_id', array_element_type_id)
            self._add_array_methods(klass)

        # Add supertypes
        self._add_supertypes(klass, type_dict['supertype_ids'])

        # Add fields as properties
        for field in type_dict['fields']:
            self._add_field(klass, field)

        # Create constructors which we make work in the expected Python way
        if klass._is_array:
            klass.__new__ = self._create_array_constructor(klass)
            get_doc       = lambda: 'Constructor'
        else:
            constructors  = type_dict['constructors']
            klass.__new__ = self._create_constructors(klass, constructors)
            get_doc       = lambda: self._get_doc(klass, True, 'new', constructors)

        # And add methods, all the methods with the same name get the same
        # function handler (which will disambiguate them according to their
        # arguments).
        for (name, methods) in type_dict['methods'].items():
            setattr(klass, name, self._create_method(klass, name, methods))

        # Now a special "new" method, which represents the constructors. This
        # mirrors Java's use of the 'new' keyword for binding constructors. I.e.
        # you can do
        #   Objects.requireNonNullElseGet(blah, HashMap::new)
        # to create a Supplier from the empty HashMap constructor. This is
        # mainly here so that we have the get_bound_method semantics for
        # constructors as well as instance methods.
        setattr(klass, 'new', _JavaMethodAccessor(klass, True, get_doc))

        # Add type-specific methods
        self._add_type_specific_methods(klass)

        # Remember the PJRmi instance from whence we came
        klass._pjrmi_inst = self

        # And give back the klass which we actually created
        return klass


    def _add_supertypes(self, klass, supertype_ids):
        """
        Adds the given super-type information to the given klass.
        """

        # Possibly we should be making classes have a matching inheritance
        # hierachy. That's a wish-list item for now...

        # Figure out the list of types which we inherit from
        supertypes = set()
        for supertype_id in supertype_ids:
            supertypes.add(self._get_class(supertype_id))
        setattr(klass, "_bases", supertypes)

        # Create the instance-of method
        def _instance_of(self_, k):
            """
            Whether this class is an instance of the given class
            """
            # Pointer comparisons for speed
            if k is klass or k is self._java_lang_Object or k in self_._bases:
                return True
            else:
                for base in self_._bases:
                    if base._instance_of(k):
                        return True
                return False

        # And add it
        setattr(klass, "_instance_of", _instance_of.__get__(klass, klass.__class__))


    def _add_field(self, klass, field):
        """
        Adds a field as a property on a the given class.
        """

        # Static members have special handling, instance members are just
        # regular properties
        if field['is_static']:
            class classproperty(property):
                def __init__(self_):
                    pass

                def __get__(self_, obj, objtype):
                    return self._get_field(klass,
                                           self._NULL_HANDLE,
                                           field['index'])

                def __set__(self_, obj, value):
                    return self._set_field(klass,
                                           self._NULL_HANDLE,
                                           field['index'],
                                           self._get_class(field['type_id']),
                                           value)

            # And add it
            setattr(klass, field['name'], classproperty())

        else:
            def get_field(self_):
                return self._get_field(klass,
                                       self_._pjrmi_handle,
                                       field['index'])

            def set_field(self_, value):
                return self._set_field(klass,
                                       self_._pjrmi_handle,
                                       field['index'],
                                       self._get_class(field['type_id']),
                                       value)

            # And add it
            setattr(klass, field['name'], property(get_field, set_field))


    def _get_doc(self, klass, is_ctor, method_name, methods):
        """
        Create a Java method doc string.
        """

        # Preamble, and some things which we might need along the way
        indent        = ' ' * 4
        klass_doc_url = self._get_class_doc_url(klass)
        result        = (("A wrapper for the Java %s:\n" +
                          "%s%s#%s\n" +
                          "taking the following form%s:\n") %
                         ('class constructor' if is_ctor else 'method',
                          indent,
                          str(klass._prettyname),
                          method_name,
                          's' if len(methods) > 1 else ''))

        # All the method signatures in a sensible order.
        signatures = self._get_signatures(is_ctor, method_name, methods)

        # Add each signature to the docstring, allong with anything extra.
        for signature in signatures:
            if klass_doc_url is None:
                doc_link = ''
            else:
                doc_link = "\n%s%s%s#%s(%s)" % (indent,
                                                indent,
                                                klass_doc_url,
                                                method_name,
                                                ','.join(arg_classnames))

            # Add the details
            result += ("%s%s%s\n" % (indent, signature, doc_link))

        return result


    def _get_signatures(self, is_ctor, method_name, methods):
        """
        Create a list of pretty method signatures, in a sensible order.
        """
        # Static methods will have a 'static' prefix and so we want to align the
        # instance methods accordingly, but only if we have any static ones to
        # align against
        static_pfx = 'static '
        if any(m['is_static'] for m in methods):
            instance_indent = ' ' * len(static_pfx)
        else:
            instance_indent = ''

        # Create each method form, ordered by static vs instance, then fewest
        # arguments to the most
        signatures = [] # (method, str)
        for method in sorted(methods,
                             key = lambda m: (not m['is_static'],
                                              len(m['argument_type_ids']))):
            # Determine the class names of the arguments. These are in the
            # name form, as opposed to Java's "binary" form. I.e. '.'s
            # instead of '$'s to separate inner classes. The JavaDoc relies
            # on this and it just looks nicer anyhow.
            arg_classnames = [self._get_class(i)._prettyname
                              for i in method['argument_type_ids']]

            # Parameter Names.
            parameter_names = method['parameter_names']

            # Check if parameter names are default parameter names or not. For
            # the compiled Java classes without parameter name information, the
            # parameter names would be like arg0, arg1, etc., which are the
            # default in Java.
            are_param_names_default = all(
                re.match('^arg[0-9]+', pn) for pn in parameter_names
            )

            # If we have default parameter names, use the concatenated argument
            # class names since parameter names do not provide any extra
            # information in this case. Else, concatenate the argument class
            # names with the corresponding parameter names.
            if are_param_names_default:
                params = ', '.join(arg_classnames)
            else:
                parameter_classes_and_names = [
                    type_ + ' ' + name
                    for type_, name in zip(arg_classnames, parameter_names)
                ]
                params = ', '.join(parameter_classes_and_names)

            # Construct the signature
            return_string = '' if is_ctor else \
                '%s ' % self._get_class(method['return_type_id'])._prettyname
            signatures.append(
                "%s%s%s(%s)" %
                (static_pfx if method['is_static'] else instance_indent,
                 return_string,
                 method_name,
                 params)
            )

        return signatures


    def _create_method(self, klass, method_name, methods):
        """
        Creates a method instance for the given class type and method
        definitions. This is all for methods of a single name (i.e. they are
        overloaded in Java).
        """

        # We need to determine whether we want to use strict_types when doing
        # the argument binding for this method. If we have an overloaded method
        # (i.e. the name and number-of-arguments match) then we need to impose
        # strict typing in order to ensure that we bind to the correct one. If
        # we don't have such overloading then we don't need to do this. Without
        # strict typing we'll allow a Python int64(i) to be turned into a Java
        # byte, short or int; this could lead us to bind to the wrong method
        # (e.g. in the example below) or different ones as the value gets
        # larger. As such, if we have overloading we impose strict type (and,
        # hence, binding) rules but if we don't have overloading then we don't
        # need to do this.
        #
        # This introduces an asymmetry in the method calling since, for the
        # following Java methods:
        #
        #   public void f1(int   i) { ... }
        #   public void f2(short i) { ... }
        #   public void f2(int   i) { ... }
        #
        # we'll allow this from the Python side:
        #
        #   f1(int64(1))
        #
        # but we won't allow this:
        #
        #   f2(int64(1))
        #
        # and this difference might confuse users. However, we hope that these
        # cases will be rare and that someone, with a bit of PJRmi nous, will be
        # around to explain what's happening.
        strict_types_for_num_args = set()
        if len(methods) > 1:
            by_argnum = set()
            for method in methods:
                num_args = len(method['argument_type_ids'])
                if num_args in by_argnum:
                    strict_types_for_num_args.add(num_args)
                else:
                    by_argnum.add(num_args)

        # Define the method. This will handle all calls of a given method name
        # (handling Java overloading).
        def java_method(self_, *args, **kwargs):
            # This is surprisingly expensive to call (4us), so we cache it for
            # the duration of this method, since we call it a lot below
            log_debug = LOG.isEnabledFor(logging.DEBUG)

            # Read the keyword arguments
            return_format = kwargs.pop('__pjrmi_return_format__',
                                       self._VALUE_FORMAT_REFERENCE)
            sync_mode     = kwargs.pop('__pjrmi_sync_mode__',
                                       self.SYNC_MODE_SYNCHRONOUS)

            # Validate args
            if return_format not in self._ACCEPTED_VALUE_FORMATS:
                raise ValueError('Unhandled return format: ' + return_format)
            if sync_mode     not in self._ACCEPTED_SYNC_MODES:
                raise ValueError('Unhandled sync mode: ' + sync_mode)

            if log_debug:
                LOG.debug("Attempting to bind for %s", method_name)

            # Save the original calling arguments since we might mutate them
            # below
            call_args = args

            # Look for the right method to invoke. We'll look for the one with
            # the right number of arguments. If we fail to format the arguments
            # then we assume that we got the wrong version of it. For any
            # methods where we match we append them to the working list; if the
            # newly added method is more tightly-binding than those in the list
            # then we purge the others. If we end up with one method in the list
            # then we're happy, else we have an ambiguous match.
            exceptions = list()
            matches    = list() # list(tuple(<method>, <args>))
            for method in methods:
                # Always ignore methods which require explicit binding
                if method['is_explicit']:
                    if log_debug:
                        LOG.debug("Skipping explicit method")
                    continue

                # Reset 'args' since we might have touched them
                args = call_args

                # See if the method accepts keyword arguments. If so then we
                # need to be a little clever about how we handle the PJRmi
                # kwargs.
                unexpected_kwargs = None
                if not method['has_kwargs']:
                    # Any kwargs are unexpected for this method
                    if kwargs:
                        unexpected_kwargs = set(kwargs)
                else:
                    # First see if we have a list of accepted kwargs
                    unexpected_kwargs = set(kwargs) - method['kwarg_names']

                    # This method is expecting the kwargs so we pass them in as
                    # the last argument
                    args = args + (kwargs,)

                # If we had unexpected keyword arguments then we should fail to
                # bind
                if unexpected_kwargs:
                    msg = "Got%s unexpected keyword argument%s: %s" % (
                        ' an' if len(unexpected_kwargs) == 1 else '',
                        ''    if len(unexpected_kwargs) == 1 else 's',
                        ', '.join(unexpected_kwargs)
                    )
                    exceptions.append(msg)
                    if log_debug:
                        LOG.debug("Failed to bind kwargs for %s method: %s",
                                  method_name, msg)
                    continue

                # Now we know how many arguments we have
                num_args     = len(args)
                strict_types = num_args in strict_types_for_num_args

                # See if we had the right number of arguments
                argument_type_ids = method['argument_type_ids']
                want_args = len(argument_type_ids)
                if num_args == want_args:
                    # Got the right number of arguments. Attempt to construct
                    # the argument list to invoke it. If this fails then it will
                    # be because we were attempting to bind to the wrong
                    # argument types.
                    java_args = b""
                    exception = None
                    try:
                        for idx in range(want_args):
                            # Get the argument, and convert it into the appropriate
                            # value to send to Java
                            try:
                                argument   = args[idx]
                                arg_klass  = self._get_class(argument_type_ids[idx])
                                java_args += self._format_by_class(arg_klass,
                                                                   argument,
                                                                   strict_types=strict_types)
                            except (KeyError, ImpreciseRepresentationError) as e:
                                # Add some more information and remember the
                                # exception to possibly re-raise later on
                                if exception is None:
                                    (_, _, tb) = sys.exc_info()
                                    e = ImpreciseRepresentationError(
                                        "%s when calling %s#%s()" % (e,
                                                                     klass._classname,
                                                                     method_name)
                                    )
                                    exception = (e, tb)

                    except (TypeError, ValueError) as e:
                        # This meant that we failed to bind the arguments so
                        # we can't have an actual match. Try the next one.
                        exceptions.append(e)
                        if log_debug:
                            LOG.debug(
                                "Failed to bind variable for method %s: %s",
                                method_name, e
                            )
                        continue

                    # Did we match anything already?
                    if len(matches) == 0:
                        # No, it's safe to remember this one directly
                        if log_debug:
                            LOG.debug("Appending match %s", method)
                        matches.append((method, java_args, exception))

                    elif exception is None and matches[0][2] is not None:
                        # We already had one or more matches but we would be
                        # truncating a number to call any of them and will just
                        # throw. However, the match we just found has no such
                        # issue and so we should overwrite the current matches
                        # with what we have now.
                        #
                        # This clause, along with the one below, prevents there
                        # being a mixture of throwing and non-throwing matches
                        # in the list.
                        if log_debug:
                            LOG.debug("Replacing exception-throwing match with %s",
                                      method)
                        matches = [(method, java_args, exception)]

                    elif exception is not None and matches[0][2] is None:
                        # We already had matches which will not result in
                        # numeric truncation, but this one will. As such we
                        # don't allow this bad one to be included with those
                        # good ones.
                        #
                        # This clause, along with the one above, prevents there
                        # being a mixture of throwing and non-throwing matches
                        # in the list.
                        if log_debug:
                            LOG.debug("Ignoring match with %s which throws %s",
                                      method, exception)
                        continue

                    else:
                        # Another match
                        if log_debug:
                            LOG.debug("Handling match %s", method)

                        # We have successfully bound to more than one method. We
                        # create a new list of matches based on the relative
                        # binding specificities of the methods. If this current
                        # match is less specific than anything in the list then
                        # we drop it; if it is more specific than another one in
                        # the list, the we drop that one from the new list; if
                        # it is incomparable then we keep both in the list.

                        # This algorithm works because there can never be more
                        # than one method in the list which has a relative
                        # specificity to the one you are inserting. For example,
                        # given any 3 methods, A, B & C -- in that order of
                        # specificity, you can never have A and C in the list
                        # since A will have removed C or C will have not been
                        # inserted in the first place. As such, when you come to
                        # insert B you only have an either-or case to worry
                        # about; you don't have to account for it being both
                        # less specific than some elements and more specific
                        # than others. It's also worth noting that, if any two
                        # methods are uncomparable, then any third method will
                        # be uncomparable to _at least_ one of them. I.e. you
                        # can't have A < X and X < B without A < B, owing to the
                        # way inheritance works for the arguments.

                        # We'll build up a new list of matches with this method
                        # in it. If it turns out to be less specific than what
                        # we have so far then we simply throw away the new list.
                        new_matches = list()
                        new_matches.append((method, java_args, exception))

                        # Look at what we have. We'll check our new match
                        # against the working list. We'll also keep track of
                        # whether we want to assign over that new list to be our
                        # working set of matches.
                        assign = True
                        for match in matches:
                            # Get the method index of the current match
                            cur_index = match[0]['index']

                            # Get the relative specificity of the current match.
                            # This is cmp(method, match) and so a negative value
                            # means that this method is more specific, a
                            # positive one means the match is more specific, and
                            # zero means that they are incomparable.
                            rel_spec = method['relative_specificities'][cur_index]
                            if rel_spec > 0:
                                # A current match is more specific so we can
                                # ignore the new match and keep the current list
                                if log_debug:
                                    LOG.debug("Ignoring match, "
                                              "in favour of more specific %s",
                                              match[0])
                                assign = False
                                break
                            elif rel_spec == 0:
                                # The method is incomparable with the current
                                # match. The means we should keep it in our new
                                # list.
                                if log_debug:
                                    LOG.debug("Accepting old match %s", match[0])
                                new_matches.append(match)
                            else:
                                # Otherwise, rel_spec < 0 which means we want to
                                # drop this method from new_matches. That means
                                # that we simply don't append it.
                                if log_debug:
                                    LOG.debug("Dropping old match %s", match[0])
                                pass

                        # See if we want to use new_matches as our new list
                        if assign:
                            if log_debug:
                                LOG.debug("Replacing existing matches with new set")
                            matches = new_matches

            # If we bound to a method then 'matches' will contain it. If there
            # is only one match then we're golden.
            if len(matches) == 1:
                # Validate the PJRmi kwargs
                if return_format not in self._ACCEPTED_VALUE_FORMATS:
                    raise ValueError('Unhandled return format: ' + return_format)
                if sync_mode     not in self._ACCEPTED_SYNC_MODES:
                    raise ValueError('Unhandled sync mode: ' + sync_mode)

                # Pull out the details so we can call it, or raise any exception
                (method, java_args, exception) = matches[0]

                # Any exception to raise
                if exception is not None:
                    (e, tb) = exception
                    try:
                        raise e.with_traceback(tb)
                    finally:
                        del tb

                # Before we call make sure that we're not trying to call an
                # instance-method statically
                if self_ is None and not method['is_static']:
                    raise TypeError(
                        'Attempt to call instance method %s#%s() in a static context' %
                        (klass._classname, method_name)
                    )

                # If this method is marked as deprecated then we log accordingly
                if method['is_deprecated']:
                    fully_qualified = \
                        '%s.%s' % (klass._classname, method_name)
                    msg = ('%s(%s) is marked as deprecated in Java' %
                           (method_name,
                            ', '.join(str(i.__class__) for i in args)))
                    self._log_deprecated(fully_qualified.replace('$', '.'), msg)

                # Now we know it's safe to call
                return self._call_method(self_,
                                         klass._type_id,
                                         False, # Non-CTOR
                                         return_format,
                                         sync_mode,
                                         method['index'],
                                         java_args)

            elif len(matches) > 1:
                # We have an ambiguous match; the arguments could legitimately
                # match a number of methods. We need to error out at this point.
                def fmt(m):
                    return "%s(%s)" % (
                        m['name'],
                        ', '.join(self._get_class(i)._classname
                                  for i in m['argument_type_ids'])
                    )
                raise TypeError(
                    "Call to %s(%s) is ambiguous; multiple matches: %s" %
                    (method_name,
                     ', '.join(str(type(arg)) for arg in args),
                     ', '.join(fmt(m[0])      for m   in matches))
                )

            else:
                # matches was empty so we found nothing, but report the possible
                # method signatures so the user can adjust their parameters
                signatures = self._get_signatures(False, method_name, methods)

                raise TypeError(
                    "Could not find a method matching %s#%s(%s) in:%s%s" %
                    (klass._classname,
                     method_name,
                     ', '.join(str(i.__class__) for i in args),
                     ''  .join('\n    %s' % (s,) for s in signatures),
                     '' if not exceptions else '\n' + '; '.join(map(str, exceptions)))
                )

        # Give it a better name
        java_method.__name__ = method_name

        # Now create with the appropriate magic around it. The Java method's
        # docstring is deferred via a method since it involves calling
        # _get_class() which may infinitely recurse.
        return _JavaMethodAccessor(
            java_method,
            False,
            lambda: self._get_doc(klass, False, method_name, methods)
        )


    def _create_array_constructor(self, klass):
        """
        Create the array constructor for a given class.
        """

        # Define the method
        def new(*args, **kwargs):
            # Internal call?
            if len(args) == 1 and 'rmi' in kwargs and 'handle' in kwargs:
                result = klass.__real_new__(args[0])
                super(klass, args[0]).__init__(result, kwargs['rmi'], kwargs['handle'])
                return result
            elif len(args) == 2:
                return self._create_array(klass, args[1])
            else:
                raise TypeError(
                    "Array constructor for type '%s' takes a single argument but had: %s" %
                    (klass._classname, (', '.join(str(i.__class__) for i in args[1:])))
                )

        # Give it back
        return staticmethod(new)


    def _create_constructors(self, klass, ctors):
        """
        Create the constructor method for a given class.
        """

        # Whether we want to use strict types when calling this method. This
        # mimics the code in _create_method() and we won't repeat all the
        # comments here. Suffice to say, if we have overloading then we want
        # strict_types.
        strict_types_for_num_args = set()
        if len(ctors) > 1:
            by_argnum = set()
            for ctor in ctors:
                num_args = len(ctor['argument_type_ids'])
                if num_args in by_argnum:
                    strict_types_for_num_args.add(num_args)
                else:
                    by_argnum.add(num_args)

        # Define the method
        def __new__(*args, **kwargs):
            # This is surprisingly expensive to call (4us), so we cache it for
            # the duration of this method, since we call it a lot below
            log_debug = LOG.isEnabledFor(logging.DEBUG)

            if log_debug:
                LOG.debug("Attempting to bind for constructor")

            # Pop off the first argument (klass) and save the calling arguments
            call_args   = args[1:]
            call_kwargs = kwargs

            # Look for the right method to invoke
            exceptions = list()
            matches    = list() # list(tuple(ctor, args))
            for ctor in ctors:
                # Always ignore methods which require explicit binding
                if ctor['is_explicit']:
                    if log_debug:
                        LOG.debug("Skipping explicit constructor")
                    continue

                # First thing to do is to reset back to the call arguments,
                # since we might have touched these in a previous loop. Don't
                # bother to create a new kwargs dict if it was empty anyhow.
                args   = call_args
                kwargs = dict(call_kwargs) if call_kwargs else call_kwargs

                # See if the method accepts keyword arguments. If so then we
                # need to be a little clever about how we handle the PJRmi
                # kwargs.
                unexpected_kwargs = None
                if not ctor['has_kwargs']:
                    # Anything given is not expected since the CTOR doesn't
                    # accept kwargs
                    if kwargs:
                        unexpected_kwargs = set(kwargs)

                else:
                    # This method accepts kwargs so we add kwargs to the list of
                    # arguments
                    args = args + (kwargs,)

                    # Simply compute the difference between what's accepted and
                    # what's given, if we have a limiting set
                    kwarg_names = ctor['kwarg_names']
                    if kwarg_names:
                        unexpected_kwargs = set(kwargs) - kwarg_names

                # If we got unexpected kwargs then we should fail to bind
                if unexpected_kwargs:
                    msg = "Got%s unexpected keyword argument%s: %s" % (
                        ' an' if len(unexpected_kwargs) == 1 else '',
                        ''    if len(unexpected_kwargs) == 1 else 's',
                        ', '.join(unexpected_kwargs)
                    )
                    exceptions.append(msg)
                    if log_debug:
                        LOG.debug("Failed to bind kwargs for %s constructor: %s",
                                  klass._classname, msg)
                    continue

                # Now we know how many arguments we have
                num_args     = len(args)
                strict_types = num_args in strict_types_for_num_args

                # See if we had the right number of arguments
                argument_type_ids = ctor['argument_type_ids']
                want_args = len(argument_type_ids)
                if num_args == want_args:
                    # Got the right number of arguments for the constructor,
                    # create the argument list to invoke it. If this fails then
                    # we probably had the wrong method (overloaded with a
                    # differnt type).
                    java_args = b""
                    exception = None
                    try:
                        for idx in range(want_args):
                            try:
                                # Get the argument, and convert it into the
                                # appropriate value to send to Java
                                argument   = args[idx]
                                arg_klass  = self._get_class(argument_type_ids[idx])
                                java_args += self._format_by_class(arg_klass,
                                                                   argument,
                                                                   strict_types=strict_types)

                            except ImpreciseRepresentationError as e:
                                # Add some more information and remember the
                                # exception to possibly re-raise later on
                                if exception is None:
                                    (_, _, tb) = sys.exc_info()
                                    e = ImpreciseRepresentationError(
                                        "%s when calling %s's constructor" %
                                        (e, klass._classname)
                                    )
                                    exception = (e, tb)

                    except (TypeError, ValueError) as e:
                        exceptions.append(str(e))
                        if log_debug:
                            LOG.debug("Failed to bind variable for %s constructor: %s",
                                      klass._classname, e)
                        continue

                    # Did we match anything already?
                    if len(matches) == 0:
                        # No, it's safe to remember this one
                        matches.append((ctor, java_args, exception))

                    elif exception is None and matches[0][2] is not None:
                        # This replaces the existing matches since it has no
                        # truncation exception, whereas they do.
                        matches = [(ctor, java_args, exception)]

                    elif exception is not None and matches[0][2] is None:
                        # The existing matches are better since they have no
                        # truncation exception.
                        continue

                    else:
                        # We have successfully bound to two constructors. Now
                        # the same logic as there is in the method code follows.
                        # The comments here will be condensed versions.

                        # The new list of matches. This is created from the
                        # current one but differs depending on the relative
                        # specificity of our new match.
                        new_matches = list()
                        new_matches.append((ctor, java_args, exception))

                        # Create the new list, or maybe not.
                        assign = True
                        for match in matches:
                            # Get the method index of the current match
                            cur_index = match[0]['index']

                            # Get the relative specificity of the current match.
                            rel_spec = ctor['relative_specificities'][cur_index]
                            if rel_spec > 0:
                                # What we have already is better than this one,
                                # discard it
                                assign = False
                                break
                            elif rel_spec == 0:
                                # It's incomparable, so keep this one from the
                                # current list
                                new_matches.append(match)
                            else:
                                # This one from the current less is not as good
                                # as our new one. Drop it.
                                pass

                        # See if we want to use new_matches as our new list
                        if assign:
                            matches = new_matches

            # If we bound to a method then 'matches' will contain it. If there
            # is only one match then we're golden.
            if len(matches) == 1:
                # Pull out the details so we can call it
                (ctor, java_args, exception) = matches[0]

                # Any exception to raise
                if exception is not None:
                    (e, tb) = exception
                    try:
                        raise e.with_traceback(tb)
                    finally:
                        del tb

                # If this constructor is marked as deprecated then we log
                # accordingly
                if ctor['is_deprecated']:
                    fully_qualified = str(klass._classname)
                    msg = ('%s(%s) is marked as deprecated in Java' %
                           (klass._classname,
                            ', '.join(str(i.__class__) for i in args)))
                    self._log_deprecated(fully_qualified.replace('$', '.'), msg)

                # Call it and give back the result
                return self._call_method(None,
                                         klass._type_id,
                                         True, # CTOR
                                         self._VALUE_FORMAT_REFERENCE,
                                         self.SYNC_MODE_SYNCHRONOUS,
                                         ctor['index'],
                                         java_args)

            elif len(matches) > 1:
                # We have an ambiguous match; the arguments could legitimately
                # match either one. We need to error out at this point.
                def fmt(c):
                    return "%s(%s)" % (
                        klass._classname,
                        ', '.join(self._get_class(i)._classname
                                  for i in c['argument_type_ids'])
                    )
                raise TypeError(
                    "Call to %s(%s) is ambiguous; multiple matches: %s" %
                    (klass._classname,
                     ', '.join(str(type(arg)) for arg in args),
                     ', '.join(fmt(m[0]) for m in matches))
                )

            else:
                # matches was empty so we found nothing
                raise TypeError(
                    "Could not find a constructor matching %s(%s): %s" %
                    (klass._classname,
                     ', '.join(str(i.__class__) for i in args),
                     '; '.join(exceptions))
                )

        # Give it back
        return staticmethod(__new__)


    def _add_array_methods(self, klass):
        """
        Add methods required for array usage to the class.
        """

        # Define the methods. Array elements are modelled as fields.
        def __getitem__(self_, key):
            return self._get_field(klass,
                                   self_._pjrmi_handle,
                                   strict_number(numpy.int32, key))

        def __setitem__(self_, key, value):
            self._set_field(klass,
                            self_._pjrmi_handle,
                            strict_number(numpy.int32, key),
                            self._get_class(self_._array_element_type_id), value)

        def __len__(self_):
            return self_._length

        def __iter__(self_):
            i = 0
            while i < self_._length:
                yield self._get_field(klass, self_._pjrmi_handle, i)
                i += 1

        def _repr_pretty_(self_, p, cycle):
            if cycle:
                p.text("[...]")
            else:
                with p.group(1, "[", "]"):
                    for i, el in enumerate(self_):
                        if i >= 100:
                            p.text(",")
                            p.breakable()
                            p.text("...")
                            break
                        if i > 0:
                            p.text(",")
                            p.breakable()
                        p.pretty(el)

        # Add them to the class
        setattr(klass, "__getitem__",   __getitem__  )
        setattr(klass, "__setitem__",   __setitem__  )
        setattr(klass, "__len__",       __len__      )
        setattr(klass, "__iter__",      __iter__     )
        setattr(klass, "_repr_pretty_", _repr_pretty_)


    def _add_type_specific_methods(self, klass):
        """
        For a given Java class representation we add any type-specific methods.
        This is mostly making the Java stuff code play nice with the Python
        world.
        """
        def isjavasubclass(javaclass, classname, attrname):
            """
            Whether the `javaclass` is a subclass of the one specified via the given
            details.
            """
            # None can't be an instance. Otherwise look for an exact match (by
            # name) and then a subclass match. The attr could be missing if we
            # are in the middle of bootstrapping so handle that. (E.g. we could
            # be running this for Object before we have built the String class.)
            if javaclass is None:
                return False
            if javaclass._classname == classname:
                return True
            attr_class = getattr(self, attrname, None)
            return attr_class is not None and javaclass._instance_of(attr_class)

        # All classes get the default str() method, exceptions actually yield
        # the full stack trace, coz it's handy to have
        if issubclass(klass, JavaException):
            def __str__(self_):
                # Look for a cached value. We rely on the caching in places like
                # pjrmi.__exit__().
                if hasattr(self_, '_str'):
                    return getattr(self_, '_str')

                # Any changes to the object must happen under the guard
                with self_._pjrmi_attr_guard:
                    # This relies on the connection being up which might not be
                    # the case
                    try:
                        # Grab the stack trace from the Java side, this will
                        # never change
                        StringUtil = self.class_for_name('com.deshaw.util.StringUtil')
                        self_._pjrmi_str = str(StringUtil.stackTraceToString(self_))
                    except Exception as e:
                        # We can't do a lot here so say so
                        LOG.debug("Could not render exception: %s", e)
                        self_._pjrmi_str = "<UNKNOWN ERROR>"

                # And give it back
                return self_._pjrmi_str
        else:
            def __str__(self_):
                # Look for a cached value
                if hasattr(self_, '_pjrmi_str'):
                    return getattr(self_, '_pjrmi_str')

                # Else we need to call over to the Java side
                req_id = self._send(self._TO_STRING,
                                    self._format_int64(self_._pjrmi_handle))

                # Read the result
                result = self._read_result(req_id)

                # Cache the string value if we're wrapping an immutable Java
                # class since its toString() method will always return the same
                # value
                if klass._is_immutable:
                    # Yes, it's safe to cache the string value then
                    with self_._pjrmi_attr_guard:
                        setattr(self_, '_pjrmi_str', result)

                # Give back the result now
                return result

        setattr(klass, "__str__", __str__)

        # Now some special handling
        if isjavasubclass(klass, "java.lang.String", "_java_lang_String"):
            def __add__(self_, that):
                if isinstance(that, (str, _JavaObject)):
                    return self_.__str__() + str(that)
                else:
                    raise TypeError("cannot concatenate '%s' and '%s' objects" %
                                    (str(type(self_)), str(type(that))))

            setattr(klass, "__add__", __add__)

            def __len__(self_):
                return len(str(self_))

            setattr(klass, "__len__", __len__)

            # Strings are immutable. (At some point we'll get this info from the
            # Java side; it probably requires special handling from there...)
            setattr(klass, "_is_immutable", True)

        # Map methods. We do the explicit test for Map to avoid recursing
        # forever if we are creating a Map.
        if isjavasubclass(klass, "java.util.Map", '_java_util_Map'):
            def __getitem__(self_, index):
                return self_.get(index)

            def __len__(self_):
                return self_.size()

            def __iter__(self_):
                # Mimic dict's __iter__ idiom
                return self_.keySet().__iter__()

            def _repr_pretty_(self_, p, cycle):
                simple_name = klass._classname.split(".")[-1]
                if cycle:
                    p.text(f"{simple_name}(...)")
                else:
                    with p.group(len(simple_name) + 2,
                                 f"{simple_name}({{", "})"):
                        try:
                            limit = int(getattr(self_,
                                                '__repr_pretty_limit__',
                                                100))
                        except Exception:
                            limit = 100
                        for i, (k,v) in enumerate(self_.entrySet()):
                            if i >= limit:
                                p.text(",")
                                p.breakable()
                                p.text("...")
                                break
                            if i > 0:
                                p.text(",")
                                p.breakable()
                            p.pretty(k)
                            p.text(": ")
                            p.pretty(v)

            setattr(klass, "__getitem__",   __getitem__  )
            setattr(klass, "__len__",       __len__      )
            setattr(klass, "__iter__",      __iter__     )
            setattr(klass, "_repr_pretty_", _repr_pretty_)

        if isjavasubclass(klass, "java.util.Map$Entry", '_java_util_Map_Entry'):
            def __getitem__(self_, index):
                if index == 0:
                    return self_.getKey()
                if index == 1:
                    return self_.getValue()
                raise IndexError("MapEntry index out of range")

            def __len__(self_):
                return 2

            setattr(klass, "__getitem__", __getitem__)
            setattr(klass, "__len__",     __len__    )

        # List methods. We do the explicit test for List to avoid recursing
        # forever if we are creating a List.
        if isjavasubclass(klass, "java.util.List", '_java_util_List'):
            def __getitem__(self_, key):
                return self_.get(strict_number(numpy.int32, key))

            def __setitem__(self_, key, value):
                return self_.set(strict_number(numpy.int32, key), value)

            setattr(klass, "__getitem__",   __getitem__)
            setattr(klass, "__setitem__",   __setitem__)

        # Collection methods. We do the explicit test for Collection to avoid
        # recursing forever if we are creating a Collection.
        if isjavasubclass(klass, "java.util.Collection", '_java_util_Collection'):
            def __len__(self_):
                return self_.size()

            setattr(klass, "__len__", __len__)

        # If something is a Java Iterator or Iterable then we make it iterable
        # under Python too. We need to handle the boot-strapping case where
        # we're creating the _java_blah_blah values here too.
        if isjavasubclass(klass, 'java.util.Iterator', '_java_util_Iterator'):
            def __iter__(self_):
                # We "ask for forgiveness not permission" here (i.e., catch
                # NoSuchElementException instead of calling hasNext()) to avoid
                # an additional remote call per iteration (vs the exception
                # cost, which is incurred once at the end of the iteration).
                try:
                    while True:
                        yield self_.next()
                except JavaException as e:
                    if self.is_instance_of(e, self._java_util_NoSuchElementException):
                        return
                    raise

            setattr(klass, "__iter__", __iter__)

        if isjavasubclass(klass, 'java.lang.Iterable', '_java_lang_Iterable'):
            def __iter__(self_):
                # Hand off to the java.util.Iterator case
                return iter(self_.iterator())

            def _repr_pretty_(self_, p, cycle):
                # Whether to name the container objects as we recurse down. For
                # Hypercubes this is mainly noise down the line so we suppress
                # it as we recurse. (There is probably a better way of telling
                # how far down the stack we are over than looking at
                # 'indentation' but I don't know what it is.)
                if p.indentation > 0 and \
                   isjavasubclass(klass,
                                  'com.deshaw.hypercube.Hypercube',
                                  '_com_deshaw_hypercube_Hypercube'):
                    sn = ''  # simple name
                    op = ' ' # opening paren
                    cp = ' ' # closing paren
                else:
                    sn = klass._classname.split(".")[-1]
                    op = '('
                    cp = ')'

                if cycle:
                    p.text(f"{sn}{op}...{cp}")
                else:
                    with p.group(len(sn) + 2, f"{sn}{op}[", f"]{cp}"):
                        try:
                            limit = int(getattr(self_,
                                                '__repr_pretty_limit__',
                                                100))
                        except Exception:
                            limit = 100
                        for i, el in enumerate(self_):
                            if i >= limit:
                                p.text(",")
                                p.breakable()
                                p.text("...")
                                break
                            if i > 0:
                                p.text(",")
                                p.breakable()
                            p.pretty(el)

            setattr(klass, "__iter__",      __iter__     )
            setattr(klass, "_repr_pretty_", _repr_pretty_)

        # If something is an Comparable then we make it comparable under Python too.
        # We need to handle the boot-strapping case where we're creating the
        # _java_lang_Comparable value here too.
        if isjavasubclass(klass, 'java.lang.Comparable', '_java_lang_Comparable'):
            def __cmp__(self_, that):
                return self_.compareTo(that)

            setattr(klass, "__cmp__", __cmp__)

        # If something is an AutoCloseable then we add the __enter__() and
        # __exit__() methods for Python. We need to handle the boot-strapping
        # case where we're creating the _java_lang_AutoCloseable value here too.
        if isjavasubclass(klass, 'java.lang.AutoCloseable', '_java_lang_AutoCloseable'):
            def __enter__(self_):
                return self_
            def __exit__(self_, typ, value, traceback):
                self_.close()

            setattr(klass, "__enter__", __enter__)
            setattr(klass, "__exit__",  __exit__ )

        if isjavasubclass(klass,
                          'com.deshaw.hypercube.Hypercube',
                          '_com_deshaw_hypercube_Hypercube'):
            # We create the __array__ method so that it behaves like numpy
            # expects, here on the Python side of things. This isn't overly
            # efficient (to put it mildly) but it works, and is better than
            # having per-element access.

            def dtype_to_simplename(dtype):
                # Get the class name of the a dtype, if any
                if isinstance(dtype, numpy.dtype):
                    dtype = dtype.name
                if   dtype == 'float32': return 'Float'
                elif dtype == 'float64': return 'Double'
                elif dtype == 'int32':   return 'Integer'
                elif dtype == 'int64':   return 'Long'
                else: raise ValueError("Unhandled dtype: %s" % (dtype,))

            def __array__(self_, dtype=None):
                # Do this by best-effort assuming that it's one which we can
                # cast and pickle but, if we encounter an error, then just go
                # with doing it "by hand".

                # Try to get the class to its most specific form, since that
                # should have the methods which we care about etc.
                try:
                    self_ = self.cast_to(self_, self_.getClass())
                except Exception:
                    pass

                try:
                    # Figure out the name of the casting class to use, if any.
                    # Assume it's the same type until we know better.
                    if dtype is not None:
                        from_type = dtype_to_simplename(self_.getDType().name)
                        to_type   = dtype_to_simplename(dtype)
                        if from_type != to_type:
                            nm = 'com.deshaw.hypercube.%sFrom%sHypercube' % (
                                     to_type, from_type
                                 )
                            self_ = self.class_for_name(nm)(self_)

                    # And render the cube over the wire
                    result = self.value_of(self_)

                except Exception:
                    # Handle it manually. Use SHM-passing if we can, since it's
                    # more efficient.
                    fmt = (
                        self.VALUE_FORMAT_SHMDATA if self._use_shmdata else
                        self.VALUE_FORMAT_PICKLE
                    )

                    # Handle any type conversions, if we can
                    if hasattr(self_, 'array'):
                        if dtype is None:
                            result = self_.array()
                        elif isinstance(dtype, numpy.dtype):
                            result = self_.array(dtype.name)
                        else:
                            result = self_.array(dtype)
                    else:
                        if dtype is None:
                            result = self_
                        else:
                            raise NotImplementedError(
                                "Conversion to dtypes not supported "
                                "for cubes of type '%s'" % (type(self_))
                            )

                    # If we have something to convert then we downcast it to its
                    # actual type, since we will need to access the toArray()
                    # method, if we can.
                    if result is not None:
                        try:
                            result = self.cast_to(result, result.getClass())
                        except Exception:
                            pass

                    # Convert it to an array and hand it back if we can. If that
                    # doesn't work then we just give back the result of the
                    # array() call (which will be a Hypercube but should
                    # duck-type as an ndarray).

                    # Coerce via the toArray() method as a by-value call, which
                    # should return a numpy array which we can reshape
                    if hasattr(result, "toArray"):
                        result = result.toArray(__pjrmi_return_format__=fmt) \
                                       .reshape(result.getShape())
                    else:
                        try:
                            result = result.toObjectArray(__pjrmi_return_format__=fmt) \
                                           .reshape(result.getShape())
                        except Exception:
                            pass

                # If it's not an ndarray at this point then we attempt to
                # convert it by hand. Be careful not to infinitely recurse
                # though, if result is 'self'.
                if result is not self_ and \
                   not isinstance(result, numpy.ndarray):
                    try:
                        result = numpy.array(result)
                    except Exception:
                        pass

                # Give back what we had. Hopefully it was an ndarray at this
                # point but, if not, numpy will tell us as much.
                return result

            setattr(klass, "__array__", __array__)


    def _get_class_doc_url(self, klass):
        """
        Attempt to get the JavaDoc URL for the given class.

        :param klass: The Python class instance of the Java class.

        :return: The link text, or None if it could not be found for some
                 reason.
        """
        # By default we return None. This is a hook for client users to override
        # and give back something useful, if they so desire.
        return None


    def _create_object(self, type_id, handle, raw):
        """
        Get an object of a given type ID using the given handle and any raw
        representation.
        """

        # Check for the null pointer handle
        if handle == self._NULL_HANDLE:
            return None

        # Otherwise we attempt to create it from the given type
        klass = self._get_class(type_id)
        if klass is None:
            raise TypeError("Can't determine class for type %d" % type_id)
        else:
            # Create the object
            if issubclass(klass, Exception):
                result = Exception.__new__(klass)
            else:
                result = object.__new__(klass)

            # Set all the parts which we expect to have associated with it
            result._pjrmi_inst   = self
            result._pjrmi_handle = handle

            # Need the __len__ method for arrays
            if klass._is_array:
                # Figure out the length and add the method to the instance
                req_id = self._send(self._GET_ARRAY_LENGTH, self._format_int64(handle))
                setattr(result, "_length", self._read_result(req_id))

            # Now that we are done building the Java object, we add the
            # attribute modification guard
            result._pjrmi_attr_guard = _ContextGuard()
            result._pjrmi_attr_guard.__doc__ = '''\
The _pjrmi_attr_guard prevents accidental creation or deletion of Python
attributes on Java objects. In order to allow such changes with guard may be
used as in a `with` conntext.
'''

            # We have special handling for some Python-native-like types here.
            # We turn them into special boxed objects which look like their
            # Python counterparts. This could be invoked during initialisation
            # so account for that.
            if result is not None and self._boxes_by_type_id is not None:
                boxer = self._boxes_by_type_id.get(type_id)
                if boxer is not None:
                    box = boxer(result, raw)
                    box._java_object = result
                    result = box
                elif type_id == self._java_lang_Boolean._type_id:
                    # Given back real Python booleans. We can't box them
                    # since bool isn't a type you can subclass from. This
                    # probably doesn't matter here though since inferring
                    # their Java type is easy.
                    if raw is None:
                        result = result.booleanValue()
                    else:
                        result = (raw[0] != 0)

            # And give it back
            return result


    def _create_array(self, klass, length):
        """
        Create a new instance of the given Java class, is should be an array.
        """

        # Must be an array
        if not klass._is_array:
            raise TypeError("Not an array class: %s" % str(klass))

        # Figure out the length and add the method to the instance
        payload = (self._format_int32(klass._type_id) +
                   self._format_int32(length))

        req_id = self._send(self._NEW_ARRAY_INSTANCE, payload)
        return self._read_result(req_id)


    def _call_method(self,
                     obj,
                     type_id,
                     is_ctor,
                     value_format,
                     sync_mode,
                     method_id,
                     args):
        """
        Call a given method on the Java side
        """

        # Create the call info
        handle = self._NULL_HANDLE if obj is None else obj._pjrmi_handle
        payload = ((b'\x01' if is_ctor else b'\x00') +
                    self._format_int32(type_id)      +
                    value_format                     +
                    sync_mode                        +
                    self._format_int64(handle)       +
                    self._format_int32(method_id)    +
                    args)

        # Send it to the server
        req_id = self._send(self._METHOD_CALL, payload)

        # Read the result and give it back
        return self._read_result(req_id)


    def _drop_reference(self, type_, handle):
        """
        Drop a reference to a given handle on the Java side.
        """

        # Ignore bad handles, including the NULL one. Thus we only look at
        # positive handle values.
        if handle > 0:
            # Add it to the list of pending drops. Since append() is atomic this
            # is safe to do without a lock.
            self._pending_drops.append(handle)


    def _handle_pending_drops(self):
        """
        Possibly send the pending drops to the Java side.
        """
        # Reached our limit? We don't care about this being a little racey with
        # _drop_reference() since it's just a trigger.
        if len(self._pending_drops) < 100:
            # Nope
            return

        # Drain all the items out of the list into a new one. We know that pop()
        # is atomic so this is safe to do with other threads calling
        # _drop_reference() and appending the list at the same time. The below,
        # while slow, will ensure that the list is completely emptied by us by
        # virtue of the loop exit via the exception.
        #
        # In theory this could never terminate if _lots_ of objects are being
        # continually __del__'d but since that is only called in the GC it's at
        # least bounded by that.
        drops = []
        try:
            while True:
                drops.append(self._pending_drops.pop())
        except IndexError:
            pass

        # We can now send the messages to the Java side
        payload = (self._format_int32(len(drops)) +
                   b''.join(self._format_int64(h) for h in drops))

        # Send it to the server and reap the result
        req_id = self._send(self._DROP_REFERENCES, payload)
        self._read_result(req_id)


    def _get_field(self, object_class, handle, index):
        """
        Get a given field's value.
        """

        # Send it to the server
        payload = (self._format_int32(object_class._type_id) +
                   self._format_int64(handle)                +
                   self._format_int32(index))
        req_id = self._send(self._GET_FIELD, payload)

        # And return the result
        return self._read_result(req_id)


    def _set_field(self, object_klass, handle, index, value_klass, value):
        """
        Set a given field's value.
        """

        # Send it to the server
        payload = (self._format_int32(object_klass._type_id) +
                   self._format_int64(handle)                +
                   self._format_int32(index)                 +
                   self._format_by_class(value_klass, value))
        req_id = self._send(self._SET_FIELD, payload)

        # Read the ACK, return nothing
        self._read_result(req_id)


    @property
    def _has_receiver(self):
        """
        Whether or not this class has a receiver thread.
        """
        return self._receiver is not None


    def _get_callback_function(self, function_id):
        """
        Get the function associated with a given function ID.
        """
        with self._callback_lock:
            return self._callback_id2func.get(function_id, None)


    def _get_callback_wrapper(self, function, klass=None):
        """
        Get a callback wrapper for the given function.
        """

        # We only allow this if we support callback via a receiver thread
        if not self._has_receiver:
            raise ValueError(
                "Callbacks are not supported by the remote Java PJRmi instance"
            )

        # Go under the lock to protect threaded access to our various
        # data-structures, and to make this whole function atomic
        with self._callback_lock:
            # The type of function we want
            if klass is None:
                type_id = -1
            else:
                type_id = klass._type_id

            # A unique key for this function and its associated type. This is
            # because people might try to wrap the same function behind
            # different things.
            key = (id(function), type_id)

            # See if we have a wrapper already
            wrapper = self._callback_func2wrap.get(key, None)
            if wrapper is None:
                # Info about the function, this will throw early on if we've not
                # been given a function meaning we don't have to worry about
                # that below
                argspec = getfullargspec(function)

                # We need a unique ID for this function and to remember it
                function_id = self._callback_nextid()
                self._callback_id2func[function_id] = function

                # If the function is a bound method then the first argument is
                # known to be `self` and is implictly placed as the first when
                # the function is called. We have to account for that when we
                # are determining the number of arguments.
                if isinstance(function, (BuiltinMethodType,
                                         MethodType,
                                         MethodWrapperType)):
                    num_args = len(argspec.args) - 1
                else:
                    num_args = len(argspec.args)

                # If the above code is working then we should not have a
                # negative number of arguments. Also, Java maxes out at 255
                # arguments to a function so check for that too.
                if num_args < 0:
                    raise ValueError(
                        "%r appears to have a malformed argspec: %s" %
                        (function, argspec)
                    )
                elif num_args > 255:
                    raise ValueError(
                        "%r has too many arguments to be used as a Java function" %
                        (function,)
                    )

                # Send the request to the server
                payload = (self._format_int32(function_id) +
                           self._format_int32(type_id)     +
                           self._format_int8 (num_args))
                req_id = self._send(self._GET_CALLBACK_HANDLE, payload)

                # Get the wrapper
                wrapper = self._read_result(req_id)
                self._callback_func2wrap[key] = wrapper

            # This should be all good now
            return wrapper


    def _get_callback_object(self, object_id):
        """
        Get the object associated with a given object ID.
        """
        with self._callback_lock:
            return self._callback_id2obj.get(object_id, None)


    def _get_object_id(self, python_object):
        """
        Get the ID corresponding to a Python object instance.
        """

        # We only allow this if we support callback via a receiver thread
        if not self._has_receiver:
            raise ValueError(
                "Callbacks are not supported by the connected Java PJRmi instance"
            )

        # Null pointers get a negative ID
        if python_object is None:
            return -1

        # Go under the lock to protect threaded access to our various
        # data-structures, and to make this whole function atomic
        with self._callback_lock:
            # Already have one? Note that we use id() here since we want to
            # track the object uniquelly (and we can't use mutables like dicts
            # as keys).
            object_id = self._callback_obj2id.get(id(python_object), None)
            if object_id is None:
                # We need a unique ID for this object and to remember it
                object_id = self._callback_nextid()
                self._callback_obj2id[id(python_object)] = object_id
                self._callback_id2obj[object_id]         = python_object
                self._callback_id2ref[object_id]         = 0

            # And hand it back
            return object_id


    def _get_object_wrapper(self, python_object, java_class):
        """
        Get a wrapper for the given python object so that it may be treated as an
        instance of the given Java interface. This is done by creating a Java
        Proxy on the other side.
        """

        # We only allow this if we support callback via a receiver thread
        if not self._has_receiver:
            raise ValueError(
                "Callbacks are not supported by the connected Java PJRmi instance"
            )

        # Ensure that java_class is a JavaClass for this instance
        if not isinstance(java_class, self._JavaClass):
            raise TypeError(
                "Given Java class (%s, %s) was not a Java class for this PJRmi instance" %
                (java_class, type(java_class))
            )

        # We can only create proxies for interfaces
        if not java_class._is_interface:
            raise TypeError(
                "Given Java class (%s) was not an interface" % java_class
            )

        # A unique key for this object wrapped as the give type. This is because
        # people might try to wrap it in different types.
        wrap_key = (id(python_object), java_class._type_id)

        # Go under the lock to protect threaded access to our various
        # data-structures, and to make this whole function atomic
        with self._callback_lock:
            # Already have one?
            wrapper = self._callback_obj2wrap.get(wrap_key, None)
            if wrapper is None:
                # We walk the Java class and look for the Java methods and ensure that
                # they are present in the python object as methods
                for (name, methods) in java_class._methods.items():
                    # Ignore certain methods which are handled on the Java side
                    if name in ('getClass', 'notify', 'notifyAll', 'toString', 'wait'):
                        continue

                    # If the method is purely default then we don't need to care
                    # about it. If we have any non-default versions though, then
                    # we need to call them.
                    is_default = True
                    for method in methods:
                        if not method['is_default']:
                            is_default = False
                            break
                    if is_default:
                        continue

                    # Same for static methods
                    is_static = True
                    for method in methods:
                        if not method['is_static']:
                            is_static = False
                            break
                    if is_static:
                        continue

                    # Okay, look for it
                    pf = getattr(python_object, name, None)
                    if not isinstance(pf, (FunctionType, MethodType)):
                        raise TypeError(
                            ("Given python object (%s) "
                             "was missing required function: %s()") %
                            (python_object, name)
                        )

                # Get the ID associated with this object
                object_id = self._get_object_id(python_object)

                # Okay, it's safe to request a wrapper for this type now
                payload = (self._format_int32(object_id) +
                           self._format_int32(java_class._type_id))
                req_id = self._send(self._GET_PROXY, payload)

                # Get the wrapper
                wrapper = self._read_result(req_id)
                self._callback_obj2wrap[wrap_key] = wrapper

            # This should be all good now
            return wrapper


    def _add_object_reference(self, object_id):
        """
        Increment the refcount for a given object ID.
        """

        # We only allow this if we support callback via a receiver thread
        if not self._has_receiver:
            raise ValueError(
                "Callbacks are not supported by the connected Java PJRmi instance"
            )

        # Go under the lock to protect threaded access to our various
        # data-structures, and to make this whole function atomic
        with self._callback_lock:
            # Look for the object
            try:
                self._callback_id2ref[object_id] += 1
            except Exception:
                pass


    def _drop_object_reference(self, object_id):
        """
        Decrement the refcount for a given object ID.
        """

        # We only allow this if we support callback via a receiver thread
        if not self._has_receiver:
            raise ValueError(
                "Callbacks are not supported by the connected Java PJRmi instance"
            )

        # Go under the lock to protect threaded access to our various
        # data-structures, and to make this whole function atomic
        with self._callback_lock:
            # Look for the object
            try:
                count = self._callback_id2ref[object_id]
                count -= 1
                if count <= 0:
                    # Time to forget about it
                    obj = self._callback_id2obj  .pop(object_id)
                    self.      _callback_id2ref  .pop(object_id)
                    self.      _callback_obj2id  .pop(id(obj))
                    self.      _callback_obj2wrap.pop(id(obj))
                else:
                    self._callback_id2ref[object_id] = count
            except Exception:
                pass


    def _log_deprecated(self, method_name, message):
        """
        Log the details of a call to a deprecated Java method. Subclasses can add
        their own hooks here in order to keep track of such calls.

        :type  method_name: str
        :param method_name:
            The fully-qualified name of the deprecated method.

        :type  message: str
        :param message:
            Any extra details which might be informative to the handler.
        """
        # By default we do nothing here
        pass


def _handle_pickle_bytes_create_object(pjrmi_id, type_id, handle):
    """
    Special helper method for ``_handle_pickle_bytes()`` which can fetch the
    appropriate PJRmi instance and invoke ``_create_object`` on it using the
    given arguments.

    Not to be used for anything else..!
    """
    # We pass in None for the raw data since we don't have it and, for this, we
    # should never need it since all the boxed types can be marshalled into
    # native Python types.
    return PJRmi._INSTANCES[pjrmi_id]._create_object(type_id, handle, None)

# -----------------------------------------------------------------------------

def connect_to_socket(host,
                      port,
                      mode    ='raw',
                      store   =None,
                      password=None,
                      impl    =PJRmi,
                      timeout =60):
    """
    Connect to a PJRmi instance on the given server, with the expected server
    name.

    We retry every second until the timeout expires since it might take a while
    for the PJRmi thread to register itself and start accepting connections. If
    the timeout expires then an exception detailing the failure will be thrown.

    This is a helper method for a common connection use-case.

    :param host:     The host to connect to.
    :param port:     The port to connect to.
    :param mode:     The type of connection, either ``raw``, for a simple
                     socket, or ``ssl`` for an SSL-secured socket.
    :param store:    The path to the key store, for an SSL connection.
    :param password: The password for the store, for an SSL connection.
    :param impl:     The `PJRmi` implementation to use.
    :param timeout:  The timeout, in seconds, before we give up trying.
    """

    # Sanity check the timeout, both type and value
    timeout = int(timeout)
    if timeout < 0:
        raise ValueError("Negative value given for timeout: %d" % timeout)
    end_time = time.time() + timeout

    # Attempt to connect
    while True:
        try:
            # Attempt to find the Loader's service using the service
            # director. This may fail if it hasn't registered yet.
            if mode == 'raw':
                handle = impl(SocketTransport(host, port))
            elif mode == 'ssl':
                handle = impl(SSLSocketTransport(host, port, store, password))
            else:
                raise ValueError(f"Unknown connection mode: {mode}")

            # Attempt to connect. This may fail if it's still starting up.
            handle.connect()

            # That worked, give it back
            return handle

        except Exception as e:
            # If we're within the timeout then wait for a second and try again,
            # else we simply rethrow the exception and let the user deal with it
            if time.time() < end_time:
                LOG.info("Got a %s whilst trying to connect, will retry: %s ",
                         type(e).__name__, e)
                time.sleep(1)
            else:
                # Just rethrow
                raise


def connect_to_inprocess_jvm(classpath=(), java_args=(), application_args=(),
                             impl=PJRmi):
    """
    Create an in-process JVM instance and connect to it. See
    `connect_to_child_jvm` for details of the ``application_args``.

    This may only be called once per process.

    Health Warning: This type of connection is wholly dependent on the Java VM
    and Python VM playing nicely with one another in the same process. They do
    today but...

    This connection type requires the C extension in order to function.

    :param classpath: A sequence of strings defining the Java classpath.
    :param java_args: A sequence of arguments to pass to the Java command.
    :param impl:      The `PJRmi` implementation to use.
    """

    c = impl(InprocessTransport(classpath       =classpath,
                                java_args       =java_args,
                                application_args=application_args))
    c.connect()

    return c


def connect_to_child_jvm(main_class='com.deshaw.pjrmi.UnixFifoProvider',
                         environment=None, java_executable=None,
                         classpath=(), java_args=(), application_args=(), timeout=60,
                         stdin='/dev/stdin', stdout='/dev/stdout', stderr='/dev/stderr',
                         interactive_mode=True, use_shm_arg_passing=False,
                         use_pjrmi_agent=False,
                         impl=PJRmi):
    """
    Create a child JVM instance and connect to it.

    The stdin, stdout and stderr arguments below may be file handles, file
    numbers or strings that will act as the standard input, standard output, and
    standard error for the child JVM process, respectively. If None is passed
    then the handle is deleted and input/output is suppressed.

    The current arguments supported by the child JVM are:
      ``num_workers`` -- ``int``
          The number of worker threads to use. A non-zero value is required for
          callbacks to work.
      ``use_locking`` -- ``bool``
          Whether global locking should be enabled internally.
      ``block_non_allowlisted_classes`` -- ``tuple<str>``
          Whether to enable class block-listing.
      ``additional_allowlisted_classes`` -- ``bool``
          The list of classes to allow, if blocklisting is enabled.
      ``allow_class_injection`` -- ``bool``
          Whether to allow class / source injection.
    Application args should be of the form "foo=bar".

    :param environment:         A dict to set the environment with, if any.
    :param java_executable:     The preferred ``java`` executable to use, if any.
    :param classpath:           A sequence of strings defining the Java classpath.
    :param java_args:           A sequence of arguments to pass to the Java command.
    :param application_args:    A sequence of arguments to pass to the PJRmi application.
    :param timeout:             How long to wait for the child process to connect.
    :param stdin:               the stdin file, or None to delete the handle.
    :param stdout:              The stdout file, or None to delete the handle.
    :param stderr:              The stderr file, or None to delete the handle.
    :param interactive_mode:    Whether the session is interactive, if so then we try to
                                make it more friendly to that.
    :param use_shm_arg_passing: Whether to enable passing of some values by SHM
                                copying. This requires the C extension to function.
    :param use_pjrmi_agent:     Whether to include the directive to load the
                                ``PJRmiAgent`` class on the command line.
    :param impl:                The `PJRmi` implementation to use.
    """

    # If we have been told to load the agent then we should include that as part
    # of the command line arguments
    if use_pjrmi_agent:
        if java_args is None:
            java_args = []
        else:
            java_args = list(java_args)
        java_args.append(f'-javaagent:{_PJRMI_FATJAR}')

    # Create and connect
    c = impl(UnixFifoTransport(main_class,
                               environment=environment,
                               java_executable=java_executable,
                               classpath=classpath,
                               java_args=java_args,
                               application_args=application_args,
                               timeout=timeout,
                               stdin=stdin,
                               stdout=stdout,
                               stderr=stderr),
             use_shm_arg_passing=use_shm_arg_passing)
    c.connect()

    # Turn off a couple of things which annoy people in interactive mode
    if interactive_mode:
        try:
            # Catch Ctrl-C which would otherwise rattle down and kill the Java
            # child
            UnixSignals = c.class_for_name('com.deshaw.pjrmi.UnixSignals')
            UnixSignals.ignoreSignal("INT")
        except Exception:
            pass

    return c


def become_pjrmi_minion(stdin=None, stdout=None, stderr=None,
                        use_shm_arg_passing=False,
                        impl=PJRmi):
    """
    Turn this process into a passive child of a Java process, which will drive
    us over STDIO. The resulting PJRmi connection is stored in the global
    variable _pjrmi_connection, should minion Python code need to access this
    (for example to call code on the remote Java side of the connection).

    This method does not return until stdio is closed on us.
    """

    global _pjrmi_connection
    t = StdioTransport(stdin=stdin, stdout=stdout, stderr=stderr)
    _pjrmi_connection = impl(t, use_shm_arg_passing=use_shm_arg_passing)
    _pjrmi_connection.connect()

    # Ensure that this process is connected to a worker-enabled JVM instance,
    # implying that we have a Receiver thread, else it's a little pointless.
    if not _pjrmi_connection._has_receiver:
        raise ValueError("Not connected to a worker-capable JVM")

    # Now sleep until we notice the transport go away
    while t.connected():
        time.sleep(1)

def get_config():
    """
    Return information about the pjrmi installation. This can be used by
    external scripts to determine the installation's classpath and libpath etc.
    """
    return {
        "version"   : PJRMI_VERSION,
        "classpath" : _PJRMI_FATJAR,
        "libpath"   : _PJRMI_SHAREDLIBS_PATH
    }

# -----------------------------------------------------------------------------


class char(str):
    """
    A single character string. Python does not have these so we roll our
    own. This is useful when we need to infer the concrete type of a method's
    Object argument.
    """

    def __init__(self, value):
        value = str(value)
        if len(value) != 1:
            raise ValueError("Given input was not of length 1: \"%s\"" % value)


def str2obj(str):
    """
    Given an object/function name as a string attempt to resolve it to an actual
    instance.
    """

    # Pull the name apart to figure out the module and name
    (mod_name, _, func_name) = str.rpartition('.')
    if mod_name == '':
        # If it's not qualified then assume that it's a built-in
        mod_name = 'builtins'
    mod = sys.modules[mod_name]

    # Attempt to resolve from the module first. If that fails then it could have
    # been imported so look for it in the globals. If that fails then fall back
    # to eval as a best effort.
    return (getattr(mod, func_name, None) or
            globals().get(str, None)      or
            eval(str))


def _tb2jexstr(tb):
    """
    Turn a traceback into a string which are good for printing in Java
    exceptions.
    """
    return ('      ' +
            ''.join(format_tb(tb)).rstrip().replace('\n', '\n      '))


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


class _LazyTypeError(TypeError):
    """
    `_LazyTypeError` is a `TypeError` that delays the rendering of the error
    message until it's time to raise it to the client or log it etc.

    This helps in avoiding potentially unnecessary TO_STRING requests to the
    Java side if you're wrapping any Java values. `_LazyTypeError` intentionally
    inherits from `TypeError`, so it's transparent to any existing code catching
    `TypeError` s.
    """

    def __init__(self, expected_class, actual_value):
        self._expected_class = expected_class
        self._actual_value   = actual_value
        self._error_message  = None


    def __str__(self):
        if self._error_message is None:
            try:
                self._error_message = (
                    '_LazyTypeError: Expected a %s but had %s: %s' % (
                        self._expected_class, self._actual_value.__class__,
                        self._actual_value
                    )
                )
            except Exception as e:
                self._error_message = (
                    '_LazyTypeError: Unable to render the error message'
                    ' (connection closed?): %s' % e
                )
        return self._error_message


class JavaException(Exception):
    """
    An exception raised by the Java server instance.
    """


class JavaProxyBase:
    """
    A base class for implementing Python classes which may be used as proxies.

    Note that a Python class may only be used as a proxy if the Java server
    supports callbacks. (Typically this means that the server is configured to
    have multiple workers.) If one attempts to call a Java method using a Python
    proxy class, and it fails to work (saying it can't find a match), then that
    might be why.
    """

    def equals(self, that):
        """
        Indicates whether some other object is "equal to" this one.
        """

        return (self == that)


    def hashCode(self):
        """
        Returns a hash code value for the object.
        """

        return hash(self)


    def toString(self):
        """
        A placeholder method. This can't call __str__ since that will infinitely
        recurse (since that will call this method) so we'll defer to our
        parent's value. Subclasses may override it.
        """

        return super().__str__()


class JavaMethod:
    """
    A local handle representing a bound method of a Java class or object
    instance.
    """
    def __init__(self, rmi, is_ctor, details, klass, this):
        """
        :param rmi:      The PJRmi instance.
        :param is_ctor:  Whether the method is a constructor.
        :param details:  The method details.
        :param klass:    Its container class.
        :param this:     The handle of the Java object which it was captured
                         from, if any.
        """
        self._pjrmi             = rmi
        self._is_ctor           = is_ctor
        self._details           = details
        self._is_static         = is_ctor or details['is_static']
        self._name              = 'new' if is_ctor else details['name']
        self._argument_type_ids = self._details['argument_type_ids']
        self._klass             = klass
        self._this              = this


    def _can_format_as(self, klass):
        """
        Whether this method can (probably) be rendered as the given Java class for
        passing as an argument. This is only true for Java's functional
        interfaces.
        """
        # Really a best-effort on this. We'll let the Java side figure it out
        # completely.
        return issubclass(klass, _JavaObject) and klass._is_functional


    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("...")
        else:
            p.text(self.__str__())


    def __call__(self, *args, **kwargs):
        """
        Invoke the method with the given arguments.

        If the method is captured from a class but is an instance method, then
        an instance should be passed in as the first argument.
        """
        # See if we should use the captured 'this' pointer or the one from the
        # argument list, or none at all
        if not self._is_static:
            if (len(args) == len(self._argument_type_ids) + 1 and
                type(args[0]) == self._klass):
                this = args[0]
                args = args[1:]
            else:
                this = self._this
        else:
            this = None

        # If we have no "this" pointer then we can only call a static method,
        # unless the caller gives us an instance pointer
        if this is None and not self._is_static:
            raise ValueError("Attempt to call instance method in a static context")

        # Check the number of arguments which we were given, this should match
        # the number of type IDs in the argument list of the method
        if len(self._argument_type_ids) != len(args):
            raise ValueError(
                "Wrong number of arguments given, expected %d but had %d" %
                (len(self._argument_type_ids), len(args))
            )

        # Build the argument list for Java
        java_args = b''
        for idx in range(len(args)):
            # Get the argument, and convert it into the appropriate
            # value to send to Java
            try:
                strict     = kwargs.get('strict_types', True)
                argument   = args[idx]
                arg_klass  = self._pjrmi._get_class(self._argument_type_ids[idx])
                java_args += self._pjrmi._format_by_class(arg_klass,
                                                          argument,
                                                          strict_types=strict)
            except (KeyError, ImpreciseRepresentationError) as e:
                raise ValueError(
                    "Failed to handle argument <%s>: %s" % (argument, e)
                )

        # And call it
        return self._pjrmi._call_method(this,
                                        self._klass._type_id,
                                        self._is_ctor,
                                        self._pjrmi._VALUE_FORMAT_REFERENCE,
                                        self._pjrmi.SYNC_MODE_SYNCHRONOUS,
                                        self._details['index'],
                                        java_args)


    def __str__(self):
        return "%s::%s(%s)" % (
            self._klass.__name__,
            self._name,
            ", ".join(self._pjrmi._get_class(type_id)._classname
                      for type_id in self._argument_type_ids)
        )


    def __eq__(self, other):
        return (type(other) is JavaMethod       and
                self._pjrmi   == other._pjrmi   and
                self._details == other._details and
                self._klass   == other._klass   and
                self._this    == other._this)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# The different connection transports are defined here. They should all have the
# following shape:
#
#   connect()     -- Opens the connection; throws exceptions on error.
#   disconnect()  -- Closes the connection, rendering it unusable.
#   send(bytes)   -- Sends the bag or raw bytes completely.
#   recv(count)   -- Receives at most 'count' bytes from the other side. Blocks
#                    until data is available; returns [] upon EOF.
#   __str__()     -- A brief description of the transport (optional).

class SocketTransport:
    """
    An underlying transport for talking to Java, implemented using raw sockets.
    """

    def __init__(self, host, port):
        """
        :param host:
            The host to connect to.
        :param port:
            The port to connect to.
        """

        self._host   = host
        self._port   = int(port)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def __str__(self):
        """
        A brief description of the transport.
        """

        return "%s[%s:%d]" % (self.__class__.__name__, self._host, self._port)


    def connect(self):
        """
        Connect to the server.
        """

        self._socket.connect((self._host, self._port))


    def disconnect(self):
        """
        Close the connection. This renders it unusable.
        """

        try:
            self._socket.close()
        except Exception:
            pass


    def send(self, bytes):
        """
        Send a bag of bytes over the connection.
        """

        self._socket.sendall(bytes)


    def recv(self, count):
        """
        Receive at most 'count' bytes from the connection. This will block until
        data is available and return an empty list on EOF.
        """

        return self._socket.recv(count)


    def is_localhost(self):
        """
        Returns whether we are guaranteed to be on the same host. Might return
        `False` even if we are but never `True` if we are not.
        """
        # Socket transport could happen on the same host, but we'll
        # assume it doesn't for now. We'll look into determining this at a
        # later date.
        return False


class SSLSocketTransport(SocketTransport):
    """
    A version of `SocketTransport` which uses SSL for authentication.
    """

    def __init__(self,
                 host,
                 port,
                 store,
                 password=''):
        """
        :param host:
            The host to connect to.
        :param port:
            The port to connect to.
        """
        from cryptography.hazmat.primitives               import serialization
        from cryptography.hazmat.primitives.serialization import pkcs12

        super().__init__(host, port)

        # Pull in the keystore's raw contents
        if not store:
            raise ValueError("Must specify a keystore filename")
        with open(store, "rb") as fh:
            data = fh.read()

        # Pull the store's contents out of that raw data
        (key, my_cert, other_certs) = \
            pkcs12.load_key_and_certificates(data, password.encode())

        # Get the trusted certificates out of the store
        trusted_certs = "\n".join(
            trusted_cert.public_bytes(serialization.Encoding.PEM).decode()
            for trusted_cert in other_certs
        )

        # Create the SSL context using those trusted certificates. We don't care
        # about the hostname check for our purposes.
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                             cadata=trusted_certs)
        context.check_hostname = False

        # Write out the certs to transient files in a secured directory, so that
        # we may load in the certificate chain from them.
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir, mode=0o700)
        try:
            # Create two files to write the cerficates into so that we can load
            # them into the context. (Use Python3.6-compatible `with` syntax.)
            with tempfile.NamedTemporaryFile(dir=tmpdir, buffering=0) as key_file:
                with tempfile.NamedTemporaryFile(dir=tmpdir, buffering=0) as pem_file:
                    # Write out the certs
                    pem_file.write(
                        my_cert.public_bytes(serialization.Encoding.PEM)
                    )
                    key_file.write(
                        key.private_bytes(
                            serialization.Encoding.PEM,
                            format=serialization.PrivateFormat.TraditionalOpenSSL,
                            encryption_algorithm=serialization.NoEncryption(),
                        )
                    )

                    # And pull them in
                    context.load_cert_chain(certfile=pem_file.name,
                                            keyfile =key_file.name)

            # Wrap the super-class's socket in the context, replacing the reference
            self._socket = context.wrap_socket(self._socket)

        finally:
            # This should not fail since the tempfiles should have been removed.
            # If it does then the above code is likely broken and should be
            # fixed.
            os.rmdir(tmpdir)


class InprocessTransport:
    """
    An underlying transport for talking to a JVM running in the same process.

    This may only be instantiated once per process.

    This should be considered alpha quality for now since it doesn't allow for
    multiple connections to the JVM process, nor does it allow you to connect to
    an existing JVM which you might happen to have. So don't use it for
    production. However, it's handy to use for random debugging.
    """

    def __init__(self,
                 classpath       =(),
                 java_args       =(),
                 application_args=()):
        """
        Create the JVM.

        :param classpath:        A sequence of strings defining the Java classpath.
        :param java_args:        A sequence of arguments to pass to the JVM.
        :param application_args: A sequence of arguments to pass to PJRmi.
        """

        # Always have pjrmi.jar at the end of the classpath so that people can
        # overlay in front of it.
        classpath = tuple(classpath) + (_PJRMI_FATJAR,)

        # If we haven't loaded the extension then these calls will fail with a
        # NameError
        try:
            # Now we can actually create the JVM
            pjrmi.extension.create_jvm(classpath, java_args, application_args)

            # We'll connect right away since we know it exists
            pjrmi.extension.connect()

        except NameError:
            raise ValueError("PJRmi C extension not loaded")

    def __str__(self):
        """
        A brief description of the transport.
        """
        return "In-process pipe"


    def connect(self):
        """
        This is a NOP since we connect in __init__()
        """
        pass


    def disconnect(self):
        """
        Close the connection. This renders it unusable.
        """
        pjrmi.extension.disconnect()


    def send(self, bytes):
        """
        Send a bag of bytes over the connection.
        """
        pjrmi.extension.write(bytes)


    def recv(self, count):
        """
        Receive at most 'count' bytes from the connection. This will block until
        data is available and return an empty list on EOF.
        """
        return pjrmi.extension.read(count)


    def is_localhost(self):
        """
        Returns whether we are guaranteed to be on the same host. Might return
        `False` even if we are but never `True` if we are not.
        """
        return True


class UnixFifoTransport:
    """
    An underlying transport for talking to a JVM which is launched as a new
    process and which sits on the end of a unix pipe.
    """

    def __init__(self,
                 main_class,
                 environment=None, java_executable=None,
                 classpath=(), java_args=(), application_args=(),
                 timeout=60,
                 stdin='/dev/stdin', stdout='/dev/stdout', stderr='/dev/stderr'):
        """
        Create the JVM.

        The stdin, stdout and stderr arguments below may be file handles, file
        numbers or strings that will act as the standard input, standard output,
        and standard error for the child JVM process, respectively. If None is
        passed then the handle is deleted and input/output is suppressed.

        :param environment:      a dict to set the environment with, if any.
        :param java_executable:  the preferred ``java`` executable to use, if any.
        :param classpath:        a sequence of strings defining the Java classpath.
        :param java_args:        a sequence of arguments to pass to the Java command.
        :param application_args: extra arguments to pass to the UnixFifoProvider main method.
        :param timeout:          how long to wait for the child process to connect.
        :param stdin:            the stdin file, or None to delete the handle.
        :param stdout:           the stdout file, or None to delete the handle.
        :param stderr:           the stderr file, or None to delete the handle.
        """

        classpath = tuple(classpath) + (_PJRMI_FATJAR,)

        # Sanity check to make sure that any environment value is a dict. We
        # require this when we exec below.
        if environment:
            environment = dict(environment)
        else:
            environment = {}

        # Set appropriate variables to load shared libraries.
        environment["LD_LIBRARY_PATH"] = "{}:{}".format(
            environment.get("LD_LIBRARY_PATH", ""),
            _PJRMI_SHAREDLIBS_PATH,
        )

        # We need to ensure that we don't clobber any previous values for the
        # java.library.path value, if they have been set. This code could be
        # prettier.
        java_args = list(java_args)
        added     = False
        for (i, arg) in enumerate(java_args):
            # If it looks like we have the property being set then we augment
            # its value
            arg = arg.strip()
            if arg.startswith("-Djava.library.path="):
                java_args[i] = f'{arg}:{_PJRMI_SHAREDLIBS_PATH}'
                added        = True
        if not added:
            # We don't have an existing value, so just append it
            java_args.append(f"-Djava.library.path={_PJRMI_SHAREDLIBS_PATH}")

        # Not yet closed
        self._closed = False

        # The fifo
        self._tmpdir        = tempfile.mkdtemp()
        self._from_fifoname = os.path.join(self._tmpdir, 'java_to_python')
        self._to_fifoname   = os.path.join(self._tmpdir, 'python_to_java')
        os.mkfifo(self._from_fifoname)
        os.mkfifo(self._to_fifoname)

        # Open the pipe for writing, without this the Java process will block
        # when opening its "from" pipe (our "to" one). It's important to use
        # "w+" here so that the open is non-blocking.
        self._to_fifo = open(self._to_fifoname, "w+b", buffering=0)

        # Any file-handles which we need to close when done
        self._filehandles = []

        # How to handle a file action
        def add_file_action(handle, fileno, file_actions):
            if handle is None:
                file_actions.append(lambda: os.close(fileno))
            elif isinstance(handle, int):
                file_actions.append(lambda: os.dup2(handle, fileno))
            elif isinstance(handle, io.IOBase):
                file_actions.append(lambda: os.dup2(handle.fileno(), fileno))
            elif isinstance(handle, str):
                fh = open(handle, 'ab')
                self._filehandles.append(fh)
                file_actions.append(lambda: os.dup2(fh.fileno(), fileno))
            else:
                raise TypeError("'%s' %s was not a file handle or number" %
                                (handle, type(handle)))

        # The file handles
        file_actions = []
        if stdin != '/dev/stdin':
            add_file_action(stdin, 0, file_actions)
        if stdout != '/dev/stdout':
            add_file_action(stdout, 1, file_actions)
        if stderr != '/dev/stderr':
            add_file_action(stderr, 2, file_actions)

        # Grumble if the arguments which we want to be item sequences are not
        # since the user likely didn't intend it
        for (name, arg) in (('application_args', application_args),
                            ('classpath',        classpath),
                            ('java_args',        java_args)):
            if type(arg) not in (list, tuple):
                LOG.warning(
                    "Argument %s was not a list or tuple; this may cause problems",
                    name
                )

        # Build the command which we're going to run and launch the process
        application_args = ((main_class,
                             self._to_fifoname,
                             self._from_fifoname) +
                            tuple(application_args))
        cmd = (("java", ) +
               ("-classpath", ":".join(classpath), ) +
               tuple(java_args) +
               application_args)

        # And spawn it
        pid = os.fork()
        if pid == 0:
            # We are the child. Execution will not escape this if block since we
            # do an exec() below.

            # Handle the file actions
            for action in file_actions:
                action()

            # Any environment to inherit? This will also determine where to look
            # up JAVA_HOME from, if we need to.
            if environment:
                env = dict(os.environ)
                env.update(environment)
                java_home = env.get('JAVA_HOME')
            else:
                env = None
                java_home = os.environ.get('JAVA_HOME')

            # Respect JAVA_HOME for looking up the executable location, if it's
            # in the environment and we don't have a specific executable given
            if java_executable is None:
                if java_home is not None:
                    java_executable = os.path.join(java_home, 'bin', 'java')
                else:
                    java_executable = 'java'

            # Use any given environment
            if env is not None:
                # And exec into the desired process with the new environment
                os.execlpe(java_executable, *(cmd + (env,)))
            else:
                # No environment, just exec directly
                os.execlp(java_executable, *cmd)

            # Since we have exec()'d above, this line is never reached
            assert(False)

        # We are the parent
        self._pid = pid

        # Ensure that we clean up as best we can.  Use a weakref to avoid
        # holding onto a reference to this object in the atexit queue.
        self._exited = False
        exit_lock    = Lock()
        parent_pid   = os.getpid()
        def disconnect_subprocess(selfref=weakref.ref(self)):
            # If we are called in a forked child process, we do not want to
            # have this hook do anything, since the child process can't close
            # our _from_fifo pipe sometimes.
            if os.getpid() != parent_pid:
                return

            self_ = selfref()
            if self_ is None:
                return
            # Ensure that this only happens once
            with exit_lock:
                if not self._exited:
                    self._exited = True
                    self_.disconnect()
        atexit.register(disconnect_subprocess)

        def start(self):
            self._from_fifo = open(self._from_fifoname, "rb")

        starter_thread = Thread(target=start, args=(self,))
        starter_thread.start()
        starter_thread.join(timeout)
        if starter_thread.is_alive():
            # We have timed out trying to connect to our Java subprocess.
            raise RuntimeError("Timed out connecting to Java subprocess")


    def __str__(self):
        """
        A brief description of the transport.
        """
        return "FIFO:" + self._tmpdir


    def connect(self):
        """
        This is a NOP since we connect in __init__()
        """
        pass


    def disconnect(self):
        """
        Close the connection. This renders it unusable.
        """

        if self._closed:
            return

        # Close the pipes. Best effort since a receiver thread might be doing
        # something with them and we could trip over that.
        try:
            self._to_fifo.close()
        except Exception:
            pass
        try:
            self._from_fifo.close()
        except Exception:
            pass

        # And any files we had open
        for handle in self._filehandles:
            # Don't let this cause disconnect to fail
            try:
                handle.close()
            except Exception as e:
                LOG.error("Problem closing %s: %s", handle.name, e)

        # Remove the files etc. in a best-effort fashion. In theory the Java
        # class should be watching these and will termimnate itself.
        try:
            os.remove(self._to_fifoname)
        except Exception:
            pass
        try:
            os.remove(self._from_fifoname)
        except Exception:
            pass
        try:
            os.rmdir (self._tmpdir)
        except Exception:
            pass

        # Kill the child. We do this in a thread so that we don't block the main
        # thread waiting for it.
        class Killer(Thread):
            def run(self_):
                # Junk the Java process, if the watching isn't working
                os.kill(self._pid, signal.SIGTERM)

                # Give it time to die then forcefully kill it
                try:
                    # Wait about 10s to do this
                    until = time.time() + 10
                    while time.time() < until:
                        # "Ping" the process, this will raise an execption if
                        # it's not found etc
                        os.kill(self._pid, 0)
                        time.sleep(1)

                    # If we got here then we need to really kill it
                    os.kill(self._pid, signal.SIGKILL)

                except Exception:
                    # os.kill failed, process must be dead
                    pass

        # Reap the child to collect the exit status; if we don't do this the
        # child will hang around forever as a zombie process. We do this in a
        # thread so that we don't the main thread block on waitpid().
        class Reaper(Thread):
            def run(self_):
                # Keep trying until waitpid() returns gracefully
                while True:
                    try:
                        os.waitpid(self._pid, 0)
                        break
                    except Exception:
                        pass

        # Spawn them
        Killer().start()
        Reaper().start()

        # We're closed now
        self._closed = True


    def send(self, bytes):
        """
        Write a bag of bytes into the FIFO.
        """
        self._to_fifo.write(bytes)
        self._to_fifo.flush()


    def recv(self, count):
        """
        Read at most 'count' bytes from the FIFO. This will block until data is
        available and return an empty list on EOF.
        """
        return self._from_fifo.read(count)


    def is_localhost(self):
        """
        Returns whether we are guaranteed to be on the same host. Might return
        `False` even if we are but never `True` if we are not.
        """
        return True


class StdioTransport:
    """
    An underlying transport for talking to a JVM which has spawned us as a
    sub-process and is talking to us via stdin and stdout.
    """

    # How we say we're ready. We embed some magic hex values (0xFeedBeef) to try
    # to ensure that we don't accidently clash with another hello string.
    _HELLO = b"PYTHON IS READY: %c%c%c%c" % (0xfe, 0xed, 0xbe, 0xef)

    def __init__(self, stdin=None, stdout=None, stderr=None):
        """
        Set up comms via stdio.
        """

        # Whether we have "connected"
        self._connected = False

        # Capture the stdio file handles for ourselves. The Java parent will use
        # stdin and stdout for the transport and redirect stderr to its own
        # logs. Retrieve the underlying binary buffers. We need to be able to
        # read and write binary data.
        self._to   = sys.stdout.buffer
        self._from = sys.stdin .buffer
        self._err  = sys.stderr.buffer

        # Now, handle what to do with the original stdio file handles. By
        # default we junk stdin and redirect stdout to stderr; stderr will be
        # picked up by the parent Java process.
        try:
            if stdin is None:
                sys.stdin = open(os.devnull, 'r')
            else:
                sys.stdin = open(stdin, 'r')

            if stdout is None:
                sys.stdout = sys.stderr
            else:
                sys.stdout = open(stdout, 'a')

            if stderr is not None:
                sys.stderr = open(stderr, 'a')

        except Exception as e:
            # Gripe back to the process and exit
            self._tell_java('Failed to redirect stdio: %s' % e)
            self.disconnect()


    def __str__(self):
        """
        A brief description of the transport.
        """
        return "STDIO"


    def connect(self):
        """
        Send Java the "I am ready" string.
        """

        self._tell_java('Connecting...')
        self._to.write(StdioTransport._HELLO)
        self._to.flush()
        self._connected = True


    def disconnect(self):
        """
        Close the connection. This is achieved by terminating the process.
        """

        self._tell_java('Disconnecting...')
        self._open = False
        sys.exit(0)


    def connected(self):
        """
        Whether we are open and active.
        """
        return self._connected


    def send(self, bytes):
        """
        Write a bag of bytes into the FIFO.
        """
        self._to.write(bytes)
        self._to.flush()


    def recv(self, count):
        """
        Read at most 'count' bytes from the FIFO. This will block until data is
        available and return an empty list on EOF.
        """

        # Watch for stdin closing which means our parent process has gone away
        result = self._from.read(count)
        if result == b'':
            self._connected = False
        return result


    def _tell_java(self, message):
        """
        Send an ASCII message to the Java side via our stderr stream.
        """
        bytes_message = str(message).encode('ascii')
        self._err.write(b"%s\n" % bytes_message)
        self._err.flush()


    def is_localhost(self):
        """
        Returns whether we are guaranteed to be on the same host. Might return
        `False` even if we are but never `True` if we are not.
        """
        return True


class JavaLogHandler(logging.Handler):
    """
    A Logging Handler which echoes to a Java Logger instance.

    Since there is not a one-to-one match between the loggers' levels we only
    approximate them (e.g. Java has finer-grained debugging levels and Python
    has finer-grained severe-like levels).

    Setting the level of this handler will not affect the underlying Java
    Logger's level.
    """

    def __init__(self, rmi, java_logger):
        """
        Instantiate with a PJRmi connection and a Java java.util.logger.Log
        instance.
        """

        # Ensure the base class is configured
        logging.Handler.__init__(self)

        # We don't need a lock for sending stuff to Java
        self.lock = None

        Level = rmi.class_for_name('java.util.logging.Level')

        self._java_logger = java_logger

        # Levels mapping, in order; these are specific to our RMI instance
        self._levels = (
            (_DEEP_DEBUG,   Level.FINEST),
            (logging.DEBUG, Level.FINE),
            (logging.INFO,  Level.INFO),
            (logging.WARN,  Level.WARNING),
            (logging.ERROR, Level.SEVERE)
        )


    def createLock(self):
        """
        No locking is required for us.
        """

        return None


    def emit(self, record):
        """
        Log a given record.
        """

        # Figure out the appropriate Java log level
        for (python_level, java_level) in self._levels:
            if python_level >= record.levelno:
                break

        try:
            if self._java_logger.isLoggable(java_level):
                self._java_logger.log(
                    java_level,
                    "[%s %s:%d %s] %s " % (record.name,
                                           record.filename,
                                           record.lineno,
                                           record.levelname,
                                           record.msg)
                )
        except Exception:
            # Most likely the Java process is dead, nothing to do here
            pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

class _ClassGetter:
    """
    Utility class to allow for simple lookup of Java classes. If it cannot find
    a class then it will assume that what you have given it is a namespace (so
    that one can build up namespaces with multiple instances).

    Given its implementation, this class doesn't guard against typos and you may
    find yourself with a _ClassGetter instance instead of a Java Class.
    """

    def __init__(self, pjrmi, parts):
        self._pjrmi = pjrmi
        self._parts = parts
        self._ClassNotFoundError = pjrmi.class_for_name("java.lang.ClassNotFoundException")
        self._SecurityException  = pjrmi.class_for_name("java.lang.SecurityException")


    def __getattr__(self, k):
        # Build upon ourselves by adding the latest part
        parts = self._parts + (k,)

        # We'll assume any attribute starting with __ potentially corresponds
        # to a special Python function and leave it alone. Calling code
        # accessing those attributes is likely to have particular assumptions
        # about what values they can hold (namely, that they are functions),
        # and we don't want to break those assumptions. This isn't a big
        # limitation since Java namespaces won't start with __ by convention
        # anyway.
        if k.startswith("__"):
            raise AttributeError(k)

        java_classname = ".".join(parts)

        try:
            return self._pjrmi.class_for_name(java_classname)

        except (self._ClassNotFoundError, self._SecurityException):
            # Nothing matched or we weren't allowed. This may be an incomplete
            # name, in which case we'll optimistically try returning a new magic
            # object.
            result = type(self)(self._pjrmi, parts)

        # Cache it.
        self.__dict__[k] = result
        return result


    def __repr__(self):
        return "<Class lookup namespace '%s'>" % '.'.join(self._parts)


class _ContextGuard:
    """
    A way of guarding certain operations in such a way that the guard is removed
    when it's context is entered. The context is only visible to the current
    thread.
    """
    def __init__(self):
        """
        Initialiser for Java objects.
        """
        self.__frozen__ = ThreadLocal()


    def active(self):
        """
        Whether the guard is active or not.
        """
        # A non-zero value means that the guard isn't active; zero means its
        # context has not been entered and it is active
        return getattr(self.__frozen__, 'count', 0) == 0


    def __enter__(self):
        if hasattr(self.__frozen__, 'count'):
            self.__frozen__.count += 1
        else:
            self.__frozen__.count  = 1
        return self


    def __exit__(self, typ, value, traceback):
        self.__frozen__.count -= 1


class _JavaMethod:
    """
    How we call a Java method or constructor.

    Methods may be unbound from a class or instance using []s. The Java classes
    of arguments should be passed into the []s, or ``None`` if there are no
    arguments; an unbounded slice (``:``) can be used when the method is not
    overloaded and no disambiguation is needed.

    As well as the Java arguments, optional kwargs may be passed in::
     o The ``__pjrmi_return_format__`` specifies how the result of a Java method
       call is passed back to the Python side. The default is as a Java object,
       or primitive type, but it may be optionally converted to a Python type
       where possible.
     o The ``__pjrmi_sync_mode__`` defines whether the call is being done
       synchronously or asynchronously; the former will return a result
       directly, the latter returns a Java ``Future`` which will later return
       the result. The result of a ``Future`` can be obtained using the
       ``collect()`` method.

    Any additional kwargs will be passed in a dict as the final argument to the
    method call.
    """
    _NO_ARGS = tuple()

    def __init__(self, klass, function, is_ctor, instance):
        self.__klass__ = klass
        self.__name__  = 'new' if is_ctor else function.__name__
        self.__this__  = instance
        self._is_ctor  = is_ctor
        self._function = function


    def __call__(self, *args, **kwargs):
        if self._is_ctor:
            return self._function(*args, **kwargs)
        else:
            return self._function(self.__this__, *args, **kwargs)


    def __getitem__(self, key):
        """
        Sugar method to allow the user the unbind the method from the class or
        instance.
        """
        klass = self.__klass__
        if self._is_ctor:
            gettor = lambda x: klass._pjrmi_inst.get_constructor(klass, arg_types=x)
        else:
            gettor = lambda x: klass._pjrmi_inst.get_bound_method(self, arg_types=x)

        if key is None:
            # No arguments, represents an empty sequence
            return gettor(self._NO_ARGS)
        elif type(key) is slice or key is Ellipsis:
            # No hinting, just bind to anything
            return gettor(None)
        else:
            # Given something explicit, tupleize it if needbe
            if not type(key) in (tuple, list):
                key = (key,)
            return gettor(key)


    def __str__(self):
        return self.__name__


    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("...")
        else:
            p.text(f"{self.__klass__._classname}::{self.__name__}()")


class _JavaMethodAccessor(property):
    """
    A ``property`` subclass for getting the Java methods on a class. We need
    this indirection to prevent infinite recursion when getting the doc, since
    that method calls back in.
    """
    def __init__(self, function, is_ctor, get_doc):
        self.function = function
        self.is_ctor  = is_ctor
        self.get_doc  = get_doc
        self.__doc__  = None


    def __get__(self, instance, cls):
        # Create a function which will handle this
        f = _JavaMethod(cls, self.function, self.is_ctor, instance)

        # We can now lazily instantiate the documentation. Note that we mustn't
        # evaluate __doc__ right here since that will cause an infinite
        # recursion; it's fine to pass the pointer around though.
        if self.__doc__ is None:
            self.__doc__ = self.get_doc()
        f.__doc__ = self.__doc__

        # And give it back
        return f


class _JavaObject:
    """
    A representation of a Java object instance. We will dynamically create
    subclasses of this object which reflect the Java classes.

    This is a stub class. The constructor documentation is associated with the
    class's `new` method.
    """

    def __del__(self):
        """
        Try to ensure that we drop any reference on the Java side too.
        """
        try:
            self._pjrmi_inst._drop_reference(type(self), self._pjrmi_handle)
        except Exception:
            # Best effort
            pass


    def __eq__(self, that):
        """
        True if this is the same Java object (i.e. it shares the same pointer)
        or equals() returns true.
        """

        if (self is that or
            (isinstance(that, _JavaObject) and self._pjrmi_handle == that._pjrmi_handle and
                                               self._pjrmi_inst  is that._pjrmi_inst)):
            # This is the trivial case; we know these are the same instance
            # since they are either the same Python object or because have the
            # same handle on the Java object (which means they represent the
            # same Java object instance).
            return True
        else:
            # Defer to the Java method call to resolve this
            return self.equals(that)


    def __ne__(self, that):
        """
        The inverse of __eq__()
        """
        return not (self == that)


    def __interactive_display__(self):
        # For Java objects, toString() will usually create the most meaningful
        # output to a human user in an interactive session, so make sure to use
        # that.
        return str(self)


    def __hash__(self):
        """
        We defer to the Java's hash-code in order to ensure that semantics are
        preserved. (E.g. two different instances of a String "Foo" should have
        the same hashCode() result.)
        """

        # Note that you generally do NOT want to cache the result of the call
        # back to Java here since it's possible for the Java object to change
        # and, as such, for the result of hashCode() to change also. This is a
        # subtle gotcha. However, we know that some objects are immutable and
        # it is actually safe to cache their values.
        if self._hash_code is not None:
            return self._hash_code
        else:
            result = hash(self.hashCode())
            if self._is_immutable:
                # It's safe to cache this
                self._hash_code = result
            return result


    def __reduce__(self):
        """
        Java Objects are tighly coupled to the server, none of their data lives in
        the Python process. This means that there's nothing to pickle.
        """

        raise NotImplementedError("PJRmi instances can't be pickled")


    def __setattr__(self, k, v):
        """
        Disallow calling `__setattr__` to create new attributes if the object is
        guarded. Modification is allowed since we assume that people know what
        they're doing once something is created. Also, the properties attributes
        which front the Java object members rely on __setattr__ to work.
        """
        if not hasattr(self, k) and \
           hasattr(self, '_pjrmi_attr_guard') and self._pjrmi_attr_guard.active():
            raise AttributeError(
                "Creation Python attributes outside of the _pjrmi_attr_guard "
                "context is disallowed"
            )
        else:
            object.__setattr__(self, k, v)


    def __delattr__(self, k):
        """
        Disallow calling `__delattr__` if the object is guarded.
        """
        if hasattr(self, '_pjrmi_attr_guard') and self._pjrmi_attr_guard.active():
            raise AttributeError(
                "Deletion Python attributes outside of the _pjrmi_attr_guard "
                "context is disallowed"
            )
        else:
            del self.__dict__[k]



class _JavaLock:
    """
    The way we hold the PJRmi locks. These are (generally) named locks which the
    various python instances use to coordinate between themselves.

    It is safe (and cheap) to acquire this lock multiple times from the same
    Python instance.

    If acquiring the lock would result in deadlock then an exception will be
    thrown, at which point it's up the caller to deal with handling the
    fall-out.

    >> with lock:
    >>     do stuff...
    """

    def __init__(self, rmi, name):
        """
        Instatiate a named lock with a given PJRmi instance.

        :param rmi:  The PJRmi instance this lock is using.
        :param name: The name of this lock.
        """

        # Sanity
        if not isinstance(rmi, PJRmi):
            raise ValueError("Not a PJRmi instance: %s <%s>" % (rmi, type(rmi)))
        if name is None:
            raise ValueError("Given a null lock name")

        # The PJRmi instance which we are wrapping around
        self._pjrmi = rmi

        # How many times we have acquired the lock
        self._count = 0

        # The name of the lock
        self._name = str(name)

        # Since the payload to the send() call will never change we pre-compute
        # and store it here
        self._payload = rmi._format_string(self._name)


    def __enter__(self):
        """
        Acquire the lock.
        """

        # If we don't have it already then acquire it
        if self._count == 0:
            # Acquire the lock, and read back the ACK
            req_id = self._pjrmi._send(PJRmi._LOCK, self._payload)
            self._pjrmi._read_result(req_id)

        # One more, if we got this far
        self._count += 1

        return self


    def __exit__(self, typ, value, traceback):
        """
        Drop the lock.
        """

        # One fewer
        self._count -= 1

        if self._count < 0:
            self._count = 0 # fudge back
            raise ValueError(
                "Lock <%s> was released more times than acquired" % self._name
            )

        # No more holders?
        if self._count == 0:
            # Drop the lock, and read back the ACK
            req_id = self._pjrmi._send(PJRmi._UNLOCK, self._payload)
            self._pjrmi._read_result(req_id)


class _JavaBox:
    """
    A simple class which allows us to dangle a JavaObject instance off it (e.g.
    a String).

    We do this so we have certain Java objects appear as their native Python
    types for users to play with, but so that we don't end up, say, marshalling
    large strings over the wire if they are backed by a String instance on the
    other side.

    This also deals with the case that we might wrongly infer a type for a
    method which takes an Object using a native Python type:

        >> HashMap = c.class_for_name('java.util.HashMap')
        >> Long    = c.class_for_name('java.lang.Long')
        >> m = HashMap()
        >> m.put(Long.valueOf(1), "Foo")
        >> m.get(1)
        [Nothing, since 1 is inferred to be a Byte]
        >> m.get(Long.valueOf(1))
        u'Foo'

    """
    _java_object = None


    @property
    def java_object(self):
        """
        Get back the actual Java object instance associated with this boxed object.
        """
        return self._java_object


    @property
    def python_object(self):
        """
        The native Python representation of this boxed type.

        This must be implemented by subclasses.
        """
        raise NotImplementedError("PJRmi instances can't be pickled")


    def __reduce__(self):
        """
        Java Objects are tighly coupled to the server, none of their data lives in
        the Python process. This means that there's nothing to pickle.
        """
        raise NotImplementedError("PJRmi instances can't be pickled")


    def __accurate_str__(self):
        """
        Method to allow automatic stringification in accurate_cast().
        """
        # Since we desire to render in a Python context we simply stringify the Python
        # version of this object
        return str(self.python_object)


class _JavaString(str, _JavaBox):
    """
    A boxed version of a Java String, which looks like a Python str.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = str.__new__(cls, str(arg))
            else:
                self = str.__new__(cls, raw, encoding='utf_16')
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw string instance.
        """
        return str(self)


    def __accurate_str__(self):
        """
        :see: _JavaBox.__accurate_str__
        """
        # For strings we do this via encode(), rather than str(), so as avoid
        # issues in case where the default system encoding is set to something
        # other than ASCII (say UTF-8).
        return self.python_object.encode('ascii')


class _JavaByte(numpy.int8, _JavaBox):
    """
    A boxed version of a Java Byte, which looks like a Python int.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = numpy.int8.__new__(cls, arg.byteValue())
            else:
                self = numpy.int8.__new__(cls, struct.unpack('!b', raw)[0])
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw int instance.
        """
        return int(self)


class _JavaShort(numpy.int16, _JavaBox):
    """
    A boxed version of a Java Short, which looks like a Python int.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = numpy.int16.__new__(cls, arg.shortValue())
            else:
                self = numpy.int16.__new__(cls, struct.unpack('!h', raw)[0])
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw int instance.
        """
        return int(self)


class _JavaInt(numpy.int32, _JavaBox):
    """
    A boxed version of a Java Integer, which looks like a Python int.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = numpy.int32.__new__(cls, arg.intValue())
            else:
                self = numpy.int32.__new__(cls, struct.unpack('!i', raw)[0])
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw int instance.
        """
        return int(self)


class _JavaLong(numpy.int64, _JavaBox):
    """
    A boxed version of a Java Long, which looks like a Python int.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = numpy.int64.__new__(cls, arg.longValue())
            else:
                self = numpy.int64.__new__(cls, struct.unpack('!q', raw)[0])
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw int instance.
        """
        return struct.unpack('!q', struct.pack('!q', self))[0]


class _JavaFloat(numpy.float32, _JavaBox):
    """
    A boxed version of a Java Float, which looks like a Python float.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = numpy.float32.__new__(cls, arg.floatValue())
            else:
                self = numpy.float32.__new__(cls, struct.unpack('!f', raw)[0])
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw float instance.
        """
        return float(self)


class _JavaDouble(numpy.float64, _JavaBox):
    """
    A boxed version of a Java Double, which looks like a Python float.
    """

    def __new__(cls, arg, raw):
        if isinstance(arg, cls):
            return arg
        else:
            if raw is None:
                self = numpy.float64.__new__(cls, arg.doubleValue())
            else:
                self = numpy.float64.__new__(cls, struct.unpack('!d', raw)[0])
            self._java_object = arg
            return self


    @property
    def python_object(self):
        """
        Get back the value of this object as a new raw float instance.
        """
        return float(self)
