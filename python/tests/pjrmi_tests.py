from   numpy      import arange, int16, int32, int64, float32, float64
from   plumbum    import local
from   textwrap   import dedent
from   threading  import Thread
from   unittest   import TestCase

import gc
import numpy
import os
import pjrmi
import signal
import subprocess
import sys
import tempfile
import time
import weakref


_pjrmi_connection = None


class MyType:
    def __init__(self, value):
        self.value = value


class MyPJRmi(pjrmi.PJRmi):
    """
    Special subclass of PJRmi which handles turning a `MyType` into a String
    or a Collection.
    """
    def connect(self):
        super().connect()
        self._java_util_ArrayList = self.class_for_name('java.util.ArrayList')


    def _format_by_class(self, klass, value,
                         strict_types=True, allow_format_shmdata=True):
        """
        An override of the superclass method that knows how to handle special
        conversions of MyType instances.
        """
        try:
            # Try the normal method first
            return super()._format_by_class(klass, value,
                                            strict_types=strict_types,
                                            allow_format_shmdata=allow_format_shmdata)
        except Exception:
            # Handle conversion of our special type
            if isinstance(value, MyType):
                if klass._type_id == self._java_lang_String._type_id:
                    # Test creating a String using the String.valueOf() method
                    method = self._java_lang_String.valueOf[self._java_lang_Object]
                    return super()._format_as_lambda(method, value.value,
                                                     strict_types=strict_types,
                                                     allow_format_shmdata=allow_format_shmdata)
                elif klass._type_id == self._java_util_Collection._type_id:
                    # Test creating a Collection using the ArrayList CTOR
                    method = self._java_util_ArrayList.new[self._java_util_Collection]
                    return super()._format_as_lambda(method, [value.value],
                                                     strict_types=strict_types,
                                                     allow_format_shmdata=allow_format_shmdata)

            # If we got here then the handling above didn't catch it, so
            # re-raise the original exception as if nothing special had happened
            raise


class Function(pjrmi.JavaProxyBase):
    """
    A Python interface which implements the Java C{Function} class.
    """
    def __init__(self, function):
        super().__init__()
        self._function = function


    def apply(self, t):
        """
        The Java C{Function#apply()} method implementation.
        """
        return self._function(t)


def get_pjrmi():
    # Return a PJRmi connection that's shared across unit tests.
    global _pjrmi_connection
    if _pjrmi_connection is not None:
        return _pjrmi_connection

    # Allowlist classes needed for testing as we're setting
    # block_non_allowlisted_classes=true below.
    # Note: this list is only for testing related classes, any classes needed
    # by PJRmi production code must be added to DEFAULT_CLASS_NAME_ALLOWLIST in
    # PJRmi.java so that PJRmi is not broken when class blocking is on.
    additional_allowlisted_classes = ','.join([
        'com.deshaw.pjrmi.JniPJRmi',
        'com.deshaw.pjrmi.PJRmi$WrappedArrayLike',
        'com.deshaw.pjrmi.PJRmiAsType',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$A',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$B',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$C',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$MorePrecedenceMethods',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$OneTwoThreeIterator',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$OneTwoThrowIterator',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$OverriddenMethodsDerived',
        'com.deshaw.pjrmi.test.PJRmiTestHelpers$PrecedenceMethods',
        'com.deshaw.python.NumpyArray',
        'com.deshaw.python.PythonUnpickle',
        'java.lang.ProcessHandle',
        'java.util.ArrayList',
        'java.util.Arrays',
        'java.util.HashMap',
        'java.util.HashSet',
        'java.util.Objects',
        'java.util.concurrent.Future',
    ])

    # Connect to PJRmi
    _pjrmi_connection = pjrmi.connect_to_child_jvm(
        # Turning class blocking on but allowing class injection does reduce
        # the fidelity of these tests compared to production but it's at least
        # better than not having class blocking.
        application_args=("num_workers=2",
                          "block_non_allowlisted_classes=true",
                          "additional_allowlisted_classes=%s" % additional_allowlisted_classes,
                          "allow_class_injection=true"),
        stdin=None, stdout=None, stderr=None, use_shm_arg_passing=True, impl=MyPJRmi
    )
    return _pjrmi_connection


def helper_class_for_name(classname):
    # Give back the inner class of PJRmiTestHelpers for the given name
    full_name = 'com.deshaw.pjrmi.test.PJRmiTestHelpers$%s' % classname
    return get_pjrmi().class_for_name(full_name)


def cleanup():
    global _pjrmi_connection
    if _pjrmi_connection is not None:
        _pjrmi_connection.disconnect()
        _pjrmi_connection = None


def index_of(element, elements):
    for (i, e) in enumerate(elements):
        if element == e:
            return i
    return -1


class TestPJRmi(TestCase):
    """
    Unit tests for pjrmi.
    """

    def test_equals(self):
        """
        Equals should work as expected.
        """

        Object = get_pjrmi().class_for_name('java.lang.Object')

        o1 = Object()
        o2 = Object()

        self.assertEqual(o1,       o1 )
        self.assertTrue (o1 ==     o1 )
        self.assertFalse(o1 !=     o1 )
        self.assertTrue (o1.equals(o1))

        self.assertNotEqual(o1,       o2 )
        self.assertFalse   (o1 ==     o2 )
        self.assertTrue    (o1 !=     o2 )
        self.assertFalse   (o1.equals(o2))


    def test_read_write(self):
        """
        Test the (_private) methods which do reading and writing.
        """

        v1    = 'string'
        v2, i = get_pjrmi()._read_ascii(get_pjrmi()._format_string(v1), 0)
        self.assertEqual(v1, v2)

        # Chars are sent as UTF-16 with the high byte first
        v1    = 'c'
        v2, i = get_pjrmi()._read_char(b'\x00c', 0)
        self.assertEqual(v1, v2)

        v1    = 'string'
        v2, i = get_pjrmi()._read_utf16(get_pjrmi()._format_utf16(v1), 0)
        self.assertEqual(v1, v2)

        v1    = 1.0
        v2, i = get_pjrmi()._read_float(get_pjrmi()._format_float(v1), 0)
        self.assertEqual(v1, v2)

        v1    = 1234.5678
        v2, i = get_pjrmi()._read_double(get_pjrmi()._format_double(v1), 0)
        self.assertEqual(v1, v2)

        v1    = 123
        v2, i = get_pjrmi()._read_int8(get_pjrmi()._format_int8(v1), 0)
        self.assertEqual(v1, v2)

        v1    = 1234
        v2, i = get_pjrmi()._read_int16(get_pjrmi()._format_int16(v1), 0)
        self.assertEqual(v1, v2)

        v1    = 12356
        v2, i = get_pjrmi()._read_int32(get_pjrmi()._format_int32(v1), 0)
        self.assertEqual(v1, v2)

        v1    = 123567890123
        v2, i = get_pjrmi()._read_int64(get_pjrmi()._format_int64(v1), 0)
        self.assertEqual(v1, v2)

        v1    = True
        v2, i = get_pjrmi()._read_boolean(get_pjrmi()._format_boolean(v1), 0)
        self.assertEqual(v1, v2)

        v1    = arange(10, dtype='byte')
        v2, i = get_pjrmi()._read_byte_array(get_pjrmi()._format_int32(len(v1)) +
                                             get_pjrmi()._format_array(v1, 'byte'), 0)
        self.assertTrue(numpy.array_equal(v1, numpy.array(bytearray(v2), dtype='byte')))


    def test_members(self):
        """
        Ensure we can access the static members
        """

        Boolean = get_pjrmi().class_for_name('java.lang.Boolean')
        Integer = get_pjrmi().class_for_name('java.lang.Integer')

        self.assertTrue (Boolean.TRUE)
        self.assertFalse(Boolean.FALSE)

        self.assertTrue(Integer.MAX_VALUE > 0)
        self.assertTrue(Integer.MIN_VALUE < 0)


    def test_boxing(self):
        """
        Ensure that boxing to Python native types works as expected.
        """

        Byte    = get_pjrmi().class_for_name('java.lang.Byte')
        Short   = get_pjrmi().class_for_name('java.lang.Short')
        Integer = get_pjrmi().class_for_name('java.lang.Integer')
        Long    = get_pjrmi().class_for_name('java.lang.Long')
        Float   = get_pjrmi().class_for_name('java.lang.Float')
        Double  = get_pjrmi().class_for_name('java.lang.Double')
        String  = get_pjrmi().class_for_name('java.lang.String')

        for i in range(10):
            j = i + 10

            byte_i = Byte.valueOf(i)
            byte_j = Byte.valueOf(j)
            self.assertEqual(i + j, byte_i + byte_j)
            self.assertEqual(i,     byte_i.python_object)
            self.assertEqual(j,     byte_j.python_object)

            short_i = Short.valueOf(i)
            short_j = Short.valueOf(j)
            self.assertEqual(i + j, short_i + short_j)
            self.assertEqual(i,     short_i.python_object)
            self.assertEqual(j,     short_j.python_object)

            int_i = Integer.valueOf(i)
            int_j = Integer.valueOf(j)
            self.assertEqual(i + j, int_i + int_j)
            self.assertEqual(i,     int_i.python_object)
            self.assertEqual(j,     int_j.python_object)

            long_i = Long.valueOf(i)
            long_j = Long.valueOf(j)
            self.assertEqual(i + j, long_i + long_j)
            self.assertEqual(i,     long_i.python_object)
            self.assertEqual(j,     long_j.python_object)

            float_i = Float.valueOf(i)
            float_j = Float.valueOf(j)
            self.assertEqual(i + j, float_i + float_j)
            self.assertEqual(i,     float_i.python_object)
            self.assertEqual(j,     float_j.python_object)

            double_i = Double.valueOf(i)
            double_j = Double.valueOf(j)
            self.assertEqual(i + j, double_i + double_j)
            self.assertEqual(i,     double_i.python_object)
            self.assertEqual(j,     double_j.python_object)

        s_p = "Foo"
        s_j = String.valueOf(s_p)
        self.assertEqual(s_p, s_j)
        self.assertEqual(s_p, s_j.python_object)


    def test_strings(self):
        """
        Ensure that strings behave as expected.
        """

        PJRmiTestHelpers = get_pjrmi().class_for_name('com.deshaw.pjrmi.test.PJRmiTestHelpers')
        Character        = get_pjrmi().class_for_name('java.lang.Character')
        String           = get_pjrmi().class_for_name('java.lang.String')

        # String handling can be tricky when unicode comes into play.
        english_chars = 'hello world'
        chinese_chars = '\u4f60\u597d'
        symbol_chars  = '\N{yen sign}\N{pound sign}\N{copyright sign}'

        numbers = [str(i) for i in range(10)]

        # Strings and chars should marshal back and forth correctly, even if
        # they contain unicode characters.
        for chars in (english_chars, chinese_chars, symbol_chars):
            self.assertEqual(chars,               String.valueOf(chars))
            self.assertEqual(chars.join(numbers), String.join(chars, numbers))
            self.assertEqual(chars[0],            String.valueOf(chars)[0])
            self.assertEqual(chars[1],            Character.valueOf(chars[1]).charValue())

            # A string should not be treated as a Collection
            with self.assertRaises(TypeError):
                PJRmiTestHelpers.collectionSize(chars)

            # When passed a a method which takes an Object[] it should do what
            # all other objects do, and get wrapped into a singleton array.
            self.assertEqual(1, PJRmiTestHelpers.objectArrayLength(chars))

        # Ensure that treating a string as a byte[] works as expected (or not,
        # if we have unicode in there)
        self.assertEqual(len(english_chars),
                         PJRmiTestHelpers.byteArrayLength(english_chars))
        with self.assertRaises(TypeError):
            PJRmiTestHelpers.byteArrayLength(chinese_chars)


    def test_hashing(self):
        """
        Hash codes and hash tables (both Python and Java) should work as expected.
        """

        HashMap = get_pjrmi().class_for_name('java.util.HashMap')
        Object  = get_pjrmi().class_for_name('java.lang.Object')
        Integer = get_pjrmi().class_for_name('java.lang.Integer')

        m = HashMap()
        d = dict()

        for value in range(100):
            i = Integer.valueOf(value)
            o = Object()
            m.put(i, o)
            d[i] = o

        for value in range(100):
            i = Integer.valueOf(value)
            o = m.get(i)
            self.assertNotEqual(o, None)
            self.assertEqual   (o, d[i])

        # Test pass-by-value of dicts
        m = HashMap()
        m.putAll(d)
        for (key, value) in d.items():
            self.assertEqual(m.get(key), value)

        # Test passing a dict to Java and creating a HashMap and
        # creating a Python dict from a hashmap.
        d = dict(foo='bar', guas='rocks')
        m = HashMap(d)
        self.assertEqual(get_pjrmi().value_of(m, compress=True), d)
        self.assertEqual(get_pjrmi().value_of(m, compress=False), d)


    def test_collections(self):
        """
        Collections, iterables and arrays.
        """

        Arrays    = get_pjrmi().class_for_name('java.util.Arrays')
        ArrayList = get_pjrmi().class_for_name('java.util.ArrayList')

        # A python list
        r = range(10)

        # Create and populate a Java List
        l = ArrayList()
        for i in r:
            l.add(i)

        # Turn it into a Java array
        a = l.toArray()

        # Sizes should match
        self.assertEqual(len(r), len(l))
        self.assertEqual(len(r), len(a))

        # Iteration should match
        self.assertEqual(tuple(r), tuple(l))
        self.assertEqual(tuple(r), tuple(a))

        # Passing an iterable as a Java array should work
        self.assertTrue(l.equals(Arrays.asList(r)))

        # Array access
        for i in r:
            self.assertEqual(a[i], i)

        # Check that we can turn the Java array back into our original
        # Python list using both compressed and uncompressed formats.
        self.assertEqual(get_pjrmi().value_of(a, compress=True), list(r))
        self.assertEqual(get_pjrmi().value_of(a, compress=False), list(r))


    def test_number_truncation(self):
        """
        Ensure that float truncation works
        """

        Byte  = get_pjrmi().class_for_name('java.lang.Byte')
        Float = get_pjrmi().class_for_name('java.lang.Float')

        # These should not throw any exceptions
        Float.valueOf(0.1234567890123456789)
        Byte .valueOf(12)

        # This should
        try:
            Byte.valueOf(1234)
            self.fail("Should have raised an exception")
        except ValueError:
            pass


    def test_overridden_method_calls(self):
        """
        Ensure that we call the correct overridden method, based on return type.
        """
        OverriddenMethodsDerived = helper_class_for_name('OverriddenMethodsDerived')
        String                   = get_pjrmi().class_for_name('java.lang.String')

        # Ensure that we get a String. If we would have called the base class
        # method we would get a CharSequence.
        val = OverriddenMethodsDerived().getValue()
        self.assertTrue(get_pjrmi().is_instance_of(val, String))
        self.assertEqual('DERIVED', val)


    def test_overloaded_method_calls(self):
        """
        Ensure that we call the correct overloaded methods.
        """
        # Classes
        A                     = helper_class_for_name('A')
        B                     = helper_class_for_name('B')
        C                     = helper_class_for_name('C')
        PrecedenceMethods     = helper_class_for_name('PrecedenceMethods')
        MorePrecedenceMethods = helper_class_for_name('MorePrecedenceMethods')
        Object                = get_pjrmi().class_for_name('java.lang.Object')
        String                = get_pjrmi().class_for_name('java.lang.String')

        # Instances
        a   = A()
        b   = B()
        c   = C()
        pm  = PrecedenceMethods()
        mpm = MorePrecedenceMethods()
        s   = int16(1)
        i   = int32(1)
        l   = int64(1)
        f   = float32(1)
        d   = float64(1)

        # These should all just call the appropriate constructor
        self.assertEqual(pm                     .ctor, 'v')
        self.assertEqual(PrecedenceMethods(a)   .ctor, 'a')
        self.assertEqual(PrecedenceMethods(b)   .ctor, 'b')
        self.assertEqual(PrecedenceMethods(c)   .ctor, 'c')
        self.assertEqual(PrecedenceMethods(s)   .ctor, 's')
        self.assertEqual(PrecedenceMethods(i)   .ctor, 'i')
        self.assertEqual(PrecedenceMethods(l)   .ctor, 'l')
        self.assertEqual(PrecedenceMethods(f)   .ctor, 'f')
        self.assertEqual(PrecedenceMethods(1.0) .ctor, 'f')
        self.assertEqual(PrecedenceMethods(d)   .ctor, 'd')
        self.assertEqual(PrecedenceMethods(a, a).ctor, 'aa')
        self.assertEqual(PrecedenceMethods(a, b).ctor, 'ab')
        self.assertEqual(PrecedenceMethods(b, a).ctor, 'ba')

        # And the same for the equivalent methods
        self.assertEqual(pm.f(a),    'cs_f_a')
        self.assertEqual(pm.f(b),    'cs_f_b')
        self.assertEqual(pm.f(c),    'cs_f_c')
        self.assertEqual(pm.f(1.0),  'cs_f_f')
        self.assertEqual(pm.f(f),    'cs_f_f')
        self.assertEqual(pm.f(d),    'cs_f_d')
        self.assertEqual(pm.f(s),    'cs_f_s')
        self.assertEqual(pm.f(i),    'cs_f_i')
        self.assertEqual(pm.f(l),    'cs_f_l')
        self.assertEqual(pm.g(a),    'cs_g_a')
        self.assertEqual(pm.g(b),    'cs_g_b')
        self.assertEqual(pm.g(c),    'cs_g_c')
        self.assertEqual(pm.g(1.0),  'cs_g_f')
        self.assertEqual(pm.g(f),    'cs_g_f')
        self.assertEqual(pm.g(d),    'cs_g_d')
        self.assertEqual(pm.g(s),    'cs_g_s')
        self.assertEqual(pm.g(i),    'cs_g_i')
        self.assertEqual(pm.g(l),    'cs_g_l')
        self.assertEqual(pm.f(a, a), 'cs_f_aa')
        self.assertEqual(pm.f(a, b), 'cs_f_ab')
        self.assertEqual(pm.f(b, a), 'cs_f_ba')

        # This should call the overridden version
        self.assertEqual(mpm.f(a, a), 's_f_aa')

        # We expect this call to fail since it should bind to both f(A,B) and
        # f(B,A), which is ambiguous
        try:
            pm.f(c, c)
            self.fail("Call to PrecedenceMethods.f(c, c) should have failed")
        except TypeError:
            # Good
            pass

        # Also ambiguous, for similar reasons
        try:
            pm.f(1, 1, 1)
            self.fail("Call to PrecedenceMethods.f(1, 1, 1) should have failed")
        except TypeError:
            # Good
            pass

        # This should be fine
        self.assertEqual(pm.f(1, 's', 1), 'cs_f_isn')

        # Ensure that we bind to the most specific method when it's in a set
        # where some are incomparable. First we make sure that the methods are
        # in the order which we expect, in order for that test to be useful.
        # This is because this test relies on the fact that the less specific
        # methods will be found first by the Python code.
        klass   = MorePrecedenceMethods.getClass()
        cs_f_so = klass.getMethod('f', (String.getClass(), Object.getClass()))
        cs_f_os = klass.getMethod('f', (Object.getClass(), String.getClass()))
        cs_f_ss = klass.getMethod('f', (String.getClass(), String.getClass()))
        methods = klass.getMethods()

        # Now see which methods come first by grabbing their indices in the
        # array of methods
        cs_f_so_index = index_of(cs_f_so, methods)
        cs_f_os_index = index_of(cs_f_os, methods)
        cs_f_ss_index = index_of(cs_f_ss, methods)

        # Ensure that the most specific method comes last. If these checks fail
        # then it's not the end of the world but it means that the assert which
        # they protect is vacuous. It seems that this can vary from run to run
        # so we don't assert it being true for now.
        if cs_f_so_index < cs_f_ss_index and \
           cs_f_os_index < cs_f_ss_index:
            # It's okay to check the binding works
            self.assertEqual(mpm.f('s', 's'), 'cs_f_ss')

        # Check to see when strict typing gets enforced. This should only happen
        # for overloaded methods.
        self.assertEqual(pm.ff(l), "cs_ff_i")
        try:
            pm.gg(l)
            self.fail("Binding to PrecedenceMethods.gg(int) should have failed")
        except TypeError:
            # Good
            pass


    def test_chatter_for_overloaded_method_calls(self):
        """
        Ensure we have minimal chatter to find the correct overloaded methods.
        """
        pjrmi = get_pjrmi()
        send = pjrmi._send
        send_calls_count = 0

        def instrumented_send(*args, **kwargs):
            nonlocal send_calls_count
            send_calls_count += 1
            return send(*args, **kwargs)

        pjrmi._send = instrumented_send

        PrecedenceMethods = helper_class_for_name('PrecedenceMethods')
        Object            = pjrmi.class_for_name('java.lang.Object')
        Integer           = pjrmi.class_for_name('java.lang.Integer')

        pm = PrecedenceMethods()
        o = Object()
        i = Integer(42)

        before = send_calls_count
        assert before > 0, "instrumented_send doesn't seem to be working"
        self.assertEqual(pm.f(o, i, i), 'cs_f_oni')
        # As of January 2023, 12 calls are made without the _LazyTypeError
        # optimization and only 7 with it. This is admittedly somewhat of a
        # brittle assertion, but OTOH, we should strive to make as fewer calls
        # as possible so it's probably worth the tradeoff.
        self.assertTrue((send_calls_count - before) < 8)


    def test_method_not_found(self):
        """
        Ensure we have consistent error reporting with _LazyTypeError.
        """
        PrecedenceMethods = helper_class_for_name('PrecedenceMethods')
        Object = get_pjrmi().class_for_name('java.lang.Object')

        pm = PrecedenceMethods()
        o = Object()

        with self.assertRaises(TypeError) as e:
            pm.f(o, o, o)
        self.assertTrue(
            str(e.exception).startswith('Could not find a method matching'))


    def test_connection_is_freed(self):
        """
        Test that a disconnected PJRmi instance is freed.

        We are guarding against an issue on Py2 where PJRmi instances are not
        freed because they define __del__ and are part of reference cycles.
        """
        conn = pjrmi.connect_to_child_jvm(stdin=None, stdout=None, stderr=None)
        ref = weakref.ref(conn)
        conn.disconnect()
        conn = None

        # Let all the PJRmi helper threads die in the background. This might
        # take a "while" if the machine is loaded.
        until = time.time() + 5
        while ref() is not None and time.time() < until:
            time.sleep(0.2)
            gc.collect()
        self.assertIsNone(ref())


    def test_object_method_redeclation(self):
        """
        Make sure that putting Object's methods into an interface doesn't break
        things.
        """
        # Calling the method is sufficient to ensure that this works
        PJRmiTestHelpers = get_pjrmi().class_for_name('com.deshaw.pjrmi.test.PJRmiTestHelpers')
        obj = PJRmiTestHelpers.getObjectInterface()
        obj.hashCode()


    def test_proxies(self):
        """
        Make sure that proxy classes work.
        """
        String  = get_pjrmi().class_for_name('java.lang.String')
        HashMap = get_pjrmi().class_for_name('java.util.HashMap')

        # Our Python function and two Python proxy class instances which wrap it
        def add_one(value):
            return value + 1
        proxy1 = Function(add_one)
        proxy2 = Function(add_one)

        # Ensure that calling equals() on two proxies works as expected
        self.assertEqual   (proxy1, proxy1)
        self.assertEqual   (proxy2, proxy2)
        self.assertNotEqual(proxy1, proxy2)
        self.assertNotEqual(proxy2, proxy1)

        # Make sure they work on the Python side
        self.assertEqual(add_one     (1), 2)
        self.assertEqual(proxy1.apply(1), 2)

        # Now make sure that things work on the Java side. Create a Map which we
        # are going to call a method on, where that method takes an interface.
        hash_map = HashMap()
        hash_map.put(1, 2)

        # Now use the function directly and via the proxy interface
        self.assertEqual(hash_map.computeIfAbsent(2, add_one), 3)
        self.assertEqual(hash_map.computeIfAbsent(2, add_one), 3)
        self.assertEqual(hash_map.computeIfAbsent(3, proxy1 ), 4)
        self.assertEqual(hash_map.computeIfAbsent(3, proxy1 ), 4)

        # This should render as a String. Note that String.valueOf() takes an
        # Object here; we are testing that as well as the fact that proxy's
        # toString() method calls str() on the Java side.
        python_string = str           (proxy1)
        java_string   = String.valueOf(proxy1)
        self.assertEqual(java_string, python_string)


    def test_forked_process_cleanup(self):
        """
        Test that if the Python process forks, the child process gets shutdown
        properly.

        Indirectly, this verifies that the child process does not do connection
        cleanup because if the child process tries to do this cleanup, it would
        get stuck while trying to close some of our resources.
        """
        def _kill_alive_process(pid):
            # Return False if the pid is already dead, otherwise kill the given
            # pid and return True.
            try:
                os.kill(pid, signal.SIGKILL)
                return True
            except OSError:
                return False

        command = os.path.join(os.path.dirname(__file__),
                               'forked_process_cleanup.py')
        tmp_file = tempfile.mktemp()
        test_process = subprocess.Popen([command, tmp_file],
                                        stdout=subprocess.PIPE)

        # We will consider this test as passed if all the processes are dead
        # in some time. Otherwise fail the test and cleanup all the processes by
        # ourselves. Also we need to wait for the temporary file to exist. It
        # should be created by the script we launched after setting up the PJRmi
        # connection and forking the Python child.
        timeout = 10
        while timeout > 0 and not os.path.exists(tmp_file):
            time.sleep(1)
            timeout -= 1

        # If the file now exists, then get the child PIDs else something is
        # wrong. We will handle the None values later.
        try:
            with open(tmp_file) as fh:
                child_python_pid, java_pid = \
                    (int(pid) for pid in fh.read().split(" "))
        except IOError:
            child_python_pid, java_pid = (None, None)

        # Now if we have found the file, we will wait for 1 second to let the
        # processes cleanup.
        if child_python_pid:
            time.sleep(1)

        # Need to poll the parent process, so that it gets cleared from defunct
        # state, if it has exited.
        test_process.poll()
        parent_pid = test_process.pid
        alive_processes = []
        unborn_processes = []
        for (pid, info) in ((java_pid,         "Java child process"),
                            (parent_pid,       "Python parent process"),
                            (child_python_pid, "Forked child Python process")):
            if pid is None:
                unborn_processes.append(info)
            elif _kill_alive_process(pid):
                alive_processes.append(info)

        # We can remove the tmp_file now.
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

        # Finally, assert that all processes were started and died fine.
        self.assertEqual(unborn_processes,
                          [],
                          ", ".join(unborn_processes) + " did not start.")
        self.assertEqual(alive_processes,
                          [],
                          ", ".join(alive_processes) + " did not die cleanly.")


    def test_inject_source(self):
        """
        Make sure that `:func:inject_source` works.
        """
        class_name = "TestInjectSource"
        source     = """
public class TestInjectSource {
    public static int foo(int i) {
        return i+1;
    }
}
"""

        class_foo1 = get_pjrmi().inject_source(class_name, source)
        class_foo2 = get_pjrmi().class_for_name(class_name)

        instance1  = class_foo1()
        instance2  = class_foo2()

        self.assertEqual(instance1.foo(1), 2)
        self.assertEqual(instance2.foo(1), 2)


    def test_parameter_names(self):
        """
        Make sure that we are receiving parameter names of java methods in
        python.
        """

        # Create a java class using injecting source technique.
        class_name = "TestParameterNames"
        source = """
                public class TestParameterNames {
                    public int add(int i, int j) {
                        return i + j;
                    }
                }
                """

        test_param_names_class = get_pjrmi().inject_source(class_name, source)

        # Get the method parameter names and check if they are equal to the
        # parameter names defined above in the injected source code.
        add_method_param_names = \
            test_param_names_class._methods['add'][0]['parameter_names']

        self.assertEqual(add_method_param_names[0], "i")
        self.assertEqual(add_method_param_names[1], "j")

        # Now, test the functionality by using an already defined class.

        # Get the PJRmiTestHelpers class.
        pjrmi_test_helpers_class = \
            get_pjrmi().class_for_name("com.deshaw.pjrmi.test.PJRmiTestHelpers")

        # Use a pre-defined 'add' method added specifically for this testing in
        # the PJRmiTestHelpers class.
        pre_defined_add_method_param_names = \
            pjrmi_test_helpers_class._methods['add'][0]['parameter_names']

        # Check if the parameter names received for the 'add' method are 'num1'
        # and 'num2' respectively.
        self.assertEqual(pre_defined_add_method_param_names[0], "num1")
        self.assertEqual(pre_defined_add_method_param_names[1], "num2")


    def test_native_array(self):
        """
        Make sure that various functions that use native array handling work.

        Current tested functionality:
        - value_of()
        - format_by_class() handling of numpy arrays
        - Native functions in pjrmi/extension.C
        """
        # We'll declare some variables and classes here that will be used
        # frequently for each test.

        PJRmiTestHelpers = get_pjrmi().class_for_name('com.deshaw.pjrmi.test.PJRmiTestHelpers')
        Arrays = get_pjrmi().javaclass.java.util.Arrays

        # Python handles on Java arrays that will be used:
        java_bool_array_class   = get_pjrmi().class_for_name('[Z')
        java_byte_array_class   = get_pjrmi().class_for_name('[B')
        java_short_array_class  = get_pjrmi().class_for_name('[S')
        java_int_array_class    = get_pjrmi().class_for_name('[I')
        java_long_array_class   = get_pjrmi().class_for_name('[J')
        java_float_array_class  = get_pjrmi().class_for_name('[F')
        java_double_array_class = get_pjrmi().class_for_name('[D')
        java_string_array_class = get_pjrmi().class_for_name('[Ljava.lang.String;')

        # We initialize one array of each Java array type with some values
        java_empty_array = java_bool_array_class(0)
        java_empty_array_length = 0

        java_bool_array = java_bool_array_class(3)
        java_bool_array_length = 3
        java_bool_array[0] = True
        java_bool_array[1] = False
        java_bool_array[2] = True

        java_byte_array = java_byte_array_class(2)
        java_byte_array_length = 2
        java_byte_array[0] = 2
        java_byte_array[1] = 4

        java_short_array = java_short_array_class(3)
        java_short_array_length = 3
        java_short_array[0] = 5
        java_short_array[1] = 4
        java_short_array[2] = 3

        java_int_array = java_int_array_class(3)
        java_int_array_length = 3
        java_int_array[0] = -7
        java_int_array[1] = 4
        java_int_array[2] = 3

        java_long_array = java_long_array_class(3)
        java_long_array_length = 3
        java_long_array[0] = 1
        java_long_array[1] = 2
        java_long_array[2] = 3

        java_float_array = java_float_array_class(3)
        java_float_array_length = 3
        java_float_array[0] = -7.1
        java_float_array[1] = 4.2
        java_float_array[2] = 3.3

        java_double_array = java_double_array_class(4)
        java_double_array_length = 4
        java_double_array[0] = 1.2
        java_double_array[1] = 3.4
        java_double_array[2] = 5.67
        java_double_array[3] = 8.9

        java_string_array = java_string_array_class(2)
        java_string_array_length = 2
        java_string_array[0] = "string"
        java_string_array[1] = "fun"

        # We also initialize numpy arrays with the same types and values
        python_empty_array  = numpy.full(0, True)
        python_bool_array   = numpy.array([True, False, True])
        python_byte_array   = numpy.array([2, 4],                dtype='b')
        python_short_array  = numpy.array([5, 4, 3],             dtype='h')
        python_int_array    = numpy.array([-7, 4, 3],            dtype='i')
        python_long_array   = numpy.array([1, 2, 3],             dtype='l')
        python_float_array  = numpy.array([-7.1, 4.2, 3.3],      dtype='f')
        python_double_array = numpy.array([1.2, 3.4, 5.67, 8.9], dtype='d')
        python_string_array = numpy.array(["string", "fun"])

        python_bytes        = python_byte_array.tobytes()

        def test_native_array_value_of():
            """
            Make sure that value_of() works with native array handling.
            """
            # We will create java arrays and render them to python with the
            # value_of() function.
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_empty_array) ==
                              python_empty_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_bool_array) ==
                              python_bool_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_byte_array) ==
                              python_byte_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_short_array) ==
                              python_short_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_int_array) ==
                              python_int_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_long_array) ==
                              python_long_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_float_array) ==
                              python_float_array))
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_double_array) ==
                              python_double_array))

            # Although we cannot natively handle string arrays, we check to
            # make sure we aren't breaking anything
            self.assertTrue(numpy.all(get_pjrmi().value_of(java_string_array) ==
                                      python_string_array))


        def test_native_array_format_numpy():
            """
            Make sure that _format_by_class() works with native
            array handling of one-dimensional numpy arrays.

            This calls Java functions with Object parameters as well as Java
            functions with primitive array (i.e. int[]) parameters.
            """
            # Call helper methods which take a Java Object.
            # These check whether the length of the same array is the same in
            # Java and Python.
            self.assertEqual(PJRmiTestHelpers.objectLength(python_empty_array),
                              len(python_empty_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_bool_array),
                              len(python_bool_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_byte_array),
                              len(python_byte_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_short_array),
                              len(python_short_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_int_array),
                              len(python_int_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_long_array),
                              len(python_long_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_float_array),
                              len(python_float_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_double_array),
                              len(python_double_array))
            self.assertEqual(PJRmiTestHelpers.objectLength(python_string_array),
                              len(python_string_array))

            # Call helper methods which take a Java primitive array
            # parameters and give them numpy arrays of the corresponding type.
            # These check whether the length of the same array is the same in
            # Java and Python.
            self.assertEqual(PJRmiTestHelpers.booleanArrayLength(python_empty_array),
                              len(python_empty_array))
            self.assertEqual(PJRmiTestHelpers.booleanArrayLength(python_bool_array),
                              len(python_bool_array))
            self.assertEqual(PJRmiTestHelpers.byteArrayLength   (python_byte_array),
                              len(python_byte_array))
            self.assertEqual(PJRmiTestHelpers.shortArrayLength  (python_short_array),
                              len(python_short_array))
            self.assertEqual(PJRmiTestHelpers.intArrayLength    (python_int_array),
                              len(python_int_array))
            self.assertEqual(PJRmiTestHelpers.longArrayLength   (python_long_array),
                              len(python_long_array))
            self.assertEqual(PJRmiTestHelpers.floatArrayLength  (python_float_array),
                              len(python_float_array))
            self.assertEqual(PJRmiTestHelpers.doubleArrayLength (python_double_array),
                              len(python_double_array))
            self.assertEqual(PJRmiTestHelpers.byteArrayLength   (python_bytes),
                              len(python_bytes))

            # These helper methods return a reference to a Java array. We call
            # these now because we know from the above that we can successfully
            # format numpy arrays to primitive arrays.
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.booleanArrayIdentity(python_empty_array),
                                          java_empty_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.booleanArrayIdentity(python_bool_array),
                                          java_bool_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.byteArrayIdentity   (python_byte_array),
                                          java_byte_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.shortArrayIdentity  (python_short_array),
                                          java_short_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.intArrayIdentity    (python_int_array),
                                          java_int_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.longArrayIdentity   (python_long_array),
                                          java_long_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.floatArrayIdentity  (python_float_array),
                                          java_float_array))
            self.assertTrue(Arrays.equals(PJRmiTestHelpers.doubleArrayIdentity (python_double_array),
                                          java_double_array))


        def test_native_array_extension():
            """
            Make sure that the JniPJRmi Java class and the extension code
            that use the PJRmi library in src/lib/pjrmi/ work.
            """
            # Our JniPJRmi class
            JniPJRmi = get_pjrmi().javaclass.com.deshaw.pjrmi.JniPJRmi

            # We will use the numpy and Java arrays above and then use the
            # extension code to read and write the arrays to a memory-mapped
            # file. We expect that we can write an array to memory using Java
            # and read it back with the extension code and vice versa, yielding
            # the same array each time.

            ###################################################################
            # Python write -> Java read

            # Method to test extension writing with Native reading
            # input_array:  numpy array
            # input_type:   type of array
            # java class:   pjrmi handle on Java array class of type input_type
            def python_to_java(input_array, input_type, java_class):
                input_length = len(input_array)

                # Write array using extension
                write_info = pjrmi.extension.write_array(input_array)

                # Check the returned parameters
                self.assertEqual(3, len(write_info))
                write_filename = write_info[0]
                write_length = write_info[1]
                write_type = write_info[2].decode()
                self.assertEqual(input_length, write_length)
                self.assertEqual(input_type, write_type)

                # Read array using Native class
                java_output = JniPJRmi.readArray(write_filename,
                                                    write_length,
                                                    write_type)

                # Cast the Java object to a numpy array
                python_result = get_pjrmi().cast_to(java_output, java_class)

                # Check that the two arrays are equal
                self.assertEqual(len(input_array), len(python_result))
                for i in range(input_length):
                    self.assertEqual(input_array[i], python_result[i])

            python_to_java(python_empty_array,  'z', java_bool_array_class)
            python_to_java(python_bool_array,   'z', java_bool_array_class)
            python_to_java(python_byte_array,   'b', java_byte_array_class)
            python_to_java(python_short_array,  's', java_short_array_class)
            python_to_java(python_int_array,    'i', java_int_array_class)
            python_to_java(python_long_array,   'j', java_long_array_class)
            python_to_java(python_float_array,  'f', java_float_array_class)
            python_to_java(python_double_array, 'd', java_double_array_class)

            ###################################################################
            # Java write -> Python read

            # Method to test Native writing with extension reading
            # input_array:  Java array
            # input_length: number of elements
            # input_type:   type of array
            def java_to_python(input_array, input_length, input_type):
                # Write array using Native class
                info = JniPJRmi.writeArray(input_array)

                # Check the returned parameters
                self.assertEqual(info.numElems, input_length)
                self.assertEqual(info.type, input_type)

                # Read array using extension
                output = pjrmi.extension.read_array(info.filename,
                                                    info.numElems,
                                                    info.type.encode('utf-8'))

                # Check that the two arrays are equal
                self.assertEqual(len(input_array), len(output))
                for i in range(len(input_array)):
                    self.assertEqual(input_array[i], output[i])

            java_to_python(java_empty_array,  java_empty_array_length,  'z')
            java_to_python(java_bool_array,   java_bool_array_length,   'z')
            java_to_python(java_byte_array,   java_byte_array_length,   'b')
            java_to_python(java_short_array,  java_short_array_length,  's')
            java_to_python(java_int_array,    java_int_array_length,    'i')
            java_to_python(java_long_array,   java_long_array_length,   'j')
            java_to_python(java_float_array,  java_float_array_length,  'f')
            java_to_python(java_double_array, java_double_array_length, 'd')

        test_native_array_value_of()
        test_native_array_format_numpy()
        test_native_array_extension()


    def test_method_handles(self):
        """
        Check that it works to unbind a method from a Java class and use it
        accordingly.
        """
        # We'll look at the toString method on Integers
        Integer = get_pjrmi().class_for_name('java.lang.Integer')
        HashMap = get_pjrmi().class_for_name('java.util.HashMap')
        Objects = get_pjrmi().class_for_name('java.util.Objects')

        # Get an Integer instance. Handle the fact that it's a boxed type
        integer = Integer.valueOf(12345678).java_object

        # Get the bound and unbound versions of the method. We can represent "no
        # arguments" as an empty sequence or None.
        bound   = integer.toString[None]
        unbound = Integer.toString[None]
        self.assertEqual(bound,   integer.toString[[]])
        self.assertEqual(unbound, Integer.toString[[]])

        # Attempt to invoke the bound one, this should give us back the expected
        # string
        self.assertEqual(integer.toString(), bound())

        # Use the unbound method to get values from a map
        hm1 = HashMap()
        number = 87654321
        value = hm1.computeIfAbsent(number, unbound)
        self.assertEqual(str(number), value)

        # And try the same for a constructor
        ctor = HashMap.new[None]
        self.assertEqual(ctor, HashMap.new[[]])

        # Make a map directly, and via a supplier call. Make sure that they are
        # the same.
        hm2 = HashMap()
        hm3 = Objects.requireNonNullElseGet(None, ctor)
        self.assertEqual(hm2, hm3)


    def test_can_format_shmdata(self):
        """
        Make sure that we can use shmdata passing when appropriate.

        This test ensures that shmdata support is not accidentally removed.
        Users do not and should not need to call `_can_format_shmdata``
        typically.
        """
        java_bool_array_class   = get_pjrmi().class_for_name('[Z')
        java_byte_array_class   = get_pjrmi().class_for_name('[B')
        java_short_array_class  = get_pjrmi().class_for_name('[S')
        java_int_array_class    = get_pjrmi().class_for_name('[I')
        java_long_array_class   = get_pjrmi().class_for_name('[J')
        java_float_array_class  = get_pjrmi().class_for_name('[F')
        java_double_array_class = get_pjrmi().class_for_name('[D')
        java_object_class       = get_pjrmi().class_for_name('java.lang.Object')
        java_object_array_class = get_pjrmi().class_for_name('[Ljava.lang.Object;')

        python_bool_array   = numpy.array([True])
        python_byte_array   = numpy.array([2],    dtype='b')
        python_short_array  = numpy.array([5],    dtype='h')
        python_int_array    = numpy.array([-7],   dtype='i')
        python_long_array   = numpy.array([1],    dtype='l')
        python_float_array  = numpy.array([-3.4], dtype='f')
        python_double_array = numpy.array([1.2],  dtype='d')

        self.assertTrue(get_pjrmi()._can_format_shmdata(python_bool_array,
                                                        java_bool_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_byte_array,
                                                        java_byte_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_short_array,
                                                        java_short_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_int_array,
                                                        java_int_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_long_array,
                                                        java_long_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_float_array,
                                                        java_float_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_double_array,
                                                        java_double_array_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_double_array,
                                                        java_object_class))
        self.assertTrue(get_pjrmi()._can_format_shmdata(python_double_array,
                                                        java_object_array_class))

        # We can't format for different types
        self.assertFalse(get_pjrmi()._can_format_shmdata(python_float_array,
                                                         java_double_array_class))

        # Some classes we currently cannot format; the below tests make sure we
        # don't try to use shmdata passing with them
        LLdouble                = get_pjrmi().class_for_name('[[D')
        java_string_array_class = get_pjrmi().class_for_name('[Ljava.lang.String;')

        python_string_array = numpy.array(["string"])

        self.assertFalse(get_pjrmi()._can_format_shmdata(python_double_array,
                                                         LLdouble))
        self.assertFalse(get_pjrmi()._can_format_shmdata(python_string_array,
                                                         java_double_array_class))
        self.assertFalse(get_pjrmi()._can_format_shmdata(python_string_array,
                                                         java_string_array_class))
        self.assertFalse(get_pjrmi()._can_format_shmdata(python_double_array,
                                                         java_string_array_class))


    def test_arraylike(self):
        """
        Kick the tires on ArrayLike operations.
        """
        # We'll wrap a double[][] and play with it
        Ldouble          = get_pjrmi().class_for_name('[D')
        LLdouble         = get_pjrmi().class_for_name('[[D')
        WrappedArrayLike = get_pjrmi().class_for_name('com.deshaw.pjrmi.PJRmi$WrappedArrayLike')

        # Create and populate the double[][]. This is a bit of a clunky way of
        # doing it but it is simple and works.
        dim = 3
        array2d = LLdouble(dim)
        for i in range(dim):
            array1d = Ldouble(dim)
            for j in range(dim):
                array1d[j] = i * dim + j
            array2d[i] = array1d
        wrapped = WrappedArrayLike(array2d)

        # Now try accessing it using both composite and individual keys
        for i in range(dim):
            for j in range(dim):
                self.assertEqual(array2d[i][j], wrapped[i][j])
                self.assertEqual(array2d[i][j], wrapped[i, j])


    def test_iterators(self):
        """
        Ensure that we handle iterators (and especially) exceptions from
        Iterators (NoSuchElementException vs something else) correctly.
        """

        OneTwoThreeIterator = helper_class_for_name('OneTwoThreeIterator')
        l1 = list(OneTwoThreeIterator())
        # OneTwoThreeIterator throws a NoSuchElementException at the end but
        # that should be handled gracefully by PJRmi and not propagated.
        self.assertEqual(l1, [1, 2, 3])

        ArrayList = get_pjrmi().class_for_name('java.util.ArrayList')
        Object = get_pjrmi().class_for_name('java.lang.Object')
        l2 = ArrayList()
        OneTwoThreeIterator().forEachRemaining(l2.add[Object])
        # Make sure how we read Iterators in PJRmi matches with how Java does.
        self.assertEqual(l1, list(l2))

        OneTwoThrowIterator = helper_class_for_name('OneTwoThrowIterator')
        i = iter(OneTwoThrowIterator())
        self.assertEqual(next(i), 1)
        self.assertEqual(next(i), 2)
        # OneTwoThrowIterator throws a UnsupportedOperationException at the end,
        # which should _not_ should be handled by PJRmi but propagated.
        with self.assertRaises(pjrmi.JavaException):
            next(i)


    def test_extended_types(self):
        """
        Ensure that the handling of extended types works.
        """
        Integer = get_pjrmi().class_for_name('java.lang.Integer')
        HashSet = get_pjrmi().class_for_name('java.util.HashSet')

        # The MyPJRmi subclass should know to turn MyType into a String and a
        # singleton Collection for methods which need those types as arguments.
        test_type = MyType(12345678)

        # Test calling a method and a CTOR
        self.assertEqual(Integer.valueOf(test_type),
                         test_type.value)
        self.assertEqual(tuple(HashSet(test_type)),
                         tuple((test_type.value,)))


    @classmethod
    def tearDownClass(cls):
        # Disconnect the connection (which terminates the subprocess).  If we
        # don't do this here it will still be done atexit, but we may as well
        # do it sooner since the connection is no longer needed.  There may be
        # many more tests to run.  Cleaning up earlier minimizes the risk that
        # a later hard kill prevents us from cleaning up the subprocess.
        cleanup()


class TestBecomePJRmiMinion(TestCase):
    """
    Tests for become_pjrmi_minion.
    """
    def _test_smoke(self):
        """
        Smoke test.
        """
        # Guard against the subprocess hanging.
        def run():
            p = local[sys.executable]["-c", dedent("""\
                from pjrmi import become_pjrmi_minion
                become_pjrmi_minion()
            """)].popen()
            stdout, stderr = p.communicate()
            self.assertRegex(stderr, br'^Connecting...\n')
            # We get other connection information after the initial handshake
            # string so only look up until what we expect
            expect = pjrmi.StdioTransport._HELLO + pjrmi.PJRmi._HELLO
            self.assertEqual(stdout[:len(expect)], expect)

        # Spawn it
        thread = Thread(target=run)
        thread.is_daemon = True
        thread.start()

        # Wait for it to run, this should happen _relatively_ quickly
        thread.join(timeout=15)

        # It should be done
        if thread.is_alive:
            self.fail("Failed to create a minion")
