import numpy
import pickle

from   tests.pjrmi_tests  import get_pjrmi
from   unittest           import TestCase

def send_object_to_java(obj):
    """
    Pickle an object in Python and unpickle it in Java.
    """
    # Only pickle versions [0, 2] are supported in com.deshaw.python.PythonUnpickle
    pickle_as_bytestring = pickle.dumps(obj, protocol=2)
    def unsigned_byte_to_signed_byte(x):
        # PythonUnpickle uses byte[], and Java's byte is signed.
        return (x + 128) % 256 - 128
    pickle_as_list_of_bytes = [
        unsigned_byte_to_signed_byte(x)
        for x in pickle_as_bytestring
    ]

    pjrmi = get_pjrmi()
    PythonUnpickle = pjrmi.class_for_name("com.deshaw.python.PythonUnpickle")
    return PythonUnpickle.loadPickle(pickle_as_list_of_bytes)


class TestJavaPythonUnpickle(TestCase):
    """
    These units tests exercise the Java code in ``com.deshaw.python.PythonUnpickle``.
    In principle they are more like unit tests for the Java code rather than the
    PJRmi module, but because we want to drive the tests from Python, it is more
    practical to add them under PJRmi's tests.
    """

    def test_unicode_string(self):
        """
        Test Java's unpickling of a Unicode string.
        """
        hello_world = send_object_to_java(u"Hello World")
        self.assertTrue(hello_world == "Hello World")


    def test_byte_string(self):
        """
        Test Java's unpickling of a byte string.
        """
        hello_world = send_object_to_java(b"Hello World")
        self.assertTrue(hello_world == "Hello World")


    def test_list_integer(self):
        """
        Test Java's unpickling of various lists of integers.
        """
        for test_list in [
                [-1, 1], # A list containing a BININT and a BININT1
                [128],   # A list containing a BININT1, unsigned matters.
                [32768], # A list containing a BININT2, unsigned matters.
                list(numpy.arange(131072)) # A long list of numpy.int64.
        ]:
            resulting_list = send_object_to_java(test_list)
            self.assertTrue(str(test_list) == resulting_list.toString())


    def test_numpy_array_integer(self):
        """
        Test Java's unpickling of various numpy arrays of integers.
        """
        pjrmi = get_pjrmi()
        NumpyArray = pjrmi.class_for_name("com.deshaw.python.NumpyArray")
        for (start_ix, stop_ix) in [
                ( 0,  0),  # an empty array
                ( 0,  5),  # a short array
                ( 0, 129), # an array whose length is a BININT1, unsigned matters
                (-1, 257), # a longer array, including a negative value.
        ]:
            test_array = numpy.arange(start_ix, stop_ix)
            resulting_array = send_object_to_java(test_array)
            resulting_array = pjrmi.cast_to(resulting_array, NumpyArray)
            resulting_int_array = pjrmi.cast_to(resulting_array.toIntArray(),
                                                pjrmi._L_java_lang_int)
            self.assertEqual(test_array.tolist(), list(resulting_int_array))
