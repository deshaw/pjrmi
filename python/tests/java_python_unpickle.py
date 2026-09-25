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
            # As of NumPy 2 a numpy scalar reprs as np.int64(0) rather than 0,
            # so we stringify a plain-int copy of the list to compare against
            # what Java renders. Otherwise the expected string reads:
            #   [np.int64(0), np.int64(1), np.int64(2), ...]
            resulting_list = send_object_to_java(test_list)
            self.assertEqual(str(list(map(int, test_list))),
                             resulting_list.toString())


    def test_wide_integers(self):
        """
        Test Java's unpickling of integers which are too big for a BININT.
        """
        # Python writes these as a LONG1, using the fewest bytes which hold
        # the value, so each of these arrives with a different byte count and
        # the ones with a high bit set in their top byte are negative.
        for value in (2**31, -2**31 - 1,
                      2**39, -2**39,
                      2**47, -2**47,
                      2**63 - 1, -2**63):
            self.assertEqual(str(value), str(send_object_to_java(value)))


    def test_integer_too_wide_is_refused(self):
        """
        Test that Java refuses an integer which will not fit in a long.

        Python integers are unbounded, so this is reachable from any peer.
        """
        with self.assertRaises(Exception) as context:
            send_object_to_java(2**80)
        self.assertIn("Unsupported LONG1 size", str(context.exception))


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
