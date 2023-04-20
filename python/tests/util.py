from   pjrmi      import _util
from   unittest   import TestCase

import numpy

class TestStrict(TestCase):
    """
    Unit tests for ``_util.strict_foo()``.
    """

    def test_strict_bool(self):
        # These should work
        self.assertEqual(_util.strict_bool(True ), True)
        self.assertEqual(_util.strict_bool(False), False)
        for i in (0, 1):
            self.assertEqual(_util.strict_bool(i), bool(i))

        # These should not
        for i in (5, 'a', None):
            try:
                _util.strict_bool(i)
                self.fail(f"strict_bool({i}) should have failed")
            except (TypeError,ValueError):
                pass


    def test_strict_int(self):
        # These should work
        for i in range(-5, 6):
            self.assertEqual(_util.strict_int(    i ), i)
            self.assertEqual(_util.strict_int(str(i)), i)

        # These should not
        for i in ('a', None, 1.1, numpy.nan):
            try:
                _util.strict_int(i)
                self.fail(f"strict_int({i}) should have failed")
            except (TypeError, ValueError):
                pass


    def test_strict_number(self):
        types = (numpy.int8,
                 numpy.int16,
                 numpy.int32,
                 numpy.int64,
                 numpy.float32,
                 numpy.float64)

        # These should work
        for i in range(-50, 50):
            for typ in types:
                v = typ(i / 10.0)
                self.assertEqual(_util.strict_number(typ, v), v)

        # These should not because of types
        for i in ('a', None):
            for typ in types:
                try:
                    _util.strict_number(typ, i)
                    self.fail(f"strict_number({typ}, {i}) should have failed")
                except (TypeError, ValueError):
                    pass

        # These should not because of limits of representation
        for (typ, v) in ((numpy.int8,    int(1e3)),
                         (numpy.int8,    1.1),
                         (numpy.int8,    numpy.nan),
                         (numpy.int16,   int(1e6)),
                         (numpy.int16,   1.1),
                         (numpy.int16,   numpy.nan),
                         (numpy.int32,   int(1e12)),
                         (numpy.int32,   1.1),
                         (numpy.int32,   numpy.nan),
                         (numpy.int64,   1.1),
                         (numpy.int64,   numpy.nan),
                         (numpy.float32, 123456789.0)):
            try:
                _util.strict_number(typ, v)
                self.fail(f"strict_number({typ}, {v}) should have failed")
            except (TypeError, ValueError):
                pass


    def test_strict_array(self):
        # What to test with
        bytez    = numpy.array((1,    2,    3   ), dtype=numpy.byte   )
        int8s    = numpy.array((1e1,  2e1,  3e1 ), dtype=numpy.int8   )
        int16s   = numpy.array((1e2,  2e2,  3e2 ), dtype=numpy.int16  )
        int32s   = numpy.array((1e4,  2e4,  3e4 ), dtype=numpy.int32  )
        int64s   = numpy.array((1e12, 2e12, 3e12), dtype=numpy.int64  )
        float32s = numpy.array((1.1,  2.2,  3.3 ), dtype=numpy.float32)
        float64s = numpy.array((1.1,  2.2,  3.3 ), dtype=numpy.float64)
        strings  = numpy.array(('a',  'b',  'c' ))
        nones    = numpy.array((None, None, None))

        # These should work. This includes up-casting to a wider type.
        _util.strict_array(numpy.byte,  bytez )
        self.assertTrue(numpy.all(_util.strict_array(numpy.byte,  bytez) == bytez))
        for typ in (numpy.int8, numpy.int16, numpy.int32, numpy.int64,
                    numpy.float32, numpy.float64):
            self.assertTrue(numpy.all(_util.strict_array(typ,  int8s) == int8s))
        for typ in (numpy.int16, numpy.int32, numpy.int64,
                    numpy.float32, numpy.float64):
            self.assertTrue(numpy.all(_util.strict_array(typ, int16s) == int16s))
        for typ in (numpy.int32, numpy.int64,
                    numpy.float32, numpy.float64):
            self.assertTrue(numpy.all(_util.strict_array(typ, int32s) == int32s))
        for typ in (numpy.int64,
                    numpy.float64): # numpy.float32 isn't rich enough here
            self.assertTrue(numpy.all(_util.strict_array(typ, int64s) == int64s))
        for typ in (numpy.float32, numpy.float64):
            self.assertTrue(numpy.all(_util.strict_array(typ, float32s) == float32s))
        self.assertTrue(numpy.all(_util.strict_array(numpy.float64, float64s) == float64s))

        # These should not because of types
        for a in (strings, nones):
            for typ in (numpy.int8, numpy.int16, numpy.int32, numpy.int64,
                        numpy.float32, numpy.float64):
                try:
                    _util.strict_array(typ, a)
                    self.fail(f"strict_array({typ}, {a}) should have failed")
                except (TypeError, ValueError):
                    pass

        # These should not because of limits of representation
        for (typ, a) in ((numpy.int8,    float32s),
                         (numpy.int16,   float32s),
                         (numpy.int32,   float32s),
                         (numpy.int64,   float32s),
                         (numpy.float32, int64s)):
            try:
                _util.strict_array(type, a)
                self.fail(f"strict_array({typ}, {a}) should have failed")
            except (TypeError, ValueError):
                pass
