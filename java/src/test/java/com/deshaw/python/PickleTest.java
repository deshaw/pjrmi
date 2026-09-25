package com.deshaw.python;

import com.deshaw.python.DType;
import com.deshaw.python.NumpyArray;
import com.deshaw.python.PythonPickle;
import com.deshaw.python.PythonUnpickle;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verify the operation of the Python picking code.
 */
public class PickleTest
{
    /**
     * The protocol 2 header which every raw stream below opens with.
     */
    private static final String PROTO2 = "\u0080\u0002";

    /*
     * Empty arrays
     */
    private static final int[]    EMPTY_INT    = new int   [0];
    private static final long[]   EMPTY_LONG   = new long  [0];
    private static final double[] EMPTY_DOUBLE = new double[0];


    /**
     * Objects of various primitive types.
     */
    private static final Object[] OBJECTS = {
        null,
        true, false,
        -100.0f, 0.0f, 100.0f,
        -1000.0, 0.0, 1000.0,
        -10000000, 10000000,
        -10000000000L, 10000000000L,
        "hello", "world", ""
    };

    // ----------------------------------------------------------------------

    /**
     * Test pickling and unpickling of primitives.
     */
    @Test
    public void testPrimitives()
        throws Exception
    {
        for (Object object : OBJECTS) {
            doInOut(object);
        }
    }

    /**
     * Test arrays.
     */
    @Test
    public void testArrays()
        throws Exception
    {
        doInOut(new int[]    {     -10000000,     10000000 });
        doInOut(new long[]   { -10000000000L, 10000000000L });
        doInOut(new double[] {       -1000.0,       1000.0 });
    }

    /**
     * Test a map.
     */
    @Test
    public void testMap()
        throws Exception
    {
        final Map<Object,Object> map = new HashMap<>();
        for (Object object : OBJECTS) {
            if (object != null) {
                map.put(object, object);
            }
        }
        doInOut(map);
    }

    /**
     * {@code numpy.frombuffer} takes the data and the descriptor, and nothing
     * else. The complaint names the global that was called and states the
     * order the two are expected in.
     */
    @Test
    public void testFrombufferRejectsBadArguments()
        throws Exception
    {
        final String complaint =
            "Invalid arguments passed to numpy.frombuffer: " +
            "expecting 2-tuple (data, dtype)";

        // The argument is not a tuple at all:
        //   PROTO 2; GLOBAL numpy frombuffer; BININT1 1; REDUCE; STOP
        assertMalformed(complaint,
                        PROTO2 + "cnumpy\nfrombuffer\n" +
                        "K\u0001" + "R" + ".");

        // The argument is a tuple of the wrong size:
        //   PROTO 2; GLOBAL numpy frombuffer; BININT1 1; TUPLE1; REDUCE; STOP
        assertMalformed(complaint,
                        PROTO2 + "cnumpy\nfrombuffer\n" +
                        "K\u0001" + "\u0085" + "R" + ".");
    }

    /**
     * A byte which is not an opcode at all is named, rather than being left to
     * trip over a null further in.
     */
    @Test
    public void testUnknownOpcodeIsRejected()
    {
        assertMalformed("Unknown opcode 0xff", PROTO2 + "\u00ff" + ".");
    }

    /**
     * Python integers are unbounded, so a stream can carry one too wide for a
     * {@code long}. That is refused rather than truncated.
     */
    @Test
    public void testWideLong1IsRejected()
    {
        // PROTO 2; LONG1 whose length byte is 13 ('\r'); 13 bytes; STOP
        assertMalformed("Unsupported LONG1 size 13",
                        PROTO2 + "\u008a\r" + "\u0001".repeat(13) + ".");
    }

    /**
     * Python writes the fewest bytes which hold the value, so most
     * {@code LONG1} streams carry fewer than eight and the missing high bytes
     * come from the sign of the last one. Every integer between the
     * {@code int} and {@code long} bounds arrives this way, so refusing the
     * narrow forms would reject ordinary Python output.
     */
    @Test
    public void testNarrowLong1IsRead()
        throws Exception
    {
        // Two bytes, little-endian, giving 0x0100
        Assertions.assertEquals(
            256L,
            PythonUnpickle.loadPickle(
                bytes(PROTO2 + "\u008a\u0002\u0000\u0001" + ".")
            )
        );

        // One byte with its high bit set, so the value is negative
        Assertions.assertEquals(
            -1L,
            PythonUnpickle.loadPickle(
                bytes(PROTO2 + "\u008a\u0001\u00ff" + ".")
            )
        );

        // No bytes at all, which is the value zero
        Assertions.assertEquals(
            0L,
            PythonUnpickle.loadPickle(
                bytes(PROTO2 + "\u008a\u0000" + ".")
            )
        );
    }

    /**
     * A dict is built from alternating keys and values, so an odd count is
     * malformed and is said to be, rather than the last entry being dropped.
     */
    @Test
    public void testOddItemCountsAreRejected()
    {
        // PROTO 2; MARK; BININT1 1; DICT; STOP
        assertMalformed("Odd number of items for DICT",
                        PROTO2 + "(" + "K\u0001" + "d" + ".");

        // PROTO 2; EMPTY_DICT; MARK; BININT1 1; SETITEMS; STOP
        assertMalformed("Odd number of items for SETITEMS",
                        PROTO2 + "}" + "(" + "K\u0001" + "u" + ".");
    }

    /**
     * The {@code NONE} opcode puts a real null on the stack, so the opcodes
     * which hand a stack entry to a global say so themselves rather than
     * leaving each global to find out.
     */
    @Test
    public void testNullsPassedToGlobalsAreRejected()
    {
        // PROTO 2; GLOBAL numpy dtype; NONE; REDUCE; STOP
        assertMalformed("Null argument tuple passed to numpy.dtype",
                        PROTO2 + "cnumpy\ndtype\n" + "N" + "R" + ".");

        // Build a dtype and then hand BUILD a null state:
        //   ...; SHORT_BINSTRING f8; TUPLE1; REDUCE; NONE; BUILD; STOP
        assertMalformed("Null state passed to __setstate__()",
                        PROTO2 + "cnumpy\ndtype\n" +
                        "U\u0002f8" + "\u0085" + "R" + "N" + "b" + ".");
    }

    /**
     * {@code numpy.dtype} needs its descriptor, so an empty argument tuple is
     * refused instead of being indexed off the end of.
     */
    @Test
    public void testDtypeWithNoArgumentsIsRejected()
    {
        // PROTO 2; GLOBAL numpy dtype; EMPTY_TUPLE; REDUCE; STOP
        assertMalformed("Expected at least 1 argument to dtype",
                        PROTO2 + "cnumpy\ndtype\n" + ")" + "R" + ".");
    }

    /**
     * A length is whatever the stream said it was, so a negative one is
     * refused before it reaches the read.
     */
    @Test
    public void testNegativeLengthIsRejected()
    {
        // PROTO 2; BINSTRING whose length reads as -1; STOP
        assertMalformed("Negative length in the stream: -1",
                        PROTO2 + "T\u00ff\u00ff\u00ff\u00ff" + ".");
    }

    /**
     * numpy renamed the multiarray module, so both spellings are registered.
     * Each global reports the module it was reached through, that name being
     * the only clue a reader gets as to which of the two a stream used.
     */
    @Test
    public void testNumpyGlobalsRenderTheirOwnModule()
    {
        for (String module : new String[] { "numpy.core.multiarray",
                                            "numpy._core.multiarray" }) {
            // A one-element tuple is the wrong shape for scalar(), and the
            // complaint names the global which refused it.
            //   PROTO 2; GLOBAL <module> scalar; BININT1 1; TUPLE1; REDUCE; STOP
            assertMalformed(
                "Invalid arguments passed to " + module + ".scalar()",
                PROTO2 + "c" + module + "\nscalar\n" +
                "K\u0001" + "\u0085" + "R" + "."
            );
        }
    }

    /**
     * A pickle which fails part way through must leave the pickler fit to be
     * used again.
     *
     * <p>A pickle refers to a value it has already written by its position in
     * the memo, so every one of those positions has to have been written by
     * the same stream which refers to it. An object which a failed pickle
     * memoised is not in the next pickle's stream, and a back-reference to it
     * makes a pickle which is only found to be broken when it is read.
     */
    @Test
    public void testPicklerRecoversFromAFailedPickle()
        throws Exception
    {
        final PythonPickle pickle = new PythonPickle();

        // A value which the failing pickle gets as far as writing, and so
        // memoises, before it meets something it cannot handle
        final List<String> shared = Arrays.asList("alpha", "beta");

        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> pickle.toByteArray(Arrays.asList(shared, new Object())),
            "A plain Object should not have been picklable"
        );

        // The next pickle has to stand alone, so it must write the value out
        // again rather than pointing at where the failed pickle put it
        Assertions.assertEquals(
            shared,
            PythonUnpickle.loadPickle(pickle.toByteArray(shared)),
            "the pickle taken after a failed one"
        );

        // And the pickler is otherwise none the worse for it
        Assertions.assertEquals(
            "hello",
            PythonUnpickle.loadPickle(pickle.toByteArray("hello")),
            "a later pickle"
        );
    }

    // ----------------------------------------------------------------------

    /**
     * Turn a pickle stream, given as a string in which each character stands
     * for one byte, into the bytes it denotes.
     */
    private static byte[] bytes(final String stream)
    {
        return stream.getBytes(StandardCharsets.ISO_8859_1);
    }

    /**
     * Assert that a pickle stream, given as a string in which each character
     * stands for one byte, is rejected with a complaint containing the given
     * text.
     */
    private void assertMalformed(final String expected, final String stream)
    {
        final MalformedPickleException e = Assertions.assertThrows(
            MalformedPickleException.class,
            () -> PythonUnpickle.loadPickle(bytes(stream)),
            "Should have been rejected"
        );
        Assertions.assertTrue(
            e.getMessage() != null && e.getMessage().contains(expected),
            "Wrong complaint: " + e.getMessage()
        );
    }

    /**
     * Actually test some pickling.
     */
    private void doInOut(Object in)
        throws Exception
    {
        final PythonPickle pickle = new PythonPickle();
        final Object out = PythonUnpickle.loadPickle(pickle.toByteArray(in));
        Assertions.assertTrue(equals(in, out),
                              "IN[" + describe(in) + "] != OUT[" + describe(out) + "]");
    }

    /**
     * Whether two things are equal.
     */
    private boolean equals(final Object in, final Object out)
    {
        // Null check
        if (in == null && out == null) {
            return true;
        }
        if (in == null && out != null ||
            in != null && out == null)
        {
            return false;
        }

        // Unwrap arrays
        final boolean isArray = in.getClass().isArray();
        if (isArray) {
            // Should be a numpy array
            if (!(out instanceof NumpyArray)) {
                return false;
            }
            final NumpyArray array = (NumpyArray)out;

            // Check size
            final int length = array.size();
            if (length != Array.getLength(in)) {
                return false;
            }

            if (in.getClass().equals(EMPTY_INT.getClass())) {
                if (DType.Type.INT32 != array.dtype().type()) {
                    return false;
                }
                for (int i=0; i < length; i++) {
                    if (array.getInt(i) != Array.getInt(in, i)) {
                        return false;
                    }
                }
                return true;
            }
            else if (in.getClass().equals(EMPTY_LONG.getClass())) {
                if (DType.Type.INT64 != array.dtype().type()) {
                    return false;
                }
                for (int i=0; i < length; i++) {
                    if (array.getLong(i) != Array.getLong(in, i)) {
                        return false;
                    }
                }
                return true;
            }
            else if (in.getClass().equals(EMPTY_DOUBLE.getClass())) {
                if (DType.Type.FLOAT64 != array.dtype().type()) {
                    return false;
                }
                for (int i=0; i < length; i++) {
                    if (array.getDouble(i) != Array.getDouble(in, i)) {
                        return false;
                    }
                }
                return true;
            }
            else {
                // Unhandled
                return false;
            }
        }

        // Compare
        return Objects.equals(in, out);
    }

    /**
     * Describe an object.
     */
    private String describe(final Object object)
    {
        final StringBuilder sb = new StringBuilder();
        if (object == null) {
            sb.append("null");
        }
        else if (object.getClass().isArray()) {
            final int length = Array.getLength(object);
            sb.append('[');
            for (int i=0; i < length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(Array.get(object, i));
            }
            sb.append(']');
        }
        else if (object instanceof CharSequence) {
            sb.append('"').append(object).append('"');
        }
        else {
            sb.append(String.valueOf(object));
        }
        if (object != null) {
            sb.append(" <");
            sb.append(object.getClass());
            sb.append('>');
        }
        return sb.toString();
    }
}
