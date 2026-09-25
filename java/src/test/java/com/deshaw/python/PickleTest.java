package com.deshaw.python;

import com.deshaw.python.DType;
import com.deshaw.python.NumpyArray;
import com.deshaw.python.PythonPickle;
import com.deshaw.python.PythonUnpickle;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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

    /**
     * The protocol 4 header, which is what a stream carrying a
     * {@code BINBYTES8} declares.
     */
    private static final String PROTO4 = "\u0080\u0004";

    /**
     * The {@code BINBYTES8} opcode.
     */
    private static final String BINBYTES8 = "\u008e";

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
     * BINBYTES8 carries a little-endian int64 length. Nothing else in the tree
     * reads one back, so this is what pins the byte order of readInt64(): a
     * transposed shift term would make every large payload coming from Python
     * decode as garbage, and would otherwise go unnoticed until it did.
     */
    @Test
    public void testBinBytes8IsRead()
        throws Exception
    {
        // PROTO 4; BINBYTES8 len=3 (little-endian int64); "abc"; STOP
        final Object out = PythonUnpickle.loadPickle(
            bytes(PROTO4 + BINBYTES8 + int64le(3) + "abc" + ".")
        );
        Assertions.assertEquals("abc", String.valueOf(out),
                                "BINBYTES8 did not round-trip");
    }

    /**
     * A BINBYTES8 length is a signed int64 off the wire, so a negative one is
     * refused rather than being cast down to an int and used.
     */
    @Test
    public void testBinBytes8NegativeLengthIsRejected()
    {
        // PROTO 4; BINBYTES8 len=-1; STOP
        assertMalformed("-1", PROTO4 + BINBYTES8 + int64le(-1) + ".");
    }

    /**
     * A BINBYTES8 length larger than a byte[] can hold is refused up front,
     * rather than being truncated by the cast into something plausible.
     */
    @Test
    public void testBinBytes8OversizedLengthIsRejected()
    {
        // PROTO 4; BINBYTES8 len=2^32; STOP. The low 32 bits of that length
        // are zero, so a narrowing cast would read it as an empty string and
        // raise nothing at all -- which is what makes this length, rather than
        // one just over Integer.MAX_VALUE, able to tell the two apart. At
        // 2^31 the cast gives Integer.MIN_VALUE, whose text still contains
        // "2147483648", so the complaint would look right either way.
        assertMalformed(String.valueOf(1L << 32),
                        PROTO4 + BINBYTES8 + int64le(1L << 32) + ".");
    }

    /**
     * Protocol 4 is accepted, since that is what a pickle carrying a BINBYTES8
     * declares, but only that one opcode of it is implemented. Protocols 3 and
     * 5 are refused at the header, where the complaint names the version,
     * rather than being let through to fail later on an unknown opcode.
     */
    @Test
    public void testProtocolVersionGate()
        throws Exception
    {
        // Protocol 4 gets in
        Assertions.assertEquals(
            1,
            PythonUnpickle.loadPickle(bytes(PROTO4 + "K\u0001" + ".")),
            "Protocol 4 should be accepted"
        );

        // Protocol 3 and 5 do not
        assertMalformed("Unsupported pickle version 3",
                        proto(3) + "K\u0001" + ".");
        assertMalformed("Unsupported pickle version 5",
                        proto(5) + "K\u0001" + ".");
    }

    /**
     * A pickler is reused, so each pickle must start from a cleared buffer and
     * at the oldest protocol which will carry it, and each result must survive
     * the next call.
     *
     * <p>Note what this does not reach: the protocol is raised only by a
     * {@code BINBYTES8}, which needs a value longer than
     * {@code Integer.MAX_VALUE}, and one that size cannot be handed back as a
     * {@code byte[]} at all. Exercising the reset therefore needs a multi-
     * gigabyte value, which is left to the opt-in test in
     * {@code python/tests/pjrmi_tests.py}; everything below stays at
     * protocol 2 throughout. The other thing it does not reach is the buffer
     * being handed back rather than emptied, which needs a value past the
     * retention cap; that is
     * {@code testPicklerRecoversFromAnOutsizedValue} below.
     */
    @Test
    public void testPicklerReuseIsIndependent()
        throws Exception
    {
        final PythonPickle pickle = new PythonPickle();

        // A short value needs nothing newer than protocol 2
        final byte[] first = pickle.toByteArray("hello");
        assertEquals((byte)2, first[1], "first pickle protocol");

        // Reusing the instance must not have left the protocol raised, nor the
        // buffer holding the previous contents
        final byte[] second = pickle.toByteArray("world");
        assertEquals((byte)2, second[1], "protocol after reuse");
        assertEquals("world", PythonUnpickle.loadPickle(second),
                     "contents after reuse");

        // And the two results are independent: toByteArray() gives back a copy
        // each time, so the first is not rewritten by the second
        assertEquals("hello", PythonUnpickle.loadPickle(first),
                     "the earlier result should not have been overwritten");
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

    /**
     * A pickle large enough to make the pickler hand its buffer back, followed
     * by an ordinary one.
     *
     * <p>{@code toPickle()} either empties the buffer or replaces it, according
     * to whether it has grown past the retention cap. Every other test here
     * pickles a handful of bytes and so only ever takes the first of those two
     * paths. This one takes the second, which is the one that matters: a
     * long-lived thread-local pickler which mishandles it has one big message
     * poison every small one after it.
     *
     * <p>The value is sized just over the 64MB default rather than by anything
     * cleverer, since the cap is not visible from here. Held one at a time, it
     * costs well under the test heap.
     */
    @Test
    public void testPicklerRecoversFromAnOutsizedValue()
        throws Exception
    {
        final PythonPickle pickle = new PythonPickle();

        // Comfortably past the 64MB default, so the buffer is replaced rather
        // than emptied when this pickle is done with
        // Sentinels every so often rather than a value per element, so that
        // checking them back does not cost a pass over seventy million of
        // them. Kept inside 0..127 so that nothing here turns on how a byte is
        // widened on the way out.
        final byte[] big = new byte[70 * 1024 * 1024];
        for (int i=0; i < big.length; i += 4096) {
            big[i] = (byte)((i / 4096) & 0x7f);
        }

        /*scope*/ {
            final byte[] pickled = pickle.toByteArray(big);
            assertEquals((byte)2, pickled[1], "outsized pickle protocol");

            // A pickled Java array comes back as a NumpyArray, the same as in
            // the other round trips here
            final NumpyArray out =
                (NumpyArray)PythonUnpickle.loadPickle(pickled);
            assertEquals(big.length, out.size(), "outsized value length");
            for (int i=0; i < big.length; i += 4096) {
                assertEquals((int)big[i], out.getInt(i),
                             "outsized value at " + i);
            }
        }

        // The buffer has now been handed back. The next pickle has to start
        // from an empty one: a replacement which arrived holding the previous
        // contents, or which was never cleared, shows up as a longer stream
        // than "world" needs and as the wrong value coming out of it.
        final byte[] small = pickle.toByteArray("world");
        assertEquals((byte)2, small[1], "protocol after an outsized value");
        assertEquals("world", PythonUnpickle.loadPickle(small),
                     "contents after an outsized value");
        Assertions.assertTrue(
            small.length < 32,
            "a pickle of \"world\" should be a few bytes, not " + small.length
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
     * A {@code PROTO} header declaring the given version.
     */
    private static String proto(final int version)
    {
        // The PROTO opcode, spelt as an escape: a literal one is a
        // non-ASCII byte in the source, which the commit hook rejects
        return "\u0080" + (char)version;
    }

    /**
     * A little-endian {@code int64}, as one character per byte, in the order
     * the pickle format writes one.
     */
    private static String int64le(final long value)
    {
        final StringBuilder sb = new StringBuilder(Long.BYTES);
        for (int i=0; i < Long.BYTES; i++) {
            sb.append((char)((value >>> (8 * i)) & 0xff));
        }
        return sb.toString();
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

    // ---------------------------------------------------------------------- //

    /**
     * Simple test method which exercises pickling data which is too large for
     * the 32bit length opcodes. Not a unit test since it has a large memory
     * footprint.<pre>
     *   java -Xmx10g -cp java/build/classes/java/main:java/build/classes/java/test \
     *        com.deshaw.python.PickleTest [file]
     * </pre>
     *
     * <p>If a file is named then the pickle is written there as well, so that
     * CPython can be pointed at it to check that it really does read back:<pre>
     *   python3 -c "import pickle
     *   a = pickle.load(open('file','rb'), encoding='bytes')
     *   print(len(a), a[0], a[-1])"
     * </pre>
     *
     * @param args  An optional file to write the pickle to.
     *
     * @throws IOException if the pickling failed.
     */
    public static void main(final String[] args)
        throws IOException
    {
        // Just over Integer.MAX_VALUE bytes, so that the length no longer fits
        // in the 32bit BINSTRING opcode
        final int    numElems = (1 << 28) + 1;
        final long   numBytes = 8L * numElems;
        final double[] data   = new double[numElems];
        data[0]            = 1.5;
        data[1]            = -2.5;
        data[numElems - 1] = 3.25;
        System.out.println("Pickling " + numBytes + " bytes of data...");

        // Keep the head of the stream so that we can look at the opcodes, and
        // count the rest rather than keeping another copy of it all
        final byte[] head  = new byte[128];
        final long[] total = new long[] { 0 };
        final OutputStream sink =
            new OutputStream() {
                @Override
                public void write(final int b)
                {
                    write(new byte[] { (byte)b }, 0, 1);
                }

                @Override
                public void write(final byte[] b, final int off, final int len)
                {
                    for (int i=0; i < len && total[0] + i < head.length; i++) {
                        head[(int)(total[0] + i)] = b[off + i];
                    }
                    total[0] += len;
                }
            };

        final PythonPickle pickle = new PythonPickle();
        pickle.toStream(data, sink);

        // The stream must declare protocol 4, since that is the first one with
        // an opcode carrying a 64bit length
        assertEquals((byte)0x80, head[0], "PROTO opcode");
        assertEquals((byte)4,    head[1], "protocol version");

        // And somewhere in the header there should be a BINBYTES8 carrying the
        // length, little-endian
        final byte[] expected = new byte[9];
        expected[0] = (byte)0x8e;
        for (int i=0; i < 8; i++) {
            expected[i + 1] = (byte)(numBytes >>> (8 * i));
        }
        boolean found = false;
        for (int i=0; i + expected.length <= head.length && !found; i++) {
            found = Arrays.equals(head,     i, i + expected.length,
                                  expected, 0, expected.length);
        }
        if (!found) {
            throw new AssertionError(
                "No BINBYTES8 with a length of " + numBytes + " in " +
                Arrays.toString(head)
            );
        }

        // The whole thing should be the data plus a modest amount of framing
        if (total[0] <= numBytes || total[0] > numBytes + 1024) {
            throw new AssertionError(
                "Pickle was " + total[0] + " bytes, for " + numBytes +
                " bytes of data"
            );
        }
        System.out.println("Pickle was " + total[0] + " bytes; opcodes good!");

        // Hand it to the caller to give to CPython, if they asked
        if (args.length > 0) {
            System.out.println("Writing the pickle to " + args[0] + "...");
            try (OutputStream out =
                     new BufferedOutputStream(new FileOutputStream(args[0]),
                                              1024 * 1024))
            {
                pickle.toStream(data, out);
            }
        }

        System.out.println("Done!");
    }

    /*
     * Assertion method.
     */
    private static void assertEquals(final Object expected,
                                     final Object actual,
                                     final String msg)
    {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(
                "Expected " + expected + " != actual " + actual + "; " + msg
            );
        }
    }
}
