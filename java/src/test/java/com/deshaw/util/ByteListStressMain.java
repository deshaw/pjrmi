package com.deshaw.util;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Objects;

/**
 * Large-memory checks on {@link ByteList}'s sub-array boundary handling.
 *
 * <p>These live here, rather than in {@code ByteListTest}, because each one
 * needs a gigabyte or more and so is too heavy for the unit-test tier. The
 * cheaper cases, including the two single-seam tests which do fit, are in
 * {@code ByteListTest}.
 *
 * <p>The build runs these as the {@code byteListStress} task, which {@code
 * check} depends on. That task gives them an 8GB heap and turns itself off on
 * a machine without the memory to spare, so an ordinary run simply skips them
 * and says so; see {@code java/build.gradle} for the gate and for the
 * environment variable which overrides it either way. To run them directly:
 * <pre>
 *   ./gradlew :java:byteListStress
 * </pre>
 * or, without the build:<pre>
 *   java -Xmx8g \
 *       -cp java/build/classes/java/main:java/build/classes/java/test \
 *       com.deshaw.util.ByteListStressMain
 * </pre>
 *
 * <p>Kept out of {@link ByteList} itself so that the shipped class holds only
 * the data structure; a reader looking for the index arithmetic should not have
 * to scroll past a test harness to reach it. It lives in the test tree, and not
 * beside {@link ByteList}, because it is test code and has no business in the
 * shipped jar.
 */
public class ByteListStressMain
{
    /**
     * How many elements the straddling tests move across the seam. Large enough
     * that the copy loop has to walk both sub-arrays, small enough to be free
     * next to the gigabyte the seam itself costs.
     */
    private static final int BLOCK_SIZE = 1000;

    /**
     * Where the straddling tests start writing, so that they land astride the
     * boundary between the first and second sub-arrays.
     */
    private static final long BLOCK_BASE =
        ByteList.SUB_ARRAY_SIZE - BLOCK_SIZE / 2;

    // ----------------------------------------------------------------------

    /**
     * Run the checks.
     *
     * @param args  Ignored.
     *
     * @throws IOException if one of the writeTo() tests failed to write.
     */
    public static void main(final String[] args)
        throws IOException
    {
        // Lots of big tests. We use scoping to allow these large lists to be
        // garbage collected.

        // Test boundary case: one element shy of a full sub-array (~1GB)
        /*scope*/ {
            System.out.println("Testing under-boundary capacity...");
            final ByteList list = new ByteList(ByteList.SUB_ARRAY_SIZE - 1);
            assertEquals(ByteList.SUB_ARRAY_SIZE - 1, list.capacity(),
                         "under capacity");
            assertEquals(0L, list.size(), "under size");
            System.out.println("Under-boundary capacity passed!");
        }

        // Test boundary case: exactly one full sub-array (~1GB)
        /*scope*/ {
            System.out.println("Testing boundary capacity...");
            final ByteList list = new ByteList(ByteList.SUB_ARRAY_SIZE);
            assertEquals(ByteList.SUB_ARRAY_SIZE, list.capacity(),
                         "boundary capacity");
            System.out.println("Boundary capacity passed!");
        }

        // Test over boundary: one element more than a full sub-array (~1GB)
        /*scope*/ {
            System.out.println("Testing over-boundary capacity...");
            final ByteList list = new ByteList(ByteList.SUB_ARRAY_SIZE + 1);
            assertEquals(ByteList.SUB_ARRAY_SIZE + 1, list.capacity(),
                         "over capacity");
            System.out.println("Over-boundary capacity passed!");
        }

        // Test an exact multiple of the sub-array size (~2GB)
        /*scope*/ {
            System.out.println("Testing exact multiple capacity...");
            final ByteList list = new ByteList(ByteList.SUB_ARRAY_SIZE * 2);
            assertEquals(ByteList.SUB_ARRAY_SIZE * 2, list.capacity(),
                         "multiple capacity");
            System.out.println("Exact multiple capacity passed!");
        }

        // Test reading elements which sit either side of a sub-array boundary,
        // and multi-byte values which straddle it (~2GB while growing)
        /*scope*/ {
            System.out.println("Testing straddling reads...");
            final long     base = ByteList.SUB_ARRAY_SIZE - 4;
            final ByteList list = new ByteList();
            list.setSize(base);
            list.append(new byte[] { (byte)0x01, (byte)0x23,
                                     (byte)0x45, (byte)0x67,
                                     (byte)0x89, (byte)0xab,
                                     (byte)0xcd, (byte)0xef });

            assertEquals(base + 8, list.size(), "straddle size");
            assertEquals((byte)0x67, list.get(ByteList.SUB_ARRAY_SIZE - 1),
                         "last of first sub-array");
            assertEquals((byte)0x89, list.get(ByteList.SUB_ARRAY_SIZE    ),
                         "first of second sub-array");
            assertEquals(0x0123456789abcdefL, list.getLong  (base    ),
                         "straddling getLong");
            assertEquals(0x456789ab,          list.getInt   (base + 2),
                         "straddling getInt");
            assertEquals((short)0x6789,       list.getShort (base + 3),
                         "straddling getShort");
            assertEquals(Double.longBitsToDouble(0x0123456789abcdefL),
                         list.getDouble(base),
                         "straddling getDouble");
            assertEquals(Float.intBitsToFloat(0x456789ab),
                         list.getFloat(base + 2),
                         "straddling getFloat");

            // Growing to reach the second sub-array leaves slack at the end of
            // it, and that slack is past the size. Reading it back means
            // reading elements which the list does not hold, whether the read
            // starts there or merely runs into it.
            final long size = list.size();
            assertTrue(list.capacity() > size, "slack beyond the size");
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.get(size),
                         "read at the size");
            assertThrows(IndexOutOfBoundsException.class,
                         () -> list.getShort(size - 1),
                         "read running past the size");
            System.out.println("Straddling reads passed!");
        }

        // Test appending a block which spans a sub-array boundary. The append
        // loop is the same for any number of sub-arrays, so two is enough to
        // show it walking them (~2GB while growing).
        /*scope*/ {
            System.out.println("Testing straddling append...");
            final byte[] block = block();

            final ByteList list = new ByteList();
            list.setSize(BLOCK_BASE);
            list.append(block);

            assertEquals(BLOCK_BASE + BLOCK_SIZE, list.size(), "append size");
            for (int i=0; i < block.length; i++) {
                assertEquals(block[i], list.get(BLOCK_BASE + i),
                             "append element " + i);
            }
            System.out.println("Straddling append passed!");
        }

        // Test appending one list to another across a sub-array boundary
        // (~2GB while growing)
        /*scope*/ {
            System.out.println("Testing straddling addAll...");
            final ByteList other = new ByteList();
            for (int i=0; i < BLOCK_SIZE; i++) {
                other.add((byte)i);
            }

            final ByteList list = new ByteList();
            list.setSize(BLOCK_BASE);
            assertEquals(Boolean.TRUE, list.addAll(other), "addAll result");

            assertEquals(BLOCK_BASE + BLOCK_SIZE, list.size(), "addAll size");
            for (int i=0; i < BLOCK_SIZE; i++) {
                assertEquals((byte)i, list.get(BLOCK_BASE + i),
                             "addAll element " + i);
            }
            System.out.println("Straddling addAll passed!");
        }

        // Test that writeTo() walks the sub-arrays and hands everything over
        // in order (~2GB while growing)
        /*scope*/ {
            System.out.println("Testing straddling writeTo()...");
            final ByteList list = new ByteList();
            list.setSize(BLOCK_BASE);
            for (int i=0; i < BLOCK_SIZE; i++) {
                list.add((byte)i);
            }

            // Count what comes out and check the tail, without keeping another
            // copy of the whole thing
            final long[] seen = new long[] { 0 };
            final byte[] tail = new byte[BLOCK_SIZE];
            list.writeTo(
                new OutputStream() {
                    @Override
                    public void write(final int b)
                    {
                        write(new byte[] { (byte)b }, 0, 1);
                    }

                    @Override
                    public void write(final byte[] b, final int off,
                                      final int len)
                    {
                        for (int i=0; i < len; i++) {
                            final long pos = seen[0] + i;
                            if (pos >= BLOCK_BASE) {
                                tail[(int)(pos - BLOCK_BASE)] = b[off + i];
                            }
                        }
                        seen[0] += len;
                    }
                }
            );

            assertEquals(BLOCK_BASE + BLOCK_SIZE, seen[0], "writeTo byte count");
            for (int i=0; i < BLOCK_SIZE; i++) {
                assertEquals((byte)i, tail[i], "writeTo element " + i);
            }
            System.out.println("Straddling writeTo() passed!");
        }

        // A list which spans two sub-arrays but still fits in a byte[] takes
        // toArray()'s stitching branch, which is the only caller of copyTo()
        // that walks more than one sub-array (~3GB while copying)
        /*scope*/ {
            System.out.println("Testing straddling toArray()...");
            final ByteList list = new ByteList();
            list.setSize(BLOCK_BASE);
            for (int i=0; i < BLOCK_SIZE; i++) {
                list.add((byte)i);
            }

            final byte[] array = list.toArray();
            assertEquals(BLOCK_BASE + BLOCK_SIZE, (long)array.length,
                         "toArray length");
            for (int i=0; i < BLOCK_SIZE; i++) {
                assertEquals((byte)i, array[(int)BLOCK_BASE + i],
                             "toArray element " + i);
            }
            System.out.println("Straddling toArray() passed!");
        }

        // subSequence() goes through appendFrom(), so a range which starts in
        // one sub-array and ends in the next exercises the two-sided walk
        // (~2GB while growing)
        /*scope*/ {
            System.out.println("Testing straddling subSequence()...");
            final ByteList list = new ByteList();
            list.setSize(BLOCK_BASE);
            for (int i=0; i < BLOCK_SIZE; i++) {
                list.add((byte)i);
            }

            final ByteList sub =
                list.subSequence(BLOCK_BASE, BLOCK_BASE + BLOCK_SIZE);
            assertEquals((long)BLOCK_SIZE, sub.size(), "subSequence size");
            for (int i=0; i < BLOCK_SIZE; i++) {
                assertEquals((byte)i, sub.get(i), "subSequence element " + i);
            }
            System.out.println("Straddling subSequence() passed!");
        }

        // A list which is too long to render as a byte[] should say so rather
        // than silently truncating (~2GB)
        /*scope*/ {
            System.out.println("Testing oversized toArray()...");
            final ByteList list = new ByteList();
            list.setSize((long)Integer.MAX_VALUE + 1);
            assertThrows(UnsupportedOperationException.class,
                         () -> list.toArray(),
                         "oversized toArray()");
            assertThrows(UnsupportedOperationException.class,
                         () -> list.length(),
                         "oversized length()");
            System.out.println("Oversized toArray() passed!");
        }

        System.out.println("Done!");
    }

    // ----------------------------------------------------------------------

    /**
     * A block of bytes with a recognisable pattern in it.
     *
     * @return the block.
     */
    private static byte[] block()
    {
        final byte[] block = new byte[BLOCK_SIZE];
        for (int i=0; i < block.length; i++) {
            block[i] = (byte)i;
        }
        return block;
    }

    /**
     * Assert that two values match.
     *
     * <p>The primitive overloads exist so that the compiler rejects a
     * comparison between different widths. Going through {@link Object} alone
     * would box them, and {@code Objects.equals(Long.valueOf(0),
     * Integer.valueOf(0))} is {@code false}, so a call which passed {@code 0}
     * where it meant {@code 0L} would compile and then fail at runtime with the
     * uninterpretable message "Expected 0 != actual 0".
     *
     * @param expected  What the value should be.
     * @param actual    What it was.
     * @param msg       What is being checked.
     */
    private static void assertEquals(final long expected,
                                     final long actual,
                                     final String msg)
    {
        if (expected != actual) {
            fail(expected, actual, msg);
        }
    }

    /**
     * Assert that two values match.
     *
     * @param expected  What the value should be.
     * @param actual    What it was.
     * @param msg       What is being checked.
     */
    private static void assertEquals(final double expected,
                                     final double actual,
                                     final String msg)
    {
        // Bitwise, since these come from round-tripping a bit pattern and an
        // exact match is what is being checked. This also makes NaN compare
        // equal to itself, which is what we want here and what == would not do.
        if (Double.doubleToRawLongBits(expected) !=
            Double.doubleToRawLongBits(actual))
        {
            fail(expected, actual, msg);
        }
    }

    /**
     * Assert that two values match.
     *
     * @param expected  What the value should be.
     * @param actual    What it was.
     * @param msg       What is being checked.
     */
    private static void assertEquals(final Object expected,
                                     final Object actual,
                                     final String msg)
    {
        if (!Objects.equals(expected, actual)) {
            fail(expected, actual, msg);
        }
    }

    /**
     * Assert that a condition holds.
     *
     * @param ok   Whether it did.
     * @param msg  What is being checked.
     */
    private static void assertTrue(final boolean ok,
                                   final String msg)
    {
        if (!ok) {
            throw new AssertionError("Not true; " + msg);
        }
    }

    /**
     * Assert that an action throws what it should.
     *
     * @param expected  The class of throwable which should come out.
     * @param action    What to invoke.
     * @param msg       What is being checked.
     */
    private static void assertThrows(final Class<? extends Throwable> expected,
                                     final Runnable action,
                                     final String msg)
    {
        try {
            action.run();
        }
        catch (Throwable t) {
            if (expected.isInstance(t)) {
                return;
            }
            throw new AssertionError(
                "Expected " + expected.getSimpleName() + " but got " + t +
                "; " + msg
            );
        }
        throw new AssertionError(
            "Expected " + expected.getSimpleName() + " but nothing was " +
            "thrown; " + msg
        );
    }

    /**
     * Throw the mismatch error for the assertEquals() family.
     *
     * @param expected  What the value should have been.
     * @param actual    What it was.
     * @param msg       What was being checked.
     */
    private static void fail(final Object expected,
                             final Object actual,
                             final String msg)
    {
        throw new AssertionError(
            "Expected " + expected + " != actual " + actual + "; " + msg
        );
    }
}
