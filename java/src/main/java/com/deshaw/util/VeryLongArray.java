package com.deshaw.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * An array of longs which can be {@code 2^60} in size, memory allowing.
 */
public class VeryLongArray
{
    /**
     * The length mask which we use. Should be 2^30-1.
     */
    private static final int MASK = 0x3fffffff;

    /**
     * The values which we hold. Each array is up to {@code 2^30} in elements
     * length.
     */
    private final long[][] myValues;

    /**
     * Constructor, with a size.
     */
    public VeryLongArray(final long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Given a negative size");
        }
        if (size >= 0xfffffffffffffffL) {
            // Java can't allocate arrays this big. It's quite a lot of memory
            // too (8388608PB) so likely won't happen.
            throw new UnsupportedOperationException(
                "Size to large: " + size
            );
        }

        // Sort of a matrix with non-aligned last row. Compute how many arrays
        // (rows), and how long the last row is (how many columns)
        final int nRows    = (int)(size >>> 30) + 1;
        final int nLastCol = (int)(size & MASK);

        myValues = new long[nRows][];
        for (int i=0; i < myValues.length; i++) {
            if (i + 1 == myValues.length) {
                myValues[i] = new long[nLastCol];
            }
            else {
                myValues[i] = new long[MASK+1];
            }
        }
    }

    /**
     * How big?
     */
    public long size()
    {
        // Derive it, since it should generally be very cheap to do so
        switch (myValues.length) {
        case 0: return 0;
        case 1: return (long)myValues[0].length;
        case 2: return (long)myValues[0].length +
                       (long)myValues[1].length;
        case 3: return (long)myValues[0].length +
                       (long)myValues[1].length +
                       (long)myValues[2].length;
        default:
            long size = 0;
            for (int i = 1; i < myValues.length; i++) {
                size += myValues[i].length;
            }
            return size;
        }
    }

    /**
     * Get the value at a given index.
     */
    public long get(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Negative index: " + index);
        }

        final int rowIdx = (int)(index >>> 30);
        final int colIdx = (int)(index & MASK);
        if (rowIdx >= myValues.length || colIdx >= myValues[rowIdx].length) {
            throw new IndexOutOfBoundsException("Index too large: " + index);
        }

        // Safe to get
        return myValues[rowIdx][colIdx];
    }

    /**
     * Set the value at a given index.
     */
    public void set(final long index, final long value)
        throws IndexOutOfBoundsException
    {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Negative index: " + index);
        }

        final int rowIdx = (int)(index >>> 30);
        final int colIdx = (int)(index & MASK);
        if (rowIdx >= myValues.length || colIdx >= myValues[rowIdx].length) {
            throw new IndexOutOfBoundsException("Index too large: " + index);
        }

        // Safe to set
        myValues[rowIdx][colIdx] = value;
    }

    /**
     * Binary search. A negative value means merely "not found".
     */
    public long binarySearch(final long value)
    {
        // We don't expect a lot of rows so walk them
        long offset = 0;
        long[] row = null;
        for (int i=0; i < myValues.length; i++) {
            row = myValues[i];
            final int length = myValues[i].length;
            if (value > myValues[i][length-1]) {
                offset += length;
            }
            else {
                break;
            }
        }
        final int idx = Arrays.binarySearch(row, value);
        return (idx >= 0) ? offset + idx : -1;
    }

    // ---------------------------------------------------------------------- //

    /**
     * Simple test method which exercises this code. Not a unit test since it
     * has a large memory footprint.<pre>
     *   java -Xmx16g -cp java/build/classes/java/main com.deshaw.util.VeryLongArray
     * </pre>
     *
     * @param args  Ignored.
     */
    public static void main(final String[] args)
    {
        // Create
        final long size = (long)(25 * (MASK+1));
        System.out.println("Allocating " + size + " element array...");
        final VeryLongArray array = new VeryLongArray(size);

        // Populate
        for (long i=0; i < size; i++) {
            if ((i & 0xffff) == 0) System.out.print("Setting " + i + "\r");
            array.set(i, i);
        }
        System.out.println();

        // Check contents etc.
        assertEquals(array.size(), size, "bad size");
        for (long i=0; i < size; i++) {
            if ((i & 0xffff) == 0) System.out.print("Getting " + i + "\r");
            assertEquals(array.get(i), i, "element " + i);
        }
        System.out.println();

        // Utility methods
        for (long i=0; i < size; i = (i + 1) << 1) {
            System.out.print("Searching " + i + "\r");
            assertTrue (array.binarySearch( i) >= 0, "finding " +  i);
            if (i != 0) {
                assertFalse(array.binarySearch(-i) >= 0, "finding " + -i);
            }
        }
        System.out.println();

        System.out.println("Done!");
    }

    /*
     * Assertion methods.
     */
    private static void assertEquals(final Object actual, final Object expected,
                                     final String msg)
    {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(
                "Expected, " + expected + " != actual " + actual + "; " + msg
            );
        }
    }
    private static void assertTrue(final boolean bool, final String msg)
    {
        if (!bool) {
            throw new AssertionError("Not true: " + msg);
        }
    }
    private static void assertFalse(final boolean bool, final String msg)
    {
        if (bool) {
            throw new AssertionError("Not false: " + msg);
        }
    }
}
