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
     * The maximum length of each sub-array.
     */
    private static final int MAX_SUBLENGTH = MASK+1;

    /**
     * All the empty arrays can use the same representation.
     */
    private static final long[][] EMPTY = new long[0][];    

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

        // Handle empty arrays specially
        if (size == 0) {
            myValues = EMPTY;
        }
        else {
            // Sort of a matrix with non-aligned last row. Compute how many arrays
            // (rows), and how long the last row is (how many columns)
            int nRows    = (int)(size >>> 30) + 1;
            int nLastCol = (int)(size & MASK);

            // In the event that size is an exact multiple of MAX_SUBLENGTH then
            // the above will yield an empty array at the end, which is
            // pointless. So we catch that case. Note that we will only have
            // positive multiples of MAX_SUBLENGTH here since we check for the
            // case of size==0 above.
            if (nLastCol == 0) {
                nRows--;
                nLastCol = MAX_SUBLENGTH;
            }

            // Now we can create and populate myValues
            myValues = new long[nRows][];
            for (int i=0; i < myValues.length; i++) {
                if (i + 1 == myValues.length) {
                    myValues[i] = new long[nLastCol];
                }
                else {
                    myValues[i] = new long[MAX_SUBLENGTH];
                }
            }
        }
    }

    /**
     * How big?
     */
    public long size()
    {
        // Derive it directly where we can, since it should generally be very
        // cheap to do so and will likely come up 99% of the time. Fall back to
        // the more complex (hence, expensive) for loop for larger sizes.
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
            for (int i=0; i < myValues.length; i++) {
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
        // Empty means it's not there
        if (myValues.length == 0 || myValues[0].length == 0) {
            return -1;
        }

        // We don't expect a lot of rows so walk them
        long offset = 0;
        long[] row = null;
        for (int i=0; i < myValues.length; i++) {
            row = myValues[i];
            final int length = myValues[i].length;
            if (length == 0) {
                // Shouldn't ever happen since we guard against this in the
                // constructor, but just in case
                return -1;
            }
            else if (value > myValues[i][length-1]) {
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
     *   java -Xmx48g -cp java/build/classes/java/main com.deshaw.util.VeryLongArray
     * </pre>
     *
     * @param args  Ignored.
     */
    public static void main(final String[] args)
    {
        // Lots of big tests. We use scoping to allow these huge arrays to be
        // garbage collected

        // Test boundary case: exactly one full row (2^30 elements, ~8GB)
        /*scope*/ {
            System.out.println("Testing boundary size...");
            final long boundarySize = (long)MASK + 1;
            final VeryLongArray boundaryArray =
                new VeryLongArray(boundarySize);
            assertEquals(boundaryArray.size(), boundarySize, "boundary size");
            boundaryArray.set(0,                111L);
            boundaryArray.set(boundarySize - 1, 222L);
            assertEquals(boundaryArray.get(0),                111L,
                         "boundary first");
            assertEquals(boundaryArray.get(boundarySize - 1), 222L,
                         "boundary last");
            System.out.println("Boundary size passed!");
        }

        // Test over boundary: 2^30 + 1 elements (two rows, ~8GB)
        /*scope*/ {
            System.out.println("Testing over-boundary size...");
            final long size = (long)MASK + 2;
            final VeryLongArray array =
                new VeryLongArray(size);
            assertEquals(array.size(), size, "over-boundary size");
            assertEquals(array.binarySearch(1), -1L, "out-of-range");
            System.out.println("Over-boundary size passed!");
        }

        // Test size calculation with 2 rows (~16GB)
        /*scope*/ {
            System.out.println("Testing 2 row size...");
            final long size = (long)MASK + 1 + 100;
            final VeryLongArray array = new VeryLongArray(size);
            assertEquals(array.size(), size, "2 row size");
            assertEquals(array.binarySearch(1), -1L, "out-of-range");
            System.out.println("2 row size passed!");
        }

        // Test size calculation with 2 rows of exact max-size
        /*scope*/ {
            System.out.println("Testing 2 row size as exact multiple...");
            final long size = (long)MAX_SUBLENGTH * 2;
            final VeryLongArray array = new VeryLongArray(size);
            assertEquals(array.size(), size, "2 row size");
            assertEquals(array.myValues.length, 2, "2 row length");
            assertEquals(array.myValues[0].length, MAX_SUBLENGTH,
                         "2 row [0]th length");
            assertEquals(array.myValues[1].length, MAX_SUBLENGTH,
                         "2 row [1]th length");
            assertEquals(array.binarySearch(1), -1L, "out-of-range");
            System.out.println("2 row size passed!");
        }

        // Test size calculation with 3 rows (~24GB)
        /*scope*/ {
            System.out.println("Testing 3 row size...");
            final long size = 2L * ((long)MASK + 1) + 100;
            final VeryLongArray array =
                new VeryLongArray(size);
            assertEquals(array.size(), size, "3 row size");
            assertEquals(array.binarySearch(1), -1L, "out-of-range");
            System.out.println("3 row size passed!");
        }

        // Test size calculation with 4 rows (~32GB).
        /*scope*/ {
            System.out.println("Testing 4 row size...");
            final long size = 3L * ((long)MASK + 1) + 100;
            final VeryLongArray array = new VeryLongArray(size);
            assertEquals(array.size(), size, "4 row size");
            assertEquals(array.binarySearch(1), -1L, "out-of-range");
            System.out.println("4 row size passed!");
        }

        // Test get/set across row boundaries (~16GB)
        /*scope*/ {
            System.out.println("Testing cross-row get/set...");
            final long size = 2L * ((long)MASK + 1) + 100;
            final VeryLongArray array = new VeryLongArray(size);
            array.set(0,                     111L);
            array.set(MASK,                  222L);
            array.set((long)MASK + 1,        333L);
            array.set(2L * ((long)MASK + 1), 444L);
            assertEquals(array.get(0),                     111L, "row 0 first");
            assertEquals(array.get(MASK),                  222L, "row 0 last");
            assertEquals(array.get((long)MASK + 1),        333L, "row 1 first");
            assertEquals(array.get(2L * ((long)MASK + 1)), 444L, "row 2 first");
            System.out.println("Cross-row get/set passed!");
        }

        // Test binary search across rows (~16GB)
        /*scope*/ {
            System.out.println("Testing cross-row search...");
            final long size = 2L * ((long)MASK + 1) + 100;
            final VeryLongArray array = new VeryLongArray(size);
            for (long i = 0; i < size; i++) {
                if ((i & 0xffff) == 0) {
                    System.out.print("Populating " + i + "\r");
                }
                array.set(i, i);
            }
            System.out.println();
            assertEquals(
                array.binarySearch(0L),                     0L,
                "row 0 start"
            );
            assertEquals(
                array.binarySearch(MASK),                  (long)MASK,
                "row 0 end"
            );
            assertEquals(
                array.binarySearch((long)MASK + 1),        (long)MASK + 1,
                "row 1 start"
            );
            assertEquals(
                array.binarySearch(2L * ((long)MASK + 1)), 2L * ((long)MASK + 1),
                "row 2 start"
            );
            assertEquals(
                array.binarySearch(size + 100),      -1L,
                "out-of-range"
            );
            System.out.println("Cross-row search passed!");
        }

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
}
