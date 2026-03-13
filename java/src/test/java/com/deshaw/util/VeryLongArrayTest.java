package com.deshaw.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A unit test suite for testing {@link com.deshaw.util.VeryLongArray} class.
 *
 * <p>Note: Memory-intensive tests are located in the {@code main()} method of
 * VeryLongArray.java instead of here, as they would exhaust the available heap
 * space in typical unit test environments.
 */
public class VeryLongArrayTest
{
    /**
     * Test constructor with zero size.
     */
    @Test
    public void testConstructorZeroSize()
    {
        final VeryLongArray array = new VeryLongArray(0);
        assertEquals(0, array.size());
    }

    /**
     * Test constructor with small size (single row).
     */
    @Test
    public void testConstructorSmallSize()
    {
        final VeryLongArray array = new VeryLongArray(100);
        assertEquals(100, array.size());
    }

    /**
     * Test constructor with negative size throws exception.
     */
    @Test
    public void testConstructorNegativeSize()
    {
        assertThrows(IllegalArgumentException.class, () -> {
            new VeryLongArray(-1);
        });
    }

    /**
     * Test constructor with extremely large size throws exception.
     */
    @Test
    public void testConstructorTooLargeSize()
    {
        assertThrows(UnsupportedOperationException.class, () -> {
            new VeryLongArray(0xfffffffffffffffL);
        });
    }

    /**
     * Test size() method for array with 1 row.
     */
    @Test
    public void testSizeOneRow()
    {
        final VeryLongArray array = new VeryLongArray(50);
        assertEquals(50, array.size());
    }

    /**
     * Test get() and set() methods with simple values.
     */
    @Test
    public void testGetAndSet()
    {
        final VeryLongArray array = new VeryLongArray(100);

        // Set some values
        array.set( 0, 100L);
        array.set(50, 200L);
        array.set(99, 300L);

        // Verify we can get them back
        assertEquals(100L, array.get( 0));
        assertEquals(200L, array.get(50));
        assertEquals(300L, array.get(99));
    }

    /**
     * Test get() with negative index throws exception.
     */
    @Test
    public void testGetNegativeIndex()
    {
        final VeryLongArray array = new VeryLongArray(100);
        assertThrows(IndexOutOfBoundsException.class, () -> {
            array.get(-1);
        });
    }

    /**
     * Test get() with out-of-bounds index throws exception.
     */
    @Test
    public void testGetOutOfBounds()
    {
        final VeryLongArray array = new VeryLongArray(100);
        assertThrows(IndexOutOfBoundsException.class, () -> {
            array.get(100); // Index 100 is out of bounds for size 100
        });
    }

    /**
     * Test set() with negative index throws exception.
     */
    @Test
    public void testSetNegativeIndex()
    {
        final VeryLongArray array = new VeryLongArray(100);
        assertThrows(IndexOutOfBoundsException.class, () -> {
            array.set(-1, 42L);
        });
    }

    /**
     * Test set() with out-of-bounds index throws exception.
     */
    @Test
    public void testSetOutOfBounds()
    {
        final VeryLongArray array = new VeryLongArray(100);
        assertThrows(IndexOutOfBoundsException.class, () -> {
            array.set(100, 42L); // Index 100 is out of bounds for size 100
        });
    }

    /**
     * Test binarySearch() on empty array.
     */
    @Test
    public void testBinarySearchEmptyArray()
    {
        final VeryLongArray array = new VeryLongArray(0);
        assertEquals(-1, array.binarySearch(42L));
    }

    /**
     * Test binarySearch() finding values in a sorted array.
     */
    @Test
    public void testBinarySearchFound()
    {
        final VeryLongArray array = new VeryLongArray(10);

        // Populate with sorted values
        for (int i = 0; i < 10; i++) {
            array.set(i, i * 10L);
        }

        // Search for values that exist
        assertEquals(3, array.binarySearch(30L));
        assertEquals(9, array.binarySearch(90L));
    }

    /**
     * Test binarySearch() not finding values in a sorted array.
     */
    @Test
    public void testBinarySearchNotFound()
    {
        final VeryLongArray array = new VeryLongArray(10);

        // Populate with sorted values
        for (int i = 0; i < 10; i++) {
            array.set(i, i * 10L);
        }

        // Search for values that don't exist
        assertEquals(-1, array.binarySearch(  5L));
        assertEquals(-1, array.binarySearch( 95L));
        assertEquals(-1, array.binarySearch(100L));
    }

    /**
     * Test binarySearch() returns correct index position.
     */
    @Test
    public void testBinarySearchCorrectIndex()
    {
        final VeryLongArray array = new VeryLongArray(100);

        // Populate with sorted values
        for (int i = 0; i < 100; i++) {
            array.set(i, i);
        }

        // Verify binarySearch returns the correct index
        assertEquals( 0, array.binarySearch( 0L));
        assertEquals(50, array.binarySearch(50L));
        assertEquals(99, array.binarySearch(99L));
    }

    /**
     * Test setting and getting all values in a larger array.
     */
    @Test
    public void testComprehensiveSetAndGet()
    {
        final long size = 10000;
        final VeryLongArray array = new VeryLongArray(size);

        // Set all values
        for (long i = 0; i < size; i++) {
            array.set(i, i * 2);
        }

        // Verify all values
        for (long i = 0; i < size; i++) {
            assertEquals(i * 2, array.get(i), "Mismatch at index " + i);
        }
    }

    /**
     * Test default initialized values are zero.
     */
    @Test
    public void testDefaultValuesAreZero()
    {
        final VeryLongArray array = new VeryLongArray(100);

        // All values should be 0 by default
        for (int i = 0; i < 100; i++) {
            assertEquals(0L, array.get(i),
                         "Default value at index " + i + " should be 0");
        }
    }
}
