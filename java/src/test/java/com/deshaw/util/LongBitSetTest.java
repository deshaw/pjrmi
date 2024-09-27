package com.deshaw.util;

import com.deshaw.util.LongBitSet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A unit test suite for testing {@link com.deshaw.util.LongBitSet} class.
 */
public class LongBitSetTest
{
    /**
     * Test sets with more than {@code MAX_INT} bits.
     */
    @Test
    public void testHugeBitSet()
    {
        // Start off empty
        final LongBitSet bitset = new LongBitSet(0);

        // Fill it up, watching it grow
        int cardinality = 0;
        for (long i=1; i <= ((long)Integer.MAX_VALUE) << 2; i <<= 1) {
            bitset.set(i, true);
            cardinality++;
        }

        // Check that what we expect to be set is set, and what we don't is not
        assertFalse(bitset.get(-1));
        for (long i=1; i <= ((long)Integer.MAX_VALUE) << 2; i <<= 1) {
            assertTrue(bitset.get(i));
        }
        for (long i=0; i < 16384; i++) {
            if (Long.bitCount(i) > 1) {
                assertFalse(bitset.get(i));
            }
        }

        // And count the bits
        assertEquals(bitset.cardinality(), cardinality);
    }

    /**
     * Test bad arguments.
     */
    @Test
    public void testBadArgs()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> new LongBitSet(-1));
        assertThrows(IllegalArgumentException.class,
                     () -> new LongBitSet(Long.MAX_VALUE));
        assertThrows(IndexOutOfBoundsException.class,
                     () -> (new LongBitSet(0)).get(Long.MAX_VALUE));
    }
}
