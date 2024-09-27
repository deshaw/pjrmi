package com.deshaw.util;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Like a {@link BitSet} but with {@code long} indices and simpler.
 */
public class LongBitSet
{
    /**
     * Words for an empty bitset.
     */
    private static final BitSet[] EMPTY = new BitSet[0];

    /**
     * Where we store the bits. We find the right bitSet by shifting down the index
     * by 32 bits, and the column by masking off.
     */
    private BitSet[] myBitSets;

    /**
     * Constructor.
     */
    public LongBitSet()
    {
        this(64);
    }

    /**
     * Constructor with a size.
     */
    public LongBitSet(final long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException(
                "Size must be non-negative, given " + size
            );
        }
        if (size > 0x3fffffffffffffffL) {
            throw new IllegalArgumentException(
                "Size was too large, given " + size
            );
        }

        if (size == 0) {
            myBitSets = EMPTY;
        }
        else {
            final long index = size - 1;
            final int rowIdx = (int)( index         & 0x7fffffffL);
            final int colIdx = (int)((index >>> 32) & 0x7fffffffL);
            myBitSets = new BitSet[colIdx+1];
            for (int i=0; i < myBitSets.length; i++) {
                myBitSets[i] = new BitSet(rowIdx+1);
            }
        }
    }

    /**
     * Constructor from a {@code boolean} array.
     */
    public LongBitSet(final boolean[] bits)
    {
        this(bits.length);
        for (int i=0; i < bits.length; i++) {
            set(i, bits[i]);
        }
    }

    /**
     * Constructor from a {@link BitSet};
     */
    public LongBitSet(final BitSet bits)
    {
        myBitSets = new BitSet[1];
        myBitSets[0] = (BitSet)bits.clone();
    }

    /**
     * Get the bit set at a given index.
     */
    public boolean get(final long index)
    {
        // Negative bits are never set
        if (index < 0) {
            return false;
        }
        if (index > 0x3fffffffffffffffL) {
            throw new IndexOutOfBoundsException(
                "Index was too large, given " + index
            );
        }

        // Figure out the offset and sotre indices
        final int rowIdx = (int)( index         & 0x7fffffffL);
        final int colIdx = (int)((index >>> 32) & 0x7fffffffL);

        // Bounds check means false
        return (colIdx < myBitSets.length) ? myBitSets[colIdx].get(rowIdx)
                                           : false;
    }

    /**
     * Set the bit set at a given index.
     */
    public void set(final long index, final boolean value)
    {
        // Negative bits are never set
        if (index < 0) {
            throw new IndexOutOfBoundsException("Negative index: " + index);
        }
        if (index > 0x3fffffffffffffffL) {
            throw new IndexOutOfBoundsException(
                "Index was too large, given " + index
            );
        }

        // Figure out the offset and store indices
        final int rowIdx = (int)( index         & 0x7fffffffL);
        final int colIdx = (int)((index >>> 32) & 0x7fffffffL);

        // Make sure we have room
        if (colIdx >= myBitSets.length) {
            final BitSet[] bitsets = Arrays.copyOf(myBitSets, colIdx + 1);
            for (int i = myBitSets.length; i < bitsets.length; i++) {
                if (i == colIdx) {
                    bitsets[i] = new BitSet(rowIdx + 1);
                }
                else {
                    bitsets[i] = new BitSet();
                }
            }
            myBitSets = bitsets;
        }

        // Set the bit in the bitset
        myBitSets[colIdx].set(rowIdx, value);
    }

    /**
     * Returns the total number of bits set to {@code true}.
     */
    public long cardinality()
    {
        long total = 0;
        for (BitSet s : myBitSets) {
            total += s.cardinality();
        }
        return total;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object clone()
    {
        try {
            final LongBitSet result = (LongBitSet)super.clone();
            for (int i=0; i < result.myBitSets.length; i++) {
                result.myBitSets[i] = (BitSet)result.myBitSets[i].clone();
            }
            return result;
        }
        catch (CloneNotSupportedException e) {
            // Should not happen
            throw new RuntimeException(e);
        }
    }
}
