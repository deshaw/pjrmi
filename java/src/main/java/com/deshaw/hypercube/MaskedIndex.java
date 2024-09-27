package com.deshaw.hypercube;

import com.deshaw.util.LongBitSet;
import com.deshaw.util.VeryLongArray;

import java.util.Arrays;

/**
 * An index which wraps another and masks out certain entries.
 */
public class MaskedIndex<T>
    extends AbstractIndex<T>
{
    /**
     * The index which we are wrapping.
     */
    private final Index<T> myWrapped;

    /**
     * The mask of that index.
     */
    private final LongBitSet myMask;

    /**
     * Our size.
     */
    private final long mySize;

    /**
     * The mapping from our direct index to the direct index of what we wrap.
     */
    private final VeryLongArray myMapping;

    /**
     * Constructor.
     */
    public MaskedIndex(final Index<T> index, final LongBitSet mask)
    {
        super(index + "<Masked>");

        if (index == null) {
            throw new NullPointerException("Given a null index");
        }
        if (mask == null) {
            throw new NullPointerException("Given a null mask");
        }

        // We don't bother bounds checking the mask, we just assume that
        // anything not explcitily set as true is masked away

        // Assign members
        mySize = mask.cardinality();
        myWrapped = index;
        myMask    = (LongBitSet)mask.clone();

        // Create the mapping
        myMapping = new VeryLongArray(mySize);
        for (long i=0, j=0; i < mySize; i++) {
            if (mask.get(i)) {
                myMapping.set(j++, i);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long indexOf(final T key)
    {
        return myMapping.binarySearch(myWrapped.indexOf(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T keyOf(final long index)
    {
        return (index < 0 || index >= myMapping.size())
            ? null
            : myWrapped.keyOf(myMapping.get(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size()
    {
        return myMapping.size();
    }
}
