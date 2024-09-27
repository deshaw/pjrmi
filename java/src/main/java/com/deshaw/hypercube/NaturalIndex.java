package com.deshaw.hypercube;

import com.deshaw.util.LongBitSet;

/**
 * An index which is a one-to-one mapping with the natural
 * numbers. This is effectively the identity index, but bounded in
 * size.
 */
public class NaturalIndex
    extends AbstractIndex<Long>
{
    /**
     * The size of the index.
     */
    private final long mySize;

    /**
     * Constructor.
     *
     * @param name  The name of this index, if any.
     * @param size  The number of values in this index.
     *
     * @throws IllegalArgumentException If the size was non-positive.
     */
    public NaturalIndex(final String name,
                        final long   size)
    {
        super(name);

        if (size <= 0) {
            throw new IllegalArgumentException(
                "Size was non-positive: " + size
            );
        }
        mySize = size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Index<Long> mask(final LongBitSet mask)
    {
        if (mask == null) {
            throw new NullPointerException("Given a null mask");
        }

        // For a natural index the masking just creates a new natural index
        return new NaturalIndex(this + "<Masked>", mask.cardinality());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long indexOf(final Long key)
    {
        return (key != null && 0 <= key && key < mySize) ? key : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long keyOf(final long index)
    {
        return (0 <= index && index < mySize) ? index : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size()
    {
        return mySize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Index<Long> subIndex(final String name,
                                final long   start,
                                final long   end)
        throws IllegalArgumentException,
               NullPointerException
    {
        // A sub-index of a NaturalIndex remains natural, otherwise slicing does
        // not work in a intuitive manner
        if (start >= end) {
            throw new IllegalArgumentException(
                "Start, " + start + " was not before end, " + end
            );
        }
        return new NaturalIndex(name, end - start);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o)
    {
        if (o == this) {
            return true;
        }
        else if (!super.equals(o)) {
            return false;
        }
        else if (o instanceof NaturalIndex) {
            // Must be the same if their sizes match
            return (this.size() == ((NaturalIndex)o).size());
        }
        else if (o instanceof Index) {
            // Heavy lifting here
            return keysEquals((Index<?>)o);
        }
        else {
            return false;
        }
    }
}
