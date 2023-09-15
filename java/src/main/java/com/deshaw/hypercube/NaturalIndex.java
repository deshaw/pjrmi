package com.deshaw.hypercube;

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
    public long indexOf(final Long key)
    {
        return (key != null && 0 <= key && key < mySize) ? key : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long indexOf(final long index)
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
