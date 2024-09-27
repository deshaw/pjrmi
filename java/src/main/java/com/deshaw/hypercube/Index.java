package com.deshaw.hypercube;

import com.deshaw.util.LongBitSet;

/**
 * The way in which a dimension is accessed in a {@link Hypercube}. This defines
 * a mapping between the natural numbers, as an index, and an ordered list of
 * values.
 *
 * @param <T> The type of the values which the index is for.
 */
public interface Index<T>
{
    /**
     * Get the name of this index, if any.
     */
    public String getName();

    /**
     * Get the index value for the given key.
     *
     * @return -1 if the key was not found.
     */
    public long indexOf(final T key);

    /**
     * Get the key value for the given index.
     *
     * @return {@code null} if the index value was not in the {@link Index}'s
     *         range.
     */
    public T keyOf(final long index);

    /**
     * The number of elements in this index.
     */
    public long size();

    /**
     * Generate a version of this index which is masked by the given array,
     * where {@code true} denotes a visible value.
     */
    public Index<T> mask(final LongBitSet mask);

    /**
     * Create a sub-index of this one.
     *
     * @param name  The name of the new index.
     * @param start The start of the range, inclusive.
     * @param end   The end of the range, exclusive.
     */
    public default Index<T> subIndex(final String name,
                                     final long   start,
                                     final long   end)
        throws IllegalArgumentException,
               NullPointerException
    {
        return new SubIndex<>(name, this, start, end);
    }
}
