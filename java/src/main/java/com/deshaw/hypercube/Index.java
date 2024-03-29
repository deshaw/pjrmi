package com.deshaw.hypercube;

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
    public T indexOf(final long index);

    /**
     * The number of elements in this index.
     */
    public long size();

    /**
     * Create a sub-index of this one.
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
