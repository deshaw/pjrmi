package com.deshaw.hypercube;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An {@link Index} implementation which maps from arbitrary keys to an index
 * (e.g. {@link String}s).
 *
 * <p>This is currently limited to {@code Integer.MAX_VALUE} keys.
 */
public class MappedIndex<T>
    extends AbstractIndex<T>
{
    /**
     * The array of Ts which define the mapping of this index.
     */
    private final T[] myKeys;

    /**
     * The mapping from key to index.
     */
    private final Map<T,Long> myKeyToIndex;

    /**
     * Constructor.
     *
     * @param name  The name of this index, if any.
     * @param keys  The list of keys which define the index, in the order of
     *              their mapping.
     *
     * @throws IllegalArgumentException If {@code keys} was empty, or if any of
     *                                  the keys were null or were repeated.
     */
    public MappedIndex(final String  name,
                       final List<T> keys)
        throws IllegalArgumentException
    {
        super(name);

        // Check, an empty list doesn't make sense
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Given an empty list of keys");
        }

        // Copy into our array
        @SuppressWarnings("unchecked")
        final T[] array = (T[])keys.toArray();
        myKeys = array;

        // And build the index
        myKeyToIndex = new HashMap<>(myKeys.length * 2);
        for (int i=0; i < myKeys.length; i++) {
            final T key = myKeys[i];
            if (key == null) {
                throw new IllegalArgumentException(
                    "Key index " + i + " was null"
                );
            }

            // Put it into the index, checking for clashes
            final Long previous = myKeyToIndex.put(key, Long.valueOf(i));
            if (previous != null) {
                throw new IllegalArgumentException(
                    "Key <" + key + "> was repeated; " +
                    "at index " + previous + " and " + i
                );
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long indexOf(final T key)
    {
        final Long index = (key == null) ? null : myKeyToIndex.get(key);
        return (index == null) ? -1 : index;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T keyOf(final long index)
    {
        return (0 <= index && index < myKeys.length) ? myKeys[(int)index] : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size()
    {
        return myKeys.length;
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
        else if (o instanceof MappedIndex) {
            // Must be the same if their keys all match
            return Arrays.equals(myKeys, ((MappedIndex<?>)o).myKeys);
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
