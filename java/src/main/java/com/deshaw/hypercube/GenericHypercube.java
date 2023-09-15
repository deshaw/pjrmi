package com.deshaw.hypercube;

/**
 * A hypercube which has Java {@link Object}s as its elements.
 *
 * @param <T> The type of the element which we store.
 */
public interface GenericHypercube<T>
    extends Hypercube<T>
{
    /**
     * Get the object instance of the element at the given indices.
     *
     * @param indices The indices of the element in the hypercube.
     *
     * @throws IndexOutOfBoundsException If the indices were bad.
     */
    public default T get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getObj(indices);
    }

    /**
     * {{@inheritDoc}}
     */
    @Override
    public default T[] flatten()
        throws UnsupportedOperationException
    {
        return toObjectArray();
    }
}
