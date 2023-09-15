package com.deshaw.hypercube;

// Recreate with `cog.py -rc Boolean1dWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_nd_wrapping_hypercube
//
//     cog.outl(primitive_nd_wrapping_hypercube.generate(1, numpy.bool8))
// ]]]
import java.util.Map;

/**
 * A 1-dimensional hypercube which has {@code boolean} values
 * as its elements and is backed by a user-supplied array.
 */
public class Boolean1dWrappingHypercube
    extends AbstractBooleanHypercube
{
    /**
     * The array(s) which we wrap.
     */
    private final boolean[] myElements;

    /**
     * Get the {@link Dimension}s of the given array.
     */
    private static Dimension<?>[] getDimensions(final boolean[] array)
    {
        final long[] shape = new long[1];
        shape[0] = Math.max(shape[0], array.length);
        return Dimension.of(shape);
    }

    /**
     * Constructor.
     */
    public Boolean1dWrappingHypercube(final boolean[] array)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(getDimensions(array));

        myElements = array;
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to get values where the underlying array data is missing will yield
     * the result of {@code false}.
     */
    @Override
    public boolean get(final long... indices)
        throws IndexOutOfBoundsException
    {
        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != 1) {
            throw new IndexOutOfBoundsException(
                "Wanted 1 indices but had " + indices.length
            );
        }
        final long index0 = indices[0];
        if (index0 < 0 || index0 >= length(0)) {
            throw new IndexOutOfBoundsException(
                "Bad index[0]: " + index0
            );
        }

        if (index0 >= myElements.length) {
            return false;
        }

        return myElements[(int)index0];
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void set(final boolean value, final long... indices)
        throws IndexOutOfBoundsException
    {
        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != 1) {
            throw new IndexOutOfBoundsException(
                "Wanted 1 indices but had " + indices.length
            );
        }
        final long index0 = indices[0];
        if (index0 < 0 || index0 >= length(0)) {
            throw new IndexOutOfBoundsException(
                "Bad index[0]: " + index0
            );
        }

        if (index0 >= myElements.length) {
            return;
        }

        myElements[(int)index0] = value;
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to get values where the underlying array is missing will yield
     * the result of {@code false}.
     */
    @Override
    public boolean getAt(long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index0 = index;

        if (index0 >= myElements.length) {
            return false;
        }

        return myElements[(int)index0];
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void setAt(long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index0 = index;

        if (index0 >= myElements.length) {
            return;
        }

        myElements[(int)index0] = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return Boolean.valueOf(getAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final Boolean value)
        throws IndexOutOfBoundsException
    {
        setAt(index, (value == null) ? false : value.booleanValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",   true );
        result.put("owndata",   false);
        result.put("writeable", true );
        return result;
    }
}

// [[[end]]] (checksum: 5abbe4da14634c64655b31c9c07b7b60)
