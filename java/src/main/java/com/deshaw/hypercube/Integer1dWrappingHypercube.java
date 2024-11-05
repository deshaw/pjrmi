package com.deshaw.hypercube;

// Recreate with `cog -rc Integer1dWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_nd_wrapping_hypercube
//
//     cog.outl(primitive_nd_wrapping_hypercube.generate(1, numpy.int32))
// ]]]
import java.util.Map;

/**
 * A 1-dimensional hypercube which has {@code int} values
 * as its elements and is backed by a user-supplied array.
 */
public class Integer1dWrappingHypercube
    extends AbstractIntegerHypercube
{
    /**
     * The array(s) which we wrap.
     */
    private final int[] myElements;

    /**
     * Get the {@link Dimension}s of the given array.
     */
    private static Dimension<?>[] getDimensions(final int[] array)
    {
        final long[] shape = new long[1];
        shape[0] = Math.max(shape[0], array.length);
        return Dimension.of(shape);
    }

    /**
     * Constructor.
     */
    public Integer1dWrappingHypercube(final int[] array)
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
     * the result of {@code 0}.
     */
    @Override
    public int weakGet(final long... indices)
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
            return 0;
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
    public void weakSet(final int value, final long... indices)
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
     * the result of {@code 0}.
     */
    @Override
    public int weakGetAt(long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index0 = index;

        if (index0 >= myElements.length) {
            return 0;
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
    public void weakSetAt(long index, final int value)
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
    public Integer weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return Integer.valueOf(weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Integer value)
        throws IndexOutOfBoundsException
    {
        weakSetAt(index, (value == null) ? 0 : value.intValue());
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

// [[[end]]] (checksum: b369eaba76e72949481ecea732237a14)
