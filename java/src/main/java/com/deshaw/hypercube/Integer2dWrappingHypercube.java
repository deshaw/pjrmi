package com.deshaw.hypercube;

// Recreate with `cog -rc Integer2dWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_nd_wrapping_hypercube
//
//     cog.outl(primitive_nd_wrapping_hypercube.generate(2, numpy.int32))
// ]]]
import java.util.Map;

/**
 * A 2-dimensional hypercube which has {@code int} values
 * as its elements and is backed by a user-supplied array.
 */
public class Integer2dWrappingHypercube
    extends AbstractIntegerHypercube
{
    /**
     * The array(s) which we wrap.
     */
    private final int[][] myElements;

    /**
     * Get the {@link Dimension}s of the given array.
     */
    private static Dimension<?>[] getDimensions(final int[][] array)
    {
        final long[] shape = new long[2];
        shape[0] = Math.max(shape[0], array.length);
        for (int[] array1 : array) {
            shape[1] = Math.max(shape[1], array1.length);
        }
        return Dimension.of(shape);
    }

    /**
     * Constructor.
     */
    public Integer2dWrappingHypercube(final int[][] array)
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
        if (indices.length != 2) {
            throw new IndexOutOfBoundsException(
                "Wanted 2 indices but had " + indices.length
            );
        }
        final long index1 = indices[1];
        if (index1 < 0 || index1 >= length(1)) {
            throw new IndexOutOfBoundsException(
                "Bad index[1]: " + index1
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
        final int[] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return 0;
        }

        return array1[(int)index1];
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
        if (indices.length != 2) {
            throw new IndexOutOfBoundsException(
                "Wanted 2 indices but had " + indices.length
            );
        }
        final long index1 = indices[1];
        if (index1 < 0 || index1 >= length(1)) {
            throw new IndexOutOfBoundsException(
                "Bad index[1]: " + index1
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
        final int[] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return;
        }

        array1[(int)index1] = value;
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
        final long index1 = (index % length(1)); index /= length(1);
        final long index0 = index;

        if (index0 >= myElements.length) {
            return 0;
        }
        final int[] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return 0;
        }

        return array1[(int)index1];
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
        final long index1 = (index % length(1)); index /= length(1);
        final long index0 = index;

        if (index0 >= myElements.length) {
            return;
        }
        final int[] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return;
        }

        array1[(int)index1] = value;
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

// [[[end]]] (checksum: 33741bae25780c5feb27430590c5a5e9)
