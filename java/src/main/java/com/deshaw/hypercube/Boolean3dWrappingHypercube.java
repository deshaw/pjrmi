package com.deshaw.hypercube;

// Recreate with `cog -rc Boolean3dWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_nd_wrapping_hypercube
//
//     cog.outl(primitive_nd_wrapping_hypercube.generate(3, numpy.bool8))
// ]]]
import java.util.Map;

/**
 * A 3-dimensional hypercube which has {@code boolean} values
 * as its elements and is backed by a user-supplied array.
 */
public class Boolean3dWrappingHypercube
    extends AbstractBooleanHypercube
{
    /**
     * The array(s) which we wrap.
     */
    private final boolean[][][] myElements;

    /**
     * Get the {@link Dimension}s of the given array.
     */
    private static Dimension<?>[] getDimensions(final boolean[][][] array)
    {
        final long[] shape = new long[3];
        shape[0] = Math.max(shape[0], array.length);
        for (boolean[][] array1 : array) {
            shape[1] = Math.max(shape[1], array1.length);
            for (boolean[] array2 : array1) {
                shape[2] = Math.max(shape[2], array2.length);
            }
        }
        return Dimension.of(shape);
    }

    /**
     * Constructor.
     */
    public Boolean3dWrappingHypercube(final boolean[][][] array)
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
    public boolean weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != 3) {
            throw new IndexOutOfBoundsException(
                "Wanted 3 indices but had " + indices.length
            );
        }
        final long index2 = indices[2];
        if (index2 < 0 || index2 >= length(2)) {
            throw new IndexOutOfBoundsException(
                "Bad index[2]: " + index2
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
            return false;
        }
        final boolean[][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return false;
        }
        final boolean[] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return false;
        }

        return array2[(int)index2];
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void weakSet(final boolean value, final long... indices)
        throws IndexOutOfBoundsException
    {
        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != 3) {
            throw new IndexOutOfBoundsException(
                "Wanted 3 indices but had " + indices.length
            );
        }
        final long index2 = indices[2];
        if (index2 < 0 || index2 >= length(2)) {
            throw new IndexOutOfBoundsException(
                "Bad index[2]: " + index2
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
        final boolean[][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return;
        }
        final boolean[] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return;
        }

        array2[(int)index2] = value;
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to get values where the underlying array is missing will yield
     * the result of {@code false}.
     */
    @Override
    public boolean weakGetAt(long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index2 = (index % length(2)); index /= length(2);
        final long index1 = (index % length(1)); index /= length(1);
        final long index0 = index;

        if (index0 >= myElements.length) {
            return false;
        }
        final boolean[][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return false;
        }
        final boolean[] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return false;
        }

        return array2[(int)index2];
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void weakSetAt(long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index2 = (index % length(2)); index /= length(2);
        final long index1 = (index % length(1)); index /= length(1);
        final long index0 = index;

        if (index0 >= myElements.length) {
            return;
        }
        final boolean[][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return;
        }
        final boolean[] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return;
        }

        array2[(int)index2] = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return Boolean.valueOf(weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Boolean value)
        throws IndexOutOfBoundsException
    {
        weakSetAt(index, (value == null) ? false : value.booleanValue());
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

// [[[end]]] (checksum: e8d7e7de9de3195089585da37cedb91a)
