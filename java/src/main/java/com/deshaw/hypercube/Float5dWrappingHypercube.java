package com.deshaw.hypercube;

// Recreate with `cog -rc Float5dWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_nd_wrapping_hypercube
//
//     cog.outl(primitive_nd_wrapping_hypercube.generate(5, numpy.float32))
// ]]]
import java.util.Map;

/**
 * A 5-dimensional hypercube which has {@code float} values
 * as its elements and is backed by a user-supplied array.
 */
public class Float5dWrappingHypercube
    extends AbstractFloatHypercube
{
    /**
     * The array(s) which we wrap.
     */
    private final float[][][][][] myElements;

    /**
     * Get the {@link Dimension}s of the given array.
     */
    private static Dimension<?>[] getDimensions(final float[][][][][] array)
    {
        final long[] shape = new long[5];
        shape[0] = Math.max(shape[0], array.length);
        for (float[][][][] array1 : array) {
            shape[1] = Math.max(shape[1], array1.length);
            for (float[][][] array2 : array1) {
                shape[2] = Math.max(shape[2], array2.length);
                for (float[][] array3 : array2) {
                    shape[3] = Math.max(shape[3], array3.length);
                    for (float[] array4 : array3) {
                        shape[4] = Math.max(shape[4], array4.length);
                    }
                }
            }
        }
        return Dimension.of(shape);
    }

    /**
     * Constructor.
     */
    public Float5dWrappingHypercube(final float[][][][][] array)
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
     * the result of {@code Float.NaN}.
     */
    @Override
    public float weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != 5) {
            throw new IndexOutOfBoundsException(
                "Wanted 5 indices but had " + indices.length
            );
        }
        final long index4 = indices[4];
        if (index4 < 0 || index4 >= length(4)) {
            throw new IndexOutOfBoundsException(
                "Bad index[4]: " + index4
            );
        }
        final long index3 = indices[3];
        if (index3 < 0 || index3 >= length(3)) {
            throw new IndexOutOfBoundsException(
                "Bad index[3]: " + index3
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
            return Float.NaN;
        }
        final float[][][][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return Float.NaN;
        }
        final float[][][] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return Float.NaN;
        }
        final float[][] array3 = array2[(int)index2];

        if (index3 >= array3.length) {
            return Float.NaN;
        }
        final float[] array4 = array3[(int)index3];

        if (index4 >= array4.length) {
            return Float.NaN;
        }

        return array4[(int)index4];
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void weakSet(final float value, final long... indices)
        throws IndexOutOfBoundsException
    {
        if (indices == null) {
            throw new NullPointerException("Given null indices");
        }
        if (indices.length != 5) {
            throw new IndexOutOfBoundsException(
                "Wanted 5 indices but had " + indices.length
            );
        }
        final long index4 = indices[4];
        if (index4 < 0 || index4 >= length(4)) {
            throw new IndexOutOfBoundsException(
                "Bad index[4]: " + index4
            );
        }
        final long index3 = indices[3];
        if (index3 < 0 || index3 >= length(3)) {
            throw new IndexOutOfBoundsException(
                "Bad index[3]: " + index3
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
        final float[][][][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return;
        }
        final float[][][] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return;
        }
        final float[][] array3 = array2[(int)index2];

        if (index3 >= array3.length) {
            return;
        }
        final float[] array4 = array3[(int)index3];

        if (index4 >= array4.length) {
            return;
        }

        array4[(int)index4] = value;
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to get values where the underlying array is missing will yield
     * the result of {@code Float.NaN}.
     */
    @Override
    public float weakGetAt(long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index4 = (index % length(4)); index /= length(4);
        final long index3 = (index % length(3)); index /= length(3);
        final long index2 = (index % length(2)); index /= length(2);
        final long index1 = (index % length(1)); index /= length(1);
        final long index0 = index;

        if (index0 >= myElements.length) {
            return Float.NaN;
        }
        final float[][][][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return Float.NaN;
        }
        final float[][][] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return Float.NaN;
        }
        final float[][] array3 = array2[(int)index2];

        if (index3 >= array3.length) {
            return Float.NaN;
        }
        final float[] array4 = array3[(int)index3];

        if (index4 >= array4.length) {
            return Float.NaN;
        }

        return array4[(int)index4];
    }

    /**
     * {@inheritDoc}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void weakSetAt(long index, final float value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
        final long index4 = (index % length(4)); index /= length(4);
        final long index3 = (index % length(3)); index /= length(3);
        final long index2 = (index % length(2)); index /= length(2);
        final long index1 = (index % length(1)); index /= length(1);
        final long index0 = index;

        if (index0 >= myElements.length) {
            return;
        }
        final float[][][][] array1 = myElements[(int)index0];

        if (index1 >= array1.length) {
            return;
        }
        final float[][][] array2 = array1[(int)index1];

        if (index2 >= array2.length) {
            return;
        }
        final float[][] array3 = array2[(int)index2];

        if (index3 >= array3.length) {
            return;
        }
        final float[] array4 = array3[(int)index3];

        if (index4 >= array4.length) {
            return;
        }

        array4[(int)index4] = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Float weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return Float.valueOf(weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Float value)
        throws IndexOutOfBoundsException
    {
        weakSetAt(index, (value == null) ? Float.NaN : value.floatValue());
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

// [[[end]]] (checksum: 0f9382ac9a7df56803f891d1f196267a)
