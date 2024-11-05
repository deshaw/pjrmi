package com.deshaw.hypercube;

// Recreate with `cog -rc IntegerSparseHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_sparse_hypercube
//
//     cog.outl(primitive_sparse_hypercube.generate(numpy.int32))
// ]]]
import com.deshaw.util.concurrent.LongToLongConcurrentCuckooHashMap;
import com.deshaw.util.concurrent.LongToLongConcurrentCuckooHashMap.Iterator;

import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has int values as its elements and stores them
 * in a sparse map.
 *
 * <p>The capacity of these sparse cubes currently maxes out somewhere around
 * 5e8 elements. The actual limit will depend on the distribution of your
 * entries in the cube.
 */
public class IntegerSparseHypercube
    extends AbstractIntegerHypercube
{
    /**
     * The map which we use to store the values.
     */
    private final LongToLongConcurrentCuckooHashMap myMap;

    /**
     * The {@code int} null value as a {@code long}.
     */
    private final long myNull;

    // ----------------------------------------------------------------------

    // Some simple mapping functions; named like this for ease of cogging.
    // They should be trivially inlined by the JVM.

    /**
     * Convert a {@code long} to a {@code double}.
     */
    private static double long2double(final long v)
    {
        return Double.longBitsToDouble(v);
    }

    /**
     * Convert a {@code double} to a {@code long}.
     */
    private static long double2long(final double v)
    {
        return Double.doubleToRawLongBits(v);
    }

    /**
     * Convert a {@code long} to a {@code float}.
     */
    private static float long2float(final long v)
    {
        return Float.intBitsToFloat((int)(v & 0xffffffffL));
    }

    /**
     * Convert a {@code float} to a {@code long}.
     */
    private static long float2long(final float v)
    {
        return ((long)Float.floatToRawIntBits(v)) & 0xffffffffL;
    }

    /**
     * Convert a {@code long} to an {@code int}.
     */
    private static int long2int(final long v)
    {
        return (int)v;
    }

    /**
     * Convert an {@code int} to a {@code long}.
     */
    private static long int2long(final int v)
    {
        return v;
    }

    /**
     * Convert a {@code long} to a {@code long}.
     */
    private static long long2long(final long v)
    {
        return v;
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor with a default {@code null} value, and a loading of
     * {@code 0.1}.
     */
    public IntegerSparseHypercube(final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               NullPointerException
    {
        this(dimensions, 0, 0.1);
    }

    /**
     * Constructor with a given {@code null} value and loading.
     *
     * @param nullValue  The value used to for missing entries.
     * @param loading    The value used to determine the initial backing space
     *                   capacity as a function of the logical size of the
     *                   hypercube.
     */
    public IntegerSparseHypercube(
        final Dimension<?>[] dimensions,
        final int nullValue,
        final double loading
    )
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions);

        if (Double.isNaN(loading)) {
            throw new IllegalArgumentException("Given a NaN loading value");
        }
        final int capacity =
            (int)Math.max(13,
                          Math.min(Integer.MAX_VALUE,
                                   getSize() * Math.max(0.0, Math.min(1.0, loading))));
        myMap = new LongToLongConcurrentCuckooHashMap(capacity);
        myNull = int2long(nullValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattenedObjs(final long srcPos,
                                final Integer[] dst,
                                final int dstPos,
                                final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dst=" + dst + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        preRead();
        for (int i=0; i < length; i++) {
            dst[dstPos + i] = long2int(myMap.get(srcPos + i));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattenedObjs(final Integer[] src,
                                  final int srcPos,
                                  final long dstPos,
                                  final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Unflattening with " +
                "src=" + src + " srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Check input
        checkUnflattenArgs(srcPos, dstPos, length);
        if (src == null) {
            throw new NullPointerException("Given a null sparse");
        }
        if (src.length - srcPos < length) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the sparse size, " + src.length
            );
        }

        // Safe to copy in
        for (int i=0; i < length; i++) {
            final Integer value = src[srcPos + i];
            mapPut(
                dstPos + i,
                int2long(
                    (value == null) ? 0 : value.intValue()
                )
            );
        }
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final int[] dst,
                            final int       dstPos,
                            final int       length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dst=" + dst + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        preRead();
        for (int i=0; i < length; i++) {
            dst[dstPos + i] = long2int(myMap.get(srcPos + i, myNull));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattened(final int[] src,
                              final int       srcPos,
                              final long      dstPos,
                              final int       length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Unflattening with " +
                "src=" + src + " srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Sanitise input
        checkUnflattenArgs(srcPos, dstPos, length);
        if (src == null) {
            throw new NullPointerException("Given a null sparse");
        }

        // Safe to copy in
        for (int i=0; i < length; i++) {
            mapPut(dstPos + i, int2long(src[srcPos + i]));
        }
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final int d, final long... indices)
        throws IndexOutOfBoundsException
    {
        weakSetAt(toOffset(indices), d);
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
    public int weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " was outside the range of the cube's size, " + getSize()
            );
        }
        return long2int(myMap.get(index, myNull));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final int value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " was outside the range of the cube's size, " + getSize()
            );
        }
        mapPut(index, int2long(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      false);
        result.put("behaved",      false);
        result.put("c_contiguous", false);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }

   /**
    * Put a value into the map, in such a way that understand null values.
    */
    private void mapPut(final long index, final long value)
    {
        // If we happen to be inserting a null then that really means we are
        // removing an entry from the sparse map
        if (value == myNull) {
            myMap.remove(index);
        }
        else {
            myMap.put(index, value);
        }
    }
}

// [[[end]]] (checksum: 3d8873b2296eb051953ae2204af134ed)
