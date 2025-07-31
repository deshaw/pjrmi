package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleSparseHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_sparse_hypercube
//
//     cog.outl(primitive_sparse_hypercube.generate(numpy.float64))
// ]]]
import com.deshaw.util.concurrent.LongToLongConcurrentCuckooHashMap;
import com.deshaw.util.concurrent.LongToLongConcurrentCuckooHashMap.Iterator;

import java.util.Map;
import java.util.logging.Level;

/**
 * A hypercube which has double values as its elements and stores them
 * in a sparse map.
 *
 * <p>The capacity of these sparse cubes currently maxes out somewhere around
 * 5e8 elements. The actual limit will depend on the distribution of your
 * entries in the cube.
 */
public class DoubleSparseHypercube
    extends AbstractDoubleHypercube
{
    /**
     * The map which we use to store the values.
     */
    private final LongToLongConcurrentCuckooHashMap myMap;

    /**
     * The {@code double} null value as a {@code long}.
     */
    private final long myNull;

    // ----------------------------------------------------------------------

    /**
     * Give back a sparse {@code double} hypercube of the given.
     * shape.
     */
    public static DoubleHypercube of(final long... shape)
    {
        return new DoubleSparseHypercube(Dimension.of(shape));
    }

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
    public DoubleSparseHypercube(final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               NullPointerException
    {
        this(dimensions, Double.NaN, 0.1);
    }

    /**
     * Constructor with a given {@code null} value and loading.
     *
     * @param nullValue  The value used to for missing entries.
     * @param loading    The value used to determine the initial backing space
     *                   capacity as a function of the logical size of the
     *                   hypercube.
     */
    public DoubleSparseHypercube(
        final Dimension<?>[] dimensions,
        final double nullValue,
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
        myNull = double2long(nullValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattenedObjs(final long srcPos,
                                final Double[] dst,
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
            dst[dstPos + i] = long2double(myMap.get(srcPos + i));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattenedObjs(final Double[] src,
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
            final Double value = src[srcPos + i];
            mapPut(
                dstPos + i,
                double2long(
                    (value == null) ? Double.NaN : value.doubleValue()
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
                            final double[] dst,
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
            dst[dstPos + i] = long2double(myMap.get(srcPos + i, myNull));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattened(final double[] src,
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
            mapPut(dstPos + i, double2long(src[srcPos + i]));
        }
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final double d, final long... indices)
        throws IndexOutOfBoundsException
    {
        weakSetAt(toOffset(indices), d);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return Double.valueOf(weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Double value)
        throws IndexOutOfBoundsException
    {
        weakSetAt(index, (value == null) ? Double.NaN : value.doubleValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " was outside the range of the cube's size, " + getSize()
            );
        }
        return long2double(myMap.get(index, myNull));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final double value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= getSize()) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " was outside the range of the cube's size, " + getSize()
            );
        }
        mapPut(index, double2long(value));
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

// [[[end]]] (checksum: a9b600661ba0cb096230fe856eb3913c)
