package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleArrayHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_array_hypercube
//
//     cog.outl(primitive_array_hypercube.generate(numpy.float64))
// ]]]
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;

/**
 * A hypercube which has double values as its elements and stores them
 * in a 1D array of effectively infinite size.
 */
public class DoubleArrayHypercube
    extends AbstractDoubleHypercube
{
    /**
     * The shift for the max array size.
     */
    private static final int MAX_ARRAY_SHIFT = 30;

    /**
     * The largest array size.
     */
    private static final long MAX_ARRAY_SIZE = (1L << MAX_ARRAY_SHIFT);

    /**
     * The mask for array index tweaking.
     */
    private static final long MAX_ARRAY_MASK = MAX_ARRAY_SIZE - 1;

    /**
     * The array-of-arrays of elements which we hold. We have multiple arrays
     * since we might have a size which is larger than what can be represented
     * by a single array. (I.e. more than 2^30 elements.)
     */
    private final AtomicReferenceArray<double[]> myElements;

    /**
     * Constructor.
     */
    public DoubleArrayHypercube(final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions);

        int numArrays = (int)(size >>> MAX_ARRAY_SHIFT);
        if (numArrays * MAX_ARRAY_SIZE < size) {
            numArrays++;
        }

        myElements = new AtomicReferenceArray<>(numArrays);
        for (int i=0; i < myElements.length(); i++) {
            final double[] elements = allocForIndex(i);
            Arrays.fill(elements, Double.NaN);
            myElements.set(i, elements);
        }
    }

    /**
     * Constructor.
     */
    @SuppressWarnings("unchecked")
    public DoubleArrayHypercube(final Dimension<?>[] dimensions,
                                       final List<Double> elements)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions);

        if (elements.size() != size) {
            throw new IllegalArgumentException(
                "Number of elements, " + elements.size() + ", " +
                "does not match expected size, " + size + " " +
                "for dimensions " + Arrays.toString(dimensions)
            );
        }

        int numArrays = (int)(size >>> MAX_ARRAY_SHIFT);
        if (numArrays * MAX_ARRAY_SIZE < size) {
            numArrays++;
        }
        myElements = new AtomicReferenceArray<>(numArrays);
        for (int i=0; i < numArrays; i++) {
            myElements.set(i, allocForIndex(i));
        }

        // There will never be more elements than MAX_ARRAY_SIZE so all these
        // will fit in the first one.
        assert(elements.size() <= MAX_ARRAY_SIZE);
        for (int i=0; i < elements.size(); i++) {
            final Double value = elements.get(i);
            myElements.get(0)[i] = (value == null) ? Double.NaN
                                                   : value.doubleValue();
       }
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
            final long pos = srcPos + i;
            final double[] array = myElements.get((int)(pos >>> MAX_ARRAY_SHIFT));
            final double d =
                (array == null) ? Double.NaN : array[(int)(pos & MAX_ARRAY_MASK)];
            dst[dstPos + i] = Double.valueOf(d);
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
            throw new NullPointerException("Given a null array");
        }
        if (src.length - srcPos < length) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the array size, " + src.length
            );
        }

        // Safe to copy in
        for (int i=0; i < length; i++) {
            final long pos = dstPos + i;
            final int  idx = (int)(pos >>> MAX_ARRAY_SHIFT);
            double[] array = myElements.get(idx);
            if (array == null) {
                array = allocForIndex(idx);
                myElements.set(idx, array);
            }
            final Double value = src[srcPos + i];
            array[(int)(pos & MAX_ARRAY_MASK)] =
                (value == null) ? Double.NaN : value.doubleValue();
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

        // These can never differ by more than 1 since length is an int. And the
        // end is non-inclusive since we're counting fence, not posts.
        preRead();
        final int startIdx = (int)((srcPos             ) >>> MAX_ARRAY_SHIFT);
        final int endIdx   = (int)((srcPos + length - 1) >>> MAX_ARRAY_SHIFT);
        if (startIdx == endIdx) {
            // What to copy? Try to avoid the overhead of the system call. If we are
            // striding through the cube then we may well have just the one.
            final double[] array = myElements.get(startIdx);
            switch (length) {
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                dst[dstPos] =
                    (array == null) ? Double.NaN : array[(int)(srcPos & MAX_ARRAY_MASK)];
                break;

            default:
                // Standard copy within the same sub-array
                if (array != null) {
                    System.arraycopy(
                        array, (int)(srcPos & MAX_ARRAY_MASK),
                        dst, dstPos,
                        length
                    );
                }
                else {
                    Arrays.fill(dst, dstPos, dstPos + length, Double.NaN);
                }
            }
        }
        else {
            // Split into two copies
            final double[] startArray = myElements.get(startIdx);
            final double[] endArray   = myElements.get(  endIdx);
            if (startArray != null && endArray != null) {
                final int startPos    = (int)(srcPos & MAX_ARRAY_MASK);
                final int startLength = length - (startArray.length - startPos);
                final int endLength   = length - startLength;
                System.arraycopy(startArray, startPos,
                                 dst,        dstPos,
                                 startLength);
                System.arraycopy(endArray,   0,
                                 dst,        dstPos + startLength,
                                 endLength);
            }
            else {
                Arrays.fill(dst, dstPos, dstPos + length, Double.NaN);
            }
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
            throw new NullPointerException("Given a null array");
        }

        // These can never differ by more than 1 since length is an int
        final int startIdx = (int)((dstPos             ) >>> MAX_ARRAY_SHIFT);
        final int endIdx   = (int)((dstPos + length - 1) >>> MAX_ARRAY_SHIFT);

        // What to copy? Try to avoid the overhead of the system call. If we are
        // striding through the cube then we may well have just the one.
        if (startIdx == endIdx) {
            // Get the array, creating if needbe
            double[] array = myElements.get(startIdx);
            if (array == null) {
                array = allocForIndex(startIdx);
                myElements.set(startIdx, array);
            }

            // And handle it
            switch (length) {
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                if (srcPos >= src.length) {
                    throw new IndexOutOfBoundsException(
                        "Source position, " + srcPos + ", " +
                        "plus length ," + length + ", " +
                        "was greater than the array size, " + src.length
                    );
                }
                array[(int)(dstPos & MAX_ARRAY_MASK)] = src[srcPos];
                break;

            default:
                // Standard copy within the same sub-array
                if (src.length - srcPos < length) {
                    throw new IndexOutOfBoundsException(
                        "Source position, " + srcPos + ", " +
                        "plus length ," + length + ", " +
                        "was greater than the array size, " + src.length
                    );
                }
                System.arraycopy(
                    src, srcPos,
                    array, (int)(dstPos & MAX_ARRAY_MASK),
                    length
                );
                break;
            }
        }
        else {
            // Split into two copies
            double[] startArray = myElements.get(startIdx);
            double[] endArray   = myElements.get(  endIdx);
            if (startArray == null) {
                startArray = allocForIndex(startIdx);
                myElements.set(startIdx, startArray);
            }
            if (endArray == null) {
                endArray = allocForIndex(endIdx);
                myElements.set(endIdx, endArray);
            }

            // And do the copy
            final int startPos    = (int)(dstPos & MAX_ARRAY_MASK);
            final int startLength = length - (startArray.length - startPos);
            final int endLength   = length - startLength;

            System.arraycopy(src,        srcPos,
                             startArray, startPos,
                             startLength);
            System.arraycopy(src,        srcPos + startLength,
                             endArray,   0,
                             endLength);
        }
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final double d, final long... indices)
        throws IndexOutOfBoundsException
    {
        setAt(toOffset(indices), d);
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        preRead();
        return Double.valueOf(getAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final Double value)
        throws IndexOutOfBoundsException
    {
        setAt(index, (value == null) ? Double.NaN : value.doubleValue());
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getAt(final long index)
        throws IndexOutOfBoundsException
    {
        preRead();
        final double[] array = myElements.get((int)(index >>> MAX_ARRAY_SHIFT));
        return (array == null) ? Double.NaN : array[(int)(index & MAX_ARRAY_MASK)];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final double value)
        throws IndexOutOfBoundsException
    {
        final int idx = (int)(index >>> MAX_ARRAY_SHIFT);
        double[] array = myElements.get(idx);
        if (array == null) {
            array = allocForIndex(idx);
            myElements.set(idx, array);
        }
        array[(int)(index & MAX_ARRAY_MASK)] = value;
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      true);
        result.put("behaved",      true);
        result.put("c_contiguous", true);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }

    /**
     * Allocate an array for the given myElements index.
     */
    private double[] allocForIndex(final int index)
    {
        // The last array in the list of arrays might not be an exact multiple
        // of MAX_ARRAY_SIZE so we look to account for that. We compute its
        // length as the 'tail' value.
        final long tail = (size & MAX_ARRAY_MASK);
        final int  sz   = (tail == 0 || index+1 < myElements.length())
                              ? (int)MAX_ARRAY_SIZE
                              : (int)tail;
        return new double[sz];
    }
}

// [[[end]]] (checksum: d6624c2fc22775908a33e2c5396ce0d0)
