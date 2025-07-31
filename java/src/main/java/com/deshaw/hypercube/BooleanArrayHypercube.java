package com.deshaw.hypercube;

// Recreate with `cog -rc BooleanArrayHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_array_hypercube
//
//     cog.outl(primitive_array_hypercube.generate(numpy.bool_))
// ]]]
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;

/**
 * A hypercube which has boolean values as its elements and stores them
 * in a 1D array of effectively infinite size.
 */
public class BooleanArrayHypercube
    extends AbstractBooleanHypercube
{
    /**
     * An empty array of booleans.
     */
    private static final boolean[] EMPTY = new boolean[0];

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
    private final boolean[][] myElements;

    /**
     * The first array in myElements. This is optimistically here to avoid an
     * extra hop through memory for accesses to smaller cubes.
     */
    private final boolean[] myElements0;

    /**
     * Give back a dense 1D {@code boolean} hypercube of the which
     * directly wraps the given array. No copying is done.
     *
     * @throws IllegalArgumentException if the array is too big to be wrapped.
     */
    public static BooleanHypercube wrap(final boolean[] elements)
        throws IllegalArgumentException
    {
        return wrap(elements, Dimension.of(elements.length));
    }

    /**
     * Give back a dense {@code boolean} hypercube of the given shape
     * which directly wraps the given array. No copying is done.
     *
     * @throws IllegalArgumentException if the array is too big to be wrapped or
     *                                  the dimensions are inconsistent.
     */
    public static BooleanHypercube wrap(final boolean[] elements,
                                              final Dimension<?>[] dimensions)
        throws IllegalArgumentException
    {
        return new BooleanArrayHypercube(dimensions, elements, false);
    }

    /**
     * Give back a dense {@code boolean} hypercube of the given shape
     * which directly wraps the given array. No copying is done.
     *
     * @throws IllegalArgumentException if the array is too big to be wrapped or
     *                                  the dimensions are inconsistent.
     */
    public static BooleanHypercube wrap(final boolean[] elements,
                                              final long... shape)
        throws IllegalArgumentException
    {
        return new BooleanArrayHypercube(Dimension.of(shape), elements, false);
    }

    /**
     * Give back a dense {@code boolean} hypercube of the given
     * shape.
     */
    public static BooleanHypercube of(final long... shape)
    {
        return new BooleanArrayHypercube(Dimension.of(shape));
    }

    /**
     * Constructor.
     */
    public BooleanArrayHypercube(final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions);

        int numArrays = (int)(size >>> MAX_ARRAY_SHIFT);
        if (numArrays * MAX_ARRAY_SIZE < size) {
            numArrays++;
        }

        myElements = new boolean[numArrays][];
        for (int i=0; i < myElements.length; i++) {
            final boolean[] elements = allocForIndex(i);
            Arrays.fill(elements, false);
            myElements[i] = elements;
        }
        myElements0 = (myElements.length == 0) ? EMPTY : myElements[0];
    }

    /**
     * Constructor copying from the given elements in flattened form.
     *
     * @throws IllegalArgumentException if the dimensions are inconsistent.
     */
    @SuppressWarnings("unchecked")
    public BooleanArrayHypercube(final Dimension<?>[] dimensions,
                                       final List<Boolean> elements)
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
        myElements = new boolean[numArrays][];
        for (int i=0; i < numArrays; i++) {
            myElements[i] = allocForIndex(i);
        }
        myElements0 = (myElements.length == 0) ? EMPTY : myElements[0];

        // Populate
        for (int i=0; i < elements.size(); i++) {
            final Boolean value = elements.get(i);
            myElements[(int)(i >>> MAX_ARRAY_SHIFT)][(int)(i & MAX_ARRAY_MASK)] =
                (value == null) ? false
                                : value.booleanValue();
        }
    }

    /**
     * Constructor copying from the given elements in flattened form.
     *
     * @throws IllegalArgumentException if the dimensions are inconsistent.
     */
    @SuppressWarnings("unchecked")
    public BooleanArrayHypercube(final Dimension<?>[] dimensions,
                                       final boolean[] elements)
        throws IllegalArgumentException,
               NullPointerException
    {
        this(dimensions, elements, true);
    }

    /**
     * Constructor from the given elements in flattened form.
     *
     * @param dimensions The shape of the cube.
     * @param elements   The source elements to populate the cube with.
     * @param copy       Whether to copy out the elements or to directly wrap
     *                   the instance.
     *
     * @throws IllegalArgumentException if the dimensions are inconsistent.
     */
    private BooleanArrayHypercube(final Dimension<?>[] dimensions,
                                        final boolean[] elements,
                                        final boolean copy)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions);

        if (elements.length != size) {
            throw new IllegalArgumentException(
                "Number of elements, " + elements.length + ", " +
                "does not match expected size, " + size + " " +
                "for dimensions " + Arrays.toString(dimensions)
            );
        }

        int numArrays = (int)(size >>> MAX_ARRAY_SHIFT);
        if (numArrays * MAX_ARRAY_SIZE < size) {
            numArrays++;
        }
        myElements = new boolean[numArrays][];

        if (copy) {
            for (int i=0; i < numArrays; i++) {
                myElements[i] = allocForIndex(i);
            }
            myElements0 = (myElements.length == 0) ? EMPTY : myElements[0];

            // Populate
            for (int i=0; i < elements.length; i++) {
                myElements[(int)(i >>> MAX_ARRAY_SHIFT)][(int)(i & MAX_ARRAY_MASK)] =
                    elements[i];
            }
        }
        else {
            if (elements.length > MAX_ARRAY_SIZE) {
                throw new IllegalArgumentException(
                    "Can't wrap an array of size " + elements.length + " " +
                    "which is greater than max size of " + MAX_ARRAY_SIZE
                );
            }
            myElements[0] = myElements0 = elements;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fill(final boolean v)
    {
        for (int i=0; i < myElements.length; i++) {
            Arrays.fill(myElements[i], v);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattenedObjs(final long srcPos,
                                final Boolean[] dst,
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
            final boolean[] array = myElements[(int)(pos >>> MAX_ARRAY_SHIFT)];
            final boolean d = array[(int)(pos & MAX_ARRAY_MASK)];
            dst[dstPos + i] = Boolean.valueOf(d);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattenedObjs(final Boolean[] src,
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
            boolean[] array = myElements[idx];
            final Boolean value = src[srcPos + i];
            array[(int)(pos & MAX_ARRAY_MASK)] =
                (value == null) ? false : value.booleanValue();
        }
        postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final boolean[] dst,
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
            final boolean[] array = myElements[startIdx];
            switch (length) {
            case 0:
                // NOP
                break;

            case 1:
                // Just the one element
                dst[dstPos] = array[(int)(srcPos & MAX_ARRAY_MASK)];
                break;

            default:
                // Standard copy within the same sub-array
                System.arraycopy(array, (int)(srcPos & MAX_ARRAY_MASK),
                                 dst, dstPos,
                                 length);
            }
        }
        else {
            // Split into two copies
            final boolean[] startArray = myElements[startIdx];
            final boolean[] endArray   = myElements[  endIdx];
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattened(final boolean[] src,
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
            boolean[] array = myElements[startIdx];

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
            boolean[] startArray = myElements[startIdx];
            boolean[] endArray   = myElements[  endIdx];

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
     * Copy the contents of given cube into this one.
     *
     * @throws IllegalArgumentException if the given cube was not compatible for
     *                                  some reason.
     */
    public void copyFrom(final BooleanArrayHypercube that)
    {
        if (that == null) {
            throw new IllegalArgumentException("Given a null cube to copy from");
        }
        if (!matches(that)) {
            throw new IllegalArgumentException("Given cube is not compatible");
        }

        for (int i=0; i < myElements.length; i++) {
            System.arraycopy(that.myElements[i], 0,
                             this.myElements[i], 0,
                             that.myElements[i].length);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final boolean d, final long... indices)
        throws IndexOutOfBoundsException
    {
        weakSetAt(toOffset(indices), d);
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
    public boolean weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < MAX_ARRAY_SIZE) {
            return myElements0[(int)index];
        }
        else {
            final boolean[] array = myElements[(int)(index >>> MAX_ARRAY_SHIFT)];
            return array[(int)(index & MAX_ARRAY_MASK)];
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        if (index < MAX_ARRAY_SIZE) {
            myElements0[(int)index] = value;
        }
        else {
            boolean[] array = myElements[(int)(index >>> MAX_ARRAY_SHIFT)];
            array[(int)(index & MAX_ARRAY_MASK)] = value;
        }
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
    private boolean[] allocForIndex(final int index)
    {
        // The last array in the list of arrays might not be an exact multiple
        // of MAX_ARRAY_SIZE so we look to account for that. We compute its
        // length as the 'tail' value.
        final long tail = (size & MAX_ARRAY_MASK);
        final int  sz   = (tail == 0 || index+1 < myElements.length)
                              ? (int)MAX_ARRAY_SIZE
                              : (int)tail;
        return new boolean[sz];
    }
}

// [[[end]]] (checksum: d55d95cbba5d5fc6d92a53ebcbd814fa)
