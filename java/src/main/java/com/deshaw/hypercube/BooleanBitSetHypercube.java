package com.deshaw.hypercube;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;

/**
 * A hypercube which has boolean values as its elements and stores them in an
 * bitset of effectively infinite size.
 */
public class BooleanBitSetHypercube
    extends AbstractBooleanHypercube
{
    /**
     * An empty BitSet.
     */
    private static final BitSet EMPTY = new BitSet(0);

    /**
     * The shift for our max bitset size.
     */
    private static final int MAX_BITSET_SHIFT = 30;

    /**
     * The largest bitset size.
     */
    private static final long MAX_BITSET_SIZE = (1L << MAX_BITSET_SHIFT);

    /**
     * The mask for bitset index tweaking.
     */
    private static final long MAX_BITSET_MASK = MAX_BITSET_SIZE - 1;

    /**
     * The array-of-bitsets of elements which we hold. We have multiple bitsets
     * since we might have a size which is larger than what can be represented
     * by a single bitset. (I.e. more than 2^31 elements.)
     *
     * The BitSets aren't technically threadsafe since they can reallocate their
     * underlying storage when you call set on them. So we force a full
     * allocation which will help to guard against that.
     */
    private final BitSet[] myElements;

    /**
     * The first array in myElements. This is optimistically here to avoid an
     * extra hop through memory for accesses to smaller cubes.
     */
    private final BitSet myElements0;

    /**
     * Give back a dense 1D {@code boolean} hypercube of the which directly
     * wraps the given {@link BitSet}. No copying is done.
     *
     * @throws IllegalArgumentException if the {@link BitSet} is too big to be
     *                                  wrapped.
     */
    public static BooleanHypercube wrap(final BitSet elements)
        throws IllegalArgumentException
    {
        return wrap(elements, Dimension.of(elements.size()));
    }

    /**
     * Give back a dense {@code boolean} hypercube of the given shape which
     * directly wraps the given {@link BitSet}. No copying is done.
     *
     * @throws IllegalArgumentException if the {@link BitSet} is too big to be
     *                                  wrapped or the dimensions are inconsistent.
     */
    public static BooleanHypercube wrap(final BitSet elements,
                                        final Dimension<?>[] dimensions)
        throws IllegalArgumentException
    {
        return new BooleanBitSetHypercube(dimensions, elements, false);
    }

    /**
     * Give back a dense {@code boolean} hypercube of the given shape
     * which directly wraps the given {@link BitSet}. No copying is done.
     *
     * @throws IllegalArgumentException if the {@link BitSet} is too big to be
     *                                  wrapped or the dimensions are inconsistent.
     */
    public static BooleanHypercube wrap(final BitSet elements,
                                        final long... shape)
        throws IllegalArgumentException
    {
        return new BooleanBitSetHypercube(Dimension.of(shape), elements, false);
    }

    /**
     * Give back a dense {@code boolean} hypercube of the given shape.
     */
    public static BooleanHypercube of(final long... shape)
    {
        return new BooleanBitSetHypercube(Dimension.of(shape));
    }

    /**
     * Constructor.
     */
    public BooleanBitSetHypercube(final Dimension<?>[] dimensions)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions);

        int numBitsets = (int)(size >>> MAX_BITSET_SHIFT);
        if (numBitsets * MAX_BITSET_SIZE < size) {
            numBitsets++;
        }

        // Force a full allocation by setting the final bit in each bitset
        myElements = new BitSet[numBitsets];
        final int maxIdx = (int)(MAX_BITSET_SIZE-1);
        for (int i=0; i < myElements.length; i++) {
            final BitSet elements = allocForIndex(i);
            elements.clear();
            elements.set(maxIdx, true );
            elements.set(maxIdx, false);
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
    public BooleanBitSetHypercube(final Dimension<?>[] dimensions,
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

        int numBitsets = (int)(size >>> MAX_BITSET_SHIFT);
        if (numBitsets * MAX_BITSET_SIZE < size) {
            numBitsets++;
        }
        myElements = new BitSet[numBitsets];
        for (int i=0; i < numBitsets; i++) {
            myElements[i] = allocForIndex(i);
        }
        myElements0 = (myElements.length == 0) ? EMPTY : myElements[0];

        // There will never be more elements than MAX_BITSET_SIZE so all these
        // will fit in the first one.
        assert(elements.size() <= MAX_BITSET_SIZE);
        for (int i=0; i < elements.size(); i++) {
            final Boolean value = elements.get(i);
            myElements[(int)(i >>> MAX_BITSET_SHIFT)].set(
                (int)(i & MAX_BITSET_MASK),
                (value != null && value)
            );
        }
    }

    /**
     * Constructor copying from the given elements in flattened form.
     *
     * @throws IllegalArgumentException if the dimensions are inconsistent.
     */
    public BooleanBitSetHypercube(final Dimension<?>[] dimensions,
                                  final boolean[] elements)
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

        int numBitsets = (int)(size >>> MAX_BITSET_SHIFT);
        if (numBitsets * MAX_BITSET_SIZE < size) {
            numBitsets++;
        }
        myElements = new BitSet[numBitsets];
        for (int i=0; i < numBitsets; i++) {
            myElements[i] = allocForIndex(i);
        }
        myElements0 = (myElements.length == 0) ? EMPTY : myElements[0];

        // There will never be more elements than MAX_BITSET_SIZE so all these
        // will fit in the first one.
        assert(elements.length <= MAX_BITSET_SIZE);
        for (int i=0; i < elements.length; i++) {
            myElements[(int)(i >>> MAX_BITSET_SHIFT)].set(
                (int)(i & MAX_BITSET_MASK),
                elements[i]
            );
        }
    }

    /**
     * Constructor copying from the given elements in flattened form.
     *
     * @throws IllegalArgumentException if the dimensions are inconsistent.
     */
    public BooleanBitSetHypercube(final Dimension<?>[] dimensions,
                                  final BitSet elements)
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
    private BooleanBitSetHypercube(final Dimension<?>[] dimensions,
                                   final BitSet elements,
                                   final boolean copy)
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

        int numBitsets = (int)(size >>> MAX_BITSET_SHIFT);
        if (numBitsets * MAX_BITSET_SIZE < size) {
            numBitsets++;
        }
        myElements = new BitSet[numBitsets];

        if (copy) {
            for (int i=0; i < numBitsets; i++) {
                myElements[i] = allocForIndex(i);
            }
            myElements0 = (myElements.length == 0) ? EMPTY : myElements[0];

            // There will never be more elements than MAX_BITSET_SIZE so all these
            // will fit in the first one.
            assert(elements.size() <= MAX_BITSET_SIZE);
            for (int i=0; i < elements.size(); i++) {
                myElements[(int)(i >>> MAX_BITSET_SHIFT)].set(
                    (int)(i & MAX_BITSET_MASK),
                    elements.get(i)
                );
            }
        }
        else {
            if (elements.size() > MAX_BITSET_SIZE) {
                throw new IllegalArgumentException(
                    "Can't wrap an array of size " + elements.size() + " " +
                    "which is greater than max size of " + MAX_BITSET_SIZE
                );
            }
            myElements[0] = myElements0 = elements;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattenedObjs(final long      srcPos,
                                final Boolean[] dst,
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
            final long pos = srcPos + i;
            final BitSet bitset = myElements[(int)(pos >>> MAX_BITSET_SHIFT)];
            final boolean b = bitset.get((int)(pos & MAX_BITSET_MASK));
            dst[dstPos + i] = b;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattenedObjs(final Boolean[] src,
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
            final int  idx = (int)(pos >>> MAX_BITSET_SHIFT);
            final BitSet bitset = myElements[idx];
            final Boolean value = src[srcPos + i];
            bitset.set((int)(pos & MAX_BITSET_MASK),
                       (value != null && value));
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
        for (int i=0; i < length; i++) {
            final long pos = srcPos + i;
            final BitSet bitset = myElements[(int)(pos >>> MAX_BITSET_SHIFT)];
            final boolean b = bitset.get((int)(pos & MAX_BITSET_MASK));
            dst[dstPos + i] = b;
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

        // Safe to copy in
        for (int i=0; i < length; i++) {
            final long pos = dstPos + i;
            final int  idx = (int)(pos >>> MAX_BITSET_SHIFT);
            final BitSet bitset = myElements[idx];
            final boolean value = src[srcPos + i];
            bitset.set((int)(pos & MAX_BITSET_MASK), value);
        }
        postWrite();
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
        return weakGetAt(index);
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
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " not within bounds [0.." + size + ")"
            );
        }

        if (index < MAX_BITSET_SIZE) {
            return myElements0.get((int)index);
        }
        else {
            final BitSet bitset = myElements[(int)(index >>> MAX_BITSET_SHIFT)];
            return bitset.get((int)(index & MAX_BITSET_MASK));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " not within bounds [0.." + size + ")"
            );
        }

        if (index < MAX_BITSET_SIZE) {
            myElements0.set((int)index, value);
        }
        else {
            final int idx = (int)(index >>> MAX_BITSET_SHIFT);
            final BitSet bitset = myElements[idx];
            bitset.set((int)(index & MAX_BITSET_MASK), value);
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
     * Allocate a bitset for the given myElements index.
     */
    private BitSet allocForIndex(final int index)
    {
        // The last bitset in the list of bitsets might not be an exact multiple
        // of MAX_BITSET_SIZE so we look to account for that. We compute its
        // length as the 'tail' value. We force a full allocation by setting the
        // final bit in each bitset
        final long tail = (size & MAX_BITSET_MASK);
        final int  sz   = (tail == 0 || index+1 < myElements.length)
                              ? (int)MAX_BITSET_SIZE
                              : (int)tail;
        final BitSet result = new BitSet(sz);
        result.set(sz-1, true );
        result.set(sz-1, false);
        return result;
    }
}
