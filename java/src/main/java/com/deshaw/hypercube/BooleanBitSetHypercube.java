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
    private final AtomicReferenceArray<BitSet> myElements;

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
        myElements = new AtomicReferenceArray<>(numBitsets);
        final int maxIdx = (int)(MAX_BITSET_SIZE-1);
        for (int i=0; i < myElements.length(); i++) {
            final BitSet elements = allocForIndex(i);
            elements.clear();
            elements.set(maxIdx, true );
            elements.set(maxIdx, false);
            myElements.set(i, elements);
        }
    }

    /**
     * Constructor.
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
        myElements = new AtomicReferenceArray<>(numBitsets);
        for (int i=0; i < numBitsets; i++) {
            myElements.set(i, allocForIndex(i));
        }

        // There will never be more elements than MAX_BITSET_SIZE so all these
        // will fit in the first one.
        assert(elements.size() <= MAX_BITSET_SIZE);
        for (int i=0; i < elements.size(); i++) {
            final Boolean value = elements.get(i);
            myElements.get(0).set(i, (value != null && value));
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
            final BitSet bitset = myElements.get((int)(pos >>> MAX_BITSET_SHIFT));
            final boolean b =
                (bitset != null && bitset.get((int)(pos & MAX_BITSET_MASK)));
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
            BitSet bitset = myElements.get(idx);
            if (bitset == null) {
                bitset = allocForIndex(idx);
                if (!myElements.compareAndSet(idx, null, bitset)) {
                    bitset = myElements.get(idx);
                }
            }
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
            final BitSet bitset = myElements.get((int)(pos >>> MAX_BITSET_SHIFT));
            final boolean b =
                (bitset != null && bitset.get((int)(pos & MAX_BITSET_MASK)));
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
            BitSet bitset = myElements.get(idx);
            if (bitset == null) {
                bitset = allocForIndex(idx);
                if (!myElements.compareAndSet(idx, null, bitset)) {
                    bitset = myElements.get(idx);
                }
            }
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
    public boolean get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final boolean d, final long... indices)
        throws IndexOutOfBoundsException
    {
        setAt(toOffset(indices), d);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        preRead();
        return getAt(index);
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
    public boolean getAt(final long index)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " not within bounds [0.." + size + ")"
            );
        }

        preRead();
        final BitSet bitset = myElements.get((int)(index >>> MAX_BITSET_SHIFT));
        return (bitset != null && bitset.get((int)(index & MAX_BITSET_MASK)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(
                "Index " + index + " not within bounds [0.." + size + ")"
            );
        }

        final int idx = (int)(index >>> MAX_BITSET_SHIFT);
        BitSet bitset = myElements.get(idx);
        if (bitset == null) {
            bitset = allocForIndex(idx);
            if (!myElements.compareAndSet(idx, null, bitset)) {
                bitset = myElements.get(idx);
            }
        }
        bitset.set((int)(index & MAX_BITSET_MASK), value);
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
     * Allocate a bitset for the given myElements index.
     */
    private BitSet allocForIndex(final int index)
    {
        // The last bitset in the list of bitsets might not be an exact multiple
        // of MAX_BITSET_SIZE so we look to account for that. We compute its
        // length as the 'tail' value. We force a full allocation by setting the
        // final bit in each bitset
        final long tail = (size & MAX_BITSET_MASK);
        final int  sz   = (tail == 0 || index+1 < myElements.length())
                              ? (int)MAX_BITSET_SIZE
                              : (int)tail;
        final BitSet result = new BitSet(sz);
        result.set(sz-1, true );
        result.set(sz-1, false);
        return result;
    }
}
