package com.deshaw.hypercube;

// Recreate with `cog -rc LongFlatRolledHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_flat_rolled_hypercube
//
//     cog.outl(primitive_flat_rolled_hypercube.generate(numpy.int64))
// ]]]
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

import java.util.Map;
import java.util.logging.Level;

/**
 * A shifted cube which is a flat-rolled view of another long-based
 * {@link Hypercube}. Essentially, the given cube is first flattened, then
 * shifted, and finally unflattened into its original shape.
 */
public class LongFlatRolledHypercube
    extends FlatRolledHypercube<Long>
    implements LongHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final LongHypercube T;

    // -----------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private LongHypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to roll.
     * @param shift      How to roll this cube. The (signed) number of elements
     *                   to roll the given {@link Hypercube} by.
     *
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public LongFlatRolledHypercube(final LongHypercube hypercube,
                                        final long shift)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(hypercube, shift);

        T = new LongTransposedHypercube(this);
        myHypercube = hypercube;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new LongSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube rollFlat(final long shift)
    {
        // Combine the nested rolls more efficiently.
        if (getSize()                       == 0 ||
           (getShift() + shift) % getSize() == 0)
        {
            return myHypercube;
        }
        else if (shift % getSize() == 0) {
            return this;
        }
        else {
            return new LongFlatRolledHypercube(myHypercube, getShift() + shift);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new LongAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final long[] dst,
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

        // Note that the getShift() is always non-negative so we don't need to
        // worry about handling negative values.
        // The case where we have a single block in our wrapped cube.
        if (srcPos + length <= getShift() || srcPos >= getShift()) {
            myHypercube.toFlattened(offset(srcPos),
                                    dst,
                                    dstPos,
                                    length);
        }
        // The case where have 2 blocks in our wrapped cube.
        else {
            // The size of the tail of the wrapped cube we have to copy
            final int tail = (int)(getShift() - srcPos);
            myHypercube.toFlattened(offset(srcPos),
                                    dst,
                                    dstPos,
                                    tail);
            myHypercube.toFlattened(0,
                                    dst,
                                    dstPos + tail,
                                    length - tail);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattened(final long[] src,
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

        // Sanitise input
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

        // Note that the getShift() is always non-negative so we don't need to
        // worry about handling negative values.
        // The case where we have a single block in our wrapped cube.
        if (dstPos + length <= getShift() || dstPos >= getShift()) {
            myHypercube.fromFlattened(src,
                                      srcPos,
                                      offset(dstPos),
                                      length);
        }
        // The case where have 2 blocks in our wrapped cube.
        else {
            // The size of the tail of the wrapped cube we have to copy
            final int tail = (int)(getShift() - dstPos);
            myHypercube.fromFlattened(src,
                                      srcPos,
                                      offset(dstPos),
                                      tail);
            myHypercube.fromFlattened(src,
                                      srcPos + tail,
                                      0,
                                      length - tail);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final long value, final long... indices)
        throws IndexOutOfBoundsException
    {
        setAt(toOffset(indices), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getAt(final long index)
        throws IndexOutOfBoundsException
    {
        // Give it back from the parent
        return myHypercube.get(getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final long value)
        throws IndexOutOfBoundsException
    {
        // Set it in the parent
        myHypercube.set(value, getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void assignFrom(final Object object)
        throws IllegalArgumentException
    {
        LongHypercube.super.assignFrom(object);
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
}

// [[[end]]] (checksum: 502c9e1b9fad0184f9a09091d4e73506)
