package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleFlatRolledHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_flat_rolled_hypercube
//
//     cog.outl(primitive_flat_rolled_hypercube.generate(numpy.float64))
// ]]]
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

import java.util.Map;
import java.util.logging.Level;

/**
 * A shifted cube which is a flat-rolled view of another double-based
 * {@link Hypercube}. Essentially, the given cube is first flattened, then
 * shifted, and finally unflattened into its original shape.
 */
public class DoubleFlatRolledHypercube
    extends FlatRolledHypercube<Double>
    implements DoubleHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final DoubleHypercube T;

    // -----------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private DoubleHypercube myHypercube;

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
    public DoubleFlatRolledHypercube(final DoubleHypercube hypercube,
                                        final long shift)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(hypercube, shift);

        T = new DoubleTransposedHypercube(this);
        myHypercube = hypercube;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public DoubleHypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new DoubleSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public DoubleHypercube rollFlat(final long shift)
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
            return new DoubleFlatRolledHypercube(myHypercube, getShift() + shift);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public DoubleHypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new DoubleAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public DoubleHypercube transpose()
    {
        return this.T;
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
    public void fromFlattened(final double[] src,
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
    public double get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final double value, final long... indices)
        throws IndexOutOfBoundsException
    {
        setAt(toOffset(indices), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getAt(final long index)
        throws IndexOutOfBoundsException
    {
        // Give it back from the parent
        return myHypercube.get(getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final double value)
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
        DoubleHypercube.super.assignFrom(object);
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

// [[[end]]] (checksum: bab3624f9731685d27d7108dd3e510d8)
