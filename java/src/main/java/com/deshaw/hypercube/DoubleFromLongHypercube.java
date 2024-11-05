package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleFromLongHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.float64, numpy.int64))
// ]]]
import java.util.Map;

/**
 * A double {@link Hypercube} which is a view of a long
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class DoubleFromLongHypercube
    extends AbstractDoubleHypercube
    implements DoubleHypercube
{
    /**
     * The hypercube which we wrap.
     */
    private LongHypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to cast to/from.
     *
     * @throws IllegalArgumentException If there was any problem with the arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public DoubleFromLongHypercube(final LongHypercube hypercube)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(hypercube.getDimensions());

        myHypercube = hypercube;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Long obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (double)(obj.longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Double value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetObjectAt(
            index,
            (value == null) ? null : (long)(value.doubleValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (double)(myHypercube.weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final double value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetAt(index, (long)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (double)(myHypercube.weakGet(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final double value, final long... indices)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSet((long)(value), indices);
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
        result.put("owndata",      false);
        result.put("writeable",    true);
        return result;
    }
}

// [[[end]]] (checksum: 6a16077264a893e1a65650014e2d60cb)
