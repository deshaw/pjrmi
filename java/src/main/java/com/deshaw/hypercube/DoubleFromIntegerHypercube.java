package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleFromIntegerHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.float64, numpy.int32))
// ]]]
import java.util.Map;

/**
 * A double {@link Hypercube} which is a view of a int
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class DoubleFromIntegerHypercube
    extends AbstractDoubleHypercube
    implements DoubleHypercube
{
    /**
     * The hypercube which we wrap.
     */
    private IntegerHypercube myHypercube;

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
    public DoubleFromIntegerHypercube(final IntegerHypercube hypercube)
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
        final Integer obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (double)(obj.intValue());
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
            (value == null) ? null : (int)(value.doubleValue())
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
        myHypercube.weakSetAt(index, (int)(value));
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
        myHypercube.weakSet((int)(value), indices);
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

// [[[end]]] (checksum: 7de92f72ad0e524320152f8185c7ace5)
