package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleFromFloatHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.float64, numpy.float32))
// ]]]
import java.util.Map;

/**
 * A double {@link Hypercube} which is a view of a float
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class DoubleFromFloatHypercube
    extends AbstractDoubleHypercube
    implements DoubleHypercube
{
    /**
     * The hypercube which we wrap.
     */
    private FloatHypercube myHypercube;

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
    public DoubleFromFloatHypercube(final FloatHypercube hypercube)
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
    public Double getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Float obj = myHypercube.getObjectAt(index);
        return (obj == null) ? null : (double)(obj.floatValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final Double value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setObjectAt(
            index,
            (value == null) ? null : (float)(value.doubleValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (double)(myHypercube.getAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final double value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setAt(index, (float)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (double)(myHypercube.get(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final double value, final long... indices)
        throws IndexOutOfBoundsException
    {
        myHypercube.set((float)(value), indices);
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

// [[[end]]] (checksum: a73cf9229b368d98d4db1945bade3117)
