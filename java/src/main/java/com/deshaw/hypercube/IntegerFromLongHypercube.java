package com.deshaw.hypercube;

// Recreate with `cog -rc IntegerFromLongHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.int32, numpy.int64))
// ]]]
import java.util.Map;

/**
 * A int {@link Hypercube} which is a view of a long
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class IntegerFromLongHypercube
    extends AbstractIntegerHypercube
    implements IntegerHypercube
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
    public IntegerFromLongHypercube(final LongHypercube hypercube)
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
    public Integer getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Long obj = myHypercube.getObjectAt(index);
        return (obj == null) ? null : (int)(obj.longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final Integer value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setObjectAt(
            index,
            (value == null) ? null : (long)(value.intValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (int)(myHypercube.getAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final int value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setAt(index, (long)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (int)(myHypercube.get(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final int value, final long... indices)
        throws IndexOutOfBoundsException
    {
        myHypercube.set((long)(value), indices);
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

// [[[end]]] (checksum: ac9b048c96b55bb63857545a59ad8ea5)
