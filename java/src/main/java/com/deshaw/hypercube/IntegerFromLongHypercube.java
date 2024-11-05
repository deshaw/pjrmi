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
    public Integer weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Long obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (int)(obj.longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Integer value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetObjectAt(
            index,
            (value == null) ? null : (long)(value.intValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (int)(myHypercube.weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final int value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetAt(index, (long)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (int)(myHypercube.weakGet(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final int value, final long... indices)
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

// [[[end]]] (checksum: 721479c8be864c826b1cff2a7832abbd)
