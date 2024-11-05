package com.deshaw.hypercube;

// Recreate with `cog -rc LongFromIntegerHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.int64, numpy.int32))
// ]]]
import java.util.Map;

/**
 * A long {@link Hypercube} which is a view of a int
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class LongFromIntegerHypercube
    extends AbstractLongHypercube
    implements LongHypercube
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
    public LongFromIntegerHypercube(final IntegerHypercube hypercube)
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
    public Long weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Integer obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (long)(obj.intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Long value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetObjectAt(
            index,
            (value == null) ? null : (int)(value.longValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (long)(myHypercube.weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final long value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetAt(index, (int)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (long)(myHypercube.weakGet(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final long value, final long... indices)
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

// [[[end]]] (checksum: dfa59e9a46cbf00f33e341d79a4ce777)
