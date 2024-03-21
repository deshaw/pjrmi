package com.deshaw.hypercube;

// Recreate with `cog -rc LongFromFloatHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.int64, numpy.float32))
// ]]]
import java.util.Map;

/**
 * A long {@link Hypercube} which is a view of a float
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class LongFromFloatHypercube
    extends AbstractLongHypercube
    implements LongHypercube
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
    public LongFromFloatHypercube(final FloatHypercube hypercube)
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
    public Long getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Float obj = myHypercube.getObjectAt(index);
        return (obj == null) ? null : (long)(obj.floatValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final Long value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setObjectAt(
            index,
            (value == null) ? null : (float)(value.longValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (long)(myHypercube.getAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final long value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setAt(index, (float)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (long)(myHypercube.get(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final long value, final long... indices)
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

// [[[end]]] (checksum: 218edd84020ae326881a39c0f0902385)
