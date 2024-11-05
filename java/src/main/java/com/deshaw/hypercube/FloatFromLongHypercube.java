package com.deshaw.hypercube;

// Recreate with `cog -rc FloatFromLongHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.float32, numpy.int64))
// ]]]
import java.util.Map;

/**
 * A float {@link Hypercube} which is a view of a long
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class FloatFromLongHypercube
    extends AbstractFloatHypercube
    implements FloatHypercube
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
    public FloatFromLongHypercube(final LongHypercube hypercube)
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
    public Float weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Long obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (float)(obj.longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Float value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetObjectAt(
            index,
            (value == null) ? null : (long)(value.floatValue())
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (float)(myHypercube.weakGetAt(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final float value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetAt(index, (long)(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (float)(myHypercube.weakGet(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final float value, final long... indices)
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

// [[[end]]] (checksum: 42faa588ab6ec6937abe373c0b3ed39e)
