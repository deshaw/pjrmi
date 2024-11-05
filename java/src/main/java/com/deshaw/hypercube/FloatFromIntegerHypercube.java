package com.deshaw.hypercube;

// Recreate with `cog -rc FloatFromIntegerHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.float32, numpy.int32))
// ]]]
import java.util.Map;

/**
 * A float {@link Hypercube} which is a view of a int
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class FloatFromIntegerHypercube
    extends AbstractFloatHypercube
    implements FloatHypercube
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
    public FloatFromIntegerHypercube(final IntegerHypercube hypercube)
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
        final Integer obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (float)(obj.intValue());
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
            (value == null) ? null : (int)(value.floatValue())
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
        myHypercube.weakSetAt(index, (int)(value));
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

// [[[end]]] (checksum: 01fa1fdd811da473511ae4dc32b6312e)
