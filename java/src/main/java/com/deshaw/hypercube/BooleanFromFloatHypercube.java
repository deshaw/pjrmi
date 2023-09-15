package com.deshaw.hypercube;

// Recreate with `cog.py -rc BooleanFromFloatHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.bool_, numpy.float32))
// ]]]
import java.util.Map;

/**
 * A boolean {@link Hypercube} which is a view of a float
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class BooleanFromFloatHypercube
    extends AbstractBooleanHypercube
    implements BooleanHypercube
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
    public BooleanFromFloatHypercube(final FloatHypercube hypercube)
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
    public Boolean getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Float obj = myHypercube.getObjectAt(index);
        return (obj == null) ? null : (boolean)(obj.floatValue() != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final Boolean value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setObjectAt(
            index,
            (value == null) ? null : (float)(value.booleanValue() ? (byte)1 : (byte)0)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (boolean)(myHypercube.getAt(index) != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        myHypercube.setAt(index, (float)(value ? (byte)1 : (byte)0));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (boolean)(myHypercube.get(indices) != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final boolean value, final long... indices)
        throws IndexOutOfBoundsException
    {
        myHypercube.set((float)(value ? (byte)1 : (byte)0), indices);
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

// [[[end]]] (checksum: f154eb331199985b2e267287f2789d3e)
