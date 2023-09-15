package com.deshaw.hypercube;

// Recreate with `cog.py -rc BooleanFromIntegerHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.bool_, numpy.int32))
// ]]]
import java.util.Map;

/**
 * A boolean {@link Hypercube} which is a view of a int
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class BooleanFromIntegerHypercube
    extends AbstractBooleanHypercube
    implements BooleanHypercube
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
    public BooleanFromIntegerHypercube(final IntegerHypercube hypercube)
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
        final Integer obj = myHypercube.getObjectAt(index);
        return (obj == null) ? null : (boolean)(obj.intValue() != 0);
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
            (value == null) ? null : (int)(value.booleanValue() ? (byte)1 : (byte)0)
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
        myHypercube.setAt(index, (int)(value ? (byte)1 : (byte)0));
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
        myHypercube.set((int)(value ? (byte)1 : (byte)0), indices);
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

// [[[end]]] (checksum: 81ebb479423679437252362c97476a73)
