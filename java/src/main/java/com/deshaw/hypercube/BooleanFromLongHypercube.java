package com.deshaw.hypercube;

// Recreate with `cog -rc BooleanFromLongHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_from_primitive_hypercube
//
//     cog.outl(primitive_from_primitive_hypercube.generate(numpy.bool_, numpy.int64))
// ]]]
import java.util.Map;

/**
 * A boolean {@link Hypercube} which is a view of a long
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class BooleanFromLongHypercube
    extends AbstractBooleanHypercube
    implements BooleanHypercube
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
    public BooleanFromLongHypercube(final LongHypercube hypercube)
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
    public Boolean weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        final Long obj = myHypercube.weakGetObjectAt(index);
        return (obj == null) ? null : (boolean)(obj.longValue() != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final Boolean value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetObjectAt(
            index,
            (value == null) ? null : (long)(value.booleanValue() ? (byte)1 : (byte)0)
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return (boolean)(myHypercube.weakGetAt(index) != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSetAt(index, (long)(value ? (byte)1 : (byte)0));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return (boolean)(myHypercube.weakGet(indices) != 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final boolean value, final long... indices)
        throws IndexOutOfBoundsException
    {
        myHypercube.weakSet((long)(value ? (byte)1 : (byte)0), indices);
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

// [[[end]]] (checksum: 86f54db595cde15689b9b1b8da0e6c99)
