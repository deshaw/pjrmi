package com.deshaw.hypercube;

// Recreate with `cog -rc LongWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_wrapping_hypercube
//
//     cog.outl(primitive_wrapping_hypercube.generate(numpy.int64))
// ]]]
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

/**
 * A cube which is a view of another long-based {@link Hypercube}.
 */
public class LongWrappingHypercube
    extends WrappingHypercube<Long>
    implements LongHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final LongHypercube T;

    /**
     * The number of bytes required to store the hypercube's data.
     */
    public final long nbytes;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private LongHypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param dimensions  The dimensions of this hypercube.
     * @param hypercube   The hypercube to wrap.
     *
     * @throws DimensionalityException  If the dimensions are inconsistent with
     *                                  the {@code hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public LongWrappingHypercube(final Dimension<?>[] dimensions,
                                          final LongHypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube);

        T = new LongTransposedHypercube(this);
        nbytes = size * itemsize;

        myHypercube = hypercube;
    }

    /**
     * Constructor.
     *
     * @param dimensions  The dimensions of this hypercube.
     * @param hypercube   The hypercube to wrap.
     *
     * @throws DimensionalityException  If the dimensions are inconsistent with
     *                                  the {@code hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public LongWrappingHypercube(final Dimension<?>[] dimensions,
                                          final LongHypercube hypercube,
                                          final LongHypercube transposed)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube, transposed);

        T = transposed;
        nbytes = size * itemsize;

        myHypercube = hypercube;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube slice(final Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new LongSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube roll(final Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new LongAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final long value)
        throws IndexOutOfBoundsException
    {
        getWrapped().weakSetAt(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final long d, final long... indices)
        throws IndexOutOfBoundsException
    {
        getWrapped().weakSetAt(toOffset(indices), d);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void preRead()
        throws IndexOutOfBoundsException
    {
        getWrapped().preRead();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void postWrite()
        throws IndexOutOfBoundsException
    {
        getWrapped().postWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected LongHypercube getWrapped()
    {
        return myHypercube;
    }
}

// [[[end]]] (checksum: d6659377da03fc8367dc5a8dddf451c0)
