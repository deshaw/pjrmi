package com.deshaw.hypercube;

// Recreate with `cog -rc BooleanWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_wrapping_hypercube
//
//     cog.outl(primitive_wrapping_hypercube.generate(numpy.bool8))
// ]]]
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

/**
 * A cube which is a view of another boolean-based {@link Hypercube}.
 */
public class BooleanWrappingHypercube
    extends WrappingHypercube<Boolean>
    implements BooleanHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final BooleanHypercube T;

    /**
     * The number of bytes required to store the hypercube's data.
     */
    public final long nbytes;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private BooleanHypercube myHypercube;

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
    public BooleanWrappingHypercube(final Dimension<?>[] dimensions,
                                          final BooleanHypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube);

        T = new BooleanTransposedHypercube(this);
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
    public BooleanWrappingHypercube(final Dimension<?>[] dimensions,
                                          final BooleanHypercube hypercube,
                                          final BooleanHypercube transposed)
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
    public BooleanHypercube slice(final Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new BooleanSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube roll(final Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new BooleanAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final boolean value)
        throws IndexOutOfBoundsException
    {
        getWrapped().weakSetAt(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final boolean d, final long... indices)
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
    protected BooleanHypercube getWrapped()
    {
        return myHypercube;
    }
}

// [[[end]]] (checksum: 523ca174de75679bec5d13c0272880eb)
