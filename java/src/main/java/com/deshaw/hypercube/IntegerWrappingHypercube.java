package com.deshaw.hypercube;

// Recreate with `cog -rc IntegerWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_wrapping_hypercube
//
//     cog.outl(primitive_wrapping_hypercube.generate(numpy.int32))
// ]]]
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

/**
 * A cube which is a view of another int-based {@link Hypercube}.
 */
public class IntegerWrappingHypercube
    extends WrappingHypercube<Integer>
    implements IntegerHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final IntegerHypercube T;

    /**
     * The number of bytes required to store the hypercube's data.
     */
    public final long nbytes;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private IntegerHypercube myHypercube;

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
    public IntegerWrappingHypercube(final Dimension<?>[] dimensions,
                                          final IntegerHypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube);

        T = new IntegerTransposedHypercube(this);
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
    public IntegerWrappingHypercube(final Dimension<?>[] dimensions,
                                          final IntegerHypercube hypercube,
                                          final IntegerHypercube transposed)
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
    public IntegerHypercube slice(final Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new IntegerSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public IntegerHypercube roll(final Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new IntegerAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public IntegerHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final int value)
        throws IndexOutOfBoundsException
    {
        getWrapped().weakSetAt(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final int d, final long... indices)
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
    protected IntegerHypercube getWrapped()
    {
        return myHypercube;
    }
}

// [[[end]]] (checksum: c56b4c34b18680845e708d4590defb77)
