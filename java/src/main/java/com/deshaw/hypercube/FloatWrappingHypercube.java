package com.deshaw.hypercube;

// Recreate with `cog -rc FloarWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_wrapping_hypercube
//
//     cog.outl(primitive_wrapping_hypercube.generate(numpy.float32))
// ]]]
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

/**
 * A cube which is a view of another float-based {@link Hypercube}.
 */
public class FloatWrappingHypercube
    extends WrappingHypercube<Float>
    implements FloatHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final FloatHypercube T;

    /**
     * The number of bytes required to store the hypercube's data.
     */
    public final long nbytes;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private FloatHypercube myHypercube;

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
    public FloatWrappingHypercube(final Dimension<?>[] dimensions,
                                          final FloatHypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube);

        T = new FloatTransposedHypercube(this);
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
    public FloatWrappingHypercube(final Dimension<?>[] dimensions,
                                          final FloatHypercube hypercube,
                                          final FloatHypercube transposed)
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
    public FloatHypercube slice(final Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new FloatSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public FloatHypercube roll(final Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new FloatAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public FloatHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final float value)
        throws IndexOutOfBoundsException
    {
        getWrapped().weakSetAt(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final float d, final long... indices)
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
    protected FloatHypercube getWrapped()
    {
        return myHypercube;
    }
}

// [[[end]]] (checksum: 09ea51b9829dc7bb8cb9e6ff7f674737)
