package com.deshaw.hypercube;

// Recreate with `cog -rc DoubleWrappingHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_wrapping_hypercube
//
//     cog.outl(primitive_wrapping_hypercube.generate(numpy.float64))
// ]]]
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

/**
 * A cube which is a view of another double-based {@link Hypercube}.
 */
public class DoubleWrappingHypercube
    extends WrappingHypercube<Double>
    implements DoubleHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final DoubleHypercube T;

    /**
     * The number of bytes required to store the hypercube's data.
     */
    public final long nbytes;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private DoubleHypercube myHypercube;

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
    public DoubleWrappingHypercube(final Dimension<?>[] dimensions,
                                          final DoubleHypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube);

        T = new DoubleTransposedHypercube(this);
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
    public DoubleWrappingHypercube(final Dimension<?>[] dimensions,
                                          final DoubleHypercube hypercube,
                                          final DoubleHypercube transposed)
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
    public DoubleHypercube slice(final Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new DoubleSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public DoubleHypercube roll(final Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new DoubleAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public DoubleHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getWrapped().getAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAt(final long index, final double value)
        throws IndexOutOfBoundsException
    {
        getWrapped().setAt(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double get(final long... indices)
        throws IndexOutOfBoundsException
    {
        return getWrapped().getAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final double d, final long... indices)
        throws IndexOutOfBoundsException
    {
        getWrapped().setAt(toOffset(indices), d);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    protected DoubleHypercube getWrapped()
    {
        return myHypercube;
    }
}

// [[[end]]] (checksum: 0d2068921f87c30edbe41e620e708b7c)
