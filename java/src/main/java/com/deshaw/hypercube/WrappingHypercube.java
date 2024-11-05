package com.deshaw.hypercube;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.ByteOrder;

/**
 * The superclass for {@link Hypercube}s which wrap another in some way.
 *
 * @param <T> The type of the element which we store.
 */
public class WrappingHypercube<T>
    extends AbstractHypercube<T>
{
    /**
     * The Hypercube which we ultimately wrap.
     */
    public final Hypercube<T> base;

    /**
     * The hypercube which we wrap.
     */
    private Hypercube<T> myHypercube;

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
    protected WrappingHypercube(final Dimension<?>[] dimensions,
                                final Hypercube<T>   hypercube)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube.getElementType());

        base = (hypercube instanceof WrappingHypercube)
            ? ((WrappingHypercube<T>)hypercube).base
            : hypercube;
        myHypercube = hypercube;
    }

    /**
     * Constructor.
     *
     * @param dimensions  The dimensions of this hypercube.
     * @param hypercube   The hypercube to wrap.
     * @param transposed  The transposed view of this hypercube.
     *
     * @throws DimensionalityException  If the dimensions are inconsistent with
     *                                  the {@code hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    protected WrappingHypercube(final Dimension<?>[] dimensions,
                                final Hypercube<T>   hypercube,
                                final Hypercube<T>   transposed)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(dimensions, hypercube.getElementType(), transposed);

        base = (hypercube instanceof WrappingHypercube)
            ? ((WrappingHypercube<T>)hypercube).base
            : hypercube;
        myHypercube = hypercube;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush()
        throws IOException
    {
        getWrapped().flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeElement(final T                el,
                             final DataOutputStream os,
                             final ByteOrder        bo)
        throws IOException
    {
        getWrapped().writeElement(el, os, bo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T readElement(final DataInputStream is,
                         final ByteOrder       bo)
        throws IOException
    {
        return getWrapped().readElement(is, bo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T weakGetObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        return getWrapped().weakGetObjectAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetObjectAt(final long index, final T object)
        throws IndexOutOfBoundsException
    {
        getWrapped().weakSetObjectAt(index, object);
    }

    /**
     * Get the hypercube instance which we are wrapping.
     */
    protected Hypercube<T> getWrapped()
    {
        return myHypercube;
    }
}
