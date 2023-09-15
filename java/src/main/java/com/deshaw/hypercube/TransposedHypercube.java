package com.deshaw.hypercube;

import java.util.logging.Level;

/**
 * A cube which is a transposed view of another {@link Hypercube}. This
 * effectively means that its accessors are reversed in order.
 *
 * @param <T> The type of the element which we store.
 */
public class TransposedHypercube<T>
    extends WrappingHypercube<T>
{
    // Public members which look like numpy.ndarray ones

    /**
     * The view on the transpose of this cube. Since transposing is its own
     * inverse function, this is actually the cube which this one wraps.
     */
    public final Hypercube<T> T;

    // -------------------------------------------------------------------------
    /**
     * The indices for getWrappedIndices(), so we don't need to keep new'ng them
     * each time we call the function.
     */
    private final ThreadLocal<long[]> myWrappedIndices;

    // ----------------------------------------------------------------------

    /**
     * Determine the dimensions of the transposed cube, given a hypercube.
     */
    private static Dimension<?>[] getDimensions(final Hypercube<?> hypercube)
        throws NullPointerException
    {
        if (hypercube == null) {
            throw new NullPointerException("Given a null hypercube");
        }

        // Reverse the dimensions of the given cube
        final Dimension<?>[] dims = hypercube.getDimensions();
        final Dimension<?>[] result = new Dimension<?>[dims.length];
        for (int i = 0, j = dims.length-1; i < dims.length; i++, j--) {
            result[i] = dims[j];
        }
        return result;
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to transpose.
     *
     * @throws NullPointerException If a {@code null} pointer was encountered.
     */
    public TransposedHypercube(final Hypercube<T> hypercube)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(getDimensions(hypercube), hypercube, hypercube);

        // The transpose of the transpose is the original
        T = hypercube;

        myWrappedIndices = new ThreadLocal<>() {
            @Override protected long[] initialValue() {
                return new long[getNDim()];
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObj(final long... indices)
        throws IndexOutOfBoundsException
    {
        final long[] wrapped = myWrappedIndices.get();
        if (indices.length != wrapped.length) {
            throw new IllegalArgumentException(
                "Bad indices; " +
                "length is " + indices.length + " " +
                "not " + wrapped.length
            );
        }

        // Copy in so we don't mutate the caller's values
        switch (indices.length) {
        case 5: wrapped[4] = indices[4];
        case 4: wrapped[3] = indices[3];
        case 3: wrapped[2] = indices[2];
        case 2: wrapped[1] = indices[1];
        case 1: wrapped[0] = indices[0];
        case 0: break;
        default: System.arraycopy(indices, 0, wrapped, 0, indices.length); break;
        }
        reverseIndices(wrapped);

        // And ask our wrapped cube
        return getWrapped().getObj(wrapped);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObj(final T obj, final long... indices)
        throws IndexOutOfBoundsException
    {
        final long[] wrapped = myWrappedIndices.get();
        if (indices.length != wrapped.length) {
            throw new IllegalArgumentException(
                "Bad indices; " +
                "length is " + indices.length + " " +
                "not " + wrapped.length
            );
        }

        // Copy in so we don't mutate the caller's values
        switch (indices.length) {
        case 5: wrapped[4] = indices[4];
        case 4: wrapped[3] = indices[3];
        case 3: wrapped[2] = indices[2];
        case 2: wrapped[1] = indices[1];
        case 1: wrapped[0] = indices[0];
        case 0: break;
        default: System.arraycopy(indices, 0, wrapped, 0, indices.length); break;
        }
        reverseIndices(wrapped);

        // And hand off to our wrapped cube
        getWrapped().setObj(obj, wrapped);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {
        // Give it back from the parent
        return getWrapped().getObj(getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectAt(final long index, final T obj)
        throws IndexOutOfBoundsException
    {
        // Set it in the parent
        getWrapped().setObj(obj, getWrappedIndices(index));
    }

    // ----------------------------------------------------------------------

    /**
     * Get the inner hypercube's indices, suitably offset, for the given index.
     */
    protected long[] getWrappedIndices(final long index)
    {
        return getWrappedIndices(index, myWrappedIndices.get());
    }

    /**
     * Get the inner hypercube's indices, suitably offset, for the given index.
     * Populate these into the given array and return it.
     */
    protected long[] getWrappedIndices(final long   index,
                                       final long[] wrapped)
    {
        // Check the given args. Since only controlled classes should be calling
        // this we're a little terse with the error cases.
        if (wrapped.length != getNDim()) {
            throw new IllegalArgumentException(
                "Bad arrays; " +
                "wrapped length is " + wrapped.length + " " +
                "not " + getNDim()
            );
        }

        // Get the indices, transpose them, and give them back
        fromOffset(index, wrapped);
        reverseIndices(wrapped);
        return wrapped;
    }

    /**
     * Reverse the indices in the given array.
     */
    protected void reverseIndices(final long[] indices)
    {
        // Flip them around in place, with the aid of a temporary variable
        long tmp;
        switch (indices.length) {
        case 0:
        case 1:
            // NOP
            break;

        case 2:
            tmp        = indices[0];
            indices[0] = indices[1];
            indices[1] = tmp;
            break;

        case 3:
            tmp        = indices[0];
            indices[0] = indices[2];
            indices[2] = tmp;
            // indices[1] unchanged
            break;

        case 4:
            tmp        = indices[0];
            indices[0] = indices[3];
            indices[3] = tmp;
            tmp        = indices[1];
            indices[1] = indices[2];
            indices[2] = tmp;
            break;

        case 5:
            tmp        = indices[0];
            indices[0] = indices[4];
            indices[4] = tmp;
            tmp        = indices[1];
            indices[1] = indices[3];
            indices[3] = tmp;
            // indices[2] unchanged
            break;

        default:
            for (int i = 0, j = indices.length-1; i < j; i++, j--) {
                tmp        = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }
        }
    }
}
