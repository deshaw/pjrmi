package com.deshaw.hypercube;

import java.util.logging.Level;

/**
 * A {@link Hypercube} which is a flat-rolled view of another.
 *
 * @param <T> The type of the element which we store.
 */
public class FlatRolledHypercube<T>
    extends WrappingHypercube<T>
{
    /**
     * The number of elements to shift the wrapped hypercube by. This shift
     * value is always non-negative.
     */
    private final long myShift;

    /**
     * The indices for getWrappedIndices(), so we don't need to keep making
     * them.
     */
    private final ThreadLocal<long[]> myWrappedIndices;

    // ----------------------------------------------------------------------

    /**
     * Determine the dimensions of the shifted cube, given a hypercube.
     */
    private static <T> Dimension<?>[] getDimensions(final Hypercube<T> hypercube)
        throws DimensionalityException,
               NullPointerException
    {
        if (hypercube == null) {
            throw new NullPointerException("Given a null hypercube");
        }

        return hypercube.getDimensions();
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to roll.
     * @param shift      The (signed) number of elements to roll the given
     *                   {@link Hypercube} by.
     *
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public FlatRolledHypercube(final Hypercube<T> hypercube,
                               final long         shift)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(getDimensions(hypercube), hypercube);

        // Make sure the shift value is always non-negative.
        myShift = (getSize() == 0) ? 0
                                   : (shift % getSize() + getSize()) % getSize();

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
    public void toFlattenedObjs(final long srcPos,
                                final T[]  dst,
                                final int  dstPos,
                                final int  length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dst=" + dst + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        // Note that the myShift is always non-negative so we don't need to
        // worry about handling negative values.

        // The case where we have a single block in our wrapped cube.
        if (srcPos + length <= myShift || srcPos >= myShift) {
            getWrapped().toFlattenedObjs(offset(srcPos),
                                         dst,
                                         dstPos,
                                         length);
        }
        // The case where have 2 blocks in our wrapped cube.
        else {
            // The size of the tail of the wrapped cube we have to copy
            final int tail = (int)(myShift - srcPos);
            getWrapped().toFlattenedObjs(offset(srcPos),
                                         dst,
                                         dstPos,
                                         tail);
            getWrapped().toFlattenedObjs(0,
                                         dst,
                                         dstPos + tail,
                                         length - tail);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattenedObjs(final T[]  src,
                                  final int  srcPos,
                                  final long dstPos,
                                  final int  length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(
                "Unflattening with " +
                "src=" + src + " srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }

        // Sanitise input
        checkUnflattenArgs(srcPos, dstPos, length);
        if (src == null) {
            throw new NullPointerException("Given a null array");
        }
        if (src.length - srcPos < length) {
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the array size, " + src.length
            );
        }

        // Note that the myShift is always non-negative so we don't need to
        // worry about handling negative values.
        // The case where we have a single block in our wrapped cube.
        if (dstPos + length <= myShift || dstPos >= myShift) {
            getWrapped().fromFlattenedObjs(src,
                                           srcPos,
                                           offset(dstPos),
                                           length);
        }
        // The case where have 2 blocks in our wrapped cube.
        else {
            // The size of the tail of the wrapped cube we have to copy in
            final int tail = (int)(myShift - dstPos);
            getWrapped().fromFlattenedObjs(src,
                                           srcPos,
                                           offset(dstPos),
                                           tail);
            getWrapped().fromFlattenedObjs(src,
                                           srcPos + tail,
                                           0,
                                           length - tail);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Hypercube<T> rollFlat(final long shift)
    {
        // Combine the nested rolls more efficiently.
        if (getSize()                     == 0 ||
            (myShift + shift) % getSize() == 0)
        {
            return getWrapped();
        }
        else if (shift % getSize() == 0) {
            return this;
        }
        else {
            return new FlatRolledHypercube<>(getWrapped(), myShift + shift);
        }
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
     * Get the number of elements the original wrapped cube is shifted by.
     * This value is always within the range [0, getSize()).
     */
    protected long getShift()
    {
        return myShift;
    }

    /**
     * Offset the given index in our rolled cube into an index in the wrapped cube.
     */
    protected long offset(long index)
    {
        return (getSize() == 0) ? 0 : (index - myShift + getSize()) % getSize();
    }

    /**
     * Get the inner hypercube's indices, suitably offset, for the given index.
     */
    protected long[] getWrappedIndices(final long index)
    {
        final long[] wrapped = myWrappedIndices.get();
        getWrappedIndices(index, wrapped);
        return wrapped;
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

        // Get the offsets in our wrapped terms
        fromOffset(offset(index), wrapped);

        // And return them
        return wrapped;
    }
}
