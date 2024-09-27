package com.deshaw.hypercube;

import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.util.LongBitSet;
import com.deshaw.util.VeryLongArray;

import java.util.Arrays;
import java.util.logging.Level;

/**
 * A sub-cube which is a view of another {@link Hypercube} with the first
 * dimension selectively masked.
 *
 * @param <T> The type of the element which we store.
 */
public class MaskedHypercube<T>
    extends WrappingHypercube<T>
{
    /**
     * The mapping from the indices of the primary dimension to that of the
     * wrapped cube.
     */
    private final VeryLongArray myMapping;

    // ----------------------------------------------------------------------

    /**
     * Determine the dimensions of this sub-cube, given a hypercube and a
     * boolean mask.
     */
    protected static <T> Dimension<?>[] getDimensions(final Hypercube<T> hypercube,
                                                      final LongBitSet   mask)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        if (hypercube == null) {
            throw new NullPointerException("Given a null hypercube");
        }
        if (mask == null) {
            throw new NullPointerException("Given a null mask");
        }

        // Create the new dimensions
        final int ndim = hypercube.getNDim();
        final Dimension<?>[] dimensions = new Dimension<?>[ndim];

        // We need to mutate the first dimension to account for the mask, but
        // the others remain the same
        for (int i=0; i < dimensions.length; i++) {
            final Dimension<?> dim = hypercube.dim(i);
            dimensions[i] = (i == 0) ? new Dimension<>(dim.getIndex().mask(mask))
                                     : dim;
        }

        // And we're done
        return dimensions;
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to slice.
     * @param mask       How to mask off the first dimension of the cube. The
     *                   mask length must match that of the cube's first
     *                   dimension.
     *
     * @throws DimensionalityException  If the mask did not match the
     *                                  {@code hypercube}'s first dimension.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public MaskedHypercube(final Hypercube<T> hypercube,
                           final boolean[]    mask)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        this(hypercube, new LongBitSet(mask));
    }

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to slice.
     * @param mask       How to mask off the first dimension of the cube.
     *
     * @throws DimensionalityException  If the mask did not match the
     *                                  {@code hypercube}'s first dimension.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public MaskedHypercube(final Hypercube<T> hypercube,
                           final LongBitSet   mask)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(getDimensions(hypercube, mask), hypercube);

        // The size of the mapping will be that of the first dimension. It tells
        // us how an index of this cube's first dimension goes to that of the
        // wrapped cube's first dimension.
        final long length = length(0);
        myMapping = new VeryLongArray(length);
        for (long i=0, j=0; i < hypercube.length(0); i++) {
            // If it's visible then set it
            if (mask.get(i)) {
                // It should never be the case that we are trying to set outside
                // of the mask's vision, if the getDimensions() function is
                // working
                if (j >= length) {
                    throw new IllegalStateException(
                        "Internal error computing the mapping; " +
                        "wrapped first dimension has index " +
                        hypercube.dim(0).getIndex() +
                        "[" + hypercube.length(0) + "], " +
                        "local dimension has " +
                        dim(0).getIndex() + "[" + length(0) + "], " +
                        "and the mask has cardinality of " + mask.cardinality()
                    );
                }

                // Safe to set
                myMapping.set(j++, i);
            }
        }
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

        // If we only have a single dimension then this is just a walk along the
        // mapping array
        if (getNDim() == 1) {
            // Validate. This should be logically the same as the test above
            if (srcPos + length >= myMapping.size()) {
                throw new IndexOutOfBoundsException(
                    "Source position, " + srcPos + ", " +
                    "plus length ," + length + ", " +
                    "was greater than the dimension size, " + myMapping.size()
                );
            }

            // Just walk it directly and set. We could make this more efficient
            // by handling ranges of values in bulk but that adds complexity and
            // so we hope that this is mostly fine for now.
            for (int i = 0; i < length; i++) {
                final long index = myMapping.get(i + srcPos);
                dst[i + dstPos] = getWrapped().getObjectAt(index);
            }
        }
        else {
            // Index array working space
            final long[] indices = new long[ndim];

            // The lowest dimension of the cube which we're wrapping, this will
            // be used to determine our stride length later on
            final int dim = ndim - 1;

            // We start at dstPos and walk to dstPos+length. We do this in steps
            // such that we copy as much of the lowest dimension as possible at a
            // time.
            for (long pos = 0; pos < length; /*incr below*/) {
                // Determine the wrapped indices from the current position
                getWrappedIndices((long)srcPos + pos, indices);

                // Figure out the offset within the wrapped cube
                final long wrappedSrcPos = getWrapped().toOffset(indices);

                // Now figure out how far we want to walk within the wrapped cube
                final long dimLeft =
                    Math.min(length - pos,
                             getWrapped().length(dim) - indices[dim]);

                // Now we simply flatten from the cube into the destination array
                getWrapped().toFlattenedObjs(wrappedSrcPos,
                                             dst,
                                             (int)(dstPos + pos),
                                             (int)dimLeft);

                // And step forward over what we copied
                pos += dimLeft;
            }
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

        // If we only have a single dimension then this is just a walk along the
        // mapping array
        if (getNDim() == 1) {
            // Validate. This should be logically the same as the test above
            if (dstPos + length >= myMapping.size()) {
                throw new IndexOutOfBoundsException(
                    "Destination position, " + dstPos + ", " +
                    "plus length ," + length + ", " +
                    "was greater than the dimension size, " + myMapping.size()
                );
            }

            // Just walk it directly and set. We could make this more efficient
            // by handling ranges of values in bulk but that adds complexity and
            // so we hope that this is mostly fine for now.
            for (int i = 0; i < length; i++) {
                final long index = myMapping.get(i + dstPos);
                getWrapped().setObjectAt(index, src[i + srcPos]);
            }
        }
        else {
            // Index array working space
            final long[] indices = new long[ndim];

            // The lowest dimension of the cube which we're wrapping, this will
            // be used to determine our stride length later on
            final int dim = ndim - 1;

            // We start at srcPos and walk to srcPos+length. We do this in steps
            // such that we copy as much of the lowest dimension as possible at a
            // time.
            for (long pos = 0; pos < length; /*incr below*/) {
                // Determine the wrapped indices from the current position
                getWrappedIndices((long)srcPos + pos, indices);

                // Figure out the offset within the wrapped cube
                final long wrappedSrcPos = getWrapped().toOffset(indices);

                // Now figure out how far we want to walk within the wrapped cube
                final long dimLeft =
                    Math.min(length - pos,
                             getWrapped().length(dim) - indices[dim]);

                // Now we simply flatten from the cube into the result array
                getWrapped().fromFlattenedObjs(src,
                                               srcPos + (int)pos,
                                               wrappedSrcPos,
                                               (int)dimLeft);

                // And step forward over what we copied
                pos += dimLeft;
            }
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
     * Get a handle on the mapping.
     */
    protected final VeryLongArray getMapping()
    {
        return myMapping;
    }

    /**
     * Get the inner hypercube's indices, suitably offset, for the given index.
     */
    protected long[] getWrappedIndices(final long index)
    {
        final long[] indices = myIndices.get();
        getWrappedIndices(index, indices);
        return indices;
    }
    /** For use only by getWrappedIndices() */
    private final ThreadLocal<long[]> myIndices =
        ThreadLocal.withInitial(() -> new long[getNDim()]);

    /**
     * Get the wrapped and this hypercube's indices, suitably offset, for the
     * given index. Populate these into the give arrays.
     */
    protected long[] getWrappedIndices(final long   index,
                                       final long[] indices)
    {
        // Check the given args. Since only controlled classes should be calling
        // this we're a little terse with the error cases.
        if (indices.length != getNDim()) {
            throw new IllegalArgumentException(
                "Bad array: indices length is " +
                indices.length  + " " + "not " + getNDim()
            );
        }

        // Get the offsets in our local terms
        fromOffset(index, indices);

        // Check to make sure it's not iffy
        if (indices[0] < 0 || indices[0] >= myMapping.size()) {
            throw new IllegalStateException(
                "Got a bad local start index: " + indices[0]
            );
        }

        // Now remap it
        indices[0] = myMapping.get(indices[0]);

        // And return the full indicies
        return indices;
    }
}
