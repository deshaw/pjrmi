package com.deshaw.hypercube;

import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;

import java.util.Arrays;
import java.util.logging.Level;

/**
 * A sub-cube which is a sliced view of another {@link Hypercube}.
 *
 * @param <T> The type of the element which we store.
 */
public class SlicedHypercube<T>
    extends WrappingHypercube<T>
{
    /**
     * The accessors we were given. These must be of the same dimensionality as
     * the wrapped cube, since it's that which they are "accessing".
     */
    private final Accessor<?>[] myAccessors;

    /**
     * The slices of the contained hypercube which define this sub-cube. This
     * array's length matches our dimensionality.
     */
    private Slice<?>[] mySlices;

    /**
     * The mapping from wrapped dimension indices to our ones. As such, this
     * array's length matches the wrapped hypercube's dimensionality.
     *
     * <p>If a value in this is negative then it represents an absolute offset
     * into the wrapped cube.
     */
    private final long[] myMapping;

    // ----------------------------------------------------------------------

    /**
     * Determine the dimensions of this sub-cube, given a hypercube and a set of
     * slices.
     */
    private static <T> Dimension<?>[] getDimensions(final Hypercube<T>  hypercube,
                                                    final Accessor<?>[] accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        if (hypercube == null) {
            throw new NullPointerException("Given a null hypercube");
        }
        if (accessors == null) {
            throw new NullPointerException("Given a null array of accessors");
        }

        // Check the lengths are compatible, Any missing Accessors will be
        // treated as unconstrained slices later on.
        if (accessors.length > hypercube.getNDim()) {
            throw new DimensionalityException(
                "Number of accessors, " + accessors.length + ", didn't match " +
                "number of dimensions " + hypercube.getNDim()
            );
        }

        // Figure out how many slices (or nulls) we have. This will determine
        // the dimensionality. If we have none then that's a problem (since we
        // can't have zero dimensions.
        int ndim = 0;
        for (int i=0; i < hypercube.getNDim(); i++) {
            final Accessor<?> accessor =
                (i < accessors.length) ? accessors[i] : null;
            if (accessor == null || accessor instanceof Slice) {
                ndim++;
            }
        }
        if (ndim == 0) {
            throw new IllegalArgumentException(
                "No slices in " + Arrays.toString(accessors)
            );
        }

        // What we'll return
        final Dimension<?>[] dimensions = new Dimension<?>[ndim];

        // Now walk them and create the new dimension for each one
        for (int i=0, j=0; j < ndim; i++) {
            // What we're slicing and how we're slicing it
            Dimension<?> dimension = hypercube.dim(i);
            Accessor<?>  accessor  = (i < accessors.length) ? accessors[i] : null;

            // If the slice was null then we aren't slicing this dimension
            if (accessor instanceof Coordinate) {
                // This does not produce a dimension, it's a direct mapping
                continue;
            }
            else if (accessor == null) {
                // Unconstrained, same as before
                dimensions[j++] = dimension;
            }
            else if (accessor instanceof Slice) {
                final Slice<?> slice = (Slice<?>)accessor;

                // Make sure it matches
                if (!dimension.equals(slice.getDimension())) {
                    throw new DimensionalityException(
                        "Dimension of slice[" + i + "], " +
                        slice.getDimension() + ", didn't match " +
                        "hypercube dimension " + dimension
                    );
                }

                // And create the new dimension
                dimensions[j++] =
                    new Dimension<>(
                        dimension.getIndex().subIndex(
                            dimension.getIndex().getName() + "[" + slice + "]",
                            slice.start(),
                            slice.end()
                        )
                    );
            }
        }

        // And we're done
        return dimensions;
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to slice.
     * @param accessors  How to slice this cube. The number of accessors must
     *                   not exceed the number of dimensions of the given
     *                   {@link Hypercube}. However, a slice may be {@code null}
     *                   if it does not apply.
     *
     * @throws DimensionalityException  If the slices did not match the
     *                                  {@code hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public SlicedHypercube(final Hypercube<T>  hypercube,
                           final Accessor<?>[] accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(getDimensions(hypercube, accessors), hypercube);

        myAccessors = Arrays.copyOf(accessors, hypercube.getNDim());
        mySlices    = new Slice<?>[getNDim()];
        myMapping   = new long    [hypercube.getNDim()];

        // Create the mapping
        for (int i=0, j=0; i < myMapping.length; i++) {
            final Accessor<?> accessor =
                (i < accessors.length) ? accessors[i] : null;
            if (accessor == null ||
                accessor instanceof Slice)
            {
                // This is a wildcard or a slice which we must save, either way
                // we copy it
                myMapping[i] = j;
                mySlices [j] = (Slice<?>)accessor;
                j++;
            }
            else if (accessor instanceof Coordinate) {
                // This is a direct offset into the contained hypercube. We flag
                // it as such by making it negative (offsetting because it might
                // be zero).
                myMapping[i] = -(((Coordinate<?>)accessor).get() + 1);
            }
            else {
                throw new IllegalArgumentException(
                    "Unhandled Accessor type: " + accessor
                );
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

        // The lowest dimension of the cube which we're wrapping, and the
        // accessor for it
        final int         dim      = getWrapped().getNDim() - 1;
        final Accessor<?> accessor = getAccessor(dim);

        // Index arrays, working space
        final long[] local   = new long[             getNDim()];
        final long[] wrapped = new long[getWrapped().getNDim()];

        // We start at srcPos and walk to srcPos+length. We do this in steps
        // such that we copy as much of the lowest dimension as possible at a
        // time.
        for (int pos = 0; pos < length; /*incr below*/) {
            // Determine the wrapped indices from the current position
            getWrappedIndices(srcPos + pos, local, wrapped);

            // Figure out the offset within the wrapped cube
            final long wrappedSrcPos = getWrapped().toOffset(wrapped);

            // Now figure out how far we want to walk within the wrapped cube
            final long left = length - pos;
            final long dimLeft;
            if (accessor == null) {
                // No restriction, go from the index to the end of the dimension
                dimLeft = Math.min(left,
                                   getWrapped().length(dim) - wrapped[dim]);
            }
            else if (accessor instanceof Slice) {
                // Go from the index to the end of the slice
                final Slice<?> slice = (Slice<?>)accessor;
                dimLeft = Math.min(left, slice.end() - wrapped[dim]);
            }
            else if (accessor instanceof Coordinate) {
                // A coordinate means just the single value
                dimLeft = 1;
            }
            else {
                throw new IllegalArgumentException(
                    "Unhandled Accessor type: " + accessor
                );
            }

            // Now we simply flatten from the cube into the destination array
            getWrapped().toFlattenedObjs(wrappedSrcPos,
                                         dst,
                                         dstPos + pos,
                                         (int)dimLeft);

            // And step forward over what we copied
            pos += dimLeft;
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

        // The lowest dimension of the cube which we're wrapping, and the
        // accessor for it
        final int         dim      = getWrapped().getNDim() - 1;
        final Accessor<?> accessor = getAccessor(dim);

        // Index arrays, working space
        final long[] local   = new long[             getNDim()];
        final long[] wrapped = new long[getWrapped().getNDim()];

        // We start at srcPos and walk to srcPos+length. We do this in steps
        // such that we copy as much of the lowest dimension as possible at a
        // time.
        for (long pos = 0; pos < length; /*incr below*/) {
            // Determine the wrapped indices from the current position
            getWrappedIndices((long)srcPos + pos, local, wrapped);

            // Figure out the offset within the wrapped cube
            final long wrappedSrcPos = getWrapped().toOffset(wrapped);

            // Now figure out how far we want to walk within the wrapped cube
            final long left = length - pos;
            final long dimLeft;
            if (accessor == null) {
                // No restriction, go from the index to the end of the dimension
                dimLeft = Math.min(left,
                                   getWrapped().length(dim) - wrapped[dim]);
            }
            else if (accessor instanceof Slice) {
                // Go from the index to the end of the slice
                final Slice<?> slice = (Slice<?>)accessor;
                dimLeft = Math.min(left, slice.end() - wrapped[dim]);
            }
            else if (accessor instanceof Coordinate) {
                // A coordinate means just the single value
                dimLeft = 1;
            }
            else {
                throw new IllegalArgumentException(
                    "Unhandled Accessor type: " + accessor
                );
            }

            // Now we simply flatten from the cube into the result array
            getWrapped().fromFlattenedObjs(src,
                                           srcPos + (int)pos,
                                           wrappedSrcPos,
                                           (int)dimLeft);

            // And step forward over what we copied
            pos += dimLeft;
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
     * Get the accessor for the given dimension index in the wrapped hypercube.
     */
    protected Accessor<?> getAccessor(final int i)
    {
        return myAccessors[i];
    }

    /**
     * Get the inner hypercube's indices, suitably offset, for the given index.
     */
    protected long[] getWrappedIndices(final long index)
    {
        final long[] local   = new long[mySlices .length];
        final long[] wrapped = new long[myMapping.length];
        getWrappedIndices(index, local, wrapped);
        return wrapped;
    }

    /**
     * Get the wrapped and this hypercube's indices, suitably offset, for the
     * given index. Populate these into the give arrays.
     */
    protected long[] getWrappedIndices(final long   index,
                                       final long[] local,
                                       final long[] wrapped)
    {
        // Check the given args. Since only controlled classes should be calling
        // this we're a little terse with the error cases.
        if (local  .length != mySlices .length ||
            wrapped.length != myMapping.length)
        {
            throw new IllegalArgumentException(
                "Bad arrays; " +
                "local length is "   + local .length  + " " +
                "not " + mySlices.length + ", " +
                "wrapped length is " + wrapped.length + " " +
                "not " + mySlices.length
            );
        }

        // Get the offsets in our local terms
        fromOffset(index, local);

        // Adjust by the slice offsets
        for (int i=0; i < myMapping.length; i++) {
            // See what the mapping says
            final long mapping = myMapping[i];
            if (mapping < 0) {
                // This is just a direct offset, flagged by its sign and
                // removing the offset
                wrapped[i] = -mapping - 1;
            }
            else {
                // Copy over the value
                wrapped[i] = local[(int)mapping];

                // And offset it with any slice information
                final Slice<?> slice = mySlices[(int)mapping];
                if (slice != null) {
                    wrapped[i] += slice.start();
                }
            }
        }

        // And return them
        return wrapped;
    }
}
