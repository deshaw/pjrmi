package com.deshaw.hypercube;

import com.deshaw.hypercube.Dimension.Roll;

import java.util.Arrays;
import java.util.logging.Level;

/**
 * A shifted cube which is a rolled view of another {@link Hypercube} across
 * one or multiple axes.
 *
 * @param <T> The type of the element which we store.
 */
public class AxisRolledHypercube<T>
    extends WrappingHypercube<T>
{
    /**
     * The rolls of the contained hypercube which define this shifted cube. This
     * array's length matches our dimensionality.
     */
    private final Roll<?>[] myRolls;

    /**
     * The local indices for various functions, so we don't need to keep making
     * them.
     */
    private final ThreadLocal<long[]> myLocalIndices;

    /**
     * The wrapped indices for various functions, so we don't need to keep
     * making them.
     */
    private final ThreadLocal<long[]> myWrappedIndices;

    // ----------------------------------------------------------------------

    /**
     * Determine the dimensions of this shifted cube, given a hypercube and a
     * set of rolls.
     */
    private static <T> Dimension<?>[] getDimensions(final Hypercube<T> hypercube,
                                                    final Roll<?>[]    rolls)
        throws DimensionalityException,
               NullPointerException
    {
        if (hypercube == null) {
            throw new NullPointerException("Given a null hypercube");
        }
        if (rolls == null) {
            throw new NullPointerException("Given a null array of rolls");
        }

        // Check the lengths are compatible, Any missing Roll will be treated
        // as an unconstrained roll later on.
        if (rolls.length != hypercube.getNDim()) {
            throw new DimensionalityException(
                "Number of rolls, " + rolls.length + ", didn't match " +
                "number of dimensions " + hypercube.getNDim()
            );
        }

        for (int i=0; i < rolls.length; i++) {
            Dimension<?> dimension = hypercube.dim(i);
            if (rolls[i] != null && !dimension.equals(rolls[i].getDimension())) {
                throw new DimensionalityException(
                    "Dimension of roll[" + i + "], " +
                    rolls[i].getDimension() + ", didn't match " +
                    "hypercube dimension " + dimension
                );
            }
        }
        return hypercube.getDimensions();
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to roll.
     * @param rolls      How to roll this cube. The number of rolls must match
     *                   the number of dimensions of the given {@link Hypercube}.
     *                   However, a roll may be {@code null} if it does not apply.
     *
     * @throws DimensionalityException  If the rolls did not match the
     *                                  {@code hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public AxisRolledHypercube(final Hypercube<T> hypercube,
                               final Roll<?>[]    rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(getDimensions(hypercube, rolls), hypercube);

        myRolls = Arrays.copyOf(rolls, rolls.length);

        myWrappedIndices = new ThreadLocal<>() {
            @Override protected long[] initialValue() {
                return new long[getNDim()];
            }
        };
        myLocalIndices = new ThreadLocal<>() {
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

        // The lowest dimension of the cube which we're wrapping, and the
        // roll for it
        final int     dim  = getNDim() - 1;
        final Roll<?> roll = getRoll(dim);

        // Index arrays, working space
        final long[] local   = myLocalIndices  .get();
        final long[] wrapped = myWrappedIndices.get();

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
            if (roll == null || local[dim] < roll.shift()) {
                // No restriction, go from the index to the end of the dimension.
                dimLeft = Math.min(left, length(dim) - wrapped[dim]);
            }
            else {
                // Make sure to not cross the roll boundary.
                dimLeft = Math.min(left, length(dim) - wrapped[dim] - roll.shift());
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
        // roll for it
        final int     dim  = getNDim() - 1;
        final Roll<?> roll = getRoll(dim);

        // Index arrays, working space
        final long[] local   = myLocalIndices  .get();
        final long[] wrapped = myWrappedIndices.get();

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
            if (roll == null || local[dim] < roll.shift()) {
                // No restriction, go from the index to the end of the dimension.
                dimLeft = Math.min(left, length(dim) - wrapped[dim]);
            }
            else {
                // Make sure to not cross the roll boundary.
                dimLeft = Math.min(left, length(dim) - wrapped[dim] - roll.shift());
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
    public Hypercube<T> roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        // Combine the nested rolls more efficiently.

        // Check for dimension mismatch
        if (getWrapped().getNDim() != rolls.length) {
            throw new DimensionalityException(
                "Number of rolls, " + rolls.length + ", didn't match " +
                "number of dimensions " + getWrapped().getNDim()
            );
        }

        Roll<?>[] newRolls = new Roll[myRolls.length];
        for (int i=0; i < rolls.length; i++) {
            // Check for dimension mismatch
            Dimension<?> dimension = getWrapped().dim(i);
            if (rolls[i] != null && !dimension.equals(rolls[i].getDimension())) {
                throw new DimensionalityException(
                    "Dimension of roll[" + i + "], " +
                    rolls[i].getDimension() + ", didn't match " +
                    "hypercube dimension " + dimension
                );
            }

            // Otherwise combine the rolls together
            // roll.shift() always returns a non-negative integer, so we don't
            // have to worry about handling negative shifts here.
            long shift = (((rolls [i] != null) ? rolls  [i].shift() : 0)  +
                         ((myRolls[i] != null) ? myRolls[i].shift() : 0)) % dimension.length();

            // Only create a roll in this dimension if it applies.
            if (shift != 0) {
                newRolls[i] = new Roll<>(dimension, shift);
            }
        }
        // And give back the rolled cube while avoiding nested wrappers.
        return new AxisRolledHypercube<T>(this.getWrapped(), newRolls);
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
     * Get the roll for the given dimension index in the wrapped hypercube.
     */
    protected Roll<?> getRoll(final int i)
    {
        return myRolls[i];
    }

    /**
     * Get the inner hypercube's indices, suitably offset, for the given index.
     */
    protected long[] getWrappedIndices(final long index)
    {
        final long[] local   = myLocalIndices  .get();
        final long[] wrapped = myWrappedIndices.get();
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
        if (local  .length != myRolls.length ||
            wrapped.length != myRolls.length)
        {
            throw new IllegalArgumentException(
                "Bad arrays; " +
                "local length is "   + local .length  + " " +
                "not " + myRolls.length + ", " +
                "wrapped length is " + wrapped.length + " " +
                "not " + myRolls.length
            );
        }

        // Get the offsets in our local terms
        fromOffset(index, local);

        // Adjust by the roll offsets
        for (int i=0; i < myRolls.length; i++) {
            // Apply the shift
            // Make sure the wrapped index is non-negative
            wrapped[i] = (myRolls[i] != null)
                ? ((local[i] - myRolls[i].shift() + myRolls[i].getDimension().length()) %
                   myRolls[i].getDimension().length())
                : local[i];
        }

        // And return them
        return wrapped;
    }
}
