package com.deshaw.hypercube;

// Recreate with `cog -rc LongMaskedHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_masked_hypercube
//
//     cog.outl(primitive_masked_hypercube.generate(numpy.int64))
// ]]]
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;
import com.deshaw.util.LongBitSet;

import java.util.Map;
import java.util.logging.Level;

/**
 * A sub-cube which is a masked view of another long-based
 * {@link Hypercube}.
 */
public class LongMaskedHypercube
    extends MaskedHypercube<Long>
    implements LongHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final LongHypercube T;

    // -----------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private LongHypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to slice.
     * @param mask       How to mask off the first dimension of the cube. The
     *                   mask length must match that of the cube's first
     *                   dimension.
     *
     * @throws DimensionalityException  If the slices did not match the
     *                                  {@code hypercube}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {@code null} pointer was
     *                                  encountered.
     */
    public LongMaskedHypercube(final LongHypercube hypercube,
                                        final boolean[] mask)
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
    public LongMaskedHypercube(final LongHypercube hypercube,
                                        final LongBitSet mask)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(hypercube, mask);

        T = new LongTransposedHypercube(this);
        myHypercube = hypercube;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new LongSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new LongAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public LongHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattened(final long srcPos,
                            final long[] dst,
                            final int dstPos,
                            final int length)
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
            // Validate. This should be logically the same as the test above.
            if (srcPos + length > getMapping().size()) {
                throw new IndexOutOfBoundsException(
                    "Source position, " + srcPos + ", " +
                    "plus length, " + length + ", " +
                    "was greater than the dimension size, " + getMapping().size()
                );
            }

            // Just walk it directly and set. We could make this more efficient
            // by handling ranges of values in bulk but that adds complexity and
            // so we hope that this is mostly fine for now.
            for (int i = 0; i < length; i++) {
                final long index = getMapping().get(i + srcPos);
                dst[i + dstPos] = myHypercube.getAt(index);
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
                final long wrappedSrcPos = myHypercube.toOffset(indices);

                // Now figure out how far we want to walk within the wrapped cube
                final long dimLeft =
                    Math.min(length - pos,
                             myHypercube.length(dim) - indices[dim]);

                // Now we simply flatten from the cube into the destination array
                myHypercube.toFlattened(wrappedSrcPos,
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
    public void fromFlattened(final long[] src,
                              final int srcPos,
                              final long dstPos,
                              final int length)
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
            // Validate. This should be logically the same as the test above.
            if (dstPos + length > getMapping().size()) {
                throw new IndexOutOfBoundsException(
                    "Destination position, " + dstPos + ", " +
                    "plus length, " + length + ", " +
                    "was greater than the dimension size, " + getMapping().size()
                );
            }

            // Just walk it directly and set. We could make this more efficient
            // by handling ranges of values in bulk but that adds complexity and
            // so we hope that this is mostly fine for now.
            for (int i = 0; i < length; i++) {
                final long index = getMapping().get(i + dstPos);
                myHypercube.setObjectAt(index, src[i + srcPos]);
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
                final long wrappedSrcPos = myHypercube.toOffset(indices);

                // Now figure out how far we want to walk within the wrapped cube
                final long dimLeft =
                    Math.min(length - pos,
                             myHypercube.length(dim) - indices[dim]);

                // Now we simply flatten from the cube into the result array
                myHypercube.fromFlattened(src,
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
    public long weakGet(final long... indices)
        throws IndexOutOfBoundsException
    {
        return weakGetAt(toOffset(indices));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final long value, final long... indices)
        throws IndexOutOfBoundsException
    {
        weakSetAt(toOffset(indices), value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        // Give it back from the parent
        return myHypercube.weakGet(getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final long value)
        throws IndexOutOfBoundsException
    {
        // Set it in the parent
        myHypercube.weakSet(value, getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void assignFrom(final Object object)
        throws IllegalArgumentException
    {
        LongHypercube.super.assignFrom(object);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      false);
        result.put("behaved",      false);
        result.put("c_contiguous", false);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }
}

// [[[end]]] (checksum: 83b8dbc10f46569ab9d67a79236d0a4b)
