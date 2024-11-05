package com.deshaw.hypercube;

// Recreate with `cog -rc BooleanTransposedHypercube.java`
// [[[cog
//     import cog
//     import numpy
//     import primitive_transposed_hypercube
//
//     cog.outl(primitive_transposed_hypercube.generate(numpy.bool_))
// ]]]

import com.deshaw.pjrmi.PJRmi.GenericReturnType;

import java.util.Map;
import java.util.logging.Level;

/**
 * A cube which is a transposed view of another boolean-based
 * {@link Hypercube}. This effectively means that its accessors are reversed
 * in order.
 */
public class BooleanTransposedHypercube
    extends TransposedHypercube<Boolean>
    implements BooleanHypercube
{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final BooleanHypercube T;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private final BooleanHypercube myHypercube;

    /**
     * The indices for getWrappedIndices(), so we don't need to keep making
     * them.
     */
    private final ThreadLocal<long[]> myWrappedIndices;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to transpose.
     *
     * @throws NullPointerException If a {@code null} pointer was encountered.
     */
    public BooleanTransposedHypercube(final BooleanHypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        super(hypercube);

        // The transpose of the transpose is the original
        T = myHypercube = hypercube;

        myWrappedIndices = new ThreadLocal<>();
        myWrappedIndices.set(new long[getNDim()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        return new BooleanSlicedHypercube(this, accessors);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {
        if (rolls == null) {
            throw new NullPointerException("Given a null array of rolls");
        }

        // Check the lengths are compatible, Any missing Roll will be treated
        // as an unconstrained roll later on.
        if (rolls.length != getNDim()) {
            throw new DimensionalityException(
                "Number of rolls, " + rolls.length + ", didn't match " +
                "number of dimensions " + getNDim()
            );
        }

        // A flag to check for NOP (all zero/null) rolls.
        boolean isNop = true;

        for (int i=0; i < rolls.length; i++) {
            if (rolls[i] != null && !dim(i).equals(rolls[i].getDimension())) {
                throw new DimensionalityException(
                    "Dimension of roll[" + i + "], " +
                    rolls[i].getDimension() + ", didn't match " +
                    "hypercube dimension " + dim(i)
                );
            }
            else if (rolls[i] != null && rolls[i].shift() != 0) {
                isNop = false;
            }
        }

        // Simply return the original hypercube if all the rolls are NOP.
        if (isNop) {
            return this;
        }

        return new BooleanAxisRolledHypercube(this, rolls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @GenericReturnType
    public BooleanHypercube transpose()
    {
        return this.T;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final boolean[] dst,
                            final int       dstPos,
                            final int       length)
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

        // Copy...
        for (int i=0; i < length; i++) {
            dst[i + dstPos] = getAt(i + srcPos);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void fromFlattened(final boolean[] src,
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

        // Copy in...
        for (int i=0; i < length; i++) {
            setAt(i + dstPos, src[i + srcPos]);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGet(final long... indices)
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
        return myHypercube.weakGet(wrapped);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSet(final boolean value, final long... indices)
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
        myHypercube.weakSet(value, wrapped);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean weakGetAt(final long index)
        throws IndexOutOfBoundsException
    {
        // Give it back from the parent
        return myHypercube.weakGet(getWrappedIndices(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void weakSetAt(final long index, final boolean value)
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
        BooleanHypercube.super.assignFrom(object);
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
        result.put("owndata",      false);
        result.put("writeable",    true);
        return result;
    }
}

// [[[end]]] (checksum: a5609f556af0423215ef9f2d68da1402)
