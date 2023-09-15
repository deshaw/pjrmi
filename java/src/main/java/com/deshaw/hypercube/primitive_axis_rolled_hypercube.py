import params

_PRIMITIVE_AXIS_ROLLED_HYPERCUBE = '''\
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

import java.util.Map;
import java.util.logging.Level;

/**
 * A shifted cube which is a rolled view of another {primitive_type}-based
 * {{@link Hypercube}} across one or multiple axes.
 */
public class {object_type}AxisRolledHypercube
    extends AxisRolledHypercube<{object_type}>
    implements {object_type}Hypercube
{{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final {object_type}Hypercube T;

    // -----------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private {object_type}Hypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to roll.
     * @param rolls      How to roll this cube. The number of rolls must match
     *                   the number of dimensions of the given {{@link Hypercube}}.
     *                   However, a roll may be {{@code null}} if it does not apply.
     *
     * @throws DimensionalityException  If the rolls did not match the
     *                                  {{@code hypercube}}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {{@code null}} pointer was
     *                                  encountered.
     */
    public {object_type}AxisRolledHypercube(final {object_type}Hypercube hypercube,
                                        final Roll<?>[] rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {{
        super(hypercube, rolls);

        T = new {object_type}TransposedHypercube(this);
        myHypercube = hypercube;
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public {object_type}Hypercube slice(final Dimension.Accessor<?>... accessors)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {{
        return new {object_type}SlicedHypercube(this, accessors);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public {object_type}Hypercube roll(final Dimension.Roll<?>... rolls)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {{
        // Combine the nested rolls more efficiently.

        // Check for dimension mismatch
        if (getWrapped().getNDim() != rolls.length) {{
            throw new DimensionalityException(
                "Number of rolls, " + rolls.length + ", didn't match " +
                "number of dimensions " + getWrapped().getNDim()
            );
        }}

        // A flag to check for NOP (all zero/null) rolls.
        boolean isNop = true;

        Roll<?>[] newRolls = new Roll[getNDim()];
        for (int i=0; i < rolls.length; i++) {{
            // Check for dimension mismatch
            Dimension<?> dimension = getWrapped().dim(i);
            if (rolls[i] != null && !dimension.equals(rolls[i].getDimension())) {{
                throw new DimensionalityException(
                    "Dimension of roll[" + i + "], " +
                    rolls[i].getDimension() + ", didn't match " +
                    "hypercube dimension " + dimension
                );
            }}

            // Otherwise combine the rolls together
            // roll.shift() always returns a non-negative integer, so we don't
            // have to worry about handling negative shifts here.
            long shift = ((rolls [i] != null ? rolls  [i].shift() : 0)  +
                         (getRoll(i) != null ? getRoll(i).shift() : 0)) %
                         dimension.length();

            // Only create a roll in this dimension if it applies.
            if (shift != 0) {{
                newRolls[i] = new Roll<>(dimension, shift);
                isNop = false;
            }}
        }}

        // Simply return the wrapped hypercube if the rolls don't result in any
        // transformation.
        if (isNop) {{
            return myHypercube;
        }}

        // And give back the axis-rolled cube while avoiding nested wrappers.
        return new {object_type}AxisRolledHypercube(myHypercube, newRolls);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public {object_type}Hypercube transpose()
    {{
        return this.T;
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void toFlattened(final long      srcPos,
                            final {primitive_type}[] dst,
                            final int       dstPos,
                            final int       length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               UnsupportedOperationException
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Flattening with " +
                "srcPos=" + srcPos + " dst=" + dst + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }}
        // Check the arguments
        checkFlattenArgs(srcPos, dst, dstPos, length);

        // The lowest dimension of the cube which we're wrapping, and the
        // accessor for it
        final int     dim  = getNDim() - 1;
        final Roll<?> roll = getRoll(dim);

        // Index arrays, working space
        final long[] local   = new long[getNDim()];
        final long[] wrapped = new long[getNDim()];

        // We start at srcPos and walk to srcPos+length. We do this in steps
        // such that we copy as much of the lowest dimension as possible at a
        // time.
        for (int pos = 0; pos < length; /*incr below*/) {{
            // Determine the wrapped indices from the current position
            getWrappedIndices(srcPos + pos, local, wrapped);

            // Figure out the offset within the wrapped cube
            final long wrappedSrcPos = getWrapped().toOffset(wrapped);

            // Now figure out how far we want to walk within the wrapped cube
            // Note that roll.shift() is always non-negative (as per the specs),
            // so we don't need to worry about negative shifts in here.
            final long left = length - pos;
            final long dimLeft;
            if (roll == null || local[dim] < roll.shift()) {{
                // No restriction, go from the index to the end of the dimension.
                dimLeft = Math.min(left, length(dim) - wrapped[dim]);
            }}
            else {{
                // Make sure to not cross the roll boundary.
                dimLeft = Math.min(left, length(dim) - wrapped[dim] - roll.shift());
            }}

            // This should fit into an int given the logic above. If it's not
            // then something has gone wrong.
            assert(dimLeft <= Integer.MAX_VALUE);

            // Now we simply flatten from the cube into the result array
            myHypercube.toFlattened(wrappedSrcPos,
                                    dst,
                                    dstPos + pos,
                                    (int)dimLeft);

            // And step forward over what we copied
            pos += dimLeft;
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void fromFlattened(final {primitive_type}[] src,
                              final int srcPos,
                              final long dstPos,
                              final int length)
        throws IllegalArgumentException,
               IndexOutOfBoundsException,
               NullPointerException
    {{
        if (LOG.isLoggable(Level.FINEST)) {{
            LOG.finest(
                "Unflattening with " +
                "src=" + src + " srcPos=" + srcPos + " dstPos=" + dstPos + " " +
                "length=" + length
            );
        }}

        // Sanitise input
        checkUnflattenArgs(srcPos, dstPos, length);
        if (src == null) {{
            throw new NullPointerException("Given a null array");
        }}
        if (src.length - srcPos < length) {{
            throw new IndexOutOfBoundsException(
                "Source position, " + srcPos + ", " +
                "plus length ," + length + ", " +
                "was greater than the array size, " + src.length
            );
        }}

        // The lowest dimension of the cube which we're wrapping, and the
        // accessor for it
        final int     dim  = getNDim() - 1;
        final Roll<?> roll = getRoll(dim);

        // Index arrays, working space
        final long[] local   = new long[getNDim()];
        final long[] wrapped = new long[getNDim()];

        // We start at srcPos and walk to srcPos+length. We do this in steps
        // such that we copy as much of the lowest dimension as possible at a
        // time.
        for (int pos = 0; pos < length; /*incr below*/) {{
            // Determine the wrapped indices from the current position
            getWrappedIndices((long)srcPos + pos, local, wrapped);

            // Figure out the offset within the wrapped cube
            final long wrappedSrcPos = getWrapped().toOffset(wrapped);

            // Now figure out how far we want to walk within the wrapped cube
            // Note that roll.shift() is always non-negative (as per the specs),
            // so we don't need to worry about negative shifts in here.
            final long left = length - pos;
            final long dimLeft;
            if (roll == null || local[dim] < roll.shift()) {{
                // No restriction, go from the index to the end of the dimension.
                dimLeft = Math.min(left, length(dim) - wrapped[dim]);
            }}
            else {{
                // Make sure to not cross the roll boundary.
                dimLeft = Math.min(left, length(dim) - wrapped[dim] - roll.shift());
            }}

            // This should fit into an int given the logic above. If it's not
            // then something has gone wrong.
            assert(dimLeft <= Integer.MAX_VALUE);

            // Now we simply flatten from the cube into the result array
            myHypercube.fromFlattened(src,
                                      srcPos + pos,
                                      wrappedSrcPos,
                                      (int)dimLeft);

            // And step forward over what we copied
            pos += dimLeft;
        }}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} get(final long... indices)
        throws IndexOutOfBoundsException
    {{
        return getAt(toOffset(indices));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void set(final {primitive_type} value, final long... indices)
        throws IndexOutOfBoundsException
    {{
        setAt(toOffset(indices), value);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} getAt(final long index)
        throws IndexOutOfBoundsException
    {{
        // Give it back from the parent
        return myHypercube.get(getWrappedIndices(index));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setAt(final long index, final {primitive_type} value)
        throws IndexOutOfBoundsException
    {{
        // Set it in the parent
        myHypercube.set(value, getWrappedIndices(index));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void assignFrom(final Object object)
        throws IllegalArgumentException
    {{
        {object_type}Hypercube.super.assignFrom(object);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {{
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",      false);
        result.put("behaved",      false);
        result.put("c_contiguous", false);
        result.put("owndata",      true);
        result.put("writeable",    true);
        return result;
    }}
}}
'''

def generate(dtype):
    return _PRIMITIVE_AXIS_ROLLED_HYPERCUBE.format(**params.get_kwargs(dtype))
