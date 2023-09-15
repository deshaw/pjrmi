import params

_PRIMITIVE_FLAT_ROLLED_HYPERCUBE = '''\
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

import java.util.Map;
import java.util.logging.Level;

/**
 * A shifted cube which is a flat-rolled view of another {primitive_type}-based
 * {{@link Hypercube}}. Essentially, the given cube is first flattened, then
 * shifted, and finally unflattened into its original shape.
 */
public class {object_type}FlatRolledHypercube
    extends FlatRolledHypercube<{object_type}>
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
     * @param shift      How to roll this cube. The (signed) number of elements
     *                   to roll the given {{@link Hypercube}} by.
     *
     * @throws NullPointerException     If a {{@code null}} pointer was
     *                                  encountered.
     */
    public {object_type}FlatRolledHypercube(final {object_type}Hypercube hypercube,
                                        final long shift)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {{
        super(hypercube, shift);

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
    public {object_type}Hypercube rollFlat(final long shift)
    {{
        // Combine the nested rolls more efficiently.
        if (getSize()                       == 0 ||
           (getShift() + shift) % getSize() == 0)
        {{
            return myHypercube;
        }}
        else if (shift % getSize() == 0) {{
            return this;
        }}
        else {{
            return new {object_type}FlatRolledHypercube(myHypercube, getShift() + shift);
        }}
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
        return new {object_type}AxisRolledHypercube(this, rolls);
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

        // Note that the getShift() is always non-negative so we don't need to
        // worry about handling negative values.
        // The case where we have a single block in our wrapped cube.
        if (srcPos + length <= getShift() || srcPos >= getShift()) {{
            myHypercube.toFlattened(offset(srcPos),
                                    dst,
                                    dstPos,
                                    length);
        }}
        // The case where have 2 blocks in our wrapped cube.
        else {{
            // The size of the tail of the wrapped cube we have to copy
            final int tail = (int)(getShift() - srcPos);
            myHypercube.toFlattened(offset(srcPos),
                                    dst,
                                    dstPos,
                                    tail);
            myHypercube.toFlattened(0,
                                    dst,
                                    dstPos + tail,
                                    length - tail);
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

        // Note that the getShift() is always non-negative so we don't need to
        // worry about handling negative values.
        // The case where we have a single block in our wrapped cube.
        if (dstPos + length <= getShift() || dstPos >= getShift()) {{
            myHypercube.fromFlattened(src,
                                      srcPos,
                                      offset(dstPos),
                                      length);
        }}
        // The case where have 2 blocks in our wrapped cube.
        else {{
            // The size of the tail of the wrapped cube we have to copy
            final int tail = (int)(getShift() - dstPos);
            myHypercube.fromFlattened(src,
                                      srcPos,
                                      offset(dstPos),
                                      tail);
            myHypercube.fromFlattened(src,
                                      srcPos + tail,
                                      0,
                                      length - tail);
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
    return _PRIMITIVE_FLAT_ROLLED_HYPERCUBE.format(**params.get_kwargs(dtype))
