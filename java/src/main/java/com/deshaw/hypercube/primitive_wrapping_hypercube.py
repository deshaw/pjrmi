import params

_PRIMITIVE_WRAPPING_HYPERCUBE = '''\
import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Coordinate;
import com.deshaw.hypercube.Dimension.Slice;
import com.deshaw.hypercube.Dimension.Roll;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;

/**
 * A cube which is a view of another {primitive_type}-based {{@link Hypercube}}.
 */
public class {object_type}WrappingHypercube
    extends WrappingHypercube<{object_type}>
    implements {object_type}Hypercube
{{
    // Public members which look like numpy.ndarray ones

    /**
     * The transpose of this cube.
     */
    public final {object_type}Hypercube T;

    /**
     * The number of bytes required to store the hypercube's data.
     */
    public final long nbytes;

    // -------------------------------------------------------------------------

    /**
     * The hypercube which we wrap.
     */
    private {object_type}Hypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param dimensions  The dimensions of this hypercube.
     * @param hypercube   The hypercube to wrap.
     *
     * @throws DimensionalityException  If the dimensions are inconsistent with
     *                                  the {{@code hypercube}}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {{@code null}} pointer was
     *                                  encountered.
     */
    public {object_type}WrappingHypercube(final Dimension<?>[] dimensions,
                                          final {object_type}Hypercube hypercube)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {{
        super(dimensions, hypercube);

        T = new {object_type}TransposedHypercube(this);
        nbytes = size * itemsize;

        myHypercube = hypercube;
    }}

    /**
     * Constructor.
     *
     * @param dimensions  The dimensions of this hypercube.
     * @param hypercube   The hypercube to wrap.
     *
     * @throws DimensionalityException  If the dimensions are inconsistent with
     *                                  the {{@code hypercube}}'s dimensions.
     * @throws IllegalArgumentException If there was any other problem with the
     *                                  arguments.
     * @throws NullPointerException     If a {{@code null}} pointer was
     *                                  encountered.
     */
    public {object_type}WrappingHypercube(final Dimension<?>[] dimensions,
                                          final {object_type}Hypercube hypercube,
                                          final {object_type}Hypercube transposed)
        throws DimensionalityException,
               IllegalArgumentException,
               NullPointerException
    {{
        super(dimensions, hypercube, transposed);

        T = transposed;
        nbytes = size * itemsize;

        myHypercube = hypercube;
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    @GenericReturnType
    public {object_type}Hypercube slice(final Accessor<?>... accessors)
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
    public {object_type}Hypercube roll(final Roll<?>... rolls)
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
    public {primitive_type} getAt(final long index)
        throws IndexOutOfBoundsException
    {{
        return getWrapped().getAt(index);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setAt(final long index, final {primitive_type} value)
        throws IndexOutOfBoundsException
    {{
        getWrapped().setAt(index, value);
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} get(final long... indices)
        throws IndexOutOfBoundsException
    {{
        return getWrapped().getAt(toOffset(indices));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void set(final {primitive_type} d, final long... indices)
        throws IndexOutOfBoundsException
    {{
        getWrapped().setAt(toOffset(indices), d);
    }}
    /**
     * {{@inheritDoc}}
     */
    @Override
    protected {object_type}Hypercube getWrapped()
    {{
        return myHypercube;
    }}
}}
'''

def generate(dtype):
    return _PRIMITIVE_WRAPPING_HYPERCUBE.format(**params.get_kwargs(dtype))
