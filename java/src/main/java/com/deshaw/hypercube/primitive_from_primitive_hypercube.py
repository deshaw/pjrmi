import params

_PRIMITIVE_CASTING_HYPERCUBE = '''\
import java.util.Map;

/**
 * A {primitive_type} {{@link Hypercube}} which is a view of a {inner_primitive_type}
 * one that casts values from one type to another.
 *
 * <p>The casting follows Java language semantics meaning null values may not be
 * preserved.
 */
public class {object_type}From{inner_object_type}Hypercube
    extends Abstract{object_type}Hypercube
    implements {object_type}Hypercube
{{
    /**
     * The hypercube which we wrap.
     */
    private {inner_object_type}Hypercube myHypercube;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param hypercube  The hypercube to cast to/from.
     *
     * @throws IllegalArgumentException If there was any problem with the arguments.
     * @throws NullPointerException     If a {{@code null}} pointer was
     *                                  encountered.
     */
    public {object_type}From{inner_object_type}Hypercube(final {inner_object_type}Hypercube hypercube)
        throws IllegalArgumentException,
               NullPointerException
    {{
        super(hypercube.getDimensions());

        myHypercube = hypercube;
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {object_type} getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {{
        final {inner_object_type} obj = myHypercube.getObjectAt(index);
        return (obj == null) ? null : ({primitive_type})(obj.{inner_primitive_type}Value(){num_to_primitive});
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setObjectAt(final long index, final {object_type} value)
        throws IndexOutOfBoundsException
    {{
        myHypercube.setObjectAt(
            index,
            (value == null) ? null : ({inner_primitive_type})(value.{primitive_type}Value(){num_from_primitive})
        );
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} getAt(final long index)
        throws IndexOutOfBoundsException
    {{
        return ({primitive_type})(myHypercube.getAt(index){num_to_primitive});
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setAt(final long index, final {primitive_type} value)
        throws IndexOutOfBoundsException
    {{
        myHypercube.setAt(index, ({inner_primitive_type})(value{num_from_primitive}));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {primitive_type} get(final long... indices)
        throws IndexOutOfBoundsException
    {{
        return ({primitive_type})(myHypercube.get(indices){num_to_primitive});
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void set(final {primitive_type} value, final long... indices)
        throws IndexOutOfBoundsException
    {{
        myHypercube.set(({inner_primitive_type})(value{num_from_primitive}), indices);
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
        result.put("owndata",      false);
        result.put("writeable",    true);
        return result;
    }}
}}
'''

def generate(dtype, inner_dtype):
    kwargs = params.get_kwargs(dtype)
    for (k, v) in params.get_kwargs(inner_dtype).items():
        kwargs['inner_' + k] = v
    return _PRIMITIVE_CASTING_HYPERCUBE.format(**kwargs)
