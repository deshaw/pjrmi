import params

_PRIMITIVE_ND_WRAPPING_HYPERCUBE = '''\
import java.util.Map;

/**
 * A {ndims}-dimensional hypercube which has {{@code {primitive_type}}} values
 * as its elements and is backed by a user-supplied array.
 */
public class {object_type}{ndims}dWrappingHypercube
    extends Abstract{object_type}Hypercube
{{
    /**
     * The array(s) which we wrap.
     */
    private final {primitive_type}{array_defn} myElements;

    /**
     * Get the {{@link Dimension}}s of the given array.
     */
    private static Dimension<?>[] getDimensions(final {primitive_type}{array_defn} array)
    {{
{get_dimensions}
    }}

    /**
     * Constructor.
     */
    public {object_type}{ndims}dWrappingHypercube(final {primitive_type}{array_defn} array)
        throws IllegalArgumentException,
               NullPointerException
    {{
        super(getDimensions(array));

        myElements = array;
    }}

    /**
     * {{@inheritDoc}}
     *
     * Attempts to get values where the underlying array data is missing will yield
     * the result of {{@code {primitive_from_null}}}.
     */
    @Override
    public {primitive_type} get(final long... indices)
        throws IndexOutOfBoundsException
    {{
{get_at_indices}
    }}

    /**
     * {{@inheritDoc}}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void set(final {primitive_type} value, final long... indices)
        throws IndexOutOfBoundsException
    {{
{set_at_indices}
    }}

    /**
     * {{@inheritDoc}}
     *
     * Attempts to get values where the underlying array is missing will yield
     * the result of {{@code {primitive_from_null}}}.
     */
    @Override
    public {primitive_type} getAt(long index)
        throws IndexOutOfBoundsException
    {{
{get_at_offset}
    }}

    /**
     * {{@inheritDoc}}
     *
     * Attempts to set values where the underlying array capacity is too small
     * will be silently ignored.
     */
    @Override
    public void setAt(long index, final {primitive_type} value)
        throws IndexOutOfBoundsException
    {{
{set_at_offset}
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public {object_type} getObjectAt(final long index)
        throws IndexOutOfBoundsException
    {{
        return {object_type}.valueOf(getAt(index));
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void setObjectAt(final long index, final {object_type} value)
        throws IndexOutOfBoundsException
    {{
        setAt(index, (value == null) ? {primitive_from_null} : value.{primitive_type}Value());
    }}

    /**
     * {{@inheritDoc}}
     */
    @Override
    protected Map<String,Boolean> createFlags()
    {{
        final Map<String,Boolean> result = super.createFlags();
        result.put("aligned",   true );
        result.put("owndata",   false);
        result.put("writeable", true );
        return result;
    }}
}}
'''

def _get_dimensions(ndims, kwargs):
    head = '''\
        final long[] shape = new long[{ndims}];
'''.format(ndims=ndims)
    tail = '''\
        return Dimension.of(shape);'''

    prev_array = 'array'
    for dim in range(ndims):
        indent = '    ' * dim
        subarray = 'array%d' % (dim+1)

        head += '''\
        {indent}shape[{dim}] = Math.max(shape[{dim}], {prev_array}.length);
'''.format(dim       =dim,
           prev_array=prev_array,
           indent    =indent)

        if dim < (ndims-1):
            head += '''\
        {indent}for ({primitive_type}{array_defn} {subarrary} : {prev_array}) {{
'''.format(primitive_type=kwargs['primitive_type'],
           array_defn    ='[]' * (ndims - dim - 1),
           subarrary     =subarray,
           prev_array    =prev_array,
           indent        =indent)

            tail = '''\
        {indent}}}
'''.format(indent=indent) + tail

        prev_array = subarray

    return head + tail


def _get_set_at(ndims, kwargs, retval, is_offset):
    result = ''

    if is_offset:
        result += '''\
        if (index < 0 || index >= size) {{
            throw new IndexOutOfBoundsException(
                "Bad index: " + index
            );
        }}
'''
        for dim in range(ndims):
            if dim < ndims-1:
                result += '''\
        final long index{dim} = (index % length({dim})); index /= length({dim});
'''.format(dim=(ndims-dim-1))
            else:
                result += '''\
        final long index{dim} = index;
'''.format(dim=(ndims-dim-1))
    else:
        result += '''\
        if (indices == null) {{
            throw new NullPointerException("Given null indices");
        }}
        if (indices.length != {ndims}) {{
            throw new IndexOutOfBoundsException(
                "Wanted {ndims} indices but had " + indices.length
            );
        }}
'''.format(ndims=ndims)
        for dim in range(ndims):
            result += '''\
        final long index{dim} = indices[{dim}];
        if (index{dim} < 0 || index{dim} >= length({dim})) {{
            throw new IndexOutOfBoundsException(
                "Bad index[{dim}]: " + index{dim}
            );
        }}
'''.format(dim=(ndims-dim-1))

    prev_array = 'myElements'
    for dim in range(ndims):
        result += '''
        if (index{dim} >= {prev_array}.length) {{
            return{retval};
        }}
'''.format(dim       =dim,
           prev_array=prev_array,
           retval    =retval)
        if dim < ndims-1:
            subarray = 'array%d' % (dim+1)
            result += '''\
        final {primitive_type}{array_defn} {subarray} = {prev_array}[(int)index{dim}];
'''.format(dim           =dim,
           primitive_type=kwargs['primitive_type'],
           array_defn    ='[]' * (ndims - dim - 1),
           subarray      =subarray,
           prev_array    =prev_array)

            prev_array = subarray

    return (result, prev_array)


def _get_at(ndims, kwargs, is_offset):
    (result, prev_array) = _get_set_at(ndims, kwargs, ' ' + kwargs['primitive_from_null'], is_offset)
    result += '''
        return {prev_array}[(int)index{dim}];\
'''.format(prev_array=prev_array,
           dim       =ndims-1)
    return result


def _set_at(ndims, kwargs, is_offset):
    (result, prev_array) = _get_set_at(ndims, kwargs, '', is_offset)
    result += '''
        {prev_array}[(int)index{dim}] = value;\
'''.format(prev_array=prev_array,
           dim       =ndims-1)

    return result


def generate(ndims, dtype):
    kwargs = dict(params.get_kwargs(dtype))
    kwargs['ndims'         ] = ndims
    kwargs['get_dimensions'] = _get_dimensions(ndims, kwargs)
    kwargs['get_at_indices'] = _get_at        (ndims, kwargs, False)
    kwargs['set_at_indices'] = _set_at        (ndims, kwargs, False)
    kwargs['get_at_offset' ] = _get_at        (ndims, kwargs, True)
    kwargs['set_at_offset' ] = _set_at        (ndims, kwargs, True)
    kwargs['array_defn'    ] = '[]' * ndims
    return _PRIMITIVE_ND_WRAPPING_HYPERCUBE.format(**kwargs)
