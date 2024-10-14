"""
Cog code to generate a vectorized cube math library.

For a list of supported data types, refer to ``DTYPES`` below.
"""

import numpy
import params
import cube_math

# A list of all operations that we support vectorization for. Make sure to keep
# this updated, in case of any changes.
_VECTORIZED_OPS = [
    'add', 'subtract', 'multiply', 'divide',
    'power', 'minimum', 'maximum',
    'negative', 'abs',
    'and', 'or', 'xor', 'not',
    'cos', 'cosh', 'sin', 'sinh', 'tan', 'tanh',
    'exp', 'log', 'log10',
    'sum', 'min', 'max', 'nansum',
    'equal', 'not_equal', 'less', 'greater', 'less_equal', 'greater_equal',
    'any', 'all',
    'popcount',
]


# A list of all operations that we do not support vectorization for. Make sure
# to keep this updated, in case of any changes.
_STANDARD_OPS = [
    'mod',
    'floor', 'round', 'ceil',
    'extract'
]


_VECTORIZED_CUBE_MATH_IMPL = {
    # A dictionary of required keyword arguments for formatting the cog code.
    # This dictionary should always include 'class_name'.
    'KWARGS': {
        'class_name': 'VectorizedCubeMath',
    },

    'DTYPES': [
        numpy.bool_,
        numpy.int32,
        numpy.int64,
        numpy.float32,
        numpy.float64
    ],

    'IMPORTS': '''\
import com.deshaw.pjrmi.KwargUtil;
import com.deshaw.pjrmi.PJRmi.GenericReturnType;
import com.deshaw.pjrmi.PJRmi.Kwargs;
import com.deshaw.python.DType;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import jdk.incubator.vector.*;
''',

    'HEADER': '''\

/**
 * Vectorized Math operations on {{@link Hypercube}} instances.

 * <p>This class uses the Vector API, an incubating module in Java that enables
 * efficient SIMD operations for enhanced parallel processing, if the underlying
 * hardware architecture is available.
 *
 * <p><b>IMPORTANT</b>: This class should be considered highly experimental.
 *
 * <p><b>IMPORTANT</b>: The Vector API is an incubating module in Java, meaning that it is
 * still under development and subject to change. Be aware of the following:<ol>
 *
 * <li>Compatibility: Incubator modules may not be compatible with future versions
 *                    of Java, and their API may change as the module evolves.
 *
 * <li>Documentation: Refer to official Java documentation and resources for the
 *                    latest information on incubator modules and their usage.
 *
 * <li>Performance: While the Vector API is designed to enable efficient SIMD
 *                  operations, actual performance gains is dependent on the
 *                  underlying hardware architecture and other factors.
 *
 * <li>Future Releases: APIs and features in incubating modules may be refined
 *                      or deprecated before being promoted to standard Java
 *                      modules. Keep this in mind when planning for long-term
 *                      development.
 * </ol>
 * <p>Incubator modules should be used with caution. Make sure to monitor
 * updates and announcements related to the Vector API incubating module.
 *
 * <p>For more information, visit: https://openjdk.java.net/jeps/338
 *
 * <p>Note: This library provides a vectorized implementation for a select list
 * of operations whenever possible, and falls back to the non-vectorized (standard)
 * implementation when necessary. The full list of supported vectorized operations
 * can be found below:<ul>{supported_vectorized_ops}
 * </ul>
 */
public class {class_name}
{{
    /**
     * The logger for all the Hypercube code.
     */
    private static final Logger LOG = Logger.getLogger("com.deshaw.hypercube");

''',

    # Helper cog code to generate all additional static declarations for Vector API.
    'SPECIES_DECLARATION': '''\
    /**
     * The {{@code VectorSpecies}} used for processing {vector_primitive_type} cubes using Vector API.
     */
    private static final VectorSpecies<{vector_object_type}> {vector_object_type_uppercase}_SPECIES = {vector_type}.SPECIES_MAX;

    /**
     * The number of {vector_primitive_type} values each {{@code {vector_type}}} can process.
     */
    private static final int {vector_object_type_uppercase}_SPECIES_LENGTH = {vector_object_type_uppercase}_SPECIES.length();

''',

    'ADDITIONAL_DECLARATIONS': '''''', # To be cogged from SPECIES_DECLARATION

    # Make definitions for handling different operations below. This is particularly
    # useful if we decide to change the logic for handling an operation, without
    # needing to modify the main code. We can then feed this into ``cube_math.py``
    # to create a vectorized version of the library.
    'BINARY_NUMERIC_OPS': '''\
            case ADD:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.add(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case SUB:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.sub(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case MUL:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.mul(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case DIV:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.div(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case MIN:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.min(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case MAX:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.max(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case POW:
{vectorized_power}\
            case MOD:
                // As of 20230810, Vector API doesn't support module operation,
                // so do it naively.
                for (int j = 0; j < len; j++) {{
                    ar[j] = aa[j] % ab[j];
                }}
                break;
''',

    'UNARY_NUMERIC_OPS': '''\
            case NEG:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .neg().{into_array}(ar, j);
                }}
                break;
            case ABS:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .abs().{into_array}(ar, j);
                }}
                break;
''',

    'BINARY_LOGIC_OPS': '''\
            case AND:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.and(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case OR:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.or(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
            case XOR:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.lanewise(
                        VectorOperators.XOR,
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
''',

    'UNARY_LOGIC_OPS': '''\
            case NOT:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .not().{into_array}(ar, j);
                }}
                break;
''',

    'UNARY_REAL_OPS': '''\
            case FLOOR:
                // As of 20230810, Vector API doesn't support floor operation,
                // so do it naively.
                for (int j = 0; j < len; j++) {{
                    ar[j] = {cast}Math.floor(aa[j]);
                }}
                break;
            case ROUND:
                // As of 20230810, Vector API doesn't support rounding operation,
                // so do it naively.
                for (int j = 0; j < len; j++) {{
                    ar[j] = {cast}Math.round(aa[j]);
                }}
                break;
            case CEIL:
                // As of 20230810, Vector API doesn't support ceil operation,
                // so do it naively.
                for (int j = 0; j < len; j++) {{
                    ar[j] = {cast}Math.ceil(aa[j]);
                }}
                break;
            case COS:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.COS).{into_array}(ar, j);
                }}
                break;
            case COSH:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.COSH).{into_array}(ar, j);
                }}
                break;
            case SIN:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.SIN).{into_array}(ar, j);
                }}
                break;
            case SINH:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.SINH).{into_array}(ar, j);
                }}
                break;
            case TAN:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.TAN).{into_array}(ar, j);
                }}
                break;
            case TANH:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.TANH).{into_array}(ar, j);
                }}
                break;
            case EXP:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.EXP).{into_array}(ar, j);
                }}
                break;
            case LOG:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.LOG).{into_array}(ar, j);
                }}
                break;
            case LOG10:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .lanewise(VectorOperators.LOG10).{into_array}(ar, j);
                }}
                break;
''',

    'ASSOCIATIVE_OPS': '''\
            case ADD:
                if (ww != null) {{
                    throw new UnsupportedOperationException(
                        "Where clauses not supported for vectorized operations"
                    );
                }}
                else {{
                    int j = 0;
                    for (; j <= len - {species_length}{nan_check_for_r}; j += {species_length}) {{
                        // "Reduce" this chunk through an associative add operation
                        r += {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                                    .reduceLanes(VectorOperators.ADD);
                    }}
                    if (j < len{nan_check_for_r}) {{
                        // Same as above, only make sure to only consider elements
                        // up until len.
                        r += {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                                    .reduceLanes(VectorOperators.ADD,
                                                 VectorMask.fromLong({species},
                                                                     (1L << (len - j)) - 1));
                    }}
                }}
                break;
            case NANADD:
                if (ww != null) {{
                    throw new UnsupportedOperationException(
                        "Where clauses not supported for vectorized operations"
                    );
                }}
                else {{
                    int j = 0;
                    for (; j <= len - {species_length}; j += {species_length}) {{
                        // "Reduce" this chunk through an associative add operation,
                        // ignoring values that are NaN.
                        final {vector_type} v = {to_vector_prefix}({species}, aa, j){to_vector_suffix};
                        r += v.reduceLanes(VectorOperators.ADD{vector_loop_nan_check});
                    }}
                    if (j < len) {{
                        // Same as above, only make sure to only consider elements
                        // up until len.
                        final {vector_type} v = {to_vector_prefix}({species}, aa, j){to_vector_suffix};
                        r += v.reduceLanes(VectorOperators.ADD,
                                           VectorMask.fromLong({species},
                                                               (1L << (len - j)) - 1){vector_if_nan_check});
                    }}
                }}
                break;
            case MIN:
                if (ww != null) {{
                    throw new UnsupportedOperationException(
                        "Where clauses not supported for vectorized operations"
                    );
                }}
                else {{
                    int j = 0;
                    for (; j <= len - {species_length}{nan_check_for_r}; j += {species_length}) {{
                        // "Reduce" this chunk through an associative min operation
                        final {primitive_type} v = {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                            .reduceLanes(VectorOperators.MIN);
                        r = (r < v ? r : v);
                    }}
                    if (j < len{nan_check_for_r}) {{
                        // Same as above, only make sure to only consider elements
                        // up until len.
                        final {primitive_type} v = {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                            .reduceLanes(VectorOperators.MIN,
                                         VectorMask.fromLong({species},
                                                             (1L << (len - j)) - 1));
                        r = (r < v ? r : v);
                    }}
                }}
                break;
            case MAX:
                if (ww != null) {{
                    throw new UnsupportedOperationException(
                        "Where clauses not supported for vectorized operations"
                    );
                }}
                else {{
                    int j = 0;
                    for (; j <= len - {species_length}{nan_check_for_r}; j += {species_length}) {{
                        // "Reduce" this chunk through an associative max operation
                        final {primitive_type} v = {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                            .reduceLanes(VectorOperators.MAX);
                        r = (r > v ? r : v);
                    }}
                    if (j < len{nan_check_for_r}) {{
                        // Same as above, only make sure to only consider elements
                        // up until len.
                        final {primitive_type} v = {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                            .reduceLanes(VectorOperators.MAX,
                                         VectorMask.fromLong({species},
                                                             (1L << (len - j)) - 1));
                        r = (r > v ? r : v);
                    }}
                }}
                break;
''',

    'COMPARISON_OPS': '''\
            case EQ:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .compare(VectorOperators.EQ,
                                 {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).intoArray(ar, j);
                }}
                break;
            case NE:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .compare(VectorOperators.NE,
                                 {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).intoArray(ar, j);
                }}
                break;
            case LT:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .compare(VectorOperators.LT,
                                 {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).intoArray(ar, j);
                }}
                break;
            case GT:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .compare(VectorOperators.GT,
                                 {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).intoArray(ar, j);
                }}
                break;
            case LE:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .compare(VectorOperators.LE,
                                 {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).intoArray(ar, j);
                }}
                break;
            case GE:
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                        .compare(VectorOperators.GE,
                                 {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).intoArray(ar, j);
                }}
                break;
''',

    'REDUCTIVE_LOGIC_OPS': '''\
            case ANY:
                /*scope*/ {{
                    int j = 0;
                    for (; j <= len - {species_length}; j += {species_length}) {{
                        r |= {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                                  .eq(({vector_primitive_type})0)
                                  .not()
                                  .anyTrue();
                    }}
                    if (j < len) {{
                        r |= {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                                  .eq(({vector_primitive_type})0)
                                  .not()
                                  .and(VectorMask.fromLong({species},
                                                           (1L << (len - j)) - 1))
                                  .anyTrue();
                    }}
                }} break;
            case ALL:
                /*scope*/ {{
                    int j = 0;
                    for (; j <= len - {species_length}; j += {species_length}) {{
                        r &= {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                                  .eq(({vector_primitive_type})0)
                                  .not()
                                  .allTrue();
                    }}
                    if (j < len) {{
                        r &= {to_vector_prefix}({species}, aa, j){to_vector_suffix}
                                  .eq(({vector_primitive_type})0)
                                  .not()
                                  .or(VectorMask.fromLong({species},
                                                          (1L << (len - j)) - 1)
                                                .not())
                                  .allTrue();
                    }}
                }} break;
''',

    'POPCOUNT': '''\
            /*scope*/ {{
                int j = 0;
                for (; j <= len - {species_length}; j += {species_length}) {{
                    r += VectorMask.fromArray({species}, aa, j).trueCount();
                }}
                if (j < len) {{
                    r += VectorMask.fromArray({species}, aa, j).and(
                             VectorMask.fromLong({species},
                                                 (1L << (len - j)) - 1)
                         ).trueCount();
                }}
            }}
''',

    'VECTORIZED_POWER': '''\
                for (int j = 0; j < len; j += {species_length}) {{
                    {to_vector_prefix}({species}, aa, j){to_vector_suffix}.pow(
                        {to_vector_prefix}({species}, ab, j){to_vector_suffix}
                    ).{into_array}(ar, j);
                }}
                break;
''',

    'NAIVE_POWER': '''\
                // As of 20230810, Vector API doesn't support power operation,
                // so do it naively.
                for (int j = 0; j < len; j++) {{
                    ar[j] = ({primitive_type})Math.pow(aa[j], ab[j]);
                }}
                break;
''',

    'MATMUL_DOT_OP': '''\
                            // The length of the column and row are the same
                            final {primitive_type}[] arow = new {primitive_type}[bcol.length];
                            final long[] ai = new long[] {{ 0,  0 }};
                            final long[] bi = new long[] {{ 0, jf }};
                            final long[] ri = new long[] {{ 0, jf }};
                            final boolean exact = (arow.length % {species_length} == 0);
                            final {primitive_type}[] array =
                                exact ? null : new {primitive_type}[{species_length} << 1];
                            for (long i = startIndex; i < endIndex; i++) {{
                                ai[0] = ri[0] = i;
                                long ao = a.toOffset(ai);
                                a.toFlattened(ao, arow, 0, arow.length);

                                // Now do the dot product
                                {primitive_type} sum = 0;
                                int offset = 0;
                                for (final int end = exact ? bcol.length
                                                           : bcol.length - {species_length};
                                     offset < end;
                                     offset += {species_length})
                                {{
                                    sum += {to_vector_prefix}({species}, arow, offset){to_vector_suffix}.mul(
                                               {to_vector_prefix}({species}, bcol, offset){to_vector_suffix}
                                           ).reduceLanes(VectorOperators.ADD);
                                }}
                                if (!exact) {{
                                    // Same as above, only make sure to only consider
                                    // elements up until what's left. We copy the
                                    // values into a single array to save on allocations
                                    // and since it will be more cache-friendly. We only
                                    // need to copy bcols once since it won't change
                                    // between runs.
                                    final int left = bcol.length - offset;
                                    System.arraycopy(arow, offset, array, 0, left);
                                    if (i == startIndex) {{
                                        System.arraycopy(bcol, offset, array, {species_length}, left);
                                    }}
                                    sum += {to_vector_prefix}({species}, array, 0){to_vector_suffix}.mul(
                                               {to_vector_prefix}({species}, array, {species_length}){to_vector_suffix}
                                           ).reduceLanes(
                                               VectorOperators.ADD,
                                               VectorMask.fromLong({species}, ((1L << left) - 1))
                                           );
                                }}
                                r.set(sum, ri);
                            }}
''',

    'MATMUL_DOT_OP_NAIVE': '''\
                // The length of the column and row are the same
                final {primitive_type}[] arow  = new {primitive_type}[bcol.length];
                final boolean exact = (arow.length % {species_length} == 0);
                final {primitive_type}[] array =
                    exact ? null : new {primitive_type}[{species_length} << 1];

                // Where we start striding, see below
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                ai[1] = bi[0] = 0;
                bi[1] = ri[1] = j;

                // Walk all the rows and dot them against the column
                for (long i=0; i < numRows; i++) {{
                    // The row index, and a copy of it for the vectors
                    ai[0] = ri[0] = i;
                    long ao = a.toOffset(ai);
                    a.toFlattened(ao, arow, 0, arow.length);

                    // Now do the dot product
                    {primitive_type} sum = 0;
                    int offset = 0;
                    for (final int end = exact ? bcol.length
                                               : bcol.length - {species_length};
                         offset < end;
                         offset += {species_length})
                    {{
                        sum += {to_vector_prefix}({species}, arow, offset){to_vector_suffix}.mul(
                                   {to_vector_prefix}({species}, bcol, offset){to_vector_suffix}
                               ).reduceLanes(VectorOperators.ADD);
                    }}
                    if (!exact) {{
                        // Same as above, only make sure to only consider
                        // elements up until what's left. We copy the
                        // values into a single array to save on allocations
                        // and since it will be more cache-friendly. We only
                        // need to copy bcols once since it won't change
                        // between runs.
                        final int left = bcol.length - offset;
                        System.arraycopy(arow, offset, array, 0, left);
                        if (i == 0) {{
                            System.arraycopy(bcol, offset, array, {species_length}, left);
                        }}
                        sum += {to_vector_prefix}({species}, array, 0){to_vector_suffix}.mul(
                                   {to_vector_prefix}({species}, array, {species_length}){to_vector_suffix}
                               ).reduceLanes(
                                   VectorOperators.ADD,
                                   VectorMask.fromLong({species}, ((1L << left) - 1))
                               );
                    }}
                    r.set(sum, ri);
                }}
''',
}

def add_java_docs():
    """
    Helper function to generate additional JavaDoc for public methods.
    """
    # Add the list of supported vectorized operations to the class JavaDoc for
    # better accessibility here
    _VECTORIZED_CUBE_MATH_IMPL['KWARGS']['supported_vectorized_ops'] = \
        "\n * <li>".join([''] + _VECTORIZED_OPS)

    # Add the JavaDoc for vectorized operations here
    for op in _VECTORIZED_OPS:
        _VECTORIZED_CUBE_MATH_IMPL['KWARGS'][op + "_extra_javadoc"] = '''
     *
     * <p>This method uses a vectorized implementation.\
'''

    # Add the JavaDoc for standard operations here
    for op in _STANDARD_OPS:
        _VECTORIZED_CUBE_MATH_IMPL['KWARGS'][op + "_extra_javadoc"] = '''
     *
     * <p>Vectorized support is not available for this operation. This method
     * uses the standard implementation.\
'''


def _get_kwargs_for_dtype(dtype):
    """
    Return the necessary keyword arguments for generating vectorized cogged code
    for a given dtype.

    :param dtype: The dtype to generate kwargs for.
    :type dtype: type, Required
    """
    kwargs = cube_math.DefaultMap(params.get_kwargs(dtype))

    kwargs['class_name'] = 'VectorizedCubeMath'

    # As of 20230810, Vector API has no support for booleans. Because of this,
    # we use ByteVectors for boolean processing instead.
    if dtype == numpy.bool_:
        byte_kwargs = params.get_kwargs(numpy.byte)
        kwargs['vector_type']                  = f"{byte_kwargs['short_object_type']}Vector"
        kwargs['vector_object_type']           = byte_kwargs['object_type']
        kwargs['vector_primitive_type']        = byte_kwargs['primitive_type']
        kwargs['vector_object_type_uppercase'] = byte_kwargs['object_type'].upper()
        kwargs['to_vector_prefix']             = f"(({kwargs['vector_type']})VectorMask.fromArray"
        kwargs['to_vector_suffix']             = '.toVector()).mul((byte)-1)'
        kwargs['into_array']                   = 'intoBooleanArray'
    else:
        kwargs['vector_type']                  = f"{kwargs['short_object_type']}Vector"
        kwargs['vector_object_type']           = kwargs['short_object_type']
        kwargs['vector_object_type']           = kwargs['object_type']
        kwargs['vector_primitive_type']        = kwargs['primitive_type']
        kwargs['vector_primitive_type']        = kwargs['primitive_type']
        kwargs['vector_object_type_uppercase'] = kwargs['object_type'].upper()
        kwargs['to_vector_prefix']             = f"{kwargs['vector_type']}.fromArray"
        kwargs['into_array']                   = 'intoArray'

    # Some abbreviations for our own sake
    kwargs['species']        = f"{kwargs['vector_object_type_uppercase']}_SPECIES"
    kwargs['species_length'] = f"{kwargs['vector_object_type_uppercase']}_SPECIES_LENGTH"

    # NaN checking and power operation are only supported for real value vectors
    # in Vector API. Therefore, we define them here, or leave them blank.
    if dtype in [numpy.float32, numpy.float64]:
        kwargs['vector_loop_nan_check'] = ', v.test(VectorOperators.IS_NAN).not()'
        kwargs['vector_if_nan_check']   = '.andNot(v.test(VectorOperators.IS_NAN))'

        # As of 20230810, Vector API only supports power operation for real dtypes
        kwargs['vectorized_power'] = \
            _VECTORIZED_CUBE_MATH_IMPL['VECTORIZED_POWER'].format_map(kwargs)

    else:
        # Use the naive implementation of power for integer dtypes.
        kwargs['vectorized_power'] = \
            _VECTORIZED_CUBE_MATH_IMPL['NAIVE_POWER'].format_map(kwargs)

    # Returned the augmented kwargs for the dtype
    return kwargs


################################################################################
# Public method(s) are listed below.

def generate():
    """
    Generate a vectorized cube math library using Vector API. This method uses
    :mod:`cube_math` which additionally cogs the :class:`CubeMath` class. As a
    result, it is expected that these two modules remain in-sync, particularly
    on the syntax of "implementations".
    """
    # All supported dtypes
    dtypes = _VECTORIZED_CUBE_MATH_IMPL['DTYPES']

    # Some operation are not supported in the Vector API. Reflect this in the
    # JavaDoc of the corresponding methods.
    add_java_docs()

    for dtype in dtypes:
        kwargs = _get_kwargs_for_dtype(dtype)
        _VECTORIZED_CUBE_MATH_IMPL['ADDITIONAL_DECLARATIONS'] += \
            _VECTORIZED_CUBE_MATH_IMPL['SPECIES_DECLARATION'].format(**kwargs)

    kwargs = {
        'implementation': _VECTORIZED_CUBE_MATH_IMPL,
        'get_kwargs': _get_kwargs_for_dtype
    }
    return cube_math.generate(**kwargs)
