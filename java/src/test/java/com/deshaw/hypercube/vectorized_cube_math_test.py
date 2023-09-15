import numpy
import params

_VECTORIZED_CUBE_MATH_TEST_PREFIX = '''\
import org.junit.jupiter.api.Test;

/**
 * The testing code for {{@code com.deshaw.hypercube.VectorizedCubeMath}}.
 */
public class VectorizedCubeMathTest
{{
    /**
     * The epsilon value for floating point comparisons.
     *
     * <p>VectorizedCubeMath actually retains better precision for many operations
     * (such as summation) because of tree-based operations, so we intentionally
     * allow for a larger error margin when testing against CubeMath here.
     */
    private static final double EPS = 1e-5;

    /**
     * The shape of all cubes we create.
     *
     * <p>We intentionally use a cube size of 2^i-1 to test VectorizedCubeMath's
     * corner case handling (since it processes data in powers of two).
     */
    private static final Dimension<?>[] DIMENSIONS = Dimension.of(127);

    // ----------------------------------------------------------------------

    /**
     * Check that two cubes are element-wise equal.
     */
    @SuppressWarnings("unchecked")
    private <T> void assertEquals(final Hypercube<T> cube1,
                                  final Hypercube<T> cube2)
    {{
        // Make sure to consider an error margin for floating-point cubes.
        if (cube1.getElementType().equals(Double.class)) {{
            assert(CubeMath.all(CubeMath.lessEqual(CubeMath.abs(CubeMath.subtract(cube1, cube2)), (T)((Double)EPS))));
        }}
        else if (cube1.getElementType().equals(Float.class)) {{
            assert(CubeMath.all(CubeMath.lessEqual(CubeMath.abs(CubeMath.subtract(cube1, cube2)), (T)(Float)((float)EPS))));
        }}
        else {{
            assert(CubeMath.all(CubeMath.equal(cube1, cube2)));
        }}
    }}
'''

_CUBE_MATH_TEST_ALL = '''\

    // ----------------------------------------------------------------------

    /**
     * Test boolean operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}BooleanOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();
        final {object_type}Hypercube b = create{object_type}Hypercube();

        // Check all methods against their non-vectorized implementations
        assertEquals(VectorizedCubeMath.equal       (a, b),
                     CubeMath          .equal       (a, b));
        assertEquals(VectorizedCubeMath.notEqual    (a, b),
                     CubeMath          .notEqual    (a, b));
        assertEquals(VectorizedCubeMath.less        (a, b),
                     CubeMath          .less        (a, b));
        assertEquals(VectorizedCubeMath.greater     (a, b),
                     CubeMath          .greater     (a, b));
        assertEquals(VectorizedCubeMath.lessEqual   (a, b),
                     CubeMath          .lessEqual   (a, b));
        assertEquals(VectorizedCubeMath.greaterEqual(a, b),
                     CubeMath          .greaterEqual(a, b));

        assert(VectorizedCubeMath.any(a) == CubeMath.any(a));
        assert(VectorizedCubeMath.all(a) == CubeMath.all(a));
    }}

    // ----------------------------------------------------------------------

    /**
     * Populate a {primitive_type} hypercube.
     */
    private void populate(final {object_type}Hypercube cube)
    {{
        for (long i=0; i < cube.getSize(); i++) {{
            cube.setAt(i, {cast}((i + 1){boolean_conversion}));
        }}
    }}

    /**
     * Create a {{@link {object_type}Hypercube}}.
     */
    private {object_type}Hypercube create{object_type}Hypercube()
    {{
        // Create
        final {object_type}Hypercube cube =
            new {object_type}{internal_rep}Hypercube(DIMENSIONS);

        // Populate
        populate(cube);

        // Give it back
        return cube;
    }}
'''

_CUBE_MATH_TEST_NUMERIC = '''\

    // -------------------------------------------------------------------------

    /**
     * Test binary operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}BinaryOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();
        final {object_type}Hypercube b = create{object_type}Hypercube();

        // Check all methods against their non-vectorized implementations
        assertEquals(VectorizedCubeMath.add     (a, b),
                     CubeMath          .add     (a, b));
        assertEquals(VectorizedCubeMath.subtract(a, b),
                     CubeMath          .subtract(a, b));
        assertEquals(VectorizedCubeMath.multiply(a, b),
                     CubeMath          .multiply(a, b));
        assertEquals(VectorizedCubeMath.divide  (a, b),
                     CubeMath          .divide  (a, b));
        assertEquals(VectorizedCubeMath.mod     (a, b),
                     CubeMath          .mod     (a, b));
        assertEquals(VectorizedCubeMath.minimum (a, b),
                     CubeMath          .minimum (a, b));
        assertEquals(VectorizedCubeMath.maximum (a, b),
                     CubeMath          .maximum (a, b));
    }}

    /**
     * Test numeric unary operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}NumericUnaryOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();

        assertEquals(VectorizedCubeMath.negative(a),
                     CubeMath          .negative(a));
        assertEquals(VectorizedCubeMath.abs     (a),
                     CubeMath          .abs     (a));
    }}

    /**
     * Test associative operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}AssociativeOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.min(a) - CubeMath.min(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.max(a) - CubeMath.max(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.sum(a) - CubeMath.sum(a)) <= EPS);
    }}

    /**
     * Test extract operation for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}Extract()
    {{
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<{object_type}> a = create{object_type}Hypercube();

        assertEquals(VectorizedCubeMath.extract(c, a),
                     CubeMath          .extract(c, a));
    }}
'''

_CUBE_MATH_TEST_REAL = '''\

    // -------------------------------------------------------------------------

    /**
     * Populate a {primitive_type} hypercube with values between 0 and 1.
     */
    private void populateNormalized(final {object_type}Hypercube cube)
    {{
        for (long i=0; i < cube.getSize(); i++) {{
            cube.setAt(i, {cast}((i + 1.) / {cast}cube.getSize()));
        }}
    }}

    /**
     * Create a {{@link {object_type}Hypercube}} with values between 0 and 1.
     */
    private {object_type}Hypercube createNormalized{object_type}Hypercube()
    {{
        // Create
        final {object_type}Hypercube cube =
            new {object_type}{internal_rep}Hypercube(DIMENSIONS);

        // Populate
        populateNormalized(cube);

        // Give it back
        return cube;
    }}

    // -------------------------------------------------------------------------

    /**
     * Test exponentiation operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}Exponentiation()
    {{
        final Hypercube<{object_type}> a = createNormalized{object_type}Hypercube();
        final Hypercube<{object_type}> e = CubeMath.full(a.getDimensions(), {cast}Math.E);

        assertEquals(VectorizedCubeMath.power(e, a),
                     CubeMath          .power(e, a));
        assertEquals(VectorizedCubeMath.exp  (a),
                     CubeMath          .exp  (a));
        assertEquals(VectorizedCubeMath.log  (a),
                     CubeMath          .log  (a));
        assertEquals(VectorizedCubeMath.log10(a),
                     CubeMath          .log10(a));
    }}

    /**
     * Test unary operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}UnaryOps()
    {{
        final {object_type}Hypercube a = createNormalized{object_type}Hypercube();

        assertEquals(VectorizedCubeMath.sin (a),
                     CubeMath          .sin (a));
        assertEquals(VectorizedCubeMath.sinh(a),
                     CubeMath          .sinh(a));
        assertEquals(VectorizedCubeMath.cos (a),
                     CubeMath          .cos (a));
        assertEquals(VectorizedCubeMath.cosh(a),
                     CubeMath          .cosh(a));
        assertEquals(VectorizedCubeMath.tan (a),
                     CubeMath          .tan (a));
        assertEquals(VectorizedCubeMath.tanh(a),
                     CubeMath          .tanh(a));
    }}

    /**
     * Test NaN handling for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}NaNHandling()
    {{
        final {object_type}Hypercube a = createNormalized{object_type}Hypercube();

        // Add a NaN value somewhere
        a.setAt(0, {object_type}.NaN);

        // Ensure that the results are NaNs
        assert({object_type}.isNaN(VectorizedCubeMath.min(a)));
        assert({object_type}.isNaN(VectorizedCubeMath.max(a)));
        assert({object_type}.isNaN(VectorizedCubeMath.sum(a)));

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.nansum(a) - CubeMath.nansum(a)) <= EPS);
    }}
'''

_CUBE_MATH_TEST_BITWISE = '''\

    // -------------------------------------------------------------------------

    /**
     * Test logic and bitwise operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}LogicOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();
        final {object_type}Hypercube b = create{object_type}Hypercube();

        assertEquals(VectorizedCubeMath.and(a, b),
                     CubeMath          .and(a, b));
        assertEquals(VectorizedCubeMath.or (a, b),
                     CubeMath          .or (a, b));
        assertEquals(VectorizedCubeMath.xor(a, b),
                     CubeMath          .xor(a, b));
        assertEquals(VectorizedCubeMath.not(a),
                     CubeMath          .not(a));
    }}
'''

_CUBE_MATH_TEST_BOOLEAN = '''\

    // -------------------------------------------------------------------------

    /**
     * Test popcount operation for boolean cubes.
     */
    @Test
    public void testBooleanPopcount()
    {{
        final BooleanHypercube a = createBooleanHypercube();
        assert(VectorizedCubeMath.popcount(a) == CubeMath.popcount(a));
    }}
'''

def _get_kwargs_for_dtype(dtype):
    kwargs = params.get_kwargs(dtype)

    if dtype == numpy.bool_:
        # Special numeric handling for booleans
        kwargs['numeric_conversion'] = ' ? 1 : 0'
        # Internal representation for initializing hypercubes
        kwargs['internal_rep']       = 'BitSet'
        kwargs['not_operation']      = '!'
        kwargs['boolean_conversion'] = ' % 2 == 1'
    elif dtype == numpy.int32:
        kwargs['numeric_conversion']   = ''
        kwargs['internal_rep']         = 'Array'
        kwargs['not_operation']        = '~'
        # Special boolean handling for other dtypes
        kwargs['boolean_conversion']   = ''
    elif dtype == numpy.int64:
        kwargs['numeric_conversion']   = ''
        kwargs['internal_rep']         = 'Array'
        kwargs['not_operation']        = '~'
        kwargs['boolean_conversion']   = ''
    elif dtype == numpy.float32:
        kwargs['numeric_conversion']   = ''
        kwargs['internal_rep']         = 'Array'
        kwargs['boolean_conversion']   = ''
    elif dtype == numpy.float64:
        kwargs['numeric_conversion']   = ''
        kwargs['internal_rep']         = 'Array'
        kwargs['boolean_conversion']   = ''
    else:
        raise Exception(f'The given dtype {dtype} is unsupported')

    # Some operations like BinaryOp.POW need to recast the result back to its
    # original dtype.
    kwargs['cast'] = f'({kwargs["primitive_type"]})'

    # Returned the augmented kwargs for the dtype
    return kwargs


def _generate_for_dtype(kwargs):
    # Since numeric operations are not supported for booleans, we handle them
    # separately.
    if kwargs['dtype'] == numpy.bool_:
        return _CUBE_MATH_TEST_BOOLEAN.format(**kwargs) + \
               _CUBE_MATH_TEST_BITWISE.format(**kwargs) + \
               _CUBE_MATH_TEST_ALL    .format(**kwargs)
    # Since bitwise operations are not supported for floating points, we handle
    # them separately.
    elif kwargs['dtype'] in [numpy.float32, numpy.float64]:
        return _CUBE_MATH_TEST_NUMERIC.format(**kwargs) + \
               _CUBE_MATH_TEST_REAL   .format(**kwargs) + \
               _CUBE_MATH_TEST_ALL    .format(**kwargs)
    # Otherwise we can cog all the operations for the given dtype.
    else:
        return _CUBE_MATH_TEST_NUMERIC.format(**kwargs) + \
               _CUBE_MATH_TEST_BITWISE.format(**kwargs) + \
               _CUBE_MATH_TEST_ALL    .format(**kwargs)


def generate():
    # We'd like to cog the tests for all these dtypes.
    dtypes = [
        numpy.bool_,
        numpy.float32,
        numpy.float64,
        numpy.int32,
        numpy.int64
    ]

    # Initiate the source code with the 'prefix' portion of the code
    _CUBE_MATH = _VECTORIZED_CUBE_MATH_TEST_PREFIX.format()
    for dtype in dtypes:
        kwargs = _get_kwargs_for_dtype(dtype)
        _CUBE_MATH += _generate_for_dtype(kwargs)

    # Don't forget the enclosing curly bracket
    _CUBE_MATH += '}\n'
    return _CUBE_MATH
