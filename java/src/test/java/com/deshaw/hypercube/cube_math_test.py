import numpy
import params

_CUBE_MATH_TEST_PREFIX = '''\
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The testing code for {{@code com.deshaw.hypercube.CubeMath}}.
 */
public class CubeMathTest
{{
    /**
     * The epsilon value for floating point comparisons.
     */
    private static final double EPS = 1e-6;

    /**
     * The shape of all cubes we create.
     */
    private static final Dimension<?>[] DIMENSIONS = Dimension.of(100);

    // ----------------------------------------------------------------------

    /**
     * Check that two cubes are element-wise equal.
     */
    private <T> void assertEquals(final Hypercube<T> cube1,
                                  final Hypercube<T> cube2)
    {{
        assert(CubeMath.all0d(CubeMath.equal(cube1, cube2)));
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

        final Hypercube<Boolean> eq = CubeMath.equal       (a, b);
        final Hypercube<Boolean> ne = CubeMath.notEqual    (a, b);
        final Hypercube<Boolean> lt = CubeMath.less        (a, b);
        final Hypercube<Boolean> gt = CubeMath.greater     (a, b);
        final Hypercube<Boolean> le = CubeMath.lessEqual   (a, b);
        final Hypercube<Boolean> ge = CubeMath.greaterEqual(a, b);

        assert( CubeMath.all0d(eq));
        assert( CubeMath.any0d(eq));
        assert(!CubeMath.any0d(lt));
        assert(!CubeMath.any0d(gt));
        assert( CubeMath.any0d(le));
        assert( CubeMath.any0d(ge));
        assert(!CubeMath.all0d(ne));
        assert(!CubeMath.any0d(ne));
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

        final Hypercube<{object_type}> q = CubeMath.divide  (a, b);
        final Hypercube<{object_type}> r = CubeMath.mod     (a, b);
        final Hypercube<{object_type}> m = CubeMath.multiply(q, b);
        final Hypercube<{object_type}> t = CubeMath.add     (m, r);
        assertEquals(t, a);

        final Hypercube<{object_type}> min = CubeMath.minimum(a, b);
        final Hypercube<{object_type}> max = CubeMath.maximum(a, b);
        for (long i=0; i < a.getSize(); i++) {{
            assert(min.getObjectAt(i) == {object_type}.min(a.getAt(i), b.getAt(i)));
            assert(max.getObjectAt(i) == {object_type}.max(a.getAt(i), b.getAt(i)));
        }}
    }}

    /**
     * Test numeric unary operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}NumericUnaryOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();
        final {object_type}Hypercube b = create{object_type}Hypercube();

        // Test negative()
        final Hypercube<{object_type}> n  = CubeMath.negative(b);
        final Hypercube<{object_type}> s1 = CubeMath.subtract(a, b);
        final Hypercube<{object_type}> s2 = CubeMath.add     (a, n);
        assertEquals(s1, s2);

        // Test abs()
        final Hypercube<{object_type}> abs = CubeMath.abs(n);
        assertEquals(abs, b);
    }}

    /**
     * Test broadcasting operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}Broadcast()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();

        // Test broadcasting
        final Hypercube<{object_type}> z = CubeMath.broadcast(DIMENSIONS, {cast}0);
        final Hypercube<{object_type}> m = CubeMath.multiply(a, z);

        // Check that all elements are zero.
        final Hypercube<Boolean> e = CubeMath.equal(m, {cast}0);
        final Hypercube<Boolean> b = CubeMath.equal(e, true);
        assert(CubeMath.all0d(b));
    }}

    /**
     * Test associative operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}AssociativeOps()
    {{
        final {object_type}Hypercube a = create{object_type}Hypercube();

        // Initialize sum
        {primitive_type}       sum = 0;
        final {primitive_type} min = CubeMath.min0d(a);
        final {primitive_type} max = CubeMath.max0d(a);

        for (long i=0; i < a.getSize(); i++) {{
            final {primitive_type} va = a.getObjectAt(i);
            sum += va;
            // Check the min and max operations
            assert(min <= va && va <= max);
        }}

        // Check the sum operation
        assert(sum == CubeMath.sum0d(a));
    }}

    /**
     * Test extract operation for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}Extract()
    {{
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<{object_type}> a = create{object_type}Hypercube();
        final Hypercube<{object_type}> e = CubeMath.extract(c, a);

        int j = 0;
        for (long i=0; i < a.getSize(); i++) {{
            if (c.getObjectAt(i)) {{
                // Make sure the element is present in the extract
                assert(j < e.getSize());
                assert(e.getObjectAt(j++).{primitive_type}Value() == a.getObjectAt(i).{primitive_type}Value());
            }}
        }}
    }}
'''

_CUBE_MATH_TEST_REAL = '''\

    // -------------------------------------------------------------------------

    /**
     * Check all the elements of a hypercube match a value within a given error
     * tolerance.
     */
    private void assertEquals{object_type}(
        final Hypercube<{object_type}> cube,
        final {primitive_type} value,
        final double eps)
    {{
        {object_type}Hypercube dcube = ({object_type}Hypercube)cube;
        for (long i=0; i < cube.getShape()[0]; i++) {{
            assert(Math.abs(value - dcube.get(i)) <= eps);
        }}
    }}

    /**
     * Check that two cubes are element-wise equal, within a given error tolerance.
     */
    private void assertEquals{object_type}(
        final Hypercube<{object_type}> cube1,
        final Hypercube<{object_type}> cube2,
        final double eps)
    {{
        assertEquals{object_type}(CubeMath.subtract(cube1, cube2), {cast}0., eps);
    }}

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
        final {object_type}Hypercube a = createNormalized{object_type}Hypercube();
        final {object_type}Hypercube e = createNormalized{object_type}Hypercube();
        e.fill({cast}Math.E);

        final Hypercube<{object_type}> exp = CubeMath.exp  (a);
        final Hypercube<{object_type}> log = CubeMath.log  (exp);
        final Hypercube<{object_type}> pow = CubeMath.power(e, a);

        assertEquals{object_type}(exp, pow, EPS);
        assertEquals{object_type}(log, a, EPS);
    }}

    /**
     * Test unary operations for {primitive_type} cubes.
     */
    @Test
    public void test{object_type}UnaryOps()
    {{
        final {object_type}Hypercube a = createNormalized{object_type}Hypercube();

        final Hypercube<{object_type}> cos  = CubeMath.cos(a);
        final Hypercube<{object_type}> sin  = CubeMath.sin(a);
        final Hypercube<{object_type}> tan  = CubeMath.tan(a);
        assertEquals{object_type}(tan, CubeMath.divide(sin, cos), EPS);

        final Hypercube<{object_type}> cos2 = CubeMath.power(cos, {cast}2);
        final Hypercube<{object_type}> sin2 = CubeMath.power(sin, {cast}2);
        final Hypercube<{object_type}> sum  = CubeMath.add  (cos2, sin2);
        assertEquals{object_type}(sum, {cast}1.0, EPS);

        final Hypercube<{object_type}> floor = CubeMath.floor(tan);
        final Hypercube<{object_type}> round = CubeMath.round(tan);
        final Hypercube<{object_type}> ceil  = CubeMath.ceil (tan);

        for (long i=0; i < tan.getSize(); i++) {{
            final {primitive_type} v = tan.getObjectAt(i);
            assert(Math.abs(floor.getObjectAt(i) - Math.floor(v)) <= EPS);
            assert(Math.abs(round.getObjectAt(i) - Math.round(v)) <= EPS);
            assert(Math.abs(ceil .getObjectAt(i) - Math.ceil (v)) <= EPS);
        }}
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
        assert({object_type}.isNaN(CubeMath.min0d(a)));
        assert({object_type}.isNaN(CubeMath.max0d(a)));

        {primitive_type} nansum = {cast}0.;
        for (long i=0; i < a.getSize(); i++) {{
            if (!{object_type}.isNaN(a.get(i))) {{
                nansum += a.get(i);
            }}
        }}

        assert({object_type}.isNaN(CubeMath.sum0d(a)));
        assert(Math.abs(CubeMath.nansum0d(a) - nansum) <= EPS);
    }}

    /**
     * Test that the {{@code where}} kwarg works as expected.
     */
    @Test
    public void test{object_type}WhereKwarg()
    {{
        // Create the cube full of values, from negative to positive, and
        // compute the sum of the non-negative ones
        {primitive_type} sum = 0;
        final {object_type}Hypercube cube = create{object_type}Hypercube();
        for (long i = 0, j = cube.getSize() / 2; i < cube.getSize(); i++, j++) {{
            cube.setAt(i, {cast}j);
            if (j >= 0) {{
                sum += j;
            }}
        }}

        // Get the mask of the values in the cube which are non-negative and use
        // that as the where clause for sum()
        final Hypercube<Boolean> where = CubeMath.greaterEqual(cube, {cast}0);
        final Object whereSum = CubeMath.sum(cube, Map.of("where", where));

        // Sum should return an object of the element type. (It could return
        // null, or another cube if the axis kwarg is used.) That value should
        // match our computed sum.
        assert(whereSum instanceof {object_type});
        assert(({object_type})whereSum == sum);
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

        final Hypercube<{object_type}> and = CubeMath.and(a, b);
        final Hypercube<{object_type}> or  = CubeMath.or (a, b);
        final Hypercube<{object_type}> xor = CubeMath.xor(a, b);
        final Hypercube<{object_type}> not = CubeMath.not(a);

        for (long i=0; i < a.getSize(); i++) {{
            assert(and.getObjectAt(i) == (a.getAt(i) & b.getAt(i)));
            assert(or .getObjectAt(i) == (a.getAt(i) | b.getAt(i)));
            assert(xor.getObjectAt(i) == (a.getAt(i) ^ b.getAt(i)));
            assert(not.getObjectAt(i) == ({not_operation}a.getAt(i)));
        }}
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

        // Initialize the popcount
        long popcount = CubeMath.popcount(a);
        for (long i=0; i < a.getSize(); i++) {{
            if (a.getObjectAt(i)) {{
                // Decrease the popcount
                popcount --;
            }}
        }}

        // Make sure the popcount is now zero
        assert(popcount == 0);
    }}
'''

_CUBE_MATH_TEST_CASTING = '''\

    // -------------------------------------------------------------------------

    /**
     * Test casting a {object_type} cube to a {{object_type}} cube.
     */
    @Test
    public void test{object_type}To{{object_type}}Hypercube()
    {{{{
        final Hypercube<{object_type}>   a = create{object_type}Hypercube();
        final Hypercube<{{object_type}}> c = {{object_type}}Hypercube.to{{object_type}}Hypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {{{{
            {primitive_type}   va = a.getObjectAt(i);
            {{primitive_type}} vc = c.getObjectAt(i);
            assert(({{primitive_type}})(va{conversion}) == vc);
        }}}}
    }}}}
'''

_CUBE_MATH_TEST_COPY = '''\

    // -------------------------------------------------------------------------

    /**
     * Test copying a {object_type} cube.
     */
    @Test
    public void test{object_type}HypercubeCopy()
    {{
        final Hypercube<{object_type}> a = create{object_type}Hypercube();
        final Hypercube<{object_type}> c = a.copy();

        // Now make sure the cube was correctly copied
        for (long i=0; i < a.getSize(); i++) {{
            {primitive_type} va = a.getObjectAt(i);
            {primitive_type} vc = c.getObjectAt(i);
            assert(va == vc);
        }}
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
               _CUBE_MATH_TEST_ALL    .format(**kwargs) + \
               _CUBE_MATH_TEST_COPY   .format(**kwargs)
    # Since bitwise operations are not supported for floating points, we handle
    # them separately.
    elif kwargs['dtype'] in [numpy.float32, numpy.float64]:
        return _CUBE_MATH_TEST_NUMERIC.format(**kwargs) + \
               _CUBE_MATH_TEST_REAL   .format(**kwargs) + \
               _CUBE_MATH_TEST_ALL    .format(**kwargs) + \
               _CUBE_MATH_TEST_COPY   .format(**kwargs)
    # Otherwise we can cog all the operations for the given dtype.
    else:
        return _CUBE_MATH_TEST_NUMERIC.format(**kwargs) + \
               _CUBE_MATH_TEST_BITWISE.format(**kwargs) + \
               _CUBE_MATH_TEST_ALL    .format(**kwargs) + \
               _CUBE_MATH_TEST_COPY   .format(**kwargs)


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
    _CUBE_MATH_TEST = _CUBE_MATH_TEST_PREFIX.format()
    for dtype in dtypes:
        kwargs = _get_kwargs_for_dtype(dtype)
        _CUBE_MATH_TEST += _generate_for_dtype(kwargs)

        # Add tests for casting cubes
        for cast_to in dtypes:
            if dtype != cast_to:
                to_kwargs = _get_kwargs_for_dtype(cast_to)

                # Converting to and from booleans has its own special handling
                if (cast_to == numpy.bool_ and dtype   != numpy.bool_):
                    kwargs['conversion'] = ' != 0'
                elif (dtype == numpy.bool_ and cast_to != numpy.bool_):
                    kwargs['conversion'] = ' ? 1 : 0'
                else:
                    kwargs['conversion'] = ''

                # Now add the test cases
                _CUBE_MATH_TEST += _CUBE_MATH_TEST_CASTING.format(**kwargs)\
                                                     .format(**to_kwargs)
    # Don't forget the enclosing curly bracket
    _CUBE_MATH_TEST += '}\n'
    return _CUBE_MATH_TEST
