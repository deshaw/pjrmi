package com.deshaw.hypercube;

// Recreate with `cog.py -rc VectorizedCubeMathTest.java`
// [[[cog
//     import cog
//     import vectorized_cube_math_test
//
//     cog.outl(vectorized_cube_math_test.generate())
// ]]]
import org.junit.jupiter.api.Test;

/**
 * The testing code for {@code com.deshaw.hypercube.VectorizedCubeMath}.
 */
public class VectorizedCubeMathTest
{
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
    {
        // Make sure to consider an error margin for floating-point cubes.
        if (cube1.getElementType().equals(Double.class)) {
            assert(CubeMath.all0d(CubeMath.lessEqual(CubeMath.abs(CubeMath.subtract(cube1, cube2)), (T)((Double)EPS))));
        }
        else if (cube1.getElementType().equals(Float.class)) {
            assert(CubeMath.all0d(CubeMath.lessEqual(CubeMath.abs(CubeMath.subtract(cube1, cube2)), (T)(Float)((float)EPS))));
        }
        else {
            assert(CubeMath.all0d(CubeMath.equal(cube1, cube2)));
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test popcount operation for boolean cubes.
     */
    @Test
    public void testBooleanPopcount()
    {
        final BooleanHypercube a = createBooleanHypercube();
        assert(VectorizedCubeMath.popcount(a) == CubeMath.popcount(a));
    }

    // -------------------------------------------------------------------------

    /**
     * Test logic and bitwise operations for boolean cubes.
     */
    @Test
    public void testBooleanLogicOps()
    {
        final BooleanHypercube a = createBooleanHypercube();
        final BooleanHypercube b = createBooleanHypercube();

        assertEquals(VectorizedCubeMath.and(a, b),
                     CubeMath          .and(a, b));
        assertEquals(VectorizedCubeMath.or (a, b),
                     CubeMath          .or (a, b));
        assertEquals(VectorizedCubeMath.xor(a, b),
                     CubeMath          .xor(a, b));
        assertEquals(VectorizedCubeMath.not(a),
                     CubeMath          .not(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Test boolean operations for boolean cubes.
     */
    @Test
    public void testBooleanBooleanOps()
    {
        final BooleanHypercube a = createBooleanHypercube();
        final BooleanHypercube b = createBooleanHypercube();

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

        assert(VectorizedCubeMath.any0d(a) == CubeMath.any0d(a));
        assert(VectorizedCubeMath.all0d(a) == CubeMath.all0d(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Populate a boolean hypercube.
     */
    private void populate(final BooleanHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (boolean)((i + 1) % 2 == 1));
        }
    }

    /**
     * Create a {@link BooleanHypercube}.
     */
    private BooleanHypercube createBooleanHypercube()
    {
        // Create
        final BooleanHypercube cube =
            new BooleanBitSetHypercube(DIMENSIONS);

        // Populate
        populate(cube);

        // Give it back
        return cube;
    }

    // -------------------------------------------------------------------------

    /**
     * Test binary operations for float cubes.
     */
    @Test
    public void testFloatBinaryOps()
    {
        final FloatHypercube a = createFloatHypercube();
        final FloatHypercube b = createFloatHypercube();

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
    }

    /**
     * Test numeric unary operations for float cubes.
     */
    @Test
    public void testFloatNumericUnaryOps()
    {
        final FloatHypercube a = createFloatHypercube();

        assertEquals(VectorizedCubeMath.negative(a),
                     CubeMath          .negative(a));
        assertEquals(VectorizedCubeMath.abs     (a),
                     CubeMath          .abs     (a));
    }

    /**
     * Test associative operations for float cubes.
     */
    @Test
    public void testFloatAssociativeOps()
    {
        final FloatHypercube a = createFloatHypercube();

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.min0d(a) - CubeMath.min0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.max0d(a) - CubeMath.max0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.sum0d(a) - CubeMath.sum0d(a)) <= EPS);
    }

    /**
     * Test extract operation for float cubes.
     */
    @Test
    public void testFloatExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Float> a = createFloatHypercube();

        assertEquals(VectorizedCubeMath.extract(c, a),
                     CubeMath          .extract(c, a));
    }

    // -------------------------------------------------------------------------

    /**
     * Populate a float hypercube with values between 0 and 1.
     */
    private void populateNormalized(final FloatHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (float)((i + 1.) / (float)cube.getSize()));
        }
    }

    /**
     * Create a {@link FloatHypercube} with values between 0 and 1.
     */
    private FloatHypercube createNormalizedFloatHypercube()
    {
        // Create
        final FloatHypercube cube =
            new FloatArrayHypercube(DIMENSIONS);

        // Populate
        populateNormalized(cube);

        // Give it back
        return cube;
    }

    // -------------------------------------------------------------------------

    /**
     * Test exponentiation operations for float cubes.
     */
    @Test
    public void testFloatExponentiation()
    {
        final Hypercube<Float> a = createNormalizedFloatHypercube();
        final Hypercube<Float> e = CubeMath.full(a.getDimensions(), (float)Math.E);

        assertEquals(VectorizedCubeMath.power(e, a),
                     CubeMath          .power(e, a));
        assertEquals(VectorizedCubeMath.exp  (a),
                     CubeMath          .exp  (a));
        assertEquals(VectorizedCubeMath.log  (a),
                     CubeMath          .log  (a));
        assertEquals(VectorizedCubeMath.log10(a),
                     CubeMath          .log10(a));
    }

    /**
     * Test unary operations for float cubes.
     */
    @Test
    public void testFloatUnaryOps()
    {
        final FloatHypercube a = createNormalizedFloatHypercube();

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
    }

    /**
     * Test NaN handling for float cubes.
     */
    @Test
    public void testFloatNaNHandling()
    {
        final FloatHypercube a = createNormalizedFloatHypercube();

        // Add a NaN value somewhere
        a.setAt(0, Float.NaN);

        // Ensure that the results are NaNs
        assert(Float.isNaN(VectorizedCubeMath.min0d(a)));
        assert(Float.isNaN(VectorizedCubeMath.max0d(a)));
        assert(Float.isNaN(VectorizedCubeMath.sum0d(a)));

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.nansum0d(a) - CubeMath.nansum0d(a)) <= EPS);
    }

    // ----------------------------------------------------------------------

    /**
     * Test boolean operations for float cubes.
     */
    @Test
    public void testFloatBooleanOps()
    {
        final FloatHypercube a = createFloatHypercube();
        final FloatHypercube b = createFloatHypercube();

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

        assert(VectorizedCubeMath.any0d(a) == CubeMath.any0d(a));
        assert(VectorizedCubeMath.all0d(a) == CubeMath.all0d(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Populate a float hypercube.
     */
    private void populate(final FloatHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (float)((i + 1)));
        }
    }

    /**
     * Create a {@link FloatHypercube}.
     */
    private FloatHypercube createFloatHypercube()
    {
        // Create
        final FloatHypercube cube =
            new FloatArrayHypercube(DIMENSIONS);

        // Populate
        populate(cube);

        // Give it back
        return cube;
    }

    // -------------------------------------------------------------------------

    /**
     * Test binary operations for double cubes.
     */
    @Test
    public void testDoubleBinaryOps()
    {
        final DoubleHypercube a = createDoubleHypercube();
        final DoubleHypercube b = createDoubleHypercube();

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
    }

    /**
     * Test numeric unary operations for double cubes.
     */
    @Test
    public void testDoubleNumericUnaryOps()
    {
        final DoubleHypercube a = createDoubleHypercube();

        assertEquals(VectorizedCubeMath.negative(a),
                     CubeMath          .negative(a));
        assertEquals(VectorizedCubeMath.abs     (a),
                     CubeMath          .abs     (a));
    }

    /**
     * Test associative operations for double cubes.
     */
    @Test
    public void testDoubleAssociativeOps()
    {
        final DoubleHypercube a = createDoubleHypercube();

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.min0d(a) - CubeMath.min0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.max0d(a) - CubeMath.max0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.sum0d(a) - CubeMath.sum0d(a)) <= EPS);
    }

    /**
     * Test extract operation for double cubes.
     */
    @Test
    public void testDoubleExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Double> a = createDoubleHypercube();

        assertEquals(VectorizedCubeMath.extract(c, a),
                     CubeMath          .extract(c, a));
    }

    // -------------------------------------------------------------------------

    /**
     * Populate a double hypercube with values between 0 and 1.
     */
    private void populateNormalized(final DoubleHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (double)((i + 1.) / (double)cube.getSize()));
        }
    }

    /**
     * Create a {@link DoubleHypercube} with values between 0 and 1.
     */
    private DoubleHypercube createNormalizedDoubleHypercube()
    {
        // Create
        final DoubleHypercube cube =
            new DoubleArrayHypercube(DIMENSIONS);

        // Populate
        populateNormalized(cube);

        // Give it back
        return cube;
    }

    // -------------------------------------------------------------------------

    /**
     * Test exponentiation operations for double cubes.
     */
    @Test
    public void testDoubleExponentiation()
    {
        final Hypercube<Double> a = createNormalizedDoubleHypercube();
        final Hypercube<Double> e = CubeMath.full(a.getDimensions(), (double)Math.E);

        assertEquals(VectorizedCubeMath.power(e, a),
                     CubeMath          .power(e, a));
        assertEquals(VectorizedCubeMath.exp  (a),
                     CubeMath          .exp  (a));
        assertEquals(VectorizedCubeMath.log  (a),
                     CubeMath          .log  (a));
        assertEquals(VectorizedCubeMath.log10(a),
                     CubeMath          .log10(a));
    }

    /**
     * Test unary operations for double cubes.
     */
    @Test
    public void testDoubleUnaryOps()
    {
        final DoubleHypercube a = createNormalizedDoubleHypercube();

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
    }

    /**
     * Test NaN handling for double cubes.
     */
    @Test
    public void testDoubleNaNHandling()
    {
        final DoubleHypercube a = createNormalizedDoubleHypercube();

        // Add a NaN value somewhere
        a.setAt(0, Double.NaN);

        // Ensure that the results are NaNs
        assert(Double.isNaN(VectorizedCubeMath.min0d(a)));
        assert(Double.isNaN(VectorizedCubeMath.max0d(a)));
        assert(Double.isNaN(VectorizedCubeMath.sum0d(a)));

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.nansum0d(a) - CubeMath.nansum0d(a)) <= EPS);
    }

    // ----------------------------------------------------------------------

    /**
     * Test boolean operations for double cubes.
     */
    @Test
    public void testDoubleBooleanOps()
    {
        final DoubleHypercube a = createDoubleHypercube();
        final DoubleHypercube b = createDoubleHypercube();

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

        assert(VectorizedCubeMath.any0d(a) == CubeMath.any0d(a));
        assert(VectorizedCubeMath.all0d(a) == CubeMath.all0d(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Populate a double hypercube.
     */
    private void populate(final DoubleHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (double)((i + 1)));
        }
    }

    /**
     * Create a {@link DoubleHypercube}.
     */
    private DoubleHypercube createDoubleHypercube()
    {
        // Create
        final DoubleHypercube cube =
            new DoubleArrayHypercube(DIMENSIONS);

        // Populate
        populate(cube);

        // Give it back
        return cube;
    }

    // -------------------------------------------------------------------------

    /**
     * Test binary operations for int cubes.
     */
    @Test
    public void testIntegerBinaryOps()
    {
        final IntegerHypercube a = createIntegerHypercube();
        final IntegerHypercube b = createIntegerHypercube();

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
    }

    /**
     * Test numeric unary operations for int cubes.
     */
    @Test
    public void testIntegerNumericUnaryOps()
    {
        final IntegerHypercube a = createIntegerHypercube();

        assertEquals(VectorizedCubeMath.negative(a),
                     CubeMath          .negative(a));
        assertEquals(VectorizedCubeMath.abs     (a),
                     CubeMath          .abs     (a));
    }

    /**
     * Test associative operations for int cubes.
     */
    @Test
    public void testIntegerAssociativeOps()
    {
        final IntegerHypercube a = createIntegerHypercube();

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.min0d(a) - CubeMath.min0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.max0d(a) - CubeMath.max0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.sum0d(a) - CubeMath.sum0d(a)) <= EPS);
    }

    /**
     * Test extract operation for int cubes.
     */
    @Test
    public void testIntegerExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Integer> a = createIntegerHypercube();

        assertEquals(VectorizedCubeMath.extract(c, a),
                     CubeMath          .extract(c, a));
    }

    // -------------------------------------------------------------------------

    /**
     * Test logic and bitwise operations for int cubes.
     */
    @Test
    public void testIntegerLogicOps()
    {
        final IntegerHypercube a = createIntegerHypercube();
        final IntegerHypercube b = createIntegerHypercube();

        assertEquals(VectorizedCubeMath.and(a, b),
                     CubeMath          .and(a, b));
        assertEquals(VectorizedCubeMath.or (a, b),
                     CubeMath          .or (a, b));
        assertEquals(VectorizedCubeMath.xor(a, b),
                     CubeMath          .xor(a, b));
        assertEquals(VectorizedCubeMath.not(a),
                     CubeMath          .not(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Test boolean operations for int cubes.
     */
    @Test
    public void testIntegerBooleanOps()
    {
        final IntegerHypercube a = createIntegerHypercube();
        final IntegerHypercube b = createIntegerHypercube();

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

        assert(VectorizedCubeMath.any0d(a) == CubeMath.any0d(a));
        assert(VectorizedCubeMath.all0d(a) == CubeMath.all0d(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Populate a int hypercube.
     */
    private void populate(final IntegerHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (int)((i + 1)));
        }
    }

    /**
     * Create a {@link IntegerHypercube}.
     */
    private IntegerHypercube createIntegerHypercube()
    {
        // Create
        final IntegerHypercube cube =
            new IntegerArrayHypercube(DIMENSIONS);

        // Populate
        populate(cube);

        // Give it back
        return cube;
    }

    // -------------------------------------------------------------------------

    /**
     * Test binary operations for long cubes.
     */
    @Test
    public void testLongBinaryOps()
    {
        final LongHypercube a = createLongHypercube();
        final LongHypercube b = createLongHypercube();

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
    }

    /**
     * Test numeric unary operations for long cubes.
     */
    @Test
    public void testLongNumericUnaryOps()
    {
        final LongHypercube a = createLongHypercube();

        assertEquals(VectorizedCubeMath.negative(a),
                     CubeMath          .negative(a));
        assertEquals(VectorizedCubeMath.abs     (a),
                     CubeMath          .abs     (a));
    }

    /**
     * Test associative operations for long cubes.
     */
    @Test
    public void testLongAssociativeOps()
    {
        final LongHypercube a = createLongHypercube();

        // Make sure to allow for an error margin
        assert(Math.abs(VectorizedCubeMath.min0d(a) - CubeMath.min0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.max0d(a) - CubeMath.max0d(a)) <= EPS);
        assert(Math.abs(VectorizedCubeMath.sum0d(a) - CubeMath.sum0d(a)) <= EPS);
    }

    /**
     * Test extract operation for long cubes.
     */
    @Test
    public void testLongExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Long> a = createLongHypercube();

        assertEquals(VectorizedCubeMath.extract(c, a),
                     CubeMath          .extract(c, a));
    }

    // -------------------------------------------------------------------------

    /**
     * Test logic and bitwise operations for long cubes.
     */
    @Test
    public void testLongLogicOps()
    {
        final LongHypercube a = createLongHypercube();
        final LongHypercube b = createLongHypercube();

        assertEquals(VectorizedCubeMath.and(a, b),
                     CubeMath          .and(a, b));
        assertEquals(VectorizedCubeMath.or (a, b),
                     CubeMath          .or (a, b));
        assertEquals(VectorizedCubeMath.xor(a, b),
                     CubeMath          .xor(a, b));
        assertEquals(VectorizedCubeMath.not(a),
                     CubeMath          .not(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Test boolean operations for long cubes.
     */
    @Test
    public void testLongBooleanOps()
    {
        final LongHypercube a = createLongHypercube();
        final LongHypercube b = createLongHypercube();

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

        assert(VectorizedCubeMath.any0d(a) == CubeMath.any0d(a));
        assert(VectorizedCubeMath.all0d(a) == CubeMath.all0d(a));
    }

    // ----------------------------------------------------------------------

    /**
     * Populate a long hypercube.
     */
    private void populate(final LongHypercube cube)
    {
        for (long i=0; i < cube.getSize(); i++) {
            cube.setAt(i, (long)((i + 1)));
        }
    }

    /**
     * Create a {@link LongHypercube}.
     */
    private LongHypercube createLongHypercube()
    {
        // Create
        final LongHypercube cube =
            new LongArrayHypercube(DIMENSIONS);

        // Populate
        populate(cube);

        // Give it back
        return cube;
    }
}

// [[[end]]] (checksum: 304b76f2b6dda57fa5735b667b3d1f87)
