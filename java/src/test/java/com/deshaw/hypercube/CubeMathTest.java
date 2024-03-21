package com.deshaw.hypercube;

// Recreate with `cog -rc CubeMathTest.java`
// [[[cog
//     import cog
//     import cube_math_test
//
//     cog.outl(cube_math_test.generate())
// ]]]
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The testing code for {@code com.deshaw.hypercube.CubeMath}.
 */
public class CubeMathTest
{
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
    {
        assert(CubeMath.all0d(CubeMath.equal(cube1, cube2)));
    }

    // -------------------------------------------------------------------------

    /**
     * Test popcount operation for boolean cubes.
     */
    @Test
    public void testBooleanPopcount()
    {
        final BooleanHypercube a = createBooleanHypercube();

        // Initialize the popcount
        long popcount = CubeMath.popcount(a);
        for (long i=0; i < a.getSize(); i++) {
            if (a.getObjectAt(i)) {
                // Decrease the popcount
                popcount --;
            }
        }

        // Make sure the popcount is now zero
        assert(popcount == 0);
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

        final Hypercube<Boolean> and = CubeMath.and(a, b);
        final Hypercube<Boolean> or  = CubeMath.or (a, b);
        final Hypercube<Boolean> xor = CubeMath.xor(a, b);
        final Hypercube<Boolean> not = CubeMath.not(a);

        for (long i=0; i < a.getSize(); i++) {
            assert(and.getObjectAt(i) == (a.getAt(i) & b.getAt(i)));
            assert(or .getObjectAt(i) == (a.getAt(i) | b.getAt(i)));
            assert(xor.getObjectAt(i) == (a.getAt(i) ^ b.getAt(i)));
            assert(not.getObjectAt(i) == (!a.getAt(i)));
        }
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
     * Test copying a Boolean cube.
     */
    @Test
    public void testBooleanHypercubeCopy()
    {
        final Hypercube<Boolean> a = createBooleanHypercube();
        final Hypercube<Boolean> c = a.copy();

        // Now make sure the cube was correctly copied
        for (long i=0; i < a.getSize(); i++) {
            boolean va = a.getObjectAt(i);
            boolean vc = c.getObjectAt(i);
            assert(va == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Boolean cube to a Float cube.
     */
    @Test
    public void testBooleanToFloatHypercube()
    {
        final Hypercube<Boolean>   a = createBooleanHypercube();
        final Hypercube<Float> c = FloatHypercube.toFloatHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            boolean   va = a.getObjectAt(i);
            float vc = c.getObjectAt(i);
            assert((float)(va ? 1 : 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Boolean cube to a Double cube.
     */
    @Test
    public void testBooleanToDoubleHypercube()
    {
        final Hypercube<Boolean>   a = createBooleanHypercube();
        final Hypercube<Double> c = DoubleHypercube.toDoubleHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            boolean   va = a.getObjectAt(i);
            double vc = c.getObjectAt(i);
            assert((double)(va ? 1 : 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Boolean cube to a Integer cube.
     */
    @Test
    public void testBooleanToIntegerHypercube()
    {
        final Hypercube<Boolean>   a = createBooleanHypercube();
        final Hypercube<Integer> c = IntegerHypercube.toIntegerHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            boolean   va = a.getObjectAt(i);
            int vc = c.getObjectAt(i);
            assert((int)(va ? 1 : 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Boolean cube to a Long cube.
     */
    @Test
    public void testBooleanToLongHypercube()
    {
        final Hypercube<Boolean>   a = createBooleanHypercube();
        final Hypercube<Long> c = LongHypercube.toLongHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            boolean   va = a.getObjectAt(i);
            long vc = c.getObjectAt(i);
            assert((long)(va ? 1 : 0) == vc);
        }
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

        final Hypercube<Float> q = CubeMath.divide  (a, b);
        final Hypercube<Float> r = CubeMath.mod     (a, b);
        final Hypercube<Float> m = CubeMath.multiply(q, b);
        final Hypercube<Float> t = CubeMath.add     (m, r);
        assertEquals(t, a);

        final Hypercube<Float> min = CubeMath.minimum(a, b);
        final Hypercube<Float> max = CubeMath.maximum(a, b);
        for (long i=0; i < a.getSize(); i++) {
            assert(min.getObjectAt(i) == Float.min(a.getAt(i), b.getAt(i)));
            assert(max.getObjectAt(i) == Float.max(a.getAt(i), b.getAt(i)));
        }
    }

    /**
     * Test numeric unary operations for float cubes.
     */
    @Test
    public void testFloatNumericUnaryOps()
    {
        final FloatHypercube a = createFloatHypercube();
        final FloatHypercube b = createFloatHypercube();

        // Test negative()
        final Hypercube<Float> n  = CubeMath.negative(b);
        final Hypercube<Float> s1 = CubeMath.subtract(a, b);
        final Hypercube<Float> s2 = CubeMath.add     (a, n);
        assertEquals(s1, s2);

        // Test abs()
        final Hypercube<Float> abs = CubeMath.abs(n);
        assertEquals(abs, b);
    }

    /**
     * Test broadcasting operations for float cubes.
     */
    @Test
    public void testFloatBroadcast()
    {
        final FloatHypercube a = createFloatHypercube();

        // Test broadcasting
        final Hypercube<Float> z = CubeMath.broadcast(DIMENSIONS, (float)0);
        final Hypercube<Float> m = CubeMath.multiply(a, z);

        // Check that all elements are zero.
        final Hypercube<Boolean> e = CubeMath.equal(m, (float)0);
        final Hypercube<Boolean> b = CubeMath.equal(e, true);
        assert(CubeMath.all0d(b));
    }

    /**
     * Test associative operations for float cubes.
     */
    @Test
    public void testFloatAssociativeOps()
    {
        final FloatHypercube a = createFloatHypercube();

        // Initialize sum
        float       sum = 0;
        final float min = CubeMath.min0d(a);
        final float max = CubeMath.max0d(a);

        for (long i=0; i < a.getSize(); i++) {
            final float va = a.getObjectAt(i);
            sum += va;
            // Check the min and max operations
            assert(min <= va && va <= max);
        }

        // Check the sum operation
        assert(sum == CubeMath.sum0d(a));
    }

    /**
     * Test extract operation for float cubes.
     */
    @Test
    public void testFloatExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Float> a = createFloatHypercube();
        final Hypercube<Float> e = CubeMath.extract(c, a);

        int j = 0;
        for (long i=0; i < a.getSize(); i++) {
            if (c.getObjectAt(i)) {
                // Make sure the element is present in the extract
                assert(j < e.getSize());
                assert(e.getObjectAt(j++).floatValue() == a.getObjectAt(i).floatValue());
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Check all the elements of a hypercube match a value within a given error
     * tolerance.
     */
    private void assertEqualsFloat(
        final Hypercube<Float> cube,
        final float value,
        final double eps)
    {
        FloatHypercube dcube = (FloatHypercube)cube;
        for (long i=0; i < cube.getShape()[0]; i++) {
            assert(Math.abs(value - dcube.get(i)) <= eps);
        }
    }

    /**
     * Check that two cubes are element-wise equal, within a given error tolerance.
     */
    private void assertEqualsFloat(
        final Hypercube<Float> cube1,
        final Hypercube<Float> cube2,
        final double eps)
    {
        assertEqualsFloat(CubeMath.subtract(cube1, cube2), (float)0., eps);
    }

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
        final FloatHypercube a = createNormalizedFloatHypercube();
        final FloatHypercube e = createNormalizedFloatHypercube();
        e.fill((float)Math.E);

        final Hypercube<Float> exp = CubeMath.exp  (a);
        final Hypercube<Float> log = CubeMath.log  (exp);
        final Hypercube<Float> pow = CubeMath.power(e, a);

        assertEqualsFloat(exp, pow, EPS);
        assertEqualsFloat(log, a, EPS);
    }

    /**
     * Test unary operations for float cubes.
     */
    @Test
    public void testFloatUnaryOps()
    {
        final FloatHypercube a = createNormalizedFloatHypercube();

        final Hypercube<Float> cos  = CubeMath.cos(a);
        final Hypercube<Float> sin  = CubeMath.sin(a);
        final Hypercube<Float> tan  = CubeMath.tan(a);
        assertEqualsFloat(tan, CubeMath.divide(sin, cos), EPS);

        final Hypercube<Float> cos2 = CubeMath.power(cos, (float)2);
        final Hypercube<Float> sin2 = CubeMath.power(sin, (float)2);
        final Hypercube<Float> sum  = CubeMath.add  (cos2, sin2);
        assertEqualsFloat(sum, (float)1.0, EPS);

        final Hypercube<Float> floor = CubeMath.floor(tan);
        final Hypercube<Float> round = CubeMath.round(tan);
        final Hypercube<Float> ceil  = CubeMath.ceil (tan);

        for (long i=0; i < tan.getSize(); i++) {
            final float v = tan.getObjectAt(i);
            assert(Math.abs(floor.getObjectAt(i) - Math.floor(v)) <= EPS);
            assert(Math.abs(round.getObjectAt(i) - Math.round(v)) <= EPS);
            assert(Math.abs(ceil .getObjectAt(i) - Math.ceil (v)) <= EPS);
        }
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
        assert(Float.isNaN(CubeMath.min0d(a)));
        assert(Float.isNaN(CubeMath.max0d(a)));

        float nansum = (float)0.;
        for (long i=0; i < a.getSize(); i++) {
            if (!Float.isNaN(a.get(i))) {
                nansum += a.get(i);
            }
        }

        assert(Float.isNaN(CubeMath.sum0d(a)));
        assert(Math.abs(CubeMath.nansum0d(a) - nansum) <= EPS);
    }

    /**
     * Test that the {@code where} kwarg works as expected.
     */
    @Test
    public void testFloatWhereKwarg()
    {
        // Create the cube full of values, from negative to positive, and
        // compute the sum of the non-negative ones
        float sum = 0;
        final FloatHypercube cube = createFloatHypercube();
        for (long i = 0, j = cube.getSize() / 2; i < cube.getSize(); i++, j++) {
            cube.setAt(i, (float)j);
            if (j >= 0) {
                sum += j;
            }
        }

        // Get the mask of the values in the cube which are non-negative and use
        // that as the where clause for sum()
        final Hypercube<Boolean> where = CubeMath.greaterEqual(cube, (float)0);
        final Object whereSum = CubeMath.sum(cube, Map.of("where", where));

        // Sum should return an object of the element type. (It could return
        // null, or another cube if the axis kwarg is used.) That value should
        // match our computed sum.
        assert(whereSum instanceof Float);
        assert((Float)whereSum == sum);
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
     * Test copying a Float cube.
     */
    @Test
    public void testFloatHypercubeCopy()
    {
        final Hypercube<Float> a = createFloatHypercube();
        final Hypercube<Float> c = a.copy();

        // Now make sure the cube was correctly copied
        for (long i=0; i < a.getSize(); i++) {
            float va = a.getObjectAt(i);
            float vc = c.getObjectAt(i);
            assert(va == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Float cube to a Boolean cube.
     */
    @Test
    public void testFloatToBooleanHypercube()
    {
        final Hypercube<Float>   a = createFloatHypercube();
        final Hypercube<Boolean> c = BooleanHypercube.toBooleanHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            float   va = a.getObjectAt(i);
            boolean vc = c.getObjectAt(i);
            assert((boolean)(va != 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Float cube to a Double cube.
     */
    @Test
    public void testFloatToDoubleHypercube()
    {
        final Hypercube<Float>   a = createFloatHypercube();
        final Hypercube<Double> c = DoubleHypercube.toDoubleHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            float   va = a.getObjectAt(i);
            double vc = c.getObjectAt(i);
            assert((double)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Float cube to a Integer cube.
     */
    @Test
    public void testFloatToIntegerHypercube()
    {
        final Hypercube<Float>   a = createFloatHypercube();
        final Hypercube<Integer> c = IntegerHypercube.toIntegerHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            float   va = a.getObjectAt(i);
            int vc = c.getObjectAt(i);
            assert((int)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Float cube to a Long cube.
     */
    @Test
    public void testFloatToLongHypercube()
    {
        final Hypercube<Float>   a = createFloatHypercube();
        final Hypercube<Long> c = LongHypercube.toLongHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            float   va = a.getObjectAt(i);
            long vc = c.getObjectAt(i);
            assert((long)(va) == vc);
        }
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

        final Hypercube<Double> q = CubeMath.divide  (a, b);
        final Hypercube<Double> r = CubeMath.mod     (a, b);
        final Hypercube<Double> m = CubeMath.multiply(q, b);
        final Hypercube<Double> t = CubeMath.add     (m, r);
        assertEquals(t, a);

        final Hypercube<Double> min = CubeMath.minimum(a, b);
        final Hypercube<Double> max = CubeMath.maximum(a, b);
        for (long i=0; i < a.getSize(); i++) {
            assert(min.getObjectAt(i) == Double.min(a.getAt(i), b.getAt(i)));
            assert(max.getObjectAt(i) == Double.max(a.getAt(i), b.getAt(i)));
        }
    }

    /**
     * Test numeric unary operations for double cubes.
     */
    @Test
    public void testDoubleNumericUnaryOps()
    {
        final DoubleHypercube a = createDoubleHypercube();
        final DoubleHypercube b = createDoubleHypercube();

        // Test negative()
        final Hypercube<Double> n  = CubeMath.negative(b);
        final Hypercube<Double> s1 = CubeMath.subtract(a, b);
        final Hypercube<Double> s2 = CubeMath.add     (a, n);
        assertEquals(s1, s2);

        // Test abs()
        final Hypercube<Double> abs = CubeMath.abs(n);
        assertEquals(abs, b);
    }

    /**
     * Test broadcasting operations for double cubes.
     */
    @Test
    public void testDoubleBroadcast()
    {
        final DoubleHypercube a = createDoubleHypercube();

        // Test broadcasting
        final Hypercube<Double> z = CubeMath.broadcast(DIMENSIONS, (double)0);
        final Hypercube<Double> m = CubeMath.multiply(a, z);

        // Check that all elements are zero.
        final Hypercube<Boolean> e = CubeMath.equal(m, (double)0);
        final Hypercube<Boolean> b = CubeMath.equal(e, true);
        assert(CubeMath.all0d(b));
    }

    /**
     * Test associative operations for double cubes.
     */
    @Test
    public void testDoubleAssociativeOps()
    {
        final DoubleHypercube a = createDoubleHypercube();

        // Initialize sum
        double       sum = 0;
        final double min = CubeMath.min0d(a);
        final double max = CubeMath.max0d(a);

        for (long i=0; i < a.getSize(); i++) {
            final double va = a.getObjectAt(i);
            sum += va;
            // Check the min and max operations
            assert(min <= va && va <= max);
        }

        // Check the sum operation
        assert(sum == CubeMath.sum0d(a));
    }

    /**
     * Test extract operation for double cubes.
     */
    @Test
    public void testDoubleExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Double> a = createDoubleHypercube();
        final Hypercube<Double> e = CubeMath.extract(c, a);

        int j = 0;
        for (long i=0; i < a.getSize(); i++) {
            if (c.getObjectAt(i)) {
                // Make sure the element is present in the extract
                assert(j < e.getSize());
                assert(e.getObjectAt(j++).doubleValue() == a.getObjectAt(i).doubleValue());
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Check all the elements of a hypercube match a value within a given error
     * tolerance.
     */
    private void assertEqualsDouble(
        final Hypercube<Double> cube,
        final double value,
        final double eps)
    {
        DoubleHypercube dcube = (DoubleHypercube)cube;
        for (long i=0; i < cube.getShape()[0]; i++) {
            assert(Math.abs(value - dcube.get(i)) <= eps);
        }
    }

    /**
     * Check that two cubes are element-wise equal, within a given error tolerance.
     */
    private void assertEqualsDouble(
        final Hypercube<Double> cube1,
        final Hypercube<Double> cube2,
        final double eps)
    {
        assertEqualsDouble(CubeMath.subtract(cube1, cube2), (double)0., eps);
    }

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
        final DoubleHypercube a = createNormalizedDoubleHypercube();
        final DoubleHypercube e = createNormalizedDoubleHypercube();
        e.fill((double)Math.E);

        final Hypercube<Double> exp = CubeMath.exp  (a);
        final Hypercube<Double> log = CubeMath.log  (exp);
        final Hypercube<Double> pow = CubeMath.power(e, a);

        assertEqualsDouble(exp, pow, EPS);
        assertEqualsDouble(log, a, EPS);
    }

    /**
     * Test unary operations for double cubes.
     */
    @Test
    public void testDoubleUnaryOps()
    {
        final DoubleHypercube a = createNormalizedDoubleHypercube();

        final Hypercube<Double> cos  = CubeMath.cos(a);
        final Hypercube<Double> sin  = CubeMath.sin(a);
        final Hypercube<Double> tan  = CubeMath.tan(a);
        assertEqualsDouble(tan, CubeMath.divide(sin, cos), EPS);

        final Hypercube<Double> cos2 = CubeMath.power(cos, (double)2);
        final Hypercube<Double> sin2 = CubeMath.power(sin, (double)2);
        final Hypercube<Double> sum  = CubeMath.add  (cos2, sin2);
        assertEqualsDouble(sum, (double)1.0, EPS);

        final Hypercube<Double> floor = CubeMath.floor(tan);
        final Hypercube<Double> round = CubeMath.round(tan);
        final Hypercube<Double> ceil  = CubeMath.ceil (tan);

        for (long i=0; i < tan.getSize(); i++) {
            final double v = tan.getObjectAt(i);
            assert(Math.abs(floor.getObjectAt(i) - Math.floor(v)) <= EPS);
            assert(Math.abs(round.getObjectAt(i) - Math.round(v)) <= EPS);
            assert(Math.abs(ceil .getObjectAt(i) - Math.ceil (v)) <= EPS);
        }
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
        assert(Double.isNaN(CubeMath.min0d(a)));
        assert(Double.isNaN(CubeMath.max0d(a)));

        double nansum = (double)0.;
        for (long i=0; i < a.getSize(); i++) {
            if (!Double.isNaN(a.get(i))) {
                nansum += a.get(i);
            }
        }

        assert(Double.isNaN(CubeMath.sum0d(a)));
        assert(Math.abs(CubeMath.nansum0d(a) - nansum) <= EPS);
    }

    /**
     * Test that the {@code where} kwarg works as expected.
     */
    @Test
    public void testDoubleWhereKwarg()
    {
        // Create the cube full of values, from negative to positive, and
        // compute the sum of the non-negative ones
        double sum = 0;
        final DoubleHypercube cube = createDoubleHypercube();
        for (long i = 0, j = cube.getSize() / 2; i < cube.getSize(); i++, j++) {
            cube.setAt(i, (double)j);
            if (j >= 0) {
                sum += j;
            }
        }

        // Get the mask of the values in the cube which are non-negative and use
        // that as the where clause for sum()
        final Hypercube<Boolean> where = CubeMath.greaterEqual(cube, (double)0);
        final Object whereSum = CubeMath.sum(cube, Map.of("where", where));

        // Sum should return an object of the element type. (It could return
        // null, or another cube if the axis kwarg is used.) That value should
        // match our computed sum.
        assert(whereSum instanceof Double);
        assert((Double)whereSum == sum);
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
     * Test copying a Double cube.
     */
    @Test
    public void testDoubleHypercubeCopy()
    {
        final Hypercube<Double> a = createDoubleHypercube();
        final Hypercube<Double> c = a.copy();

        // Now make sure the cube was correctly copied
        for (long i=0; i < a.getSize(); i++) {
            double va = a.getObjectAt(i);
            double vc = c.getObjectAt(i);
            assert(va == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Double cube to a Boolean cube.
     */
    @Test
    public void testDoubleToBooleanHypercube()
    {
        final Hypercube<Double>   a = createDoubleHypercube();
        final Hypercube<Boolean> c = BooleanHypercube.toBooleanHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            double   va = a.getObjectAt(i);
            boolean vc = c.getObjectAt(i);
            assert((boolean)(va != 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Double cube to a Float cube.
     */
    @Test
    public void testDoubleToFloatHypercube()
    {
        final Hypercube<Double>   a = createDoubleHypercube();
        final Hypercube<Float> c = FloatHypercube.toFloatHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            double   va = a.getObjectAt(i);
            float vc = c.getObjectAt(i);
            assert((float)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Double cube to a Integer cube.
     */
    @Test
    public void testDoubleToIntegerHypercube()
    {
        final Hypercube<Double>   a = createDoubleHypercube();
        final Hypercube<Integer> c = IntegerHypercube.toIntegerHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            double   va = a.getObjectAt(i);
            int vc = c.getObjectAt(i);
            assert((int)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Double cube to a Long cube.
     */
    @Test
    public void testDoubleToLongHypercube()
    {
        final Hypercube<Double>   a = createDoubleHypercube();
        final Hypercube<Long> c = LongHypercube.toLongHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            double   va = a.getObjectAt(i);
            long vc = c.getObjectAt(i);
            assert((long)(va) == vc);
        }
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

        final Hypercube<Integer> q = CubeMath.divide  (a, b);
        final Hypercube<Integer> r = CubeMath.mod     (a, b);
        final Hypercube<Integer> m = CubeMath.multiply(q, b);
        final Hypercube<Integer> t = CubeMath.add     (m, r);
        assertEquals(t, a);

        final Hypercube<Integer> min = CubeMath.minimum(a, b);
        final Hypercube<Integer> max = CubeMath.maximum(a, b);
        for (long i=0; i < a.getSize(); i++) {
            assert(min.getObjectAt(i) == Integer.min(a.getAt(i), b.getAt(i)));
            assert(max.getObjectAt(i) == Integer.max(a.getAt(i), b.getAt(i)));
        }
    }

    /**
     * Test numeric unary operations for int cubes.
     */
    @Test
    public void testIntegerNumericUnaryOps()
    {
        final IntegerHypercube a = createIntegerHypercube();
        final IntegerHypercube b = createIntegerHypercube();

        // Test negative()
        final Hypercube<Integer> n  = CubeMath.negative(b);
        final Hypercube<Integer> s1 = CubeMath.subtract(a, b);
        final Hypercube<Integer> s2 = CubeMath.add     (a, n);
        assertEquals(s1, s2);

        // Test abs()
        final Hypercube<Integer> abs = CubeMath.abs(n);
        assertEquals(abs, b);
    }

    /**
     * Test broadcasting operations for int cubes.
     */
    @Test
    public void testIntegerBroadcast()
    {
        final IntegerHypercube a = createIntegerHypercube();

        // Test broadcasting
        final Hypercube<Integer> z = CubeMath.broadcast(DIMENSIONS, (int)0);
        final Hypercube<Integer> m = CubeMath.multiply(a, z);

        // Check that all elements are zero.
        final Hypercube<Boolean> e = CubeMath.equal(m, (int)0);
        final Hypercube<Boolean> b = CubeMath.equal(e, true);
        assert(CubeMath.all0d(b));
    }

    /**
     * Test associative operations for int cubes.
     */
    @Test
    public void testIntegerAssociativeOps()
    {
        final IntegerHypercube a = createIntegerHypercube();

        // Initialize sum
        int       sum = 0;
        final int min = CubeMath.min0d(a);
        final int max = CubeMath.max0d(a);

        for (long i=0; i < a.getSize(); i++) {
            final int va = a.getObjectAt(i);
            sum += va;
            // Check the min and max operations
            assert(min <= va && va <= max);
        }

        // Check the sum operation
        assert(sum == CubeMath.sum0d(a));
    }

    /**
     * Test extract operation for int cubes.
     */
    @Test
    public void testIntegerExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Integer> a = createIntegerHypercube();
        final Hypercube<Integer> e = CubeMath.extract(c, a);

        int j = 0;
        for (long i=0; i < a.getSize(); i++) {
            if (c.getObjectAt(i)) {
                // Make sure the element is present in the extract
                assert(j < e.getSize());
                assert(e.getObjectAt(j++).intValue() == a.getObjectAt(i).intValue());
            }
        }
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

        final Hypercube<Integer> and = CubeMath.and(a, b);
        final Hypercube<Integer> or  = CubeMath.or (a, b);
        final Hypercube<Integer> xor = CubeMath.xor(a, b);
        final Hypercube<Integer> not = CubeMath.not(a);

        for (long i=0; i < a.getSize(); i++) {
            assert(and.getObjectAt(i) == (a.getAt(i) & b.getAt(i)));
            assert(or .getObjectAt(i) == (a.getAt(i) | b.getAt(i)));
            assert(xor.getObjectAt(i) == (a.getAt(i) ^ b.getAt(i)));
            assert(not.getObjectAt(i) == (~a.getAt(i)));
        }
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
     * Test copying a Integer cube.
     */
    @Test
    public void testIntegerHypercubeCopy()
    {
        final Hypercube<Integer> a = createIntegerHypercube();
        final Hypercube<Integer> c = a.copy();

        // Now make sure the cube was correctly copied
        for (long i=0; i < a.getSize(); i++) {
            int va = a.getObjectAt(i);
            int vc = c.getObjectAt(i);
            assert(va == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Integer cube to a Boolean cube.
     */
    @Test
    public void testIntegerToBooleanHypercube()
    {
        final Hypercube<Integer>   a = createIntegerHypercube();
        final Hypercube<Boolean> c = BooleanHypercube.toBooleanHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            int   va = a.getObjectAt(i);
            boolean vc = c.getObjectAt(i);
            assert((boolean)(va != 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Integer cube to a Float cube.
     */
    @Test
    public void testIntegerToFloatHypercube()
    {
        final Hypercube<Integer>   a = createIntegerHypercube();
        final Hypercube<Float> c = FloatHypercube.toFloatHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            int   va = a.getObjectAt(i);
            float vc = c.getObjectAt(i);
            assert((float)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Integer cube to a Double cube.
     */
    @Test
    public void testIntegerToDoubleHypercube()
    {
        final Hypercube<Integer>   a = createIntegerHypercube();
        final Hypercube<Double> c = DoubleHypercube.toDoubleHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            int   va = a.getObjectAt(i);
            double vc = c.getObjectAt(i);
            assert((double)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Integer cube to a Long cube.
     */
    @Test
    public void testIntegerToLongHypercube()
    {
        final Hypercube<Integer>   a = createIntegerHypercube();
        final Hypercube<Long> c = LongHypercube.toLongHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            int   va = a.getObjectAt(i);
            long vc = c.getObjectAt(i);
            assert((long)(va) == vc);
        }
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

        final Hypercube<Long> q = CubeMath.divide  (a, b);
        final Hypercube<Long> r = CubeMath.mod     (a, b);
        final Hypercube<Long> m = CubeMath.multiply(q, b);
        final Hypercube<Long> t = CubeMath.add     (m, r);
        assertEquals(t, a);

        final Hypercube<Long> min = CubeMath.minimum(a, b);
        final Hypercube<Long> max = CubeMath.maximum(a, b);
        for (long i=0; i < a.getSize(); i++) {
            assert(min.getObjectAt(i) == Long.min(a.getAt(i), b.getAt(i)));
            assert(max.getObjectAt(i) == Long.max(a.getAt(i), b.getAt(i)));
        }
    }

    /**
     * Test numeric unary operations for long cubes.
     */
    @Test
    public void testLongNumericUnaryOps()
    {
        final LongHypercube a = createLongHypercube();
        final LongHypercube b = createLongHypercube();

        // Test negative()
        final Hypercube<Long> n  = CubeMath.negative(b);
        final Hypercube<Long> s1 = CubeMath.subtract(a, b);
        final Hypercube<Long> s2 = CubeMath.add     (a, n);
        assertEquals(s1, s2);

        // Test abs()
        final Hypercube<Long> abs = CubeMath.abs(n);
        assertEquals(abs, b);
    }

    /**
     * Test broadcasting operations for long cubes.
     */
    @Test
    public void testLongBroadcast()
    {
        final LongHypercube a = createLongHypercube();

        // Test broadcasting
        final Hypercube<Long> z = CubeMath.broadcast(DIMENSIONS, (long)0);
        final Hypercube<Long> m = CubeMath.multiply(a, z);

        // Check that all elements are zero.
        final Hypercube<Boolean> e = CubeMath.equal(m, (long)0);
        final Hypercube<Boolean> b = CubeMath.equal(e, true);
        assert(CubeMath.all0d(b));
    }

    /**
     * Test associative operations for long cubes.
     */
    @Test
    public void testLongAssociativeOps()
    {
        final LongHypercube a = createLongHypercube();

        // Initialize sum
        long       sum = 0;
        final long min = CubeMath.min0d(a);
        final long max = CubeMath.max0d(a);

        for (long i=0; i < a.getSize(); i++) {
            final long va = a.getObjectAt(i);
            sum += va;
            // Check the min and max operations
            assert(min <= va && va <= max);
        }

        // Check the sum operation
        assert(sum == CubeMath.sum0d(a));
    }

    /**
     * Test extract operation for long cubes.
     */
    @Test
    public void testLongExtract()
    {
        final BooleanHypercube c = createBooleanHypercube();
        final Hypercube<Long> a = createLongHypercube();
        final Hypercube<Long> e = CubeMath.extract(c, a);

        int j = 0;
        for (long i=0; i < a.getSize(); i++) {
            if (c.getObjectAt(i)) {
                // Make sure the element is present in the extract
                assert(j < e.getSize());
                assert(e.getObjectAt(j++).longValue() == a.getObjectAt(i).longValue());
            }
        }
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

        final Hypercube<Long> and = CubeMath.and(a, b);
        final Hypercube<Long> or  = CubeMath.or (a, b);
        final Hypercube<Long> xor = CubeMath.xor(a, b);
        final Hypercube<Long> not = CubeMath.not(a);

        for (long i=0; i < a.getSize(); i++) {
            assert(and.getObjectAt(i) == (a.getAt(i) & b.getAt(i)));
            assert(or .getObjectAt(i) == (a.getAt(i) | b.getAt(i)));
            assert(xor.getObjectAt(i) == (a.getAt(i) ^ b.getAt(i)));
            assert(not.getObjectAt(i) == (~a.getAt(i)));
        }
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

    // -------------------------------------------------------------------------

    /**
     * Test copying a Long cube.
     */
    @Test
    public void testLongHypercubeCopy()
    {
        final Hypercube<Long> a = createLongHypercube();
        final Hypercube<Long> c = a.copy();

        // Now make sure the cube was correctly copied
        for (long i=0; i < a.getSize(); i++) {
            long va = a.getObjectAt(i);
            long vc = c.getObjectAt(i);
            assert(va == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Long cube to a Boolean cube.
     */
    @Test
    public void testLongToBooleanHypercube()
    {
        final Hypercube<Long>   a = createLongHypercube();
        final Hypercube<Boolean> c = BooleanHypercube.toBooleanHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            long   va = a.getObjectAt(i);
            boolean vc = c.getObjectAt(i);
            assert((boolean)(va != 0) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Long cube to a Float cube.
     */
    @Test
    public void testLongToFloatHypercube()
    {
        final Hypercube<Long>   a = createLongHypercube();
        final Hypercube<Float> c = FloatHypercube.toFloatHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            long   va = a.getObjectAt(i);
            float vc = c.getObjectAt(i);
            assert((float)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Long cube to a Double cube.
     */
    @Test
    public void testLongToDoubleHypercube()
    {
        final Hypercube<Long>   a = createLongHypercube();
        final Hypercube<Double> c = DoubleHypercube.toDoubleHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            long   va = a.getObjectAt(i);
            double vc = c.getObjectAt(i);
            assert((double)(va) == vc);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Test casting a Long cube to a Integer cube.
     */
    @Test
    public void testLongToIntegerHypercube()
    {
        final Hypercube<Long>   a = createLongHypercube();
        final Hypercube<Integer> c = IntegerHypercube.toIntegerHypercube(a);

        // Now make sure the cube was correctly casted
        for (long i=0; i < a.getSize(); i++) {
            long   va = a.getObjectAt(i);
            int vc = c.getObjectAt(i);
            assert((int)(va) == vc);
        }
    }
}

// [[[end]]] (checksum: cb66599ebcc953dd0bdc22675037170d)
