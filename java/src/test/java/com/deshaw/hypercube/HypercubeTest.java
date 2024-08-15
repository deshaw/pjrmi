package com.deshaw.hypercube;

import java.io.File;
import java.io.IOException;

import java.lang.reflect.Array;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.deshaw.hypercube.Dimension.Accessor;
import com.deshaw.hypercube.Dimension.Roll;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The testing code for the {@code com.deshaw.hypercube} package.
 */
public class HypercubeTest
{
    /**
     * The shape of all cubes we create(). We make these primes so
     * that we have annoying divisors etc.; and FTR:
     *   3 * 7 * 5 == 105
     */
    private static final Dimension<?>[] DIMENSIONS = new Dimension<?>[] {
        new Dimension<>(new NaturalIndex("I", 3)),
        new Dimension<>(new NaturalIndex("J", 7)),
        new Dimension<>(new NaturalIndex("K", 5))
    };

    // ----------------------------------------------------------------------

    /**
     * Junit-like method.
     */
    private static <T> void assertArrayEquals(final T[] expected, final T[] value)
    {
        if (!Arrays.equals(expected, value)) {
            throw new AssertionError(
                "Expected " + Arrays.toString(expected) + " " +
                "but had "  + Arrays.toString(value)
            );
        }
    }

    /**
     * Junit-like method.
     */
    private static void assertArrayEquals(final double[] expected, final double[] value)
    {
        if (!Arrays.equals(expected, value)) {
            throw new AssertionError(
                "Expected " + Arrays.toString(expected) + " " +
                "but had "  + Arrays.toString(value)
            );
        }
    }

    /**
     * Junit-like method.
     */
    private static void assertArrayEquals(final float[] expected, final float[] value)
    {
        if (!Arrays.equals(expected, value)) {
            throw new AssertionError(
                "Expected " + Arrays.toString(expected) + " " +
                "but had "  + Arrays.toString(value)
            );
        }
    }

    /**
     * Junit-like method.
     */
    private static void assertArrayEquals(final long[] expected, final long[] value)
    {
        if (!Arrays.equals(expected, value)) {
            throw new AssertionError(
                "Expected " + Arrays.toString(expected) + " " +
                "but had "  + Arrays.toString(value)
            );
        }
    }

    /**
     * Junit-like method.
     */
    private static void assertArrayEquals(final int[] expected, final int[] value)
    {
        if (!Arrays.equals(expected, value)) {
            throw new AssertionError(
                "Expected " + Arrays.toString(expected) + " " +
                "but had "  + Arrays.toString(value)
            );
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Test the array-backed hypercube.
     */
    @Test
    public void testGenericArrayHypercube()
    {
        final Hypercube<String> cube = createGenericArrayHypercube();
        exercise(cube);
    }

    /**
     * Test flattening and unflattening.
     */
    @Test
    public void testArrayGenericFlattening()
    {
        doTestGenericFlattening(createGenericArrayHypercube());
    }

    /**
     * Test the map-backed hypercube.
     */
    @Test
    public void testGenericSparseHypercube()
    {
        final Hypercube<String> cube = createGenericSparseHypercube();
        exercise(cube);
    }

    /**
     * Test flattening and unflattening.
     */
    @Test
    public void testSparseGenericFlattening()
    {
        doTestGenericFlattening(createGenericSparseHypercube());
    }

    /**
     * Test the wrapping hypercube.
     */
    @Test
    public void testGenericWrappingHypercube()
    {
        final Hypercube<Long> cube = createGenericWrappingHypercube();
        exercise(cube);
    }

    /**
     * Test flattening and unflattening.
     */
    private void doTestGenericFlattening(final Hypercube<String> cube)
    {
        // Grab a copy of what it looks like
        final String repr = cube.toString();

        // Get it flattened, and then stuff those values back in
        final String[] flattened = new String[(int)cube.getSize()];
        cube.toFlattenedObjs(flattened);
        assertEquals(cube.getSize(), (long)flattened.length);

        // Make sure what got flattened out was what we expected
        assertEquals(3, cube.getNDim());
        for (int i=0, p=0; i < cube.length(0); i++) {
            for (int j=0; j < cube.length(1); j++) {
                for (int k=0; k < cube.length(2); k++) {
                    assertEquals(i + "," + j + "," + k, flattened[p++]);
                }
            }
        }

        // And try stuffing everything back in
        cube.fill(null);
        cube.fromFlattenedObjs(flattened);

        // Make sure it looks the same
        assertEquals(repr, cube.toString());

        // And that the flattened result is the same
        final String[] flattened2 = new String[(int)cube.getSize()];
        cube.toFlattenedObjs(flattened2);
        assertArrayEquals(flattened, flattened2);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Test the array-backed hypercube.
     */
    @Test
    public void testDoubleArrayHypercube()
    {
        final DoubleHypercube cube = createDoubleArrayHypercube();
        exercise(cube);

        final DoubleHypercube copy = createDoubleArrayHypercube();
        copy.clear();
        assertFalse(cube.contentEquals(copy));
        copy.copyFrom(cube);
        assertTrue(cube.contentEquals(copy));
    }

    /**
     * Test the array-wrapping hypercube.
     */
    @Test
    public void testDouble3dWrappingHypercube()
    {
        final DoubleHypercube cube = createDouble3dWrappingHypercube();
        exercise(cube);
    }

    /**
     * Test the mmap'd hypercube.
     */
    @Test
    public void testDoubleMappedHypercube()
        throws IOException
    {
        final File file = File.createTempFile("hypercube_test", null);
        try {
            final DoubleHypercube cube =
                createDoubleMappedHypercube(file.getCanonicalPath());
            exercise(cube);
        }
        finally {
            file.delete();
        }
    }

    /**
     * Test flattening and unflattening of an array-backed double cube.
     */
    @Test
    public void testDoubleArrayFlattening()
    {
        doTestDoubleFlattening(createDoubleArrayHypercube());
    }

    /**
     * Test the map-backed sparse hypercube.
     */
    @Test
    public void testDoubleSparseHypercube()
    {
        final DoubleHypercube cube = createDoubleSparseHypercube();
        exercise(cube);

        final DoubleHypercube copy = createDoubleSparseHypercube();
        copy.clear();
        assertFalse(cube.contentEquals(copy));
        copy.copyFrom(cube);
        assertTrue(cube.contentEquals(copy));
    }

    /**
     * Test flattening and unflattening.
     */
    @Test
    public void testDoubleSparseFlattening()
    {
        doTestDoubleFlattening(createDoubleSparseHypercube());
    }

    /**
     * Test flattening and unflattening of the given hypercube.
     */
    private void doTestDoubleFlattening(DoubleHypercube cube)
    {
        // Grab a copy of what it looks like
        final String repr = cube.toString();

        // Get it flattened, and then stuff those values back in
        final double[] flattened = new double[(int)cube.getSize()];
        cube.toFlattened(flattened);
        assertEquals(cube.getSize(), (long)flattened.length);

        // Make sure what got flattened out was what we expected
        assertEquals(3, cube.getNDim());
        for (int i=0, p=0; i < cube.length(0); i++) {
            for (int j=0; j < cube.length(1); j++) {
                for (int k=0; k < cube.length(2); k++) {
                    assertEquals(i * 1000.0 * 1000.0 +
                                 j          * 1000.0 +
                                 k,
                                 flattened[p++]);
                }
            }
        }

        // And try stuffing everything back in
        cube.fill(Double.NaN);
        cube.fromFlattened(flattened);

        // Make sure it looks the same
        assertEquals(repr, cube.toString());

        // And that the flattened result is the same
        final double[] flattened2 = new double[(int)cube.getSize()];
        cube.toFlattened(flattened2);
        assertArrayEquals(flattened, flattened2);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Test the array-backed hypercube.
     */
    @Test
    public void testFloatArrayHypercube()
    {
        final FloatHypercube cube = createFloatArrayHypercube();
        exercise(cube);

        final FloatHypercube copy = createFloatArrayHypercube();
        copy.clear();
        assertFalse(cube.contentEquals(copy));
        copy.copyFrom(cube);
        assertTrue(cube.contentEquals(copy));
    }

    /**
     * Test flattening and unflattening.
     */
    @Test
    public void testFloatFlattening()
    {
        // Create it
        final FloatHypercube cube = createFloatArrayHypercube();

        // Grab a copy of what it looks like
        final String repr = cube.toString();

        // Get it flattened, and then stuff those values back in
        final float[] flattened = new float[(int)cube.getSize()];
        cube.toFlattened(flattened);
        assertEquals(cube.getSize(), (long)flattened.length);

        // Make sure what got flattened out was what we expected
        assertEquals(3, cube.getNDim());
        for (int i=0, p=0; i < cube.length(0); i++) {
            for (int j=0; j < cube.length(1); j++) {
                for (int k=0; k < cube.length(2); k++) {
                    assertEquals(i * 1000.0f * 1000.0f +
                                 j           * 1000.0f +
                                 k,
                                 flattened[p++]);
                }
            }
        }

        // And try stuffing everything back in
        cube.fill(Float.NaN);
        cube.fromFlattened(flattened);

        // Make sure it looks the same
        assertEquals(repr, cube.toString());

        // And that the flattened result is the same
        final float[] flattened2 = new float[(int)cube.getSize()];
        cube.toFlattened(flattened2);
        assertArrayEquals(flattened, flattened2);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Test the array-backed hypercube.
     */
    @Test
    public void testLongArrayHypercube()
    {
        final LongHypercube cube = createLongArrayHypercube();
        exercise(cube);

        final LongHypercube copy = createLongArrayHypercube();
        copy.clear();
        assertFalse(cube.contentEquals(copy));
        copy.copyFrom(cube);
        assertTrue(cube.contentEquals(copy));
    }

    /**
     * Test flattening and unflattening.
     */
    @Test
    public void testLongFlattening()
    {
        // Create it
        final LongHypercube cube = createLongArrayHypercube();

        // Grab a copy of what it looks like
        final String repr = cube.toString();

        // Get it flattened, and then stuff those values back in
        final long[] flattened = new long[(int)cube.getSize()];
        cube.toFlattened(flattened);
        assertEquals(cube.getSize(), (long)flattened.length);

        // Make sure what got flattened out was what we expected
        assertEquals(3, cube.getNDim());
        for (int i=0, p=0; i < cube.length(0); i++) {
            for (int j=0; j < cube.length(1); j++) {
                for (int k=0; k < cube.length(2); k++) {
                    assertEquals(i * 1000L * 1000L +
                                 j         * 1000L +
                                 k,
                                 flattened[p++]);
                }
            }
        }

        // And try stuffing everything back in
        cube.fill(Long.MIN_VALUE);
        cube.fromFlattened(flattened);

        // Make sure it looks the same
        assertEquals(repr, cube.toString());

        // And that the flattened result is the same
        final long[] flattened2 = new long[(int)cube.getSize()];
        cube.toFlattened(flattened2);
        assertArrayEquals(flattened, flattened2);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Test cubes of varying dimensions.
     */
    @Test
    public void testDimensionality()
    {
        // Use a prime number for each dimension. This will blow up to a fairly
        // big number for the 9D cube (around 223 million) but we seem to be
        // able to handle it okay.
        final int[] primes = {2, 3, 5, 7, 11, 13, 17, 19, 23};

        // Now build a cube with these dimensions
        for (int i=1; i < primes.length; i++) {
            final Dimension<?>[] dimensions = new Dimension<?>[i];
            for (int j=0; j < dimensions.length; j++) {
                final int p = primes[j];
                dimensions[j] =
                    new Dimension<>(new NaturalIndex(Integer.toString(p), p));
            }

            // Now make the hypercube
            final Hypercube<Integer> cube =
                new GenericArrayHypercube<>(dimensions, Integer.class);

            // And populate it
            final Integer[] values = new Integer[(int)cube.getSize()];
            for (int j=0; j < values.length; j++) {
                values[j] = Integer.valueOf(j);
            }
            cube.fromFlattenedObjs(values);

            // Make sure it looks right when we flatten it again
            final Integer[] values2 = new Integer[(int)cube.getSize()];
            cube.toFlattenedObjs(values2);
            assertArrayEquals(values, values2);

            // Check to make sure it has what we expect. We loop
            // around until we overflow the lowest index.
            final long[] indices = new long[i];
            for (int index = 0; indices[0] < cube.length(0); index++) {
                // Compare
                assertEquals(Integer.valueOf(index), cube.getObj(indices));

                // And move on to the next one. We add to the Nth dimension and
                // then do a ripple-add. If we overflow in the 0th one then
                // we're done (this is tested for in the for's clause).
                indices[indices.length-1]++;
                for (int j = indices.length - 1; j > 0; j--) {
                    if (indices[j] >= cube.length(j)) {
                        indices[j] = 0;
                        indices[j-1]++;
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Populate a String hypercube.
     */
    private void populate(final Hypercube<String> cube)
    {
        assertEquals(String.class, cube.getElementType());
        assertEquals(3,            DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    cube.setObj(i + "," + j + "," + k, i, j, k);
                }
            }
        }
    }

    /**
     * Check that it matches what was put in with populate().
     */
    private void check(final Hypercube<String> cube)
    {
        assertEquals(String.class, cube.getElementType());
        assertEquals(3,            DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    assertEquals(i + "," + j + "," + k, cube.getObj(i, j, k));
                }
            }
        }
    }

    /**
     * Create a {@link String} {@link GenericArrayHypercube}.
     */
    private AbstractHypercube<String> createGenericArrayHypercube()
    {
        // Create
        final AbstractHypercube<String> cube =
            new GenericArrayHypercube<>(DIMENSIONS, String.class);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    /**
     * Create a {@link String} {@link GenericSparseHypercube}.
     */
    private AbstractHypercube<String> createGenericSparseHypercube()
    {
        // Create
        final AbstractHypercube<String> cube =
            new GenericSparseHypercube<>(DIMENSIONS, String.class);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Populate a double hypercube.
     */
    private void populate(final DoubleHypercube cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    cube.set(i * 1000.0 * 1000.0 +
                             j          * 1000.0 +
                             k,
                             i, j, k);
                }
            }
        }
    }

    /**
     * Check that it matches what was put in with populate().
     */
    private void check(final DoubleHypercube cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    assertEquals(i * 1000.0 * 1000.0 +
                                 j          * 1000.0 +
                                 k,
                                 cube.get(i, j, k));
                }
            }
        }
    }

    /**
     * Create a {@link DoubleArrayHypercube}.
     */
    private AbstractDoubleHypercube createDoubleArrayHypercube()
    {
        // Create
        final AbstractDoubleHypercube cube =
            new DoubleArrayHypercube(DIMENSIONS);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    /**
     * Create a {@link DoubleSparseHypercube}.
     */
    private AbstractDoubleHypercube createDoubleSparseHypercube()
    {
        // Create
        final AbstractDoubleHypercube cube =
            new DoubleSparseHypercube(DIMENSIONS);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    /**
     * Create a {@link Double3dWrappingHypercube}.
     */
    private DoubleHypercube createDouble3dWrappingHypercube()
    {
        // Create
        final double[][][] array = new double[(int)DIMENSIONS[0].length()][][];
        for (int i=0; i < array.length; i++) {
            array[i] = new double[(int)DIMENSIONS[1].length()][];
            for (int j=0; j < array[i].length; j++) {
                array[i][j] = new double[(int)DIMENSIONS[2].length()];
            }
        }
        final DoubleHypercube cube = DoubleHypercube.wrap(array);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    /**
     * Create a {@link DoubleMappedHypercube}.
     */
    private AbstractDoubleHypercube createDoubleMappedHypercube(final String path)
        throws IOException
    {
        // Create
        final AbstractDoubleHypercube cube =
            new DoubleMappedHypercube(path, DIMENSIONS);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Populate a double hypercube.
     */
    private void populate(final FloatHypercube cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    cube.set(i * 1000.0f * 1000.0f +
                             j           * 1000.0f +
                             k,
                             i, j, k);
                }
            }
        }
    }

    /**
     * Check that it matches what was put in with populate().
     */
    private void check(final FloatHypercube cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    assertEquals(i * 1000.0f * 1000.0f +
                                 j           * 1000.0f +
                                 k,
                                 cube.get(i, j, k));
                }
            }
        }
    }

    /**
     * Create a {@link FloatArrayHypercube}.
     */
    private AbstractFloatHypercube createFloatArrayHypercube()
    {
        // Create
        final AbstractFloatHypercube cube =
            new FloatArrayHypercube(DIMENSIONS);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Populate a long hypercube.
     */
    private void populate(final LongHypercube cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    cube.set(i * 1000L * 1000L +
                             j         * 1000L +
                             k,
                             i, j, k);
                }
            }
        }
    }

    /**
     * Check that it matches what was put in with populate().
     */
    private void check(final LongHypercube cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    assertEquals(i * 1000L * 1000L +
                                 j         * 1000L +
                                 k,
                                 cube.get(i, j, k));
                }
            }
        }
    }

    /**
     * Create a {@link LongArrayHypercube}.
     */
    private AbstractLongHypercube createLongArrayHypercube()
    {
        // Create
        final AbstractLongHypercube cube =
            new LongArrayHypercube(DIMENSIONS);

        // Populate and check
        populate(cube);
        check   (cube);

        // Give it back
        return cube;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Create a {@link GenericWrappingHypercube}.
     */
    private GenericWrappingHypercube<Long> createGenericWrappingHypercube()
    {
        // The source cube
        final AbstractLongHypercube src = createLongArrayHypercube();

        // What we will wrap
        final List<List<List<Long>>> l0 = new ArrayList<>();
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            final List<List<Long>> l1 = new ArrayList<>();
            l0.add(l1);
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                final List<Long> l2 = new ArrayList<>();
                l1.add(l2);
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    l2.add(src.get(i, j, k));
                }
            }
        }

        // Create
        final GenericWrappingHypercube<Long> cube =
            new GenericWrappingHypercube<>(DIMENSIONS, l0);

        // Check
        check(cube);

        // Give it back
        return cube;
    }

    /**
     * Check that it matches what was put in with populate().
     */
    private void check(final GenericWrappingHypercube<Long> cube)
    {
        assertEquals(3, DIMENSIONS.length);
        for (int i=0; i < DIMENSIONS[0].length(); i++) {
            for (int j=0; j < DIMENSIONS[1].length(); j++) {
                for (int k=0; k < DIMENSIONS[2].length(); k++) {
                    assertEquals(i * 1000L * 1000L +
                                 j         * 1000L +
                                 k,
                                 cube.getObj(i, j, k));
                }
            }
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Perform general tests on a 3D hypercube.
     */
    private <T> void exercise(final Hypercube<T> cube)
    {
        // Ensure it looks like we expect
        assertEquals(cube,              cube);
        assertEquals(DIMENSIONS.length, cube.getNDim());
        assertEquals(DIMENSIONS.length, cube.getShape().length);
        long size = 1;
        for (int i=0; i < DIMENSIONS.length; i++) {
            assertEquals(DIMENSIONS[i].length(), cube.getShape()[i]);
            size *= DIMENSIONS[i].length();
        }
        assertEquals(size, cube.getSize());

        // Check to make sure that the flattened version looks like we expect
        assertFlattening(cube);

        // Test slicing
        assertSlicing(cube);

        // Test flat-rolling
        assertFlatRolling(cube);

        // Test axis-rolling
        assertAxisRolling(cube);

        // Test reshaping
        assertReshaping(cube);
    }

    /**
     * Perform slicing tests on a 3D hypercube.
     */
    private <T> void assertSlicing(final Hypercube<T> cube)
    {
        // Abuse of indentation here, for "clarity"
        for (int i =0; i  <= cube.dim(0).length(); i ++) {
        for (int ii=0; ii <= i;                    ii++) {
        for (int j =0; j  <= cube.dim(1).length(); j ++) {
        for (int jj=0; jj <= j;                    jj++) {
        for (int k =0; k  <= cube.dim(2).length(); k ++) {
        for (int kk=0; kk <= k;                    kk++) {
            // Now look to create the accessors. If we have matching slice
            // values then use the coordinate instead (so that we test both).

            // Be careful not to pick an out-of-bounds coordinate though
            if (i == ii && i == cube.dim(0).length() ||
                j == jj && j == cube.dim(1).length() ||
                k == kk && k == cube.dim(2).length())
            {
                continue;
            }

            // Create the accessors
            final Accessor<?> a = (i==ii) ? cube.dim(0).at   (    i)
                                          : cube.dim(0).slice(ii, i);
            final Accessor<?> b = (j==jj) ? cube.dim(1).at   (    j)
                                          : cube.dim(1).slice(jj, j);
            final Accessor<?> c = (k==kk) ? cube.dim(2).at   (    k)
                                          : cube.dim(2).slice(kk, k);

            // Now slice it. This might throw if we have all coordinates, so we
            // check that it does so correctly
            final Hypercube<T> sliced;
            try {
                // Do the slice
                sliced = cube.slice(a, b, c);

                // Check for the lack of any expected exception
                if (i == ii && j == jj && k == kk) {
                    throw new AssertionError(
                        "Should have thrown for null slice"
                    );
                }

                // Check that omitting the rightmost Accessor(s) is equivalent
                // to using the full slice for those rightmost dimension(s). Use
                // the toObjectArray() method as a convenient way to test that
                // the accessors are being used properly.
                if (kk == 0 && k == cube.dim(2).length()) {
                    assertArrayEquals(sliced          .toObjectArray(),
                                      cube.slice(a, b).toObjectArray());
                    if (jj == 0 && j == cube.dim(1).length()) {
                        assertArrayEquals(sliced       .toObjectArray(),
                                          cube.slice(a).toObjectArray());
                        if (ii == 0 && i == cube.dim(0).length()) {
                            assertArrayEquals(sliced      .toObjectArray(),
                                              cube.slice().toObjectArray());
                        }
                    }
                }
            }
            catch (IllegalArgumentException e) {
                // See if this was expected
                if (i != ii || j != jj || k != kk) {
                    throw new AssertionError(
                        "Should not have thrown for non-null slice",
                        e
                    );
                }

                // Nothing more to do here
                continue;
            }

            // Test the dimensionality-preserving slicing
            for (int iii = ii; iii < i; iii++) {
                for (int jjj = jj; jjj < j; jjj++) {
                    for (int kkk = kk; kkk < k; kkk++) {
                        assertEquals(cube  .getObj(iii,    jjj,    kkk),
                                     sliced.getObj(iii-ii, jjj-jj, kkk-kk));
                    }
                }
            }

            // Test the slicing when we knock out the J dimension
            if (j == jj) {
                for (int iii = ii; iii < i; iii++) {
                    for (int kkk = kk; kkk < k; kkk++) {
                        assertEquals(cube  .getObj(iii,    j, kkk),
                                     sliced.getObj(iii-ii,    kkk-kk));
                    }
                }
            }

            // Test flattening
            assertFlattening(sliced);
        }}}}}}
    }

    /**
     * Perform flat-rolling tests on a 3D hypercube.
     */
    private <T> void assertFlatRolling(final Hypercube<T> cube)
    {
        for (int i=0; i <= cube.getSize(); i++) {
            final Hypercube<T> rolled = cube.rollFlat(i);

            // Test the rolling
            for (int ii=0; ii < cube.getSize(); ii++) {
                assertEquals(cube  .getObjectAt(ii),
                             rolled.getObjectAt((ii + i) % cube.getSize()));
            }

            // Test slicing
            for (int ii =0; ii  <= cube.dim(0).length(); ii ++) {
            for (int iii=0; iii <= ii;                   iii++) {

                // Be careful not to pick an out-of-bounds coordinate though
                if (ii == iii && ii == cube.dim(0).length())
                    continue;

                // For the sake of time, we only slice over 1 dimension. More
                // thorough testing of the slicing operation is done separately.
                // Create the accessor
                final Accessor<?> d = (ii==iii) ? cube.dim(0).at   (     ii)
                                                : cube.dim(0).slice(iii, ii);

                final Hypercube<T> sliced = rolled.slice(d, null, null);

                // Test the dimensionality-preserving slicing
                for (int iiii= iii; iiii < ii;                   iiii++) {
                for (int jj  =   0; jj   < cube.dim(1).length(); jj  ++) {
                for (int kk  =   0; kk   < cube.dim(2).length(); kk  ++) {
                    assertEquals(rolled.getObj(iiii,     jj, kk),
                                 sliced.getObj(iiii-iii, jj, kk));
                }}}

                // Test flattening
                assertFlattening(sliced);
            }}
        }
    }

    /**
     * Perform axis-rolling tests on a 3D hypercube.
     */
    private <T> void assertAxisRolling(final Hypercube<T> cube)
    {
        // Abuse of indentation here, for "clarity"
        for (int i=0; i <= cube.dim(0).length(); i++) {
        for (int j=0; j <= cube.dim(1).length(); j++) {
        for (int k=0; k <= cube.dim(2).length(); k++) {
            // Create the rolls
            final Roll<?> a = i != 0 ? cube.dim(0).roll(i) : null;
            final Roll<?> b = j != 0 ? cube.dim(1).roll(j) : null;
            final Roll<?> c = k != 0 ? cube.dim(2).roll(k) : null;

            final Hypercube<T> rolled        = cube.roll(a, b, c);
            final Hypercube<T> defaultRolled = CubeMath.roll(cube,
                                                             new int[]{i, j, k},
                                                             Map.of("axis", new int []{0, 1, 2}));

            // Check the default rolling method too
            // Once again, abuse the indentation here for "clarity".
            for (int ii=0; ii < cube.dim(0).length(); ii++) {
            for (int jj=0; jj < cube.dim(1).length(); jj++) {
            for (int kk=0; kk < cube.dim(2).length(); kk++) {
                assertEquals(rolled       .getObj(ii, jj, kk),
                             defaultRolled.getObj(ii, jj, kk));
            }}}

            // Test the rolling
            // Once again, abuse the indentation here for "clarity".
            for (int ii=0; ii < cube.dim(0).length(); ii++) {
            for (int jj=0; jj < cube.dim(1).length(); jj++) {
            for (int kk=0; kk < cube.dim(2).length(); kk++) {
                assertEquals(cube  .getObj(ii,
                                           jj,
                                           kk),
                             rolled.getObj((ii+i) % cube.dim(0).length(),
                                           (jj+j) % cube.dim(1).length(),
                                           (kk+k) % cube.dim(2).length()));
            }}}

            // Test slicing
            for (int ii =0; ii  <= cube.dim(0).length(); ii ++) {
            for (int iii=0; iii <= ii;                   iii++) {

                // Be careful not to pick an out-of-bounds coordinate though
                if (ii == iii && ii == cube.dim(0).length())
                    continue;

                // For the sake of time, we only slice over 1 dimension. More
                // thorough testing of the slicing operation is done separately.
                // Create the accessor
                final Accessor<?> d = (ii==iii) ? cube.dim(0).at   (     ii)
                                                : cube.dim(0).slice(iii, ii);

                final Hypercube<T> sliced = rolled.slice(d, null, null);

                // Test the dimensionality-preserving slicing
                for (int iiii= iii; iiii < ii;                   iiii++) {
                for (int jj  =   0; jj   < cube.dim(1).length(); jj  ++) {
                for (int kk  =   0; kk   < cube.dim(2).length(); kk  ++) {
                    assertEquals(rolled.getObj(iiii,     jj, kk),
                                 sliced.getObj(iiii-iii, jj, kk));
                }}}

                // Test flattening
                assertFlattening(sliced);
            }}

            // Test flattening
            assertFlattening(rolled);

            // Check that opposite rolls cancel out
            final Roll<?> x = i != 0 ? cube.dim(0).roll(-i) : null;
            final Roll<?> y = j != 0 ? cube.dim(1).roll(-j) : null;
            final Roll<?> z = k != 0 ? cube.dim(2).roll(-k) : null;

            final Hypercube<T> unrolled = rolled.roll(x, y, z);

            // Test the double rolling
            // Once again, abuse the indentation here for "clarity".
            for (int ii=0; ii < cube.dim(0).length(); ii++) {
            for (int jj=0; jj < cube.dim(1).length(); jj++) {
            for (int kk=0; kk < cube.dim(2).length(); kk++) {
                assertEquals(cube    .getObj(ii, jj, kk),
                             unrolled.getObj(ii, jj, kk));
            }}}
        }}}
    }

    /**
     * Test that the flattened version is as we expect.
     */
    private <T> void assertFlattening(final Hypercube<T> cube)
    {
        // Check to make sure that the flattened version looks like we expect
        @SuppressWarnings("unchecked")
        final T[] flattened =
            (T[])Array.newInstance(cube.getElementType(), (int)cube.getSize());
        cube.toFlattenedObjs(flattened);

        int    flatIndex = 0;
        long[] indices   = new long[cube.getNDim()];

        while (flatIndex < cube.getSize()) {
            // For each flat index, calculate the index for every non-flat
            // dimension, starting with the right-most.
            long shiftedIndex = flatIndex;
            for (int axis = cube.getNDim()-1; axis >= 0; axis--) {
                final long axisLength = cube.length(axis);
                final long axisIndex  = shiftedIndex % axisLength;
                shiftedIndex         /= axisLength;
                indices[axis]         = axisIndex;
            }

            assertEquals(cube.getObj(indices),     // "expected"
                         flattened[flatIndex++],   // "actual"
                         "Mismatched values when flattening HyperCube:" +
                         "\nShaped:\n"    + cube +
                         "\nFlattened:\n" + Arrays.toString(flattened) +
                         "\nWrapped:\n"   + (cube instanceof WrappingHypercube
                                            ? ((WrappingHypercube<T>)cube).base
                                            : "N/A"));
        }
    }

    /**
     * Perform reshaping tests on a 3D hypercube.
     */
    private <T> void assertReshaping(final Hypercube<T> cube)
    {
        // Rotate the dimensions
        final Dimension<?>[] dimensions = new Dimension<?>[] {
            DIMENSIONS[1],
            DIMENSIONS[2],
            DIMENSIONS[0]
        };
        final Hypercube<T> reshaped = cube.reshape(dimensions);

        // Now these should all be the same
        assertEquals(cube.getNDim(), reshaped.getNDim());
        assertEquals(cube.getSize(), reshaped.getSize());

        // And walking the cube should still work
        for (long i=0, size = cube.getSize(); i < size; i++) {
            assertEquals(cube    .getObjectAt(i),   // "expected"
                         reshaped.getObjectAt(i));  // "actual"
        }

        // Test flattening
        assertFlattening(reshaped);
    }
}
