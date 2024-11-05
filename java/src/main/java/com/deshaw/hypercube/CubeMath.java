package com.deshaw.hypercube;

// Recreate with `cog -rc CubeMath.java`
// [[[cog
//     import cog
//     import cube_math
//
//     cog.outl(cube_math.generate())
// ]]]
import com.deshaw.pjrmi.PJRmi.GenericReturnType;
import com.deshaw.pjrmi.PJRmi.Kwargs;
import com.deshaw.pjrmi.KwargUtil;
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

/**
 * Math operations on {@link Hypercube} instances.
 *
 * <p>This class provides a subset of Python's various {@code numpy} methods
 * as well as similar utility functions. Some methods also support a subset of
 * the corresponding kwargs. It is not current feature-complete with respect to
 * the {@code numpy} module and does not aim to better it in performance.
 */
public class CubeMath
{
    /**
     * The logger for all the Hypercube code.
     */
    private static final Logger LOG = Logger.getLogger("com.deshaw.hypercube");

    /**
     * An immutable single-valued boolean cube representation, that is
     * internally useful for cube-with-value operations.
     */
    private static class BooleanSingleValueHypercube
        extends AbstractBooleanHypercube
    {
        /**
         * This cube's single value.
         */
        private final boolean myValue;

        /**
         * Constructor.
         *
         * @param dimensions  The dimension of this cube.
         * @param v           The single value of this cube.
         */
        public BooleanSingleValueHypercube(final Dimension<?>[] dimensions, final boolean v)
        {
            super(dimensions);
            myValue = v;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toFlattened(final long srcPos,
                                final boolean[] dst,
                                final int  dstPos,
                                final int  length)
        {
            Arrays.fill(dst, dstPos, dstPos + length, myValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Boolean weakGetObjectAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetObjectAt(final long index, final Boolean v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean weakGetAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetAt(final long index, final boolean v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean get(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean weakGet(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final boolean v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSet(final boolean v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * An immutable single-valued int cube representation, that is
     * internally useful for cube-with-value operations.
     */
    private static class IntegerSingleValueHypercube
        extends AbstractIntegerHypercube
    {
        /**
         * This cube's single value.
         */
        private final int myValue;

        /**
         * Constructor.
         *
         * @param dimensions  The dimension of this cube.
         * @param v           The single value of this cube.
         */
        public IntegerSingleValueHypercube(final Dimension<?>[] dimensions, final int v)
        {
            super(dimensions);
            myValue = v;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toFlattened(final long srcPos,
                                final int[] dst,
                                final int  dstPos,
                                final int  length)
        {
            Arrays.fill(dst, dstPos, dstPos + length, myValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Integer weakGetObjectAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetObjectAt(final long index, final Integer v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int weakGetAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetAt(final long index, final int v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int get(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int weakGet(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final int v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSet(final int v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * An immutable single-valued long cube representation, that is
     * internally useful for cube-with-value operations.
     */
    private static class LongSingleValueHypercube
        extends AbstractLongHypercube
    {
        /**
         * This cube's single value.
         */
        private final long myValue;

        /**
         * Constructor.
         *
         * @param dimensions  The dimension of this cube.
         * @param v           The single value of this cube.
         */
        public LongSingleValueHypercube(final Dimension<?>[] dimensions, final long v)
        {
            super(dimensions);
            myValue = v;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toFlattened(final long srcPos,
                                final long[] dst,
                                final int  dstPos,
                                final int  length)
        {
            Arrays.fill(dst, dstPos, dstPos + length, myValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Long weakGetObjectAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetObjectAt(final long index, final Long v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long weakGetAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetAt(final long index, final long v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long get(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long weakGet(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final long v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSet(final long v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * An immutable single-valued float cube representation, that is
     * internally useful for cube-with-value operations.
     */
    private static class FloatSingleValueHypercube
        extends AbstractFloatHypercube
    {
        /**
         * This cube's single value.
         */
        private final float myValue;

        /**
         * Constructor.
         *
         * @param dimensions  The dimension of this cube.
         * @param v           The single value of this cube.
         */
        public FloatSingleValueHypercube(final Dimension<?>[] dimensions, final float v)
        {
            super(dimensions);
            myValue = v;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toFlattened(final long srcPos,
                                final float[] dst,
                                final int  dstPos,
                                final int  length)
        {
            Arrays.fill(dst, dstPos, dstPos + length, myValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Float weakGetObjectAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetObjectAt(final long index, final Float v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public float getAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public float weakGetAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetAt(final long index, final float v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public float get(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public float weakGet(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final float v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSet(final float v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * An immutable single-valued double cube representation, that is
     * internally useful for cube-with-value operations.
     */
    private static class DoubleSingleValueHypercube
        extends AbstractDoubleHypercube
    {
        /**
         * This cube's single value.
         */
        private final double myValue;

        /**
         * Constructor.
         *
         * @param dimensions  The dimension of this cube.
         * @param v           The single value of this cube.
         */
        public DoubleSingleValueHypercube(final Dimension<?>[] dimensions, final double v)
        {
            super(dimensions);
            myValue = v;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void toFlattened(final long srcPos,
                                final double[] dst,
                                final int  dstPos,
                                final int  length)
        {
            Arrays.fill(dst, dstPos, dstPos + length, myValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Double weakGetObjectAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetObjectAt(final long index, final Double v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double getAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double weakGetAt(final long index)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSetAt(final long index, final double v)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double get(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double weakGet(final long... indices)
        {
            return myValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final double v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void weakSet(final double v, final long... indices)
            throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * The number of elements to stage when processing operations in bulk.
     *
     * <p>This number does not have to be very large for us to get the benefit of
     * loop unrolling. This parameter can be set by passing the Java property
     * {@code com.deshaw.hypercube.cubemath.stagingSize}. Otherwise, a default
     * value is set to be fairly small.
     */
    private static final int STAGING_SIZE;

    /**
     * The cube size threshold at which CubeMath will use multithreading.
     *
     * <p>Multithreading has an overhead cost that is only worth bearing when processing
     * a large number of operations. This threshold can be set by passing the Java
     * property {@code com.deshaw.hypercube.cubemath.threadingThreshold}. Otherwise,
     * a default threshold value is set to be fairly large. By passing 0, CubeMath
     * will always use multithreaded processing, if possible.
     */
    private static final long THREADING_THRESHOLD;

    /**
     * The number of threads used for multithreaded processing.
     *
     * <p>The number of threads can be set by passing the Java property
     * {@code com.deshaw.hypercube.cubemath.numThreads}. Otherwise, the library
     * defaults to using a fairly small number of threads. By passing 0 or 1,
     * CubeMath will not use multithreaded processing.
     */
    private static final int NUM_THREADS;

    /**
     * The executor service used for multithreading.
     */
    private static final ExecutorService ourExecutorService;

    /**
     * The numerical 2-ary operations which we can do.
     */
    private enum BinaryOp
    {
        ADD, SUB, MUL, DIV, MOD, POW,
        MIN, MAX,
        AND, OR, XOR;
    }

    /**
     * The 1-ary operations which we can do.
     */
    private enum UnaryOp
    {
        NEG, ABS,
        FLOOR, ROUND, CEIL,
        COS, COSH, SIN, SINH, TAN, TANH,
        EXP, LOG, LOG10,
        NOT;
    }

    /**
     * The associative operations which we can do.
     */
    private enum AssociativeOp
    {
        ADD, MIN, MAX, NANADD;
    }

    /**
     * The boolean comparison operations which we can do.
     */
    private enum ComparisonOp
    {
        EQ, NE, LT, GT, LE, GE;
    }

    /**
     * The reductive logic operations which we can do.
     */
    private enum ReductiveLogicOp
    {
        ANY, ALL;
    }

    static {
        // Try setting CubeMath parameters through Java properties and raise an
        // exception if bad values are passed. In case no Java properties are
        // available for a parameter, a default value is used (refer to individual
        // parameter descriptions for more detail).
        /*scope*/ {
            final String propName = "com.deshaw.hypercube.cubemath.stagingSize";
            try {
                STAGING_SIZE = Integer.parseInt(System.getProperty(propName, "128"));
                if (STAGING_SIZE < 1) {
                    throw new RuntimeException("Bad value for " + propName + ": " +
                                               propName + " has to be a positive integer, " +
                                               "but " + STAGING_SIZE + " was given.");
                }
            }
            catch (NumberFormatException e) {
                throw new RuntimeException("Bad value for " + propName, e);
            }
        }
        /*scope*/ {
            final String propName = "com.deshaw.hypercube.cubemath.threadingThreshold";
            try {
                THREADING_THRESHOLD = Integer.parseInt(System.getProperty(propName, "131072"));
                if (THREADING_THRESHOLD < 0) {
                    throw new RuntimeException("Bad value for " + propName + ": " +
                                               propName + " has to be a non-negative integer, " +
                                               "but " + THREADING_THRESHOLD + " was given.");
                }
            }
            catch (NumberFormatException e) {
                throw new RuntimeException("Bad value for " + propName, e);
            }
        }
        /*scope*/ {
            final String propName = "com.deshaw.hypercube.cubemath.numThreads";
            try {
                NUM_THREADS = Integer.parseInt(System.getProperty(propName, "4"));
                if (NUM_THREADS < 0) {
                    throw new RuntimeException("Bad value for " + propName + ": " +
                                               propName + " has to be a non-negative integer, " +
                                               "but " + NUM_THREADS + " was given.");
                }
            }
            catch (NumberFormatException e) {
                throw new RuntimeException("Bad value for " + propName, e);
            }
        }

        // Only initialize an executor service if NUM_THREADS > 1.
        if (NUM_THREADS > 1) {
            // Initialize ourExecutorService according to the NUM_THREADS variable
            // and add a hook to shutdown all threads when the main thread terminates.
            ourExecutorService = Executors.newFixedThreadPool(NUM_THREADS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                ourExecutorService.shutdown();
                try {
                    ourExecutorService.awaitTermination(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    // No need to do anything since we are shutting down.
                }
            }));
        }
        else {
            ourExecutorService = null;
        }
    }

    /**
     * Utility class only, never created.
     */
    private CubeMath()
    {
        // Nothing
    }

    // -------------------------------------------------------------------------

    /**
     * Convert the given value to a byte, throwing if truncation occurs.
     */
    /*package*/ static byte strictByte(final long value)
        throws ClassCastException
    {
        final byte v = (byte)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a byte"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a short, throwing if truncation occurs.
     */
    /*package*/ static short strictShort(final long value)
        throws ClassCastException
    {
        final short v = (short)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a short"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a int, throwing if truncation occurs.
     */
    /*package*/ static int strictInteger(final long value)
        throws ClassCastException
    {
        final int v = (int)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a int"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a long, throwing if truncation occurs.
     */
    /*package*/ static long strictLong(final long value)
        throws ClassCastException
    {
        // NOP
        return value;
    }

    /**
     * Convert the given value to a byte, throwing if truncation occurs.
     */
    /*package*/ static byte strictByte(final double value)
        throws ClassCastException
    {
        final byte v = (byte)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a byte"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a short, throwing if truncation occurs.
     */
    /*package*/ static short strictShort(final double value)
        throws ClassCastException
    {
        final short v = (short)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a short"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a int, throwing if truncation occurs.
     */
    /*package*/ static int strictInteger(final double value)
        throws ClassCastException
    {
        final int v = (int)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a int"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a long, throwing if truncation occurs.
     */
    /*package*/ static long strictLong(final double value)
        throws ClassCastException
    {
        final long v = (long)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a long"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a byte, throwing if truncation occurs.
     */
    /*package*/ static byte strictByte(final Number value)
        throws ClassCastException
    {
        if (value == null) {
            throw new NullPointerException("Given a null value");
        }
        return ((value instanceof Double) || (value instanceof Float))
            ? strictByte(value.doubleValue()) :
              strictByte(value.longValue());
    }

    /**
     * Convert the given value to a short, throwing if truncation occurs.
     */
    /*package*/ static short strictShort(final Number value)
        throws ClassCastException
    {
        if (value == null) {
            throw new NullPointerException("Given a null value");
        }
        return ((value instanceof Double) || (value instanceof Float))
            ? strictShort(value.doubleValue()) :
              strictShort(value.longValue());
    }

    /**
     * Convert the given value to a int, throwing if truncation occurs.
     */
    /*package*/ static int strictInteger(final Number value)
        throws ClassCastException
    {
        if (value == null) {
            throw new NullPointerException("Given a null value");
        }
        return ((value instanceof Double) || (value instanceof Float))
            ? strictInteger(value.doubleValue()) :
              strictInteger(value.longValue());
    }

    /**
     * Convert the given value to a long, throwing if truncation occurs.
     */
    /*package*/ static long strictLong(final Number value)
        throws ClassCastException
    {
        if (value == null) {
            throw new NullPointerException("Given a null value");
        }
        return ((value instanceof Double) || (value instanceof Float))
            ? strictLong(value.doubleValue()) :
              strictLong(value.longValue());
    }

    /**
     * Convert the given value to a float, throwing if truncation occurs.
     */
    /*package*/ static float strictFloat(final long value)
        throws ClassCastException
    {
        final float v = (float)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a float"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a double, throwing if truncation occurs.
     */
    /*package*/ static double strictDouble(final long value)
        throws ClassCastException
    {
        final double v = (double)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a double"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a float, throwing if truncation occurs.
     */
    /*package*/ static float strictFloat(final double value)
        throws ClassCastException
    {
        final float v = (float)value;
        if (value != v) {
            throw new ClassCastException(
                "Cannot convert " + value + " to a float"
            );
        }
        return v;
    }

    /**
     * Convert the given value to a double, throwing if truncation occurs.
     */
    /*package*/ static double strictDouble(final double value)
        throws ClassCastException
    {
        // NOP
        return value;
    }

    /**
     * Convert the given value to a float, throwing if truncation occurs.
     */
    /*package*/ static float strictFloat(final Number value)
        throws ClassCastException
    {
        if (value == null) {
            throw new NullPointerException("Given a null value");
        }
        return ((value instanceof Double) || (value instanceof Float))
            ? strictFloat(value.doubleValue()) :
              strictFloat(value.longValue());
    }

    /**
     * Convert the given value to a double, throwing if truncation occurs.
     */
    /*package*/ static double strictDouble(final Number value)
        throws ClassCastException
    {
        if (value == null) {
            throw new NullPointerException("Given a null value");
        }
        return ((value instanceof Double) || (value instanceof Float))
            ? strictDouble(value.doubleValue()) :
              strictDouble(value.longValue());
    }

    // -------------------------------------------------------------------------

    /**
     * Return an empty cube of the given dimensions and type.
     *
     * @param shape   The shape of the cube to return.
     * @param kwargs  <ul>
     *                  <li>{@code dtype='float'} -- The type for the cube.</li>
     *                </ul>
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     */
    @Kwargs(value="dtype")
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static Hypercube<?> empty(final Object             shape,
                                     final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        if (shape == null) {
            throw new NullPointerException("Given a null shape");
        }
        final Dimension<?>[] dims;
        if (shape instanceof Dimension[]) {
            dims = (Dimension<?>[])shape;
        }
        else {
            dims = Dimension.of(KwargUtil.toLongArray(shape));
        }

        final Object dtype = (kwargs == null) ? "float64" : kwargs.get("dtype");
        final DType.Type type = KwargUtil.toDTypeType(dtype);
        final Class<?> klass = type.objectClass;

        // See if we can do it, leveraging our already existing casting methods
        if (klass.equals(Double.class)) {
            return new DoubleArrayHypercube(dims);
        }
        else if (klass.equals(Float.class)) {
            return new FloatArrayHypercube(dims);
        }
        else if (klass.equals(Integer.class)) {
            return new IntegerArrayHypercube(dims);
        }
        else if (klass.equals(Long.class)) {
            return new LongArrayHypercube(dims);
        }
        else if (klass.equals(Boolean.class)) {
            return new BooleanBitSetHypercube(dims);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " + dtype
            );
        }
    }

    /**
     * Return an empty cube of the given dimensions and type full of zeroes.
     *
     * @param shape   The shape of the cube to return.
     * @param kwargs  <ul>
     *                  <li>{@code dtype='float'} -- The type for the cube.</li>
     *                </ul>
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     */
    @Kwargs(value="dtype")
    @GenericReturnType
    public static Hypercube<?> zeros(final Object             shape,
                                     final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        if (shape == null) {
            throw new NullPointerException("Given a null shape");
        }
        final Dimension<?>[] dims;
        if (shape instanceof Dimension[]) {
            dims = (Dimension<?>[])shape;
        }
        else {
            dims = Dimension.of(KwargUtil.toLongArray(shape));
        }

        final Object dtype = (kwargs == null) ? "float64" : kwargs.get("dtype");
        final DType.Type type = KwargUtil.toDTypeType(dtype);
        final Class<?> klass = type.objectClass;

        // See if we can do it, leveraging our already existing casting methods
        if (klass.equals(Double.class)) {
            return full(dims, (double)0);
        }
        else if (klass.equals(Float.class)) {
            return full(dims, (float)0);
        }
        else if (klass.equals(Integer.class)) {
            return full(dims, (int)0);
        }
        else if (klass.equals(Long.class)) {
            return full(dims, (long)0);
        }
        else if (klass.equals(Boolean.class)) {
            return full(dims, false);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " + dtype
            );
        }
    }

    /**
     * Return an empty cube of the given dimensions and type full of ones.
     *
     * @param shape   The shape of the cube to return.
     * @param kwargs  <ul>
     *                  <li>{@code dtype='float'} -- The type for the cube.</li>
     *                </ul>
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     */
    @Kwargs(value="dtype")
    @GenericReturnType
    public static Hypercube<?> ones(final Object             shape,
                                    final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        if (shape == null) {
            throw new NullPointerException("Given a null shape");
        }
        final Dimension<?>[] dims;
        if (shape instanceof Dimension[]) {
            dims = (Dimension<?>[])shape;
        }
        else {
            dims = Dimension.of(KwargUtil.toLongArray(shape));
        }

        final Object dtype = (kwargs == null) ? "float64" : kwargs.get("dtype");
        final DType.Type type = KwargUtil.toDTypeType(dtype);
        final Class<?> klass = type.objectClass;

        // See if we can do it, leveraging our already existing casting methods
        if (klass.equals(Double.class)) {
            return full(dims, (double)1);
        }
        else if (klass.equals(Float.class)) {
            return full(dims, (float)1);
        }
        else if (klass.equals(Integer.class)) {
            return full(dims, (int)1);
        }
        else if (klass.equals(Long.class)) {
            return full(dims, (long)1);
        }
        else if (klass.equals(Boolean.class)) {
            return full(dims, true);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " + dtype
            );
        }
    }

    /**
     * Return a new copy of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     */
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> copy(final Hypercube<T> a)
        throws NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // See if we can do it, leveraging our already existing casting methods
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)toDoubleHypercube((Hypercube<Double>)a);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)toFloatHypercube((Hypercube<Float>)a);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)toIntegerHypercube((Hypercube<Integer>)a);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)toLongHypercube((Hypercube<Long>)a);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)toBooleanHypercube((Hypercube<Boolean>)a);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Copy a given cube, putting the result into a second.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     */
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> copy(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleToDoubleHypercube((Hypercube<Double>)a,
                                                         (Hypercube<Double>)r);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatToFloatHypercube((Hypercube<Float>)a,
                                                       (Hypercube<Float>)r);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intToIntegerHypercube((Hypercube<Integer>)a,
                                                       (Hypercube<Integer>)r);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longToLongHypercube((Hypercube<Long>)a,
                                                     (Hypercube<Long>)r);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)booleanToBooleanHypercube((Hypercube<Boolean>)a,
                                                           (Hypercube<Boolean>)r);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Roll a hypercube's elements along a given axis.
     *
     * @param shift   The (signed) number of elements to roll this cube by or,
     *                if an array, in each axis.
     * @param kwargs  <ul>
     *                  <li>{@code axis=0} -- The axis to roll or, if {@code shift}
     *                      is an array, a matching array of axes.</li>
     *                </ul>
     *
     * <p>This follows {@code numpy.roll()}'s semantics.
     *
     * @return The rolled cube.
     *
     * @throws IllegalArgumentException If the shift was not valid.
     * @throws NullPointerException     If the given cube was {@code null}.
     */
    @Kwargs(value="axis")
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> roll(final Hypercube<T> a,
                                        final Object shift,
                                        final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException
    {
        if (shift instanceof Number) {
            return a.roll(((Number)shift).longValue(), kwargs);
        }
        else if (shift instanceof byte[]) {
            final byte[] shifts = (byte[])shift;
            final long[] longs  = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {
                longs[i] = shifts[i];
            }
            return a.roll(longs, kwargs);
        }
        else if (shift instanceof short[]) {
            final short[] shifts = (short[])shift;
            final long[] longs   = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {
                longs[i] = shifts[i];
            }
            return a.roll(longs, kwargs);
        }
        else if (shift instanceof int[]) {
            final int[]  shifts = (int[])shift;
            final long[] longs  = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {
                longs[i] = shifts[i];
            }
            return a.roll(longs, kwargs);
        }
        else if (shift instanceof long[]) {
            return a.roll((long[])shift, kwargs);
        }
        else if (shift instanceof Object[] &&
                 ((Object[])shift).length > 0 &&
                 ((Object[])shift)[0] instanceof Number)
        {
            final Object[] shifts = (Object[])shift;
            final long[]   longs  = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {
                longs[i] = ((Number)(shifts[i])).longValue();
            }
            return a.roll(longs, kwargs);
        }
        else {
            throw new IllegalArgumentException("Unhandle shift type: " + shift);
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Whether two cubes are equal or not.
     *
     * @return The resulting count.
     *
     * @throws NullPointerException If the given cube was {@code null}.
     */
    public static boolean arrayEquals(final Hypercube<?> a, final Hypercube<?> b)
    {
        return (a != null && a.equals(b));
    }

    /**
     * Count the number of {@code true} (populated) values in a boolean hypercube.
     *
     * @return The resulting count.
     *
     * @throws NullPointerException If the given cube was {@code null}.
     */
    public static long popcount(final Hypercube<Boolean> a)
        throws NullPointerException
    {
        return popcount(a, 0L, a.getSize());
    }

    // -------------------------------------------------------------------------

    /**
     * Pairwise-add two cubes together.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.ADD);
    }

    /**
     * Pairwise-add two cubes together, putting the result into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final Hypercube<T> a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.ADD);
    }

    /**
     * Element-wise add a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.ADD);
    }

    /**
     * Element-wise add a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final Hypercube<T> a,
                                       final T            b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.ADD);
    }

    /**
     * Element-wise add a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.ADD);
    }

    /**
     * Element-wise add a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final T            a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.ADD);
    }

    /**
     * Pairwise-subtract one cube from another.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final Hypercube<T> a,
                                            final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.SUB);
    }

    /**
     * Pairwise-subtract one cube from another, putting the result into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final Hypercube<T> a,
                                            final Hypercube<T> b,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.SUB);
    }

    /**
     * Element-wise subtract a value from a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final Hypercube<T> a,
                                            final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.SUB);
    }

    /**
     * Element-wise subtract a value from a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final Hypercube<T> a,
                                            final T            b,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.SUB);
    }

    /**
     * Element-wise subtract a cube from a value.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final T            a,
                                            final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.SUB);
    }

    /**
     * Element-wise subtract a cube from a value, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final T            a,
                                            final Hypercube<T> b,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.SUB);
    }

    /**
     * Pairwise-multiply two cubes together.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final Hypercube<T> a,
                                            final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.MUL);
    }

    /**
     * Pairwise-multiply two cubes together, putting the result into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final Hypercube<T> a,
                                            final Hypercube<T> b,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.MUL);
    }

    /**
     * Element-wise multiply a value and a cube together.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final Hypercube<T> a,
                                            final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MUL);
    }

    /**
     * Element-wise multiply a value and a cube together, putting the result
     * into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final Hypercube<T> a,
                                            final T            b,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MUL);
    }

    /**
     * Element-wise multiply a value and a cube together.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final T            a,
                                            final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MUL);
    }

    /**
     * Element-wise multiply a value and a cube together, putting the result
     * into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final T            a,
                                            final Hypercube<T> b,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MUL);
    }

    /**
     * Pairwise-divide one cube by another.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final Hypercube<T> a,
                                          final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.DIV);
    }

    /**
     * Pairwise-divide one cube by another, putting the result into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final Hypercube<T> a,
                                          final Hypercube<T> b,
                                          final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.DIV);
    }

    /**
     * Element-wise divide a cube by a value.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final Hypercube<T> a,
                                          final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.DIV);
    }

    /**
     * Element-wise divide a cube by a value, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final Hypercube<T> a,
                                          final T            b,
                                          final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.DIV);
    }

    /**
     * Element-wise divide a value by a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final T            a,
                                          final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.DIV);
    }

    /**
     * Element-wise divide a value by a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final T            a,
                                          final Hypercube<T> b,
                                          final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.DIV);
    }

    /**
     * Pairwise-modulo one cube by another.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.MOD);
    }

    /**
     * Pairwise-modulo one cube by another, putting the result into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final Hypercube<T> a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.MOD);
    }

    /**
     * Element-wise module a cube by a value.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MOD);
    }

    /**
     * Element-wise module a cube by a value, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final Hypercube<T> a,
                                       final T            b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MOD);
    }

    /**
     * Element-wise module a value by a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MOD);
    }

    /**
     * Element-wise module a value by a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final T            a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MOD);
    }

    /**
     * Perform the pairwise power operation on the elements of two cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final Hypercube<T> a,
                                         final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.POW);
    }

    /**
     * Perform the pairwise power operation on the elements of two cubes,
     * putting the result into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final Hypercube<T> a,
                                         final Hypercube<T> b,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.POW);
    }

    /**
     * Perform the element-wise power operation on a cube and a value.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final Hypercube<T> a,
                                         final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.POW);
    }

    /**
     * Perform the element-wise power operation on a cube and a value, putting
     * the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final Hypercube<T> a,
                                         final T            b,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.POW);
    }

    /**
     * Perform the element-wise power operation on a value and a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final T            a,
                                         final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.POW);
    }

    /**
     * Perform the element-wise power operation on a value and a cube, putting
     * the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final T            a,
                                         final Hypercube<T> b,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.POW);
    }

    /**
     * Pairwise compare two cubes and return a new cube containing the element-wise
     * minima.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final Hypercube<T> a,
                                           final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.MIN);
    }

    /**
     * Pairwise compare two cubes and put the result of the element-wise minima
     * into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final Hypercube<T> a,
                                           final Hypercube<T> b,
                                           final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.MIN);
    }

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise minima.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final Hypercube<T> a,
                                           final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MIN);
    }

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise minima into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final Hypercube<T> a,
                                           final T            b,
                                           final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MIN);
    }

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise minima.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final T            a,
                                           final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MIN);
    }

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise minima into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final T            a,
                                           final Hypercube<T> b,
                                           final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MIN);
    }

    /**
     * Pairwise compare two cubes and return a new cube containing the element-wise
     * maxima.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final Hypercube<T> a,
                                           final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.MAX);
    }

    /**
     * Pairwise compare two cubes and put the result of the element-wise maxima
     * into a third.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final Hypercube<T> a,
                                           final Hypercube<T> b,
                                           final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.MAX);
    }

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise maxima.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final Hypercube<T> a,
                                           final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MAX);
    }

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise maxima into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final Hypercube<T> a,
                                           final T            b,
                                           final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MAX);
    }

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise maxima.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final T            a,
                                           final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MAX);
    }

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise maxima into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final T            a,
                                           final Hypercube<T> b,
                                           final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MAX);
    }

    // -------------------------------------------------------------------------

    /**
     * Negate all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> negative(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.NEG);
    }

    /**
     * Negate all elements of a given cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> negative(final Hypercube<T> a,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.NEG);
    }

    /**
     * Apply an absolute-value operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> abs(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.ABS);
    }

    /**
     * Apply an absolute-value operation on all elements of a given cube,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> abs(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.ABS);
    }

    /**
     * Apply a floor operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> floor(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.FLOOR);
    }

    /**
     * Apply a floor operation on all elements of a given cube, putting the
     * result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> floor(final Hypercube<T> a,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.FLOOR);
    }

    /**
     * Round all the elements of a given cube to the nearest whole number.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> round(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.ROUND);
    }

    /**
     * Round all the elements of a given cube to the nearest whole number,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> round(final Hypercube<T> a,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.ROUND);
    }

    /**
     * Apply a ceil operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> ceil(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.CEIL);
    }

    /**
     * Apply a ceil operation on all elements of a given cube, putting the
     * result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> ceil(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.CEIL);
    }

    /**
     * Apply a cosine operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cos(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.COS);
    }

    /**
     * Apply a cosine operation on all elements of a given cube, putting the
     * result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cos(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.COS);
    }

    /**
     * Apply a sine operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sin(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.SIN);
    }

    /**
     * Apply a sine operation on all elements of a given cube, putting the result
     * into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sin(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.SIN);
    }

    /**
     * Apply a tangent operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tan(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.TAN);
    }

    /**
     * Apply a tangent operation on all elements of a given cube, putting the
     * result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tan(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.TAN);
    }

    /**
     * Apply an exponential operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> exp(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.EXP);
    }

    /**
     * Apply an exponential operation on all elements of a given cube, putting
     * the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> exp(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.EXP);
    }

    /**
     * Apply a natural logarithm operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.LOG);
    }

    /**
     * Apply a natural logarithm operation on all elements of a given cube,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.LOG);
    }

    /**
     * Apply a logarithm operation in base 10 on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log10(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.LOG10);
    }

    /**
     * Apply a logarithm operation in base 10 on all elements of a given cube,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log10(final Hypercube<T> a,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.LOG10);
    }

    /**
     * Apply a hyperbolic cosine operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cosh(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.COSH);
    }

    /**
     * Apply a hyperbolic cosine operation on all elements of a given cube,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cosh(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.COSH);
    }

    /**
     * Apply a hyperbolic sine operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sinh(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.SINH);
    }

    /**
     * Apply a hyperbolic sine operation on all elements of a given cube,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sinh(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.SINH);
    }

    /**
     * Apply a hyperbolic tangent operation on all elements of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tanh(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.TANH);
    }

    /**
     * Apply a hyperbolic tangent operation on all elements of a given cube,
     * putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tanh(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.TANH);
    }

    // -------------------------------------------------------------------------

    /**
     * Sum all the values of a cube.
     *
     * @return The resulting sum, which will be {@code NaN} if any of the values
     *         were {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T sum0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, null, null, AssociativeOp.ADD);
    }

    /**
     * Sum all the values of a cube.
     *
     * @return The resulting sum, which will be {@code NaN} if any of the values
     *         were {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T sum0d(final Hypercube<T> a,
                              final T initial,
                              final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, initial, where, AssociativeOp.ADD);
    }

    /**
     * Returns the minimum value of a cube.
     *
     * @return The resulting minimum, which will be {@code NaN} if any of the
     *         values were {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T min0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, null, null, AssociativeOp.MIN);
    }

    /**
     * Returns the minimum value of a cube.
     *
     * @return The resulting minimum, which will be {@code NaN} if any of the
     *         values were {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T min0d(final Hypercube<T> a,
                              final T initial,
                              final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, initial, where, AssociativeOp.MIN);
    }

    /**
     * Returns the maximum value of a cube.
     *
     * @return The resulting maximum, which will be {@code NaN} if any of the
     *         values were {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T max0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, null, null, AssociativeOp.MAX);
    }

    /**
     * Returns the maximum value of a cube.
     *
     * @return The resulting maximum, which will be {@code NaN} if any of the
     *         values were {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T max0d(final Hypercube<T> a,
                              final T initial,
                              final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, initial, where, AssociativeOp.MAX);
    }

    /**
     * Sum all the values of a cube, ignoring floating point {@code NaN} values
     * where applicable.
     *
     * @return The resulting sum, which will be zero if all the values were
     *         {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T nansum0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, null, null, AssociativeOp.NANADD);
    }

    /**
     * Sum all the values of a cube, ignoring floating point {@code NaN} values
     * where applicable.
     *
     * @return The resulting sum, which will be zero if all the values were
     *         {@code NaN}s.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T nansum0d(final Hypercube<T> a,
                                 final T initial,
                                 final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOp(a, initial, where, AssociativeOp.NANADD);
    }

    /**
     * Sum all the values of a cube, along the given axes.
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sumNd(final Hypercube<T> a,
                                         final int[] axes,
                                         final T initial,
                                         final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.ADD);
    }

    /**
     * Returns the minimum value of a cube, along the given axes.
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minNd(final Hypercube<T> a,
                                         final int[] axes,
                                         final T initial,
                                         final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.MIN);
    }

    /**
     * Returns the maximum value of a cube, along the given axes.
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maxNd(final Hypercube<T> a,
                                         final int[] axes,
                                         final T initial,
                                         final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.MAX);
    }

    /**
     * Sum all the values of a cube, ignoring floating point {@code NaN} values
     * where applicable, along the given axes.
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> nansumNd(final Hypercube<T> a,
                                            final int[] axes,
                                            final T initial,
                                            final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.NANADD);
    }

    /**
     * Sum all the values of a cube, potentially along the given axes.
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,dtype,out,keepdims,initial,where")
    @GenericReturnType
    @SuppressWarnings("unchecked")
    public static <T> Object sum(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "initial":
                    initial = KwargUtil.toClassValue(entry.getValue(),
                                                     a.getElementType());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }
            }
        }
        if (axes == null) {
            return associativeOp(a, initial, where, AssociativeOp.ADD);
        }
        else {
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.ADD);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }
    }

    /**
     * Returns the minimum value of a cube, potentially along the given axes.
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,dtype,out,keepdims,initial,where")
    @GenericReturnType
    @SuppressWarnings("unchecked")
    public static <T> Object min(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "initial":
                    initial = KwargUtil.toClassValue(entry.getValue(),
                                                     a.getElementType());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }
            }
        }
        if (axes == null) {
            return associativeOp(a, initial, where, AssociativeOp.MIN);
        }
        else {
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.MIN);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }
    }

    /**
     * Returns the maximum value of a cube, potentially along the given axes.
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,dtype,out,keepdims,initial,where")
    @GenericReturnType
    @SuppressWarnings("unchecked")
    public static <T> Object max(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "initial":
                    initial = KwargUtil.toClassValue(entry.getValue(),
                                                     a.getElementType());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }
            }
        }
        if (axes == null) {
            return associativeOp(a, initial, where, AssociativeOp.MAX);
        }
        else {
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.MAX);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }
    }

    /**
     * Sum all the values of a cube, ignoring floating point {@code NaN} values
     * where applicable, potentially along the given axes.
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,dtype,out,keepdims,initial,where")
    @GenericReturnType
    @SuppressWarnings("unchecked")
    public static <T> Object nansum(final Hypercube<T> a,
                                    final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "initial":
                    initial = KwargUtil.toClassValue(entry.getValue(),
                                                     a.getElementType());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }
            }
        }
        if (axes == null) {
            return associativeOp(a, initial, where, AssociativeOp.NANADD);
        }
        else {
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.NANADD);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }
    }

    /**
     * Handle a vector dot product operation, resulting in a scalar.
     */
    @SuppressWarnings("unchecked")
    public static <T> T dotprod(final Hypercube<T> a, final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (T)Double.valueOf(
                doubleDotProd((Hypercube<Double>)a, (Hypercube<Double>)b)
            );
        }
        else if (a.getElementType().equals(Float.class)) {
            return (T)Float.valueOf(
                floatDotProd((Hypercube<Float>)a, (Hypercube<Float>)b)
            );
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (T)Integer.valueOf(
                intDotProd((Hypercube<Integer>)a, (Hypercube<Integer>)b)
            );
        }
        else if (a.getElementType().equals(Long.class)) {
            return (T)Long.valueOf(
                longDotProd((Hypercube<Long>)a, (Hypercube<Long>)b)
            );
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to matrix multiply cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Handle a matrix multiply.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<T> matmul(final Hypercube<T> a,
                                          final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleMatMul((Hypercube<Double>)a,
                                              (Hypercube<Double>)b);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatMatMul((Hypercube<Float>)a,
                                             (Hypercube<Float>)b);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intMatMul((Hypercube<Integer>)a,
                                           (Hypercube<Integer>)b);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longMatMul((Hypercube<Long>)a,
                                            (Hypercube<Long>)b);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to matrix multiply cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Perform the pairwise logic or bitwise AND operation on the elements of two cubes.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.AND);
    }

    /**
     * Perform the pairwise logic or bitwise AND operation on the elements of two cubes,
     * putting the result into a third.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final Hypercube<T> a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.AND);
    }

    /**
     * Element-wise AND a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.AND);
    }

    /**
     * Element-wise AND a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.AND);
    }

    /**
     * Element-wise AND a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final Hypercube<T> a,
                                       final T            b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.AND);
    }

    /**
     * Element-wise AND a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final T            a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.AND);
    }

    /**
     * Perform the pairwise logic or bitwise OR operation on the elements of two cubes.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final Hypercube<T> a,
                                      final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.OR);
    }

    /**
     * Perform the pairwise logic or bitwise OR operation on the elements of two cubes,
     * putting the result into a third.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final Hypercube<T> a,
                                      final Hypercube<T> b,
                                      final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.OR);
    }

    /**
     * Element-wise OR a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final Hypercube<T> a,
                                      final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.OR);
    }

    /**
     * Element-wise OR a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final T            a,
                                      final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.OR);
    }

    /**
     * Element-wise or a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final Hypercube<T> a,
                                      final T            b,
                                      final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.OR);
    }

    /**
     * Element-wise or a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final T            a,
                                      final Hypercube<T> b,
                                      final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.OR);
    }

    /**
     * Perform the pairwise logic or bitwise XOR operation on the elements of two cubes.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, null, BinaryOp.XOR);
    }

    /**
     * Perform the pairwise logic or bitwise XOR operation on the elements of two cubes,
     * putting the result into a third.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final Hypercube<T> a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, b, r, null, BinaryOp.XOR);
    }

    /**
     * Element-wise xor a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.XOR);
    }

    /**
     * Element-wise xor a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.XOR);
    }

    /**
     * Element-wise xor a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final Hypercube<T> a,
                                       final T            b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.XOR);
    }

    /**
     * Element-wise xor a value to a cube, putting the result into a second cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final T            a,
                                       final Hypercube<T> b,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.XOR);
    }

    // -------------------------------------------------------------------------

    /**
     * Perform the element-wise logic or bitwise NOT operation on the elements of a cube.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> not(final Hypercube<T> a)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, UnaryOp.NOT);
    }

    /**
     * Perform the element-wise logic or bitwise NOT operation on the elements of
     * a cube, putting the result into a second cube.
     *
     * <p>CubeMath will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> not(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return unaryOp(a, r, UnaryOp.NOT);
    }

    // -------------------------------------------------------------------------

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of an element-wise equality operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T> a,
                                               final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, ComparisonOp.EQ);
    }

    /**
     * Pairwise compare two cubes and put the result of an element-wise
     * equality operation into a third (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T>       a,
                                               final Hypercube<T>       b,
                                               final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, r, ComparisonOp.EQ);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise equality operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T> a,
                                               final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.EQ);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * equality operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T>       a,
                                               final T                  b,
                                               final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.EQ);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise equality operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> equal(final T            a,
                                               final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.EQ);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * equality operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final T                  a,
                                               final Hypercube<T>       b,
                                               final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.EQ);
    }

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of an element-wise inequality operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T> a,
                                                  final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, ComparisonOp.NE);
    }

    /**
     * Pairwise compare two cubes and put the result of an element-wise
     * inequality operation into a third (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T>       a,
                                                  final Hypercube<T>       b,
                                                  final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, r, ComparisonOp.NE);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise inequality operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T> a,
                                                  final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.NE);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * inequality operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T>       a,
                                                  final T                  b,
                                                  final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.NE);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise inequality operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> notEqual(final T            a,
                                                  final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.NE);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * inequality operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final T                  a,
                                                  final Hypercube<T>       b,
                                                  final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.NE);
    }

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of an element-wise less-than operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T> a,
                                              final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, ComparisonOp.LT);
    }

    /**
     * Pairwise compare two cubes and put the result of an element-wise
     * less-than operation into a third (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T>       a,
                                              final Hypercube<T>       b,
                                              final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, r, ComparisonOp.LT);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T> a,
                                              final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.LT);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T>       a,
                                              final T                  b,
                                              final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.LT);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> less(final T            a,
                                              final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.LT);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final T                  a,
                                              final Hypercube<T>       b,
                                              final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.LT);
    }

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of the element-wise greater-than operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T> a,
                                                 final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, ComparisonOp.GT);
    }

    /**
     * Pairwise compare two cubes and put the result of the element-wise
     * greater-than operation into a third (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T>       a,
                                                 final Hypercube<T>       b,
                                                 final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, r, ComparisonOp.GT);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T> a,
                                                 final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.GT);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T>       a,
                                                 final T                  b,
                                                 final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.GT);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greater(final T            a,
                                                 final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.GT);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final T                  a,
                                                 final Hypercube<T>       b,
                                                 final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.GT);
    }

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of the element-wise less-than-or-equal-to operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T> a,
                                                   final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, ComparisonOp.LE);
    }

    /**
     * Pairwise compare two cubes and put the result of the element-wise
     * less-than-or-equal-to operation into a third (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T>       a,
                                                   final Hypercube<T>       b,
                                                   final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, r, ComparisonOp.LE);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than-or-equal-to operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T> a,
                                                   final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.LE);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than-or-equal-to operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T>       a,
                                                   final T                  b,
                                                   final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.LE);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than-or-equal-to operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> lessEqual(final T            a,
                                                   final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.LE);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than-or-equal-to operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final T                  a,
                                                   final Hypercube<T>       b,
                                                   final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.LE);
    }

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of the element-wise greater-than-or-equal-to operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T> a,
                                                      final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, ComparisonOp.GE);
    }

    /**
     * Pairwise compare two cubes and put the result of the element-wise
     * greater-than-or-equal-to operation into a third (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T>       a,
                                                      final Hypercube<T>       b,
                                                      final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, b, r, ComparisonOp.GE);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than-or-equal-to operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T> a,
                                                      final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.GE);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than-or-equal-to operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T>       a,
                                                      final T                  b,
                                                      final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.GE);
    }

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than-or-equal-to operation.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final T            a,
                                                      final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.GE);
    }

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than-or-equal-to operation into a second (boolean) cube.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final T                  a,
                                                      final Hypercube<T>       b,
                                                      final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.GE);
    }

    // -------------------------------------------------------------------------

    /**
     * Returns whether any elements of the given cube evaluate to {@code true}.
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> boolean any0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return reductiveLogicOp(a, null, ReductiveLogicOp.ANY);
    }

    /**
     * Returns whether all elements of the given cube evaluate to {@code true}.
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> boolean all0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        return reductiveLogicOp(a, null, ReductiveLogicOp.ALL);
    }

    /**
     * Returns whether any elements of the given cube evaluate to {@code true}.
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,out,keepdims,where")
    @GenericReturnType
    public static <T> Object any(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws NullPointerException,
               UnsupportedOperationException
    {
        int[] axes = null;
        BooleanHypercube where = null;
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                case "initial":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }
            }
        }
        if (axes == null) {
            return reductiveLogicOp(a, null, ReductiveLogicOp.ANY);
        }
        else {
            final BooleanHypercube result = reductiveLogicOpByAxes(a, axes, where, ReductiveLogicOp.ANY);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }
    }

    /**
     * Returns whether all elements of the given cube evaluate to {@code true}.
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,out,keepdims,where")
    @GenericReturnType
    public static <T> Object all(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws NullPointerException,
               UnsupportedOperationException
    {
        int[] axes = null;
        BooleanHypercube where = null;
        if (kwargs != null) {
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {
                switch (entry.getKey()) {
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                case "initial":
                    if (entry.getValue() != null) {
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }
            }
        }
        if (axes == null) {
            return reductiveLogicOp(a, null, ReductiveLogicOp.ALL);
        }
        else {
            final BooleanHypercube result = reductiveLogicOpByAxes(a, axes, where, ReductiveLogicOp.ALL);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Return the elements of a cube that satisfy some condition, provided as
     * a boolean cube. Note that this operations is always flattening, meaning
     * that the resulting cube will always be 1-dimensional (this is equivalent
     * to {@code numpy.extract}'s behavior), e.g: <pre>
     *
     *   [[1, 2, 3],              [[False, False, False],
     *    [4, 5, 6],  selected by  [False, True,  True ], becomes [5, 6, 7, 8, 9]
     *    [7, 8, 9]]               [True,  True,  True ]]
     *
     * </pre>
     *
     * @param c  A cube describing the condition. If an element of c is set to
     *           {@code True}, the corresponding element of the cube is extracted.
     * @param a  The cube to extract from.
     *
     * @return The resulting cube containing only the elements where the
     *         corresponding element in the condition evaluate to {@code True}.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If the given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> extract(final Hypercube<Boolean> c,
                                           final Hypercube<T>       a)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " + a.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleExtract(c, (Hypercube<Double>)a);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatExtract(c, (Hypercube<Float>)a);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intExtract(c, (Hypercube<Integer>)a);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longExtract(c, (Hypercube<Long>)a);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)booleanExtract(c, (Hypercube<Boolean>)a);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform extract operation on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given binary operation for two cubes.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<T> binaryOp(final Hypercube<T>     a,
                                             final Hypercube<T>     b,
                                             final BooleanHypercube w,
                                             final BinaryOp         op)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleBinaryOp((Hypercube<Double>)a,
                                                (Hypercube<Double>)b,
                                                w,
                                                op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatBinaryOp((Hypercube<Float>)a,
                                               (Hypercube<Float>)b,
                                               w,
                                               op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intBinaryOp((Hypercube<Integer>)a,
                                             (Hypercube<Integer>)b,
                                             w,
                                             op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longBinaryOp((Hypercube<Long>)a,
                                              (Hypercube<Long>)b,
                                              w,
                                              op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)booleanBinaryOp((Hypercube<Boolean>)a,
                                                 (Hypercube<Boolean>)b,
                                                 w,
                                                 op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform binary " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Handle the given binary operation for two cubes, putting the result into a
     * third.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<T> binaryOp(final Hypercube<T>     a,
                                             final Hypercube<T>     b,
                                             final Hypercube<T>     r,
                                             final BooleanHypercube w,
                                             final BinaryOp         op)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        if (!a.matches(b) || !a.matches(r)) {
            // They don't exactly match, so let's see if one is a sub-cube of the
            // other and that we can map into the result nicely
            if (a.submatches(b) && a.matches(r)) {
                // If we have weights then they need to match 'a' in shape
                if (w != null && !a.matchesInShape(w)) {
                    throw new IllegalArgumentException(
                        "Given an incompatible 'w' cube"
                    );
                }

                // How we will slice up the 'a' and 'r' cubes so that we can
                // recurse using 'b' directly
                final Dimension<?>[] aDims = a.getDimensions();
                final Dimension.Accessor<?>[] aAccessors =
                    new Dimension.Accessor<?>[aDims.length];
                final Dimension<?>[] wDims =
                    (w == null) ? null : w.getDimensions();
                final Dimension.Accessor<?>[] wAccessors =
                    (w == null) ? null : new Dimension.Accessor<?>[wDims.length];
                final Dimension<?> aDim = aDims[0];
                final Dimension<?> wDim = (w == null) ? null : wDims[0];

                // Slice with each part of the dimension and recurse on the
                // resultant subcubes
                for (long i=0; i < aDim.length(); i++) {
                    aAccessors[0] = aDim.at(i);
                    if (w != null) {
                        wAccessors[0] = wDim.at(i);
                    }
                    binaryOp(
                        a.slice(aAccessors),
                        b,
                        r.slice(aAccessors),
                        (w == null) ? null : w.slice(wAccessors),
                        op
                    );
                }
                return r;
            }

            // The converse case to the above. Code is "duplicated" since certain
            // operations are not commutative.
            if (b.submatches(a) && b.matches(r)) {
                if (w != null && !b.matchesInShape(w)) {
                    throw new IllegalArgumentException(
                        "Given an incompatible 'w' cube"
                    );
                }
                final Dimension<?>[] bDims = b.getDimensions();
                final Dimension.Accessor<?>[] bAccessors =
                    new Dimension.Accessor<?>[bDims.length];
                final Dimension<?>[] wDims =
                    (w == null) ? null : w.getDimensions();
                final Dimension.Accessor<?>[] wAccessors =
                    (w == null) ? null : new Dimension.Accessor<?>[wDims.length];
                final Dimension<?> bDim = bDims[0];
                final Dimension<?> wDim = (w == null) ? null : wDims[0];
                for (long i=0; i < bDim.length(); i++) {
                    bAccessors[0] = bDim.at(i);
                    if (w != null) {
                        wAccessors[0] = wDim.at(i);
                    }
                    binaryOp(
                        a,
                        b.slice(bAccessors),
                        r.slice(bAccessors),
                        (w == null) ? null : w.slice(wAccessors),
                        op
                    );
                }
                return r;
            }
        }

        if (w != null && !a.matchesInShape(w)) {
            throw new IllegalArgumentException("Given an incompatible 'w' cube");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleBinaryOp((Hypercube<Double>)a,
                                                (Hypercube<Double>)b,
                                                (Hypercube<Double>)r,
                                                w,
                                                op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatBinaryOp((Hypercube<Float>)a,
                                               (Hypercube<Float>)b,
                                               (Hypercube<Float>)r,
                                               w,
                                               op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intBinaryOp((Hypercube<Integer>)a,
                                             (Hypercube<Integer>)b,
                                             (Hypercube<Integer>)r,
                                             w,
                                             op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longBinaryOp((Hypercube<Long>)a,
                                              (Hypercube<Long>)b,
                                              (Hypercube<Long>)r,
                                              w,
                                              op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)booleanBinaryOp((Hypercube<Boolean>)a,
                                                 (Hypercube<Boolean>)b,
                                                 (Hypercube<Boolean>)r,
                                                 w,
                                                 op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform binary " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given unary operation for one cube and return the result in a
     * cube.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<T> unaryOp(final Hypercube<T> a,
                                            final UnaryOp      op)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleUnaryOp((Hypercube<Double>)a, op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatUnaryOp((Hypercube<Float>)a, op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intUnaryOp((Hypercube<Integer>)a, op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longUnaryOp((Hypercube<Long>)a, op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)booleanUnaryOp((Hypercube<Boolean>)a, op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform unary " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Handle the given unary operation for a cube, putting the result into a
     * second cube.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<T> unaryOp(final Hypercube<T> a,
                                            final Hypercube<T> r,
                                            final UnaryOp      op)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleUnaryOp((Hypercube<Double>)a,
                                               (Hypercube<Double>)r,
                                               op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatUnaryOp((Hypercube<Float>)a,
                                              (Hypercube<Float>)r,
                                              op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intUnaryOp((Hypercube<Integer>)a,
                                            (Hypercube<Integer>)r,
                                            op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longUnaryOp((Hypercube<Long>)a,
                                             (Hypercube<Long>)r,
                                             op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)booleanUnaryOp((Hypercube<Boolean>)a,
                                                (Hypercube<Boolean>)r,
                                                op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform unary " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given associative operation for a cube.
     */
    @SuppressWarnings("unchecked")
    private static <T> T associativeOp(final Hypercube<T>     a,
                                       final T                i,
                                       final BooleanHypercube w,
                                       final AssociativeOp    op)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // See if we can do it
        final Object o = i;
        if (a.getElementType().equals(Double.class)) {
            return (T)doubleAssociativeOp((Hypercube<Double>)a, (Double)o, w, op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return (T)floatAssociativeOp((Hypercube<Float>)a, (Float)o, w, op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (T)intAssociativeOp((Hypercube<Integer>)a, (Integer)o, w, op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return (T)longAssociativeOp((Hypercube<Long>)a, (Long)o, w, op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform associative " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Handle the given associative operation for a cube, for the given axes.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<T> associativeOpByAxes(final Hypercube<T>     c,
                                                        final int[]            a,
                                                        final T                i,
                                                        final BooleanHypercube w,
                                                        final AssociativeOp    op)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube");
        }

        // See if we can do it
        final Object o = i;
        if (c.getElementType().equals(Double.class)) {
            return (Hypercube<T>)doubleAssociativeOpByAxes((Hypercube<Double>)c, a, (Double)o, w, op);
        }
        else if (c.getElementType().equals(Float.class)) {
            return (Hypercube<T>)floatAssociativeOpByAxes((Hypercube<Float>)c, a, (Float)o, w, op);
        }
        else if (c.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)intAssociativeOpByAxes((Hypercube<Integer>)c, a, (Integer)o, w, op);
        }
        else if (c.getElementType().equals(Long.class)) {
            return (Hypercube<T>)longAssociativeOpByAxes((Hypercube<Long>)c, a, (Long)o, w, op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform associative " + op + " " +
                "on a cube with element type " +
                c.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given boolean comparison operation for two cubes.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<Boolean> comparisonOp(final Hypercube<T>  a,
                                                       final Hypercube<T>  b,
                                                       final ComparisonOp  op)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + b.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleComparisonOp((Hypercube<Double>)a,
                                      (Hypercube<Double>)b,
                                      op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatComparisonOp((Hypercube<Float>)a,
                                     (Hypercube<Float>)b,
                                     op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intComparisonOp((Hypercube<Integer>)a,
                                   (Hypercube<Integer>)b,
                                   op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longComparisonOp((Hypercube<Long>)a,
                                    (Hypercube<Long>)b,
                                    op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanComparisonOp((Hypercube<Boolean>)a,
                                       (Hypercube<Boolean>)b,
                                       op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform comparison " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Handle the given boolean comparison operation for two cubes, putting the result
     * into a third.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<Boolean> comparisonOp(final Hypercube<T>       a,
                                                       final Hypercube<T>       b,
                                                       final Hypercube<Boolean> r,
                                                       final ComparisonOp       op)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matches(b) || !a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleComparisonOp((Hypercube<Double>)a,
                                      (Hypercube<Double>)b,
                                      r,
                                      op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatComparisonOp((Hypercube<Float>)a,
                                     (Hypercube<Float>)b,
                                     r,
                                     op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intComparisonOp((Hypercube<Integer>)a,
                                       (Hypercube<Integer>)b,
                                       r,
                                       op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longComparisonOp((Hypercube<Long>)a,
                                    (Hypercube<Long>)b,
                                    r,
                                    op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanComparisonOp((Hypercube<Boolean>)a,
                                       (Hypercube<Boolean>)b,
                                       r,
                                       op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform comparison " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive logic operation for a cube.
     */
    @SuppressWarnings("unchecked")
    private static <T> boolean reductiveLogicOp(final Hypercube<T>     a,
                                                final BooleanHypercube w,
                                                final ReductiveLogicOp op)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleReductiveLogicOp ((Hypercube<Double>) a, w, op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatReductiveLogicOp  ((Hypercube<Float>)  a, w, op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intReductiveLogicOp    ((Hypercube<Integer>)a, w, op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longReductiveLogicOp   ((Hypercube<Long>)   a, w, op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanReductiveLogicOp((Hypercube<Boolean>)a, w, op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform reductive logical " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    /**
     * Handle the given reductive logic operation for a cube.
     */
    @SuppressWarnings("unchecked")
    private static BooleanHypercube reductiveLogicOpByAxes(final Hypercube<?>     a,
                                                           final int[]            axes,
                                                           final BooleanHypercube where,
                                                           final ReductiveLogicOp op)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleReductiveLogicOpByAxes ((Hypercube<Double>) a, axes, where, op);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatReductiveLogicOpByAxes  ((Hypercube<Float>)  a, axes, where, op);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intReductiveLogicOpByAxes    ((Hypercube<Integer>)a, axes, where, op);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longReductiveLogicOpByAxes   ((Hypercube<Long>)   a, axes, where, op);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanReductiveLogicOpByAxes((Hypercube<Boolean>)a, axes, where, op);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to perform reductive logical " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Create a single-valued cube of a given value that matches the dimensions
     * of a given cube.
     */
    @SuppressWarnings("unchecked")
    private static <T> Hypercube<T> singleValuedCube(final Hypercube<T> a,
                                                     final T            b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null value b");
        }

        // See if we can do it. We handle casting a little specially here since
        // we could be called from the Python side, where the Ts will become
        // Objects owing to type erasure. The strictFoo() calls will be a NOP
        // for the Java code.
        if (a.getElementType().equals(Double.class)) {
            return (Hypercube<T>)new DoubleSingleValueHypercube(a.getDimensions(),
                                                                strictDouble((Number)b));
        }
        else if (a.getElementType().equals(Float.class)) {
            return (Hypercube<T>)new FloatSingleValueHypercube(a.getDimensions(),
                                                               strictFloat((Number)b));
        }
        else if (a.getElementType().equals(Integer.class)) {
            return (Hypercube<T>)new IntegerSingleValueHypercube(a.getDimensions(),
                                                                 strictInteger((Number)b));
        }
        else if (a.getElementType().equals(Long.class)) {
            return (Hypercube<T>)new LongSingleValueHypercube(a.getDimensions(),
                                                              strictLong((Number)b));
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return (Hypercube<T>)new BooleanSingleValueHypercube(a.getDimensions(),
                                                                 (boolean)b);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to broadcast cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Cast a given cube to a Boolean cube, returning the new cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> toBooleanHypercube(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple BitSet one by default
        try {
            return toBooleanHypercube(
                a,
                new BooleanBitSetHypercube(a.getDimensions())
            );
        }
        catch (ClassCastException e) {
            return toBooleanHypercube(a, new BooleanBitSetHypercube(a.getDimensions()));
        }
    }

    /**
     * Cast a given cube to a Boolean cube, putting the result into a second.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<Boolean> toBooleanHypercube(
        final Hypercube<T> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleToBooleanHypercube((Hypercube<Double>)a, r);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatToBooleanHypercube((Hypercube<Float>)a, r);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intToBooleanHypercube((Hypercube<Integer>)a, r);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longToBooleanHypercube((Hypercube<Long>)a, r);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanToBooleanHypercube((Hypercube<Boolean>)a, r);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to cast cube with element type " +
                a.getElementType().getSimpleName() +
                "to a Boolean cube"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanToBooleanHypercube} that casts
     * a sub-array of the given Boolean cube to a Boolean cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void booleanToBooleanHypercubeHelper(
        final BooleanHypercube da,
        final BooleanHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final boolean  [] ar = new boolean  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (boolean)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Boolean cube to a Boolean cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Boolean> booleanToBooleanHypercube(
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanToBooleanHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanToBooleanHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (boolean)(va.booleanValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intToBooleanHypercube} that casts
     * a sub-array of the given Integer cube to a Boolean cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void intToBooleanHypercubeHelper(
        final IntegerHypercube da,
        final BooleanHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final boolean  [] ar = new boolean  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (boolean)(aa[j] != 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Integer cube to a Boolean cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Boolean> intToBooleanHypercube(
        final Hypercube<Integer> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intToBooleanHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intToBooleanHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (boolean)(va.intValue() != 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longToBooleanHypercube} that casts
     * a sub-array of the given Long cube to a Boolean cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void longToBooleanHypercubeHelper(
        final LongHypercube da,
        final BooleanHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final boolean  [] ar = new boolean  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (boolean)(aa[j] != 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Long cube to a Boolean cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Boolean> longToBooleanHypercube(
        final Hypercube<Long> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longToBooleanHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longToBooleanHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (boolean)(va.longValue() != 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatToBooleanHypercube} that casts
     * a sub-array of the given Float cube to a Boolean cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void floatToBooleanHypercubeHelper(
        final FloatHypercube da,
        final BooleanHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final boolean  [] ar = new boolean  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (boolean)(aa[j] != 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Float cube to a Boolean cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Boolean> floatToBooleanHypercube(
        final Hypercube<Float> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatToBooleanHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatToBooleanHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (boolean)(va.floatValue() != 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleToBooleanHypercube} that casts
     * a sub-array of the given Double cube to a Boolean cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void doubleToBooleanHypercubeHelper(
        final DoubleHypercube da,
        final BooleanHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final boolean  [] ar = new boolean  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (boolean)(aa[j] != 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Double cube to a Boolean cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Boolean> doubleToBooleanHypercube(
        final Hypercube<Double> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleToBooleanHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleToBooleanHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (boolean)(va.doubleValue() != 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Cast a given cube to a Integer cube, returning the new cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Integer> toIntegerHypercube(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        try {
            return toIntegerHypercube(
                a,
                new IntegerArrayHypercube(a.getDimensions())
            );
        }
        catch (ClassCastException e) {
            return toIntegerHypercube(a, new IntegerArrayHypercube(a.getDimensions()));
        }
    }

    /**
     * Cast a given cube to a Integer cube, putting the result into a second.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<Integer> toIntegerHypercube(
        final Hypercube<T> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleToIntegerHypercube((Hypercube<Double>)a, r);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatToIntegerHypercube((Hypercube<Float>)a, r);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intToIntegerHypercube((Hypercube<Integer>)a, r);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longToIntegerHypercube((Hypercube<Long>)a, r);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanToIntegerHypercube((Hypercube<Boolean>)a, r);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to cast cube with element type " +
                a.getElementType().getSimpleName() +
                "to a Integer cube"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanToIntegerHypercube} that casts
     * a sub-array of the given Boolean cube to a Integer cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void booleanToIntegerHypercubeHelper(
        final BooleanHypercube da,
        final IntegerHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final int  [] ar = new int  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (int)(aa[j] ? 1 : 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Boolean cube to a Integer cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Integer> booleanToIntegerHypercube(
        final Hypercube<Boolean> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanToIntegerHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanToIntegerHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (int)(va.booleanValue() ? 1 : 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intToIntegerHypercube} that casts
     * a sub-array of the given Integer cube to a Integer cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void intToIntegerHypercubeHelper(
        final IntegerHypercube da,
        final IntegerHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final int  [] ar = new int  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (int)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Integer cube to a Integer cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Integer> intToIntegerHypercube(
        final Hypercube<Integer> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intToIntegerHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intToIntegerHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (int)(va.intValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longToIntegerHypercube} that casts
     * a sub-array of the given Long cube to a Integer cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void longToIntegerHypercubeHelper(
        final LongHypercube da,
        final IntegerHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final int  [] ar = new int  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (int)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Long cube to a Integer cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Integer> longToIntegerHypercube(
        final Hypercube<Long> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longToIntegerHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longToIntegerHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (int)(va.longValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatToIntegerHypercube} that casts
     * a sub-array of the given Float cube to a Integer cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void floatToIntegerHypercubeHelper(
        final FloatHypercube da,
        final IntegerHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final int  [] ar = new int  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (int)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Float cube to a Integer cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Integer> floatToIntegerHypercube(
        final Hypercube<Float> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatToIntegerHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatToIntegerHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (int)(va.floatValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleToIntegerHypercube} that casts
     * a sub-array of the given Double cube to a Integer cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void doubleToIntegerHypercubeHelper(
        final DoubleHypercube da,
        final IntegerHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final int  [] ar = new int  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (int)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Double cube to a Integer cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Integer> doubleToIntegerHypercube(
        final Hypercube<Double> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleToIntegerHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleToIntegerHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (int)(va.doubleValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Cast a given cube to a Long cube, returning the new cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Long> toLongHypercube(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        try {
            return toLongHypercube(
                a,
                new LongArrayHypercube(a.getDimensions())
            );
        }
        catch (ClassCastException e) {
            return toLongHypercube(a, new LongArrayHypercube(a.getDimensions()));
        }
    }

    /**
     * Cast a given cube to a Long cube, putting the result into a second.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<Long> toLongHypercube(
        final Hypercube<T> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleToLongHypercube((Hypercube<Double>)a, r);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatToLongHypercube((Hypercube<Float>)a, r);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intToLongHypercube((Hypercube<Integer>)a, r);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longToLongHypercube((Hypercube<Long>)a, r);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanToLongHypercube((Hypercube<Boolean>)a, r);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to cast cube with element type " +
                a.getElementType().getSimpleName() +
                "to a Long cube"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanToLongHypercube} that casts
     * a sub-array of the given Boolean cube to a Long cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void booleanToLongHypercubeHelper(
        final BooleanHypercube da,
        final LongHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final long  [] ar = new long  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (long)(aa[j] ? 1 : 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Boolean cube to a Long cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Long> booleanToLongHypercube(
        final Hypercube<Boolean> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanToLongHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanToLongHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (long)(va.booleanValue() ? 1 : 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intToLongHypercube} that casts
     * a sub-array of the given Integer cube to a Long cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void intToLongHypercubeHelper(
        final IntegerHypercube da,
        final LongHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final long  [] ar = new long  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (long)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Integer cube to a Long cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Long> intToLongHypercube(
        final Hypercube<Integer> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intToLongHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intToLongHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (long)(va.intValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longToLongHypercube} that casts
     * a sub-array of the given Long cube to a Long cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void longToLongHypercubeHelper(
        final LongHypercube da,
        final LongHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final long  [] ar = new long  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (long)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Long cube to a Long cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Long> longToLongHypercube(
        final Hypercube<Long> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longToLongHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longToLongHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (long)(va.longValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatToLongHypercube} that casts
     * a sub-array of the given Float cube to a Long cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void floatToLongHypercubeHelper(
        final FloatHypercube da,
        final LongHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final long  [] ar = new long  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (long)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Float cube to a Long cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Long> floatToLongHypercube(
        final Hypercube<Float> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatToLongHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatToLongHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (long)(va.floatValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleToLongHypercube} that casts
     * a sub-array of the given Double cube to a Long cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void doubleToLongHypercubeHelper(
        final DoubleHypercube da,
        final LongHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final long  [] ar = new long  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (long)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Double cube to a Long cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Long> doubleToLongHypercube(
        final Hypercube<Double> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleToLongHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleToLongHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (long)(va.doubleValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Cast a given cube to a Float cube, returning the new cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Float> toFloatHypercube(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        try {
            return toFloatHypercube(
                a,
                new FloatArrayHypercube(a.getDimensions())
            );
        }
        catch (ClassCastException e) {
            return toFloatHypercube(a, new FloatArrayHypercube(a.getDimensions()));
        }
    }

    /**
     * Cast a given cube to a Float cube, putting the result into a second.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<Float> toFloatHypercube(
        final Hypercube<T> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleToFloatHypercube((Hypercube<Double>)a, r);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatToFloatHypercube((Hypercube<Float>)a, r);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intToFloatHypercube((Hypercube<Integer>)a, r);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longToFloatHypercube((Hypercube<Long>)a, r);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanToFloatHypercube((Hypercube<Boolean>)a, r);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to cast cube with element type " +
                a.getElementType().getSimpleName() +
                "to a Float cube"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanToFloatHypercube} that casts
     * a sub-array of the given Boolean cube to a Float cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void booleanToFloatHypercubeHelper(
        final BooleanHypercube da,
        final FloatHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final float  [] ar = new float  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (float)(aa[j] ? 1 : 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Boolean cube to a Float cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Float> booleanToFloatHypercube(
        final Hypercube<Boolean> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanToFloatHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanToFloatHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (float)(va.booleanValue() ? 1 : 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intToFloatHypercube} that casts
     * a sub-array of the given Integer cube to a Float cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void intToFloatHypercubeHelper(
        final IntegerHypercube da,
        final FloatHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final float  [] ar = new float  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (float)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Integer cube to a Float cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Float> intToFloatHypercube(
        final Hypercube<Integer> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intToFloatHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intToFloatHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (float)(va.intValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longToFloatHypercube} that casts
     * a sub-array of the given Long cube to a Float cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void longToFloatHypercubeHelper(
        final LongHypercube da,
        final FloatHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final float  [] ar = new float  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (float)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Long cube to a Float cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Float> longToFloatHypercube(
        final Hypercube<Long> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longToFloatHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longToFloatHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (float)(va.longValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatToFloatHypercube} that casts
     * a sub-array of the given Float cube to a Float cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void floatToFloatHypercubeHelper(
        final FloatHypercube da,
        final FloatHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final float  [] ar = new float  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (float)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Float cube to a Float cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Float> floatToFloatHypercube(
        final Hypercube<Float> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatToFloatHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatToFloatHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (float)(va.floatValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleToFloatHypercube} that casts
     * a sub-array of the given Double cube to a Float cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void doubleToFloatHypercubeHelper(
        final DoubleHypercube da,
        final FloatHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final float  [] ar = new float  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (float)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Double cube to a Float cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Float> doubleToFloatHypercube(
        final Hypercube<Double> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleToFloatHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleToFloatHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (float)(va.doubleValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Cast a given cube to a Double cube, returning the new cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Double> toDoubleHypercube(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        try {
            return toDoubleHypercube(
                a,
                new DoubleArrayHypercube(a.getDimensions())
            );
        }
        catch (ClassCastException e) {
            return toDoubleHypercube(a, new DoubleArrayHypercube(a.getDimensions()));
        }
    }

    /**
     * Cast a given cube to a Double cube, putting the result into a second.
     *
     * @return The given {@code r} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {@code null}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<Double> toDoubleHypercube(
        final Hypercube<T> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {
            return doubleToDoubleHypercube((Hypercube<Double>)a, r);
        }
        else if (a.getElementType().equals(Float.class)) {
            return floatToDoubleHypercube((Hypercube<Float>)a, r);
        }
        else if (a.getElementType().equals(Integer.class)) {
            return intToDoubleHypercube((Hypercube<Integer>)a, r);
        }
        else if (a.getElementType().equals(Long.class)) {
            return longToDoubleHypercube((Hypercube<Long>)a, r);
        }
        else if (a.getElementType().equals(Boolean.class)) {
            return booleanToDoubleHypercube((Hypercube<Boolean>)a, r);
        }
        else {
            throw new UnsupportedOperationException(
                "Don't know how to cast cube with element type " +
                a.getElementType().getSimpleName() +
                "to a Double cube"
            );
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanToDoubleHypercube} that casts
     * a sub-array of the given Boolean cube to a Double cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void booleanToDoubleHypercubeHelper(
        final BooleanHypercube da,
        final DoubleHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final double  [] ar = new double  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (double)(aa[j] ? 1 : 0);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Boolean cube to a Double cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Double> booleanToDoubleHypercube(
        final Hypercube<Boolean> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanToDoubleHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanToDoubleHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (double)(va.booleanValue() ? 1 : 0));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intToDoubleHypercube} that casts
     * a sub-array of the given Integer cube to a Double cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void intToDoubleHypercubeHelper(
        final IntegerHypercube da,
        final DoubleHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final double  [] ar = new double  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (double)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Integer cube to a Double cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Double> intToDoubleHypercube(
        final Hypercube<Integer> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intToDoubleHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intToDoubleHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (double)(va.intValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longToDoubleHypercube} that casts
     * a sub-array of the given Long cube to a Double cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void longToDoubleHypercubeHelper(
        final LongHypercube da,
        final DoubleHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final double  [] ar = new double  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (double)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Long cube to a Double cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Double> longToDoubleHypercube(
        final Hypercube<Long> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longToDoubleHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longToDoubleHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (double)(va.longValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatToDoubleHypercube} that casts
     * a sub-array of the given Float cube to a Double cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void floatToDoubleHypercubeHelper(
        final FloatHypercube da,
        final DoubleHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final double  [] ar = new double  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (double)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Float cube to a Double cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Double> floatToDoubleHypercube(
        final Hypercube<Float> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatToDoubleHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatToDoubleHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (double)(va.floatValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleToDoubleHypercube} that casts
     * a sub-array of the given Double cube to a Double cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void doubleToDoubleHypercubeHelper(
        final DoubleHypercube da,
        final DoubleHypercube dr,
        final long startIndex,
        final long endIndex
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final double  [] ar = new double  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                ar[j] = (double)(aa[j]);
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Cast the given Double cube to a Double cube, putting the
     * result into the given {@code r} cube.
     */
    private static Hypercube<Double> doubleToDoubleHypercube(
        final Hypercube<Double> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleToDoubleHypercubeHelper(da, dr, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleToDoubleHypercubeHelper(da, dr, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    r.setObjectAt(i, (double)(va.doubleValue()));
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code popcount} that counts the number of {@code true}
     * values in a sub-array of a boolean cube. The sub-arrays are specified by
     * parameters startIndex and endIndex, and the result is returned as a long.
     */
    @SuppressWarnings("inline")
    private static long popcountHelper(final BooleanHypercube da,
                                       final long             startIndex,
                                       final long             endIndex)
    {
        // Initialize the return value
        long r = 0;

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {
                r += (aa[j] ? 1 : 0);
            }
        }

        return r;
    }

    /**
     * Handle the given reductive logic operation for a sub-array of a boolean
     * cube. The sub-arrays are specified by parameters startIndex and endIndex,
     * and the result is returned as a long.
     */
    private static long popcount(final Hypercube<Boolean> a,
                                 final long               start,
                                 final long               end)
        throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Initialize the return value
        long r = 0;

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = popcountHelper(da, start, end);
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = ((end - start) / NUM_THREADS / 32 + 1) * 32;
                final long[] ar   = new long[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = start + bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? end
                                             : Math.min(start + bucket * (j+1), end);
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = popcountHelper(da, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                for (int j=0; j < NUM_THREADS; j++) {
                    r += ar[j];
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = start; i < end; i++) {
                // No need to handle missing values separately
                final Boolean va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(va)) {
                    r ++;
                }
            }
        }

        // Always return the result
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Return a new boolean cube of given dimensions where all elements are
     * equal to a given boolean value.
     *
     * <p>This method is an alias of {@code broadcast()} and is equivalent to
     * {@code numpy.full()} method.
     *
     * @return The resulting boolean cube.
     */
    public static Hypercube<Boolean> full(
        final Dimension<?>[] dimensions,
        final boolean value
    )
    {
        return broadcast(dimensions, value);
    }

    /**
     * Return a new boolean cube of given dimensions where all elements are
     * equal to a given boolean value.
     *
     * @return The resulting boolean cube.
     */
    public static Hypercube<Boolean> broadcast(
        final Dimension<?>[] dimensions,
        final boolean value
    )
    {
        final BooleanHypercube a = new BooleanBitSetHypercube(dimensions);
        a.fill(value);
        return a;
    }

    /**
     * Return a new 1-dimensional boolean cube of a given size where all
     * elements are equal to a given boolean value.
     *
     * <p>This is equivalent to calling {@code broadcast(Dimension.of(size), value)}.
     *
     * @return The resulting boolean cube.
     */
    public static Hypercube<Boolean> broadcast(
        final long size,
        final boolean value
    )
    {
        return broadcast(Dimension.of(size), value);
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanBinaryOp} that performs a binary operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void booleanBinaryOpHelper(
        final BooleanHypercube da,
        final BooleanHypercube db,
        final BooleanHypercube dr,
        final BooleanHypercube dw,
        final BinaryOp op,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final boolean[] ab = new boolean[STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];
        final boolean[] aw = (dw == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);
            if (dw != null) {
                dw.toFlattened(i, aw, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case AND: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] & ab[j]; break;
            case OR:  for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] | ab[j]; break;
            case XOR: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] ^ ab[j]; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported binary operation: " + op
                );
            }

            // Copy in
            if (aw == null) {
                dr.fromFlattened(ar, 0, i, len);
            }
            else {
                for (int j=0; j < len; j++) {
                    if (aw[j]) {
                        dr.setAt(i + j, ar[j]);
                    }
                }
            }
        }
    }

    /**
     * Handle the given binary operation for two Boolean cubes.
     */
    private static Hypercube<Boolean> booleanBinaryOp(
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> b,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // Depending on which cube is a non-strict supercube of the other, create a simple
        // BitSet destination one as a copy of the appropriate argument
        if (a.submatches(b)) {
            return binaryOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), dw, op);
        }
        if (b.submatches(a)) {
            return binaryOp(a, b, new BooleanBitSetHypercube(b.getDimensions()), dw, op);
        }

        // No match between the cubes
        throw new IllegalArgumentException(
            "Given incompatible cubes: " +
            a.toDescriptiveString() + " vs " + b.toDescriptiveString()
        );
    }

    /**
     * Handle the given binary operation for two Boolean cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> booleanBinaryOp(
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> b,
        final Hypercube<Boolean> r,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final BooleanHypercube db = (BooleanHypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanBinaryOpHelper(da, db, dr, dw, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanBinaryOpHelper(da, db, dr, dw, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                final Boolean vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case AND: r.setObjectAt(i, va & vb); break;
                    case OR:  r.setObjectAt(i, va | vb); break;
                    case XOR: r.setObjectAt(i, va ^ vb); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported binary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanUnaryOp} that performs a unary operation
     * on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is put into a second cube.
     */
    @SuppressWarnings("inline")
    private static void booleanUnaryOpHelper(
        final BooleanHypercube da,
        final BooleanHypercube dr,
        final UnaryOp op,
        final long    startIndex,
        final long    endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case NOT:   for (int j=0; j < len; j++) ar[j] =  !aa[j]; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported unary operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given unary operation for a Boolean cube.
     */
    private static Hypercube<Boolean> booleanUnaryOp(
        final Hypercube<Boolean> a,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple BitSet one by default
        return unaryOp(a, new BooleanBitSetHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given unary operation for a Boolean cube, putting the result into a
     * second.
     */
    private static Hypercube<Boolean> booleanUnaryOp(
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> r,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanUnaryOpHelper(da, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanUnaryOpHelper(da, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case NOT:   r.setObjectAt(i,  !va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported unary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanComparisonOp} that performs a comparison
     * operation on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void booleanComparisonOpHelper(
        final BooleanHypercube da,
        final BooleanHypercube db,
        final BooleanHypercube dr,
        final ComparisonOp     op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[]  aa = new boolean [STAGING_SIZE];
        final boolean[]  ab = new boolean [STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            switch (op) {
            case EQ: for (int j=0; j < len; j++) ar[j] = (aa[j] == ab[j]); break;
            case NE: for (int j=0; j < len; j++) ar[j] = (aa[j] != ab[j]); break;
            case LT: for (int j=0; j < len; j++) ar[j] = ((aa[j] ? 1 : 0)  < (ab[j] ? 1 : 0)); break;
            case GT: for (int j=0; j < len; j++) ar[j] = ((aa[j] ? 1 : 0)  > (ab[j] ? 1 : 0)); break;
            case LE: for (int j=0; j < len; j++) ar[j] = ((aa[j] ? 1 : 0) <= (ab[j] ? 1 : 0)); break;
            case GE: for (int j=0; j < len; j++) ar[j] = ((aa[j] ? 1 : 0) >= (ab[j] ? 1 : 0)); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported comparison operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given boolean comparison operation for two Boolean cubes.
     */
    private static Hypercube<Boolean> booleanComparisonOp(
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> b,
        final ComparisonOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + b.toDescriptiveString()
            );
        }

        // Create the destination, a simple bitset one by default
        return comparisonOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given binary operation for two Boolean cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> booleanComparisonOp(
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> b,
        final Hypercube<Boolean> r,
        final ComparisonOp       op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;
            final BooleanHypercube db = (BooleanHypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanComparisonOpHelper(da, db, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanComparisonOpHelper(da, db, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                final Boolean vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case EQ: r.setObjectAt(i,  va.equals(vb)); break;
                    case NE: r.setObjectAt(i, !va.equals(vb)); break;
                    case LT: r.setObjectAt(i, (va ? 1 : 0)  < (vb ? 1 : 0)); break;
                    case GT: r.setObjectAt(i, (va ? 1 : 0)  > (vb ? 1 : 0)); break;
                    case LE: r.setObjectAt(i, (va ? 1 : 0) <= (vb ? 1 : 0)); break;
                    case GE: r.setObjectAt(i, (va ? 1 : 0) >= (vb ? 1 : 0)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported comparison operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // All comparison operattions with a NaN operand evaluate
                    // to false, except for the inequality operation (!=).
                    r.setObjectAt(i, null);
                    switch (op) {
                    case NE: r.setObjectAt(i, true);  break;
                    default: r.setObjectAt(i, false); break;
                    }
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive operation for a boolean cube, for the
     * given axes.
     */
    private static BooleanHypercube booleanReductiveLogicOpByAxes(
        final Hypercube<Boolean> a,
        final int[] axes,
        final BooleanHypercube where,
        final ReductiveLogicOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[dstNDim];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final BooleanHypercube dst = new BooleanBitSetHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final boolean result = booleanReductiveLogicOp(a.slice(srcAccessors), whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index. If doing so would cause overflow then
                // we reset it to zero and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code booleanReductiveLogicOp} that performs a reductive
     * logical operation on a sub-array of the cube. The sub-array is specified
     * by parameters startIndex and endIndex, and the result is returned as a
     * boolean.
     */
    @SuppressWarnings("inline")
    private static boolean booleanReductiveLogicOpHelper(
        final BooleanHypercube da,
        final BooleanHypercube w,
        final ReductiveLogicOp op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            if (w != null) {
                w.toFlattened(i, ww, 0, len);
            }

            switch (op) {
            case ANY: for (int j=0; j < len; j++) if (ww == null || ww[j]) r |= (aa[j]); break;
            case ALL: for (int j=0; j < len; j++) if (ww == null || ww[j]) r &= (aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reductive logic operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given reductive logic operation for a Boolean cube.
     */
    private static boolean booleanReductiveLogicOp(
        final Hypercube<Boolean> a,
        final BooleanHypercube w,
        final ReductiveLogicOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = booleanReductiveLogicOpHelper(da, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final boolean[] ar = new boolean[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = booleanReductiveLogicOpHelper(da, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ANY: for (int j=0; j < NUM_THREADS; j++) r |= ar[j]; break;
                case ALL: for (int j=0; j < NUM_THREADS; j++) r &= ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported reductive logic operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case ANY: r |= (va); break;
                    case ALL: r &= (va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported reductive logic operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // Note that NaN always evaluates to true.
                    switch (op) {
                    case ANY: r = true; break;
                    default:
                        // Nothing to do.
                    }
                }
            }
        }

        // Always return the result
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code booleanExtract} that performs an extract operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube, starting
     * from the given offset index value.
     */
    @SuppressWarnings("inline")
    private static void booleanExtractHelper(
        final BooleanHypercube dc,
        final BooleanHypercube da,
        final BooleanHypercube dr,
        final long startIndex,
        final long endIndex,
        final long offset
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean         [] ac = new boolean         [STAGING_SIZE];
        final boolean[] aa = new boolean[STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Define an offset to track our progress on r
        long offsetR = offset;

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            dc.toFlattened(i, ac, 0, len);
            da.toFlattened(i, aa, 0, len);

            int k = 0;
            for (int j=0; j < len; j++) {
                if (Boolean.TRUE.equals(ac[j])) {
                    ar[k++] = aa[j];
                }
            }

            // Copy in
            dr.fromFlattened(ar, 0, offsetR, k);
            offsetR += k;
        }
    }

    /**
     * Handle the extract operation for a Boolean cube.
     */
    private static Hypercube<Boolean> booleanExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Boolean> a
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " + a.toDescriptiveString()
            );
        }

        // Create the destination, a simple BitSet one by default
        return booleanExtract(c, a, new BooleanBitSetHypercube(Dimension.of(popcount(c))));
    }

    /**
     * Handle the extract operation for a Boolean cube, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> booleanExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Boolean> a,
        final Hypercube<Boolean> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " +
                a.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube dc = (BooleanHypercube)c;
            final BooleanHypercube da = (BooleanHypercube)a;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                booleanExtractHelper(dc, da, dr, 0, a.getSize(), 0);
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // An offset to track where in r we should be, when processing
                // each bucket.
                long offset = 0;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final long offsetR = offset;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            booleanExtractHelper(dc, da, dr, startIndex, endIndex, offsetR);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });

                    // Update the offset value accordingly
                    offset += popcount(c, startIndex, endIndex);
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, j=0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean       vc = c.getObjectAt(i);
                final Boolean va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(vc)) {
                    r.setObjectAt(j++, va);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static IntegerHypercube array(final IntegerHypercube cube)
    {
        return cube.array();
    }

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static IntegerHypercube array(final int[] array)
    {
        final IntegerHypercube result =
            new IntegerArrayHypercube(Dimension.of(array.length));
        result.fromFlattened(array);
        return result;
    }

    /**
     * Return a one dimensional cube with a range per the given value.
     */
    public static IntegerHypercube arange(final int stop)
    {
        return arange(0, stop);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static IntegerHypercube arange(
        final int start,
        final int stop
    )
    {
        return arange(start, stop, 1);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static IntegerHypercube arange(
        final int start,
        final int stop,
        final int step
    )
    {
        if (step == 0) {
            throw new IllegalArgumentException("Given a step of zero");
        }
        if (step < 0 && start < stop) {
            throw new IllegalArgumentException(
                "Step was negative but start was before stop"
            );
        }
        if (step > 0 && start > stop) {
            throw new IllegalArgumentException(
                "Step was positive but start was after stop"
            );
        }
        final long length = (long)((stop - start) / step);
        final IntegerHypercube cube = new IntegerArrayHypercube(Dimension.of(length));
        for (long i=0; i < length; i++) {
            cube.setAt(i, (int)(start + step * i));
        }
        return cube;
    }

    /**
     * Handle a vector multiply, also known as a dot product.
     */
    public static int intDotProd(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (a.getNDim() != 1 || !a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible or multi-dimensional cubes"
            );
        }

        try {
            final IntegerHypercube da = (IntegerHypercube)a;
            final IntegerHypercube db = (IntegerHypercube)b;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                final AtomicReference<Integer> r = new AtomicReference<Integer>();
                intDotProdHelper(da, db, r, 0, a.getSize());
                return r.get();
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();
                @SuppressWarnings("unchecked") final AtomicReference<Integer>[] r =
                    (AtomicReference<Integer>[])new AtomicReference<?>[NUM_THREADS];
                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }
                    final AtomicReference<Integer> rv = new AtomicReference<>();
                    r[j] = rv;

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intDotProdHelper(da, db, rv, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // And sum up the result
                int sum = 0;
                for (AtomicReference<Integer> rv : r) {
                    sum += rv.get().intValue();
                }
                return sum;
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Fall back to the simple version
            int sum = 0;
            for (long i = 0, sz = a.getSize(); i < sz; i++) {
                final Integer va = a.getObjectAt(i);
                final Integer vb = b.getObjectAt(i);
                if (va == null || vb == null) {
                    // For floats nulls are NaNs but they are zeroes for ints.
                    // Cheesy test to figure out which we are.
                    if (Double.isNaN(0)) {
                        return 0;
                    }
                }
                else {
                    sum += va * vb;
                }
            }
            return sum;
        }
    }

    /**
     * Helper function for {@code intDotProd} that performs the
     * dot product on two sub-arrays. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void intDotProdHelper(
        final IntegerHypercube da,
        final IntegerHypercube db,
        final AtomicReference<Integer> r,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final int[] ab = new int[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            // Compute the dot product
            int sum = 0;
            for (int j=0; j < len; j++) {
                sum += aa[j] * ab[j];
            }

            // And give it back via the atomic
            r.set(Integer.valueOf(sum));
        }
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Integer> intMatMul(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[1].equals(bDims[0])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { aDims[0] };
                return intMatMul(a, b, new IntegerArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { bDims[0] };
                return intMatMul(a, b, new IntegerArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1]) &&
                aDims[1].equals(bDims[0]))
            {
               return intMatMul(
                    a, b,
                    new IntegerArrayHypercube(
                        new Dimension<?>[] { aDims[0], bDims[1] }
                    )
                );
            }
        }
        else if (a.getNDim() <= 1 && b.getNDim() <= 1) {
            // Nothing. We don't handle this case for matrix multiplication.
        }
        else if (a.getNDim() > 2 || b.getNDim() > 2) {
            // These need to be compatible in the corner dimensions like the 2D
            // case, and then other dimensions just need to match directly
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            final int nADim = aDims.length;
            final int nBDim = bDims.length;
            if ((bDims.length == 1 || aDims[nADim-1].equals(bDims[nBDim-2])) &&
                (aDims.length == 1 || aDims[nADim-2].equals(bDims[nBDim-1])))
            {
                final int maxNDim = Math.max(nADim, nBDim);
                final int minNDim = Math.min(nADim, nBDim);
                boolean matches = true;
                for (int i=0; matches && i < minNDim-2; i++) {
                    if (!aDims[i].equals(bDims[i])) {
                        matches = false;
                    }
                }
                if (matches) {
                    final Dimension<?>[] rDims;
                    final int offset;
                    if (aDims.length == 1 || bDims.length == 1) {
                        // We will lose a dimension when multiplying by a vector
                        rDims = new Dimension<?>[maxNDim-1];
                        rDims[rDims.length-1] =
                            (aDims.length == 1) ? bDims[bDims.length-2]
                                                : aDims[aDims.length-2];
                        offset = 1;
                    }
                    else {
                        rDims = new Dimension<?>[maxNDim];
                        rDims[rDims.length-1] = aDims[aDims.length-2];
                        rDims[rDims.length-2] = bDims[bDims.length-1];
                        offset = 0;
                    }
                    for (int i = 0; i < offset + rDims.length - 2; i++) {
                        rDims[i] = (aDims.length >= bDims.length) ? aDims[i] : bDims[i];
                    }
                    return intMatMul(a, b, new IntegerArrayHypercube(rDims));
                }
            }
        }

        // If we got here then we could not make the cubes match
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Integer> intMatMul(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Input cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }
        if (!a.getElementType().equals(r.getElementType())) {
            throw new NullPointerException(
                "Input and return cubes have different element types: " +
                a.getElementType() + " vs " + r.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            IntegerHypercube dr = null;
            try {
                dr = (IntegerHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, aDims[1].at(0) };
            if (a.slice(iSlice).matches(b) && a.slice(jSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        dr.setAt(i, intDotProd(a.slice(iSlice), b));
                    }
                }
                else {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        r.setObjectAt(i, intDotProd(a.slice(iSlice), b));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            IntegerHypercube dr = null;
            try {
                dr = (IntegerHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { bDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.matches(b.slice(iSlice)) && b.slice(iSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setAt(i, intDotProd(a, b.slice(jSlice)));
                    }
                }
                else {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setObjectAt(i, intDotProd(a, b.slice(jSlice)));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.slice(aSlice).matches(b.slice(bSlice))) {
                // A regular matrix multiply where we compute the dot product of
                // the row and column to get their intersection coordinate's
                // value.
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                try {
                    final IntegerHypercube da = (IntegerHypercube)a;
                    final IntegerHypercube db = (IntegerHypercube)b;
                    final IntegerHypercube dr = (IntegerHypercube)r;

                    // We copy out the column from 'b' for faster access if it's
                    // small enough to fit into an array. 2^30 doubles is 16GB
                    // for one column which is totally possible for a non-square
                    // matrix but, we hope, most matrices will not be quite that
                    // big. We could use an array of arrays to handle that case
                    // but this is slower for the general case.
                    if (bDims[0].length() <= 1<<30) {
                        // Hand off to the smarter method which will copy out
                        // the column and use multi-threading
                        intMatMul2D(da, db, dr);
                    }
                    else {
                        // Where we start striding, see below.
                        ai[1] = bi[0] = 0;

                        // The stride through the flattened data, to walk a column
                        // in 'b'. We know that the format of the data is C-style in
                        // flattened form, so moving one row length in distance will
                        // step down one column index.
                        final long bs = bDims[1].length();

                        // Flipped the ordering of 'i' and 'j' since it's more cache
                        // efficient to copy out the column data (once) and then to
                        // stride through the rows each time.
                        da.preRead();
                        db.preRead();
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            for (long i=0; i < aDims[0].length(); i++) {
                                // We will stride through the two cubes pulling out
                                // the values for the sum directly, since we know
                                // their shape. This is much faster than going via
                                // the coordinate-based lookup. The stride in 'a' is
                                // 1 since it's walking along a row; in b it's the
                                // the row length, since it's walking along a column.
                                // Both will be the same number of steps so we only
                                // need to know when to stop walking in 'a'.
                                ai[0] = ri[0] = i;
                                long ao = da.toOffset(ai);
                                int sum = 0;
                                final long ae = ao + aDims[1].length();
                                for (long bo = db.toOffset(bi);
                                     ao < ae; ao++,
                                     bo += bs)
                                {
                                    sum += da.weakGetAt(ao) * db.weakGetAt(bo);
                                }
                                dr.set(sum, ri);
                            }
                        }
                    }
                }
                catch (ClassCastException e) {
                    // Need to be object-based
                    for (long i=0; i < aDims[0].length(); i++) {
                        ai[0] = ri[0] = i;
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            int sum = 0;
                            for (long k=0; k < bDims[0].length(); k++) {
                                ai[1] = bi[0] = k;
                                final Integer va = a.getObj(ai);
                                final Integer vb = b.getObj(bi);
                                if (va == null || vb == null) {
                                    // For floats nulls are NaNs but they are zeroes for ints.
                                    // Cheesy test to figure out which we are.
                                    if (Double.isNaN(0)) {
                                        sum = 0;
                                        break;
                                    }
                                }
                                else {
                                    sum += va * vb;
                                }
                            }
                            r.setObj(sum, ri);
                        }
                    }
                }

                // And give it back
                return r;
            }
        }
        else if ((a.getNDim() == r.getNDim() || a.getNDim()-1 == r.getNDim()) &&
                 a.getNDim() > b.getNDim())
        {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                intMatMul(a.slice(aSlice), b, r.slice(rSlice));
            }
            return r;
        }
        else if ((b.getNDim() == r.getNDim() || b.getNDim()-1 == r.getNDim()) &&
                 b.getNDim() > a.getNDim())
        {
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = bDims[0].length(); i < sz; i++) {
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                intMatMul(a, b.slice(bSlice), r.slice(rSlice));
            }
            return r;
        }
        else if (a.getNDim() == b.getNDim() && b.getNDim() == r.getNDim()) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                intMatMul(
                    a.slice(aSlice), b.slice(bSlice), r.slice(rSlice)
                );
            }
            return r;
        }

        // If we got here then we could not do anything
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given associative operation for a int cube, for the
     * given axes.
     */
    private static IntegerHypercube intAssociativeOpByAxes(
        final Hypercube<Integer> a,
        final int[] axes,
        final Integer initial,
        final BooleanHypercube where,
        final AssociativeOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[Math.max(1, dstNDim)];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final IntegerHypercube dst = new IntegerArrayHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final Integer result =
                associativeOp(a.slice(srcAccessors), initial, whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index? If doing so would overflow the digit we
                // go back to zero for it and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code intAssociativeOp} that performs an associative
     * operation on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is returned as a int.
     */
    @SuppressWarnings("inline")
    private static int intAssociativeOpHelper(
        final IntegerHypercube da,
        final Integer    i,
        final BooleanHypercube w,
        final AssociativeOp    op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        int r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Integer.MAX_VALUE; break;
            case MAX:    r = Integer.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long ii = startIndex; ii < endIndex; ii += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - ii, STAGING_SIZE);

            // Copy out
            da.toFlattened(ii, aa, 0, len);
            if (w != null) {
                w.toFlattened(ii, ww, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD:    for (int j=0; j < len; j++) if (ww == null || ww[j]) r += aa[j]; break;
            case MIN:    for (int j=0; j < len; j++) if (ww == null || ww[j]) r  = (r < aa[j] ? r : aa[j]); break;
            case MAX:    for (int j=0; j < len; j++) if (ww == null || ww[j]) r  = (r > aa[j] ? r : aa[j]); break;
            case NANADD: for (int j=0; j < len; j++) if (ww == null || ww[j]) r += (Float.isNaN(aa[j]) ? 0 : aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given associative operation for a int cube.
     */
    private static Integer intAssociativeOp(
        final Hypercube<Integer> a,
        final Integer i,
        final BooleanHypercube w,
        final AssociativeOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // If there is a where value then it should match in shape
        if (w != null && !a.matchesInShape(w)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        int r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Integer.MAX_VALUE; break;
            case MAX:    r = Integer.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = intAssociativeOpHelper(da, i, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final int[] ar = new int[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = intAssociativeOpHelper(da, null, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ADD:    for (int j=0; j < NUM_THREADS; j++) r += ar[j];                     break;
                case MIN:    for (int j=0; j < NUM_THREADS; j++) r  = ((r < ar[j]) ? r : ar[j]); break;
                case MAX:    for (int j=0; j < NUM_THREADS; j++) r  = ((r > ar[j]) ? r : ar[j]); break;
                // The result of NaN functions on each thread can never be NaN.
                // So we can simply combine them with no NaN handling.
                case NANADD: for (int j=0; j < NUM_THREADS; j++) r += ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported associative operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            w.preRead();
            for (long ii = 0, size = a.getSize(); ii < size; ii++) {
                // Handle any 'where' clause
                if (w != null && !w.weakGetAt(ii)) {
                    continue;
                }

                // Need to handle missing values
                final Integer va = a.getObjectAt(ii);
                if (va != null) {
                    switch (op) {
                    case ADD:    r += va;                           break;
                    case MIN:    r  = (r < va           ? r : va);  break;
                    case MAX:    r  = (r > va           ? r : va);  break;
                    case NANADD: r += (Float.isNaN(va) ? 0 : va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported associative operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    switch (op) {
                    case ADD: r = Integer.valueOf(0); break;
                    case MIN: r = Integer.valueOf(0); break;
                    case MAX: r = Integer.valueOf(0); break;
                    default:
                        // Don't do anything for NaN functions
                    }
                }
            }
        }

        // Always return the result
        return Integer.valueOf(r);
    }

    /**
     * Handle a 2D matrix multiply.
     */
    private static void intMatMul2D(
        final IntegerHypercube a,
        final IntegerHypercube b,
        final IntegerHypercube r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        final Dimension<?>[] aDims = a.getDimensions();
        final Dimension<?>[] bDims = b.getDimensions();

        // We will copy out the column from 'b' for faster access, if it's small
        // enough to fit into an array. If we got here then the caller should
        // have handled getting this right for us.
        if (bDims[0].length() > 1<<30) {
            throw new IllegalArgumentException(
                "Axis was too large: " + bDims[0].length()
            );
        }
        final int[] bcol = new int[(int)bDims[0].length()];

        // The stride through the flattened data, to walk a column
        // in 'b'. We know that the format of the data is C-style in
        // flattened form, so moving one row length in distance will
        // step down one column index.
        final long bs = bDims[1].length();

        // Flipped the ordering of 'i' and 'j' since it's more cache
        // efficient to copy out the column data (once) and then to
        // stride through the rows each time.
        for (long j=0; j < bDims[1].length(); j++) {
            long bco = b.toOffset(0, j);
            b.preRead();
            for (int i=0; i < bcol.length; i++, bco += bs) {
                bcol[i] = b.weakGetAt(bco);
            }

            // We will stride through the two cubes pulling out the values
            // for the sum directly, since we know their shape. This is much
            // faster than going via the coordinate-based lookup. The stride in
            // 'a' is 1 since it's walking along a row; in b it's the the row
            // length, since it's walking along a column. Both will be the same
            // number of steps so we only need to know when to stop walking in
            // 'a'.
            //
            // Only use multi-threading if it is enabled and if the cubes are
            // large enough to justify the overhead, noting that matmul is
            // an O(n^3) operation.
            final long numRows = aDims[0].length();
            if (ourExecutorService == null ||
                (numRows * numRows * numRows) < THREADING_THRESHOLD)
            {
                // Where we start striding, see below
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                ai[1] = bi[0] = 0;
                bi[1] = ri[1] = j;

                a.preRead();
                for (long i=0; i < numRows; i++) {
                    ai[0] = ri[0] = i;
                    long ao = a.toOffset(ai);
                    int sum = 0;
                    for (int bo=0 ; bo < bcol.length; ao++, bo++) {
                        sum += a.weakGetAt(ao) * bcol[bo];
                    }
                    r.set(sum, ri);
                }
            }
            else {
                // How many threads to use. This should not be more than there
                // are rows.
                final int numThreads = (int)Math.min(numRows, NUM_THREADS);

                // Initialize a countdown to wait for all threads to finish
                // processing.
                final CountDownLatch latch = new CountDownLatch(numThreads);

                // Bucket size for each thread, we use +1 to kinda ceil the
                // result which gives a somewhat better distribution. We do
                // this in such a way as to avoid double-rounding errors.
                final long threadRows = numRows / numThreads;
                final long bucket = threadRows +
                                  ((threadRows * numThreads == numRows) ? 0 : 1);
                for (int t=0; t < numThreads; t++) {
                    final long startIndex = bucket * t;
                    final long endIndex =
                        (t == numThreads-1) ? numRows
                                            : Math.min(bucket * (t+1), numRows);

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    final long jf = j; // <-- "final j"
                    ourExecutorService.submit(() -> {
                        try {
                            final long[] ai = new long[] { 0,  0 };
                            final long[] bi = new long[] { 0, jf };
                            final long[] ri = new long[] { 0, jf };
                            a.preRead();
                            for (long i = startIndex; i < endIndex; i++) {
                                ai[0] = ri[0] = i;
                                long ao = a.toOffset(ai);
                                int sum = 0;
                                for (int bo=0; bo < bcol.length; ao++, bo++) {
                                    sum += a.weakGetAt(ao) * bcol[bo];
                                }
                                r.set(sum, ri);
                            }
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Return a new int cube of given dimensions where all elements are
     * equal to a given int value.
     *
     * <p>This method is an alias of {@code broadcast()} and is equivalent to
     * {@code numpy.full()} method.
     *
     * @return The resulting int cube.
     */
    public static Hypercube<Integer> full(
        final Dimension<?>[] dimensions,
        final int value
    )
    {
        return broadcast(dimensions, value);
    }

    /**
     * Return a new int cube of given dimensions where all elements are
     * equal to a given int value.
     *
     * @return The resulting int cube.
     */
    public static Hypercube<Integer> broadcast(
        final Dimension<?>[] dimensions,
        final int value
    )
    {
        final IntegerHypercube a = new IntegerArrayHypercube(dimensions);
        a.fill(value);
        return a;
    }

    /**
     * Return a new 1-dimensional int cube of a given size where all
     * elements are equal to a given int value.
     *
     * <p>This is equivalent to calling {@code broadcast(Dimension.of(size), value)}.
     *
     * @return The resulting int cube.
     */
    public static Hypercube<Integer> broadcast(
        final long size,
        final int value
    )
    {
        return broadcast(Dimension.of(size), value);
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intBinaryOp} that performs a binary operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void intBinaryOpHelper(
        final IntegerHypercube da,
        final IntegerHypercube db,
        final IntegerHypercube dr,
        final BooleanHypercube dw,
        final BinaryOp op,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final int[] ab = new int[STAGING_SIZE];
        final int[] ar = new int[STAGING_SIZE];
        final boolean[] aw = (dw == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);
            if (dw != null) {
                dw.toFlattened(i, aw, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] + ab[j];          break;
            case SUB: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] - ab[j];          break;
            case MUL: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] * ab[j];          break;
            case DIV: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] / ab[j];          break;
            case MOD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] % ab[j];          break;
            case MIN: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.min(aa[j], ab[j]); break;
            case MAX: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.max(aa[j], ab[j]); break;
            case POW: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = (int)Math.pow(aa[j], ab[j]); break;
            case AND: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] & ab[j]; break;
            case OR:  for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] | ab[j]; break;
            case XOR: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] ^ ab[j]; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported binary operation: " + op
                );
            }

            // Copy in
            if (aw == null) {
                dr.fromFlattened(ar, 0, i, len);
            }
            else {
                for (int j=0; j < len; j++) {
                    if (aw[j]) {
                        dr.setAt(i + j, ar[j]);
                    }
                }
            }
        }
    }

    /**
     * Handle the given binary operation for two Integer cubes.
     */
    private static Hypercube<Integer> intBinaryOp(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // Depending on which cube is a non-strict supercube of the other, create a simple
        // Array destination one as a copy of the appropriate argument
        if (a.submatches(b)) {
            return binaryOp(a, b, new IntegerArrayHypercube(a.getDimensions()), dw, op);
        }
        if (b.submatches(a)) {
            return binaryOp(a, b, new IntegerArrayHypercube(b.getDimensions()), dw, op);
        }

        // No match between the cubes
        throw new IllegalArgumentException(
            "Given incompatible cubes: " +
            a.toDescriptiveString() + " vs " + b.toDescriptiveString()
        );
    }

    /**
     * Handle the given binary operation for two Integer cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Integer> intBinaryOp(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b,
        final Hypercube<Integer> r,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final IntegerHypercube db = (IntegerHypercube)b;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intBinaryOpHelper(da, db, dr, dw, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intBinaryOpHelper(da, db, dr, dw, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                final Integer vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case ADD: r.setObjectAt(i, va + vb);          break;
                    case SUB: r.setObjectAt(i, va - vb);          break;
                    case MUL: r.setObjectAt(i, va * vb);          break;
                    case DIV: r.setObjectAt(i, va / vb);          break;
                    case MOD: r.setObjectAt(i, va % vb);          break;
                    case MIN: r.setObjectAt(i, Math.min(va, vb)); break;
                    case MAX: r.setObjectAt(i, Math.max(va, vb)); break;
                    case POW: r.setObjectAt(i, (int)Math.pow(va, vb)); break;
                    case AND: r.setObjectAt(i, va & vb); break;
                    case OR:  r.setObjectAt(i, va | vb); break;
                    case XOR: r.setObjectAt(i, va ^ vb); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported binary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intUnaryOp} that performs a unary operation
     * on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is put into a second cube.
     */
    @SuppressWarnings("inline")
    private static void intUnaryOpHelper(
        final IntegerHypercube da,
        final IntegerHypercube dr,
        final UnaryOp op,
        final long    startIndex,
        final long    endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final int[] ar = new int[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case NEG:   for (int j=0; j < len; j++) ar[j] =         -aa[j];  break;
            case ABS:   for (int j=0; j < len; j++) ar[j] = Math.abs(aa[j]); break;
            case NOT:   for (int j=0; j < len; j++) ar[j] =  ~aa[j]; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported unary operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given unary operation for a Integer cube.
     */
    private static Hypercube<Integer> intUnaryOp(
        final Hypercube<Integer> a,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        return unaryOp(a, new IntegerArrayHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given unary operation for a Integer cube, putting the result into a
     * second.
     */
    private static Hypercube<Integer> intUnaryOp(
        final Hypercube<Integer> a,
        final Hypercube<Integer> r,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intUnaryOpHelper(da, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intUnaryOpHelper(da, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case NEG:   r.setObjectAt(i, -va); break;
                    case ABS:   r.setObjectAt(i, Math.abs(va)); break;
                    case NOT:   r.setObjectAt(i,  ~va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported unary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intComparisonOp} that performs a comparison
     * operation on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void intComparisonOpHelper(
        final IntegerHypercube da,
        final IntegerHypercube db,
        final BooleanHypercube dr,
        final ComparisonOp     op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[]  aa = new int [STAGING_SIZE];
        final int[]  ab = new int [STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            switch (op) {
            case EQ: for (int j=0; j < len; j++) ar[j] = (aa[j] == ab[j]); break;
            case NE: for (int j=0; j < len; j++) ar[j] = (aa[j] != ab[j]); break;
            case LT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  < (ab[j])); break;
            case GT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  > (ab[j])); break;
            case LE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) <= (ab[j])); break;
            case GE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) >= (ab[j])); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported comparison operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given boolean comparison operation for two Integer cubes.
     */
    private static Hypercube<Boolean> intComparisonOp(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b,
        final ComparisonOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + b.toDescriptiveString()
            );
        }

        // Create the destination, a simple bitset one by default
        return comparisonOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given binary operation for two Integer cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> intComparisonOp(
        final Hypercube<Integer> a,
        final Hypercube<Integer> b,
        final Hypercube<Boolean> r,
        final ComparisonOp       op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;
            final IntegerHypercube db = (IntegerHypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intComparisonOpHelper(da, db, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intComparisonOpHelper(da, db, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                final Integer vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case EQ: r.setObjectAt(i,  va.equals(vb)); break;
                    case NE: r.setObjectAt(i, !va.equals(vb)); break;
                    case LT: r.setObjectAt(i, (va)  < (vb)); break;
                    case GT: r.setObjectAt(i, (va)  > (vb)); break;
                    case LE: r.setObjectAt(i, (va) <= (vb)); break;
                    case GE: r.setObjectAt(i, (va) >= (vb)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported comparison operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // All comparison operattions with a NaN operand evaluate
                    // to false, except for the inequality operation (!=).
                    r.setObjectAt(i, null);
                    switch (op) {
                    case NE: r.setObjectAt(i, true);  break;
                    default: r.setObjectAt(i, false); break;
                    }
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive operation for a int cube, for the
     * given axes.
     */
    private static BooleanHypercube intReductiveLogicOpByAxes(
        final Hypercube<Integer> a,
        final int[] axes,
        final BooleanHypercube where,
        final ReductiveLogicOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[dstNDim];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final BooleanHypercube dst = new BooleanBitSetHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final boolean result = intReductiveLogicOp(a.slice(srcAccessors), whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index. If doing so would cause overflow then
                // we reset it to zero and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code intReductiveLogicOp} that performs a reductive
     * logical operation on a sub-array of the cube. The sub-array is specified
     * by parameters startIndex and endIndex, and the result is returned as a
     * boolean.
     */
    @SuppressWarnings("inline")
    private static boolean intReductiveLogicOpHelper(
        final IntegerHypercube da,
        final BooleanHypercube w,
        final ReductiveLogicOp op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final int[] aa = new int[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            if (w != null) {
                w.toFlattened(i, ww, 0, len);
            }

            switch (op) {
            case ANY: for (int j=0; j < len; j++) if (ww == null || ww[j]) r |= (aa[j] != 0); break;
            case ALL: for (int j=0; j < len; j++) if (ww == null || ww[j]) r &= (aa[j] != 0); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reductive logic operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given reductive logic operation for a Integer cube.
     */
    private static boolean intReductiveLogicOp(
        final Hypercube<Integer> a,
        final BooleanHypercube w,
        final ReductiveLogicOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final IntegerHypercube da = (IntegerHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = intReductiveLogicOpHelper(da, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final boolean[] ar = new boolean[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = intReductiveLogicOpHelper(da, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ANY: for (int j=0; j < NUM_THREADS; j++) r |= ar[j]; break;
                case ALL: for (int j=0; j < NUM_THREADS; j++) r &= ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported reductive logic operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Integer va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case ANY: r |= (va != 0); break;
                    case ALL: r &= (va != 0); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported reductive logic operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // Note that NaN always evaluates to true.
                    switch (op) {
                    case ANY: r = true; break;
                    default:
                        // Nothing to do.
                    }
                }
            }
        }

        // Always return the result
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code intExtract} that performs an extract operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube, starting
     * from the given offset index value.
     */
    @SuppressWarnings("inline")
    private static void intExtractHelper(
        final BooleanHypercube dc,
        final IntegerHypercube da,
        final IntegerHypercube dr,
        final long startIndex,
        final long endIndex,
        final long offset
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean         [] ac = new boolean         [STAGING_SIZE];
        final int[] aa = new int[STAGING_SIZE];
        final int[] ar = new int[STAGING_SIZE];

        // Define an offset to track our progress on r
        long offsetR = offset;

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            dc.toFlattened(i, ac, 0, len);
            da.toFlattened(i, aa, 0, len);

            int k = 0;
            for (int j=0; j < len; j++) {
                if (Boolean.TRUE.equals(ac[j])) {
                    ar[k++] = aa[j];
                }
            }

            // Copy in
            dr.fromFlattened(ar, 0, offsetR, k);
            offsetR += k;
        }
    }

    /**
     * Handle the extract operation for a Integer cube.
     */
    private static Hypercube<Integer> intExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Integer> a
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " + a.toDescriptiveString()
            );
        }

        // Create the destination, a simple Array one by default
        return intExtract(c, a, new IntegerArrayHypercube(Dimension.of(popcount(c))));
    }

    /**
     * Handle the extract operation for a Integer cube, putting the
     * result into a third.
     */
    private static Hypercube<Integer> intExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Integer> a,
        final Hypercube<Integer> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " +
                a.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube dc = (BooleanHypercube)c;
            final IntegerHypercube da = (IntegerHypercube)a;
            final IntegerHypercube dr = (IntegerHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                intExtractHelper(dc, da, dr, 0, a.getSize(), 0);
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // An offset to track where in r we should be, when processing
                // each bucket.
                long offset = 0;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final long offsetR = offset;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            intExtractHelper(dc, da, dr, startIndex, endIndex, offsetR);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });

                    // Update the offset value accordingly
                    offset += popcount(c, startIndex, endIndex);
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, j=0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean       vc = c.getObjectAt(i);
                final Integer va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(vc)) {
                    r.setObjectAt(j++, va);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static LongHypercube array(final LongHypercube cube)
    {
        return cube.array();
    }

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static LongHypercube array(final long[] array)
    {
        final LongHypercube result =
            new LongArrayHypercube(Dimension.of(array.length));
        result.fromFlattened(array);
        return result;
    }

    /**
     * Return a one dimensional cube with a range per the given value.
     */
    public static LongHypercube arange(final long stop)
    {
        return arange(0, stop);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static LongHypercube arange(
        final long start,
        final long stop
    )
    {
        return arange(start, stop, 1);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static LongHypercube arange(
        final long start,
        final long stop,
        final long step
    )
    {
        if (step == 0) {
            throw new IllegalArgumentException("Given a step of zero");
        }
        if (step < 0 && start < stop) {
            throw new IllegalArgumentException(
                "Step was negative but start was before stop"
            );
        }
        if (step > 0 && start > stop) {
            throw new IllegalArgumentException(
                "Step was positive but start was after stop"
            );
        }
        final long length = (long)((stop - start) / step);
        final LongHypercube cube = new LongArrayHypercube(Dimension.of(length));
        for (long i=0; i < length; i++) {
            cube.setAt(i, (long)(start + step * i));
        }
        return cube;
    }

    /**
     * Handle a vector multiply, also known as a dot product.
     */
    public static long longDotProd(
        final Hypercube<Long> a,
        final Hypercube<Long> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (a.getNDim() != 1 || !a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible or multi-dimensional cubes"
            );
        }

        try {
            final LongHypercube da = (LongHypercube)a;
            final LongHypercube db = (LongHypercube)b;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                final AtomicReference<Long> r = new AtomicReference<Long>();
                longDotProdHelper(da, db, r, 0, a.getSize());
                return r.get();
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();
                @SuppressWarnings("unchecked") final AtomicReference<Long>[] r =
                    (AtomicReference<Long>[])new AtomicReference<?>[NUM_THREADS];
                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }
                    final AtomicReference<Long> rv = new AtomicReference<>();
                    r[j] = rv;

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longDotProdHelper(da, db, rv, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // And sum up the result
                long sum = 0;
                for (AtomicReference<Long> rv : r) {
                    sum += rv.get().longValue();
                }
                return sum;
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Fall back to the simple version
            long sum = 0;
            for (long i = 0, sz = a.getSize(); i < sz; i++) {
                final Long va = a.getObjectAt(i);
                final Long vb = b.getObjectAt(i);
                if (va == null || vb == null) {
                    // For floats nulls are NaNs but they are zeroes for ints.
                    // Cheesy test to figure out which we are.
                    if (Double.isNaN(0L)) {
                        return 0L;
                    }
                }
                else {
                    sum += va * vb;
                }
            }
            return sum;
        }
    }

    /**
     * Helper function for {@code longDotProd} that performs the
     * dot product on two sub-arrays. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void longDotProdHelper(
        final LongHypercube da,
        final LongHypercube db,
        final AtomicReference<Long> r,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final long[] ab = new long[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            // Compute the dot product
            long sum = 0;
            for (int j=0; j < len; j++) {
                sum += aa[j] * ab[j];
            }

            // And give it back via the atomic
            r.set(Long.valueOf(sum));
        }
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Long> longMatMul(
        final Hypercube<Long> a,
        final Hypercube<Long> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[1].equals(bDims[0])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { aDims[0] };
                return longMatMul(a, b, new LongArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { bDims[0] };
                return longMatMul(a, b, new LongArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1]) &&
                aDims[1].equals(bDims[0]))
            {
               return longMatMul(
                    a, b,
                    new LongArrayHypercube(
                        new Dimension<?>[] { aDims[0], bDims[1] }
                    )
                );
            }
        }
        else if (a.getNDim() <= 1 && b.getNDim() <= 1) {
            // Nothing. We don't handle this case for matrix multiplication.
        }
        else if (a.getNDim() > 2 || b.getNDim() > 2) {
            // These need to be compatible in the corner dimensions like the 2D
            // case, and then other dimensions just need to match directly
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            final int nADim = aDims.length;
            final int nBDim = bDims.length;
            if ((bDims.length == 1 || aDims[nADim-1].equals(bDims[nBDim-2])) &&
                (aDims.length == 1 || aDims[nADim-2].equals(bDims[nBDim-1])))
            {
                final int maxNDim = Math.max(nADim, nBDim);
                final int minNDim = Math.min(nADim, nBDim);
                boolean matches = true;
                for (int i=0; matches && i < minNDim-2; i++) {
                    if (!aDims[i].equals(bDims[i])) {
                        matches = false;
                    }
                }
                if (matches) {
                    final Dimension<?>[] rDims;
                    final int offset;
                    if (aDims.length == 1 || bDims.length == 1) {
                        // We will lose a dimension when multiplying by a vector
                        rDims = new Dimension<?>[maxNDim-1];
                        rDims[rDims.length-1] =
                            (aDims.length == 1) ? bDims[bDims.length-2]
                                                : aDims[aDims.length-2];
                        offset = 1;
                    }
                    else {
                        rDims = new Dimension<?>[maxNDim];
                        rDims[rDims.length-1] = aDims[aDims.length-2];
                        rDims[rDims.length-2] = bDims[bDims.length-1];
                        offset = 0;
                    }
                    for (int i = 0; i < offset + rDims.length - 2; i++) {
                        rDims[i] = (aDims.length >= bDims.length) ? aDims[i] : bDims[i];
                    }
                    return longMatMul(a, b, new LongArrayHypercube(rDims));
                }
            }
        }

        // If we got here then we could not make the cubes match
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Long> longMatMul(
        final Hypercube<Long> a,
        final Hypercube<Long> b,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Input cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }
        if (!a.getElementType().equals(r.getElementType())) {
            throw new NullPointerException(
                "Input and return cubes have different element types: " +
                a.getElementType() + " vs " + r.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            LongHypercube dr = null;
            try {
                dr = (LongHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, aDims[1].at(0) };
            if (a.slice(iSlice).matches(b) && a.slice(jSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        dr.setAt(i, longDotProd(a.slice(iSlice), b));
                    }
                }
                else {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        r.setObjectAt(i, longDotProd(a.slice(iSlice), b));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            LongHypercube dr = null;
            try {
                dr = (LongHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { bDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.matches(b.slice(iSlice)) && b.slice(iSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setAt(i, longDotProd(a, b.slice(jSlice)));
                    }
                }
                else {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setObjectAt(i, longDotProd(a, b.slice(jSlice)));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.slice(aSlice).matches(b.slice(bSlice))) {
                // A regular matrix multiply where we compute the dot product of
                // the row and column to get their intersection coordinate's
                // value.
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                try {
                    final LongHypercube da = (LongHypercube)a;
                    final LongHypercube db = (LongHypercube)b;
                    final LongHypercube dr = (LongHypercube)r;

                    // We copy out the column from 'b' for faster access if it's
                    // small enough to fit into an array. 2^30 doubles is 16GB
                    // for one column which is totally possible for a non-square
                    // matrix but, we hope, most matrices will not be quite that
                    // big. We could use an array of arrays to handle that case
                    // but this is slower for the general case.
                    if (bDims[0].length() <= 1<<30) {
                        // Hand off to the smarter method which will copy out
                        // the column and use multi-threading
                        longMatMul2D(da, db, dr);
                    }
                    else {
                        // Where we start striding, see below.
                        ai[1] = bi[0] = 0;

                        // The stride through the flattened data, to walk a column
                        // in 'b'. We know that the format of the data is C-style in
                        // flattened form, so moving one row length in distance will
                        // step down one column index.
                        final long bs = bDims[1].length();

                        // Flipped the ordering of 'i' and 'j' since it's more cache
                        // efficient to copy out the column data (once) and then to
                        // stride through the rows each time.
                        da.preRead();
                        db.preRead();
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            for (long i=0; i < aDims[0].length(); i++) {
                                // We will stride through the two cubes pulling out
                                // the values for the sum directly, since we know
                                // their shape. This is much faster than going via
                                // the coordinate-based lookup. The stride in 'a' is
                                // 1 since it's walking along a row; in b it's the
                                // the row length, since it's walking along a column.
                                // Both will be the same number of steps so we only
                                // need to know when to stop walking in 'a'.
                                ai[0] = ri[0] = i;
                                long ao = da.toOffset(ai);
                                long sum = 0;
                                final long ae = ao + aDims[1].length();
                                for (long bo = db.toOffset(bi);
                                     ao < ae; ao++,
                                     bo += bs)
                                {
                                    sum += da.weakGetAt(ao) * db.weakGetAt(bo);
                                }
                                dr.set(sum, ri);
                            }
                        }
                    }
                }
                catch (ClassCastException e) {
                    // Need to be object-based
                    for (long i=0; i < aDims[0].length(); i++) {
                        ai[0] = ri[0] = i;
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            long sum = 0;
                            for (long k=0; k < bDims[0].length(); k++) {
                                ai[1] = bi[0] = k;
                                final Long va = a.getObj(ai);
                                final Long vb = b.getObj(bi);
                                if (va == null || vb == null) {
                                    // For floats nulls are NaNs but they are zeroes for ints.
                                    // Cheesy test to figure out which we are.
                                    if (Double.isNaN(0L)) {
                                        sum = 0L;
                                        break;
                                    }
                                }
                                else {
                                    sum += va * vb;
                                }
                            }
                            r.setObj(sum, ri);
                        }
                    }
                }

                // And give it back
                return r;
            }
        }
        else if ((a.getNDim() == r.getNDim() || a.getNDim()-1 == r.getNDim()) &&
                 a.getNDim() > b.getNDim())
        {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                longMatMul(a.slice(aSlice), b, r.slice(rSlice));
            }
            return r;
        }
        else if ((b.getNDim() == r.getNDim() || b.getNDim()-1 == r.getNDim()) &&
                 b.getNDim() > a.getNDim())
        {
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = bDims[0].length(); i < sz; i++) {
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                longMatMul(a, b.slice(bSlice), r.slice(rSlice));
            }
            return r;
        }
        else if (a.getNDim() == b.getNDim() && b.getNDim() == r.getNDim()) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                longMatMul(
                    a.slice(aSlice), b.slice(bSlice), r.slice(rSlice)
                );
            }
            return r;
        }

        // If we got here then we could not do anything
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given associative operation for a long cube, for the
     * given axes.
     */
    private static LongHypercube longAssociativeOpByAxes(
        final Hypercube<Long> a,
        final int[] axes,
        final Long initial,
        final BooleanHypercube where,
        final AssociativeOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[Math.max(1, dstNDim)];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final LongHypercube dst = new LongArrayHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final Long result =
                associativeOp(a.slice(srcAccessors), initial, whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index? If doing so would overflow the digit we
                // go back to zero for it and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code longAssociativeOp} that performs an associative
     * operation on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is returned as a long.
     */
    @SuppressWarnings("inline")
    private static long longAssociativeOpHelper(
        final LongHypercube da,
        final Long    i,
        final BooleanHypercube w,
        final AssociativeOp    op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        long r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Long.MAX_VALUE; break;
            case MAX:    r = Long.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long ii = startIndex; ii < endIndex; ii += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - ii, STAGING_SIZE);

            // Copy out
            da.toFlattened(ii, aa, 0, len);
            if (w != null) {
                w.toFlattened(ii, ww, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD:    for (int j=0; j < len; j++) if (ww == null || ww[j]) r += aa[j]; break;
            case MIN:    for (int j=0; j < len; j++) if (ww == null || ww[j]) r  = (r < aa[j] ? r : aa[j]); break;
            case MAX:    for (int j=0; j < len; j++) if (ww == null || ww[j]) r  = (r > aa[j] ? r : aa[j]); break;
            case NANADD: for (int j=0; j < len; j++) if (ww == null || ww[j]) r += (Double.isNaN(aa[j]) ? 0 : aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given associative operation for a long cube.
     */
    private static Long longAssociativeOp(
        final Hypercube<Long> a,
        final Long i,
        final BooleanHypercube w,
        final AssociativeOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // If there is a where value then it should match in shape
        if (w != null && !a.matchesInShape(w)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        long r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Long.MAX_VALUE; break;
            case MAX:    r = Long.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = longAssociativeOpHelper(da, i, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final long[] ar = new long[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = longAssociativeOpHelper(da, null, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ADD:    for (int j=0; j < NUM_THREADS; j++) r += ar[j];                     break;
                case MIN:    for (int j=0; j < NUM_THREADS; j++) r  = ((r < ar[j]) ? r : ar[j]); break;
                case MAX:    for (int j=0; j < NUM_THREADS; j++) r  = ((r > ar[j]) ? r : ar[j]); break;
                // The result of NaN functions on each thread can never be NaN.
                // So we can simply combine them with no NaN handling.
                case NANADD: for (int j=0; j < NUM_THREADS; j++) r += ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported associative operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            w.preRead();
            for (long ii = 0, size = a.getSize(); ii < size; ii++) {
                // Handle any 'where' clause
                if (w != null && !w.weakGetAt(ii)) {
                    continue;
                }

                // Need to handle missing values
                final Long va = a.getObjectAt(ii);
                if (va != null) {
                    switch (op) {
                    case ADD:    r += va;                           break;
                    case MIN:    r  = (r < va           ? r : va);  break;
                    case MAX:    r  = (r > va           ? r : va);  break;
                    case NANADD: r += (Double.isNaN(va) ? 0 : va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported associative operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    switch (op) {
                    case ADD: r = Long.valueOf(0L); break;
                    case MIN: r = Long.valueOf(0L); break;
                    case MAX: r = Long.valueOf(0L); break;
                    default:
                        // Don't do anything for NaN functions
                    }
                }
            }
        }

        // Always return the result
        return Long.valueOf(r);
    }

    /**
     * Handle a 2D matrix multiply.
     */
    private static void longMatMul2D(
        final LongHypercube a,
        final LongHypercube b,
        final LongHypercube r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        final Dimension<?>[] aDims = a.getDimensions();
        final Dimension<?>[] bDims = b.getDimensions();

        // We will copy out the column from 'b' for faster access, if it's small
        // enough to fit into an array. If we got here then the caller should
        // have handled getting this right for us.
        if (bDims[0].length() > 1<<30) {
            throw new IllegalArgumentException(
                "Axis was too large: " + bDims[0].length()
            );
        }
        final long[] bcol = new long[(int)bDims[0].length()];

        // The stride through the flattened data, to walk a column
        // in 'b'. We know that the format of the data is C-style in
        // flattened form, so moving one row length in distance will
        // step down one column index.
        final long bs = bDims[1].length();

        // Flipped the ordering of 'i' and 'j' since it's more cache
        // efficient to copy out the column data (once) and then to
        // stride through the rows each time.
        for (long j=0; j < bDims[1].length(); j++) {
            long bco = b.toOffset(0, j);
            b.preRead();
            for (int i=0; i < bcol.length; i++, bco += bs) {
                bcol[i] = b.weakGetAt(bco);
            }

            // We will stride through the two cubes pulling out the values
            // for the sum directly, since we know their shape. This is much
            // faster than going via the coordinate-based lookup. The stride in
            // 'a' is 1 since it's walking along a row; in b it's the the row
            // length, since it's walking along a column. Both will be the same
            // number of steps so we only need to know when to stop walking in
            // 'a'.
            //
            // Only use multi-threading if it is enabled and if the cubes are
            // large enough to justify the overhead, noting that matmul is
            // an O(n^3) operation.
            final long numRows = aDims[0].length();
            if (ourExecutorService == null ||
                (numRows * numRows * numRows) < THREADING_THRESHOLD)
            {
                // Where we start striding, see below
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                ai[1] = bi[0] = 0;
                bi[1] = ri[1] = j;

                a.preRead();
                for (long i=0; i < numRows; i++) {
                    ai[0] = ri[0] = i;
                    long ao = a.toOffset(ai);
                    long sum = 0;
                    for (int bo=0 ; bo < bcol.length; ao++, bo++) {
                        sum += a.weakGetAt(ao) * bcol[bo];
                    }
                    r.set(sum, ri);
                }
            }
            else {
                // How many threads to use. This should not be more than there
                // are rows.
                final int numThreads = (int)Math.min(numRows, NUM_THREADS);

                // Initialize a countdown to wait for all threads to finish
                // processing.
                final CountDownLatch latch = new CountDownLatch(numThreads);

                // Bucket size for each thread, we use +1 to kinda ceil the
                // result which gives a somewhat better distribution. We do
                // this in such a way as to avoid double-rounding errors.
                final long threadRows = numRows / numThreads;
                final long bucket = threadRows +
                                  ((threadRows * numThreads == numRows) ? 0 : 1);
                for (int t=0; t < numThreads; t++) {
                    final long startIndex = bucket * t;
                    final long endIndex =
                        (t == numThreads-1) ? numRows
                                            : Math.min(bucket * (t+1), numRows);

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    final long jf = j; // <-- "final j"
                    ourExecutorService.submit(() -> {
                        try {
                            final long[] ai = new long[] { 0,  0 };
                            final long[] bi = new long[] { 0, jf };
                            final long[] ri = new long[] { 0, jf };
                            a.preRead();
                            for (long i = startIndex; i < endIndex; i++) {
                                ai[0] = ri[0] = i;
                                long ao = a.toOffset(ai);
                                long sum = 0;
                                for (int bo=0; bo < bcol.length; ao++, bo++) {
                                    sum += a.weakGetAt(ao) * bcol[bo];
                                }
                                r.set(sum, ri);
                            }
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Return a new long cube of given dimensions where all elements are
     * equal to a given long value.
     *
     * <p>This method is an alias of {@code broadcast()} and is equivalent to
     * {@code numpy.full()} method.
     *
     * @return The resulting long cube.
     */
    public static Hypercube<Long> full(
        final Dimension<?>[] dimensions,
        final long value
    )
    {
        return broadcast(dimensions, value);
    }

    /**
     * Return a new long cube of given dimensions where all elements are
     * equal to a given long value.
     *
     * @return The resulting long cube.
     */
    public static Hypercube<Long> broadcast(
        final Dimension<?>[] dimensions,
        final long value
    )
    {
        final LongHypercube a = new LongArrayHypercube(dimensions);
        a.fill(value);
        return a;
    }

    /**
     * Return a new 1-dimensional long cube of a given size where all
     * elements are equal to a given long value.
     *
     * <p>This is equivalent to calling {@code broadcast(Dimension.of(size), value)}.
     *
     * @return The resulting long cube.
     */
    public static Hypercube<Long> broadcast(
        final long size,
        final long value
    )
    {
        return broadcast(Dimension.of(size), value);
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longBinaryOp} that performs a binary operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void longBinaryOpHelper(
        final LongHypercube da,
        final LongHypercube db,
        final LongHypercube dr,
        final BooleanHypercube dw,
        final BinaryOp op,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final long[] ab = new long[STAGING_SIZE];
        final long[] ar = new long[STAGING_SIZE];
        final boolean[] aw = (dw == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);
            if (dw != null) {
                dw.toFlattened(i, aw, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] + ab[j];          break;
            case SUB: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] - ab[j];          break;
            case MUL: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] * ab[j];          break;
            case DIV: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] / ab[j];          break;
            case MOD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] % ab[j];          break;
            case MIN: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.min(aa[j], ab[j]); break;
            case MAX: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.max(aa[j], ab[j]); break;
            case POW: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = (long)Math.pow(aa[j], ab[j]); break;
            case AND: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] & ab[j]; break;
            case OR:  for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] | ab[j]; break;
            case XOR: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] ^ ab[j]; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported binary operation: " + op
                );
            }

            // Copy in
            if (aw == null) {
                dr.fromFlattened(ar, 0, i, len);
            }
            else {
                for (int j=0; j < len; j++) {
                    if (aw[j]) {
                        dr.setAt(i + j, ar[j]);
                    }
                }
            }
        }
    }

    /**
     * Handle the given binary operation for two Long cubes.
     */
    private static Hypercube<Long> longBinaryOp(
        final Hypercube<Long> a,
        final Hypercube<Long> b,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // Depending on which cube is a non-strict supercube of the other, create a simple
        // Array destination one as a copy of the appropriate argument
        if (a.submatches(b)) {
            return binaryOp(a, b, new LongArrayHypercube(a.getDimensions()), dw, op);
        }
        if (b.submatches(a)) {
            return binaryOp(a, b, new LongArrayHypercube(b.getDimensions()), dw, op);
        }

        // No match between the cubes
        throw new IllegalArgumentException(
            "Given incompatible cubes: " +
            a.toDescriptiveString() + " vs " + b.toDescriptiveString()
        );
    }

    /**
     * Handle the given binary operation for two Long cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Long> longBinaryOp(
        final Hypercube<Long> a,
        final Hypercube<Long> b,
        final Hypercube<Long> r,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final LongHypercube db = (LongHypercube)b;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longBinaryOpHelper(da, db, dr, dw, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longBinaryOpHelper(da, db, dr, dw, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                final Long vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case ADD: r.setObjectAt(i, va + vb);          break;
                    case SUB: r.setObjectAt(i, va - vb);          break;
                    case MUL: r.setObjectAt(i, va * vb);          break;
                    case DIV: r.setObjectAt(i, va / vb);          break;
                    case MOD: r.setObjectAt(i, va % vb);          break;
                    case MIN: r.setObjectAt(i, Math.min(va, vb)); break;
                    case MAX: r.setObjectAt(i, Math.max(va, vb)); break;
                    case POW: r.setObjectAt(i, (long)Math.pow(va, vb)); break;
                    case AND: r.setObjectAt(i, va & vb); break;
                    case OR:  r.setObjectAt(i, va | vb); break;
                    case XOR: r.setObjectAt(i, va ^ vb); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported binary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longUnaryOp} that performs a unary operation
     * on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is put into a second cube.
     */
    @SuppressWarnings("inline")
    private static void longUnaryOpHelper(
        final LongHypercube da,
        final LongHypercube dr,
        final UnaryOp op,
        final long    startIndex,
        final long    endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final long[] ar = new long[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case NEG:   for (int j=0; j < len; j++) ar[j] =         -aa[j];  break;
            case ABS:   for (int j=0; j < len; j++) ar[j] = Math.abs(aa[j]); break;
            case NOT:   for (int j=0; j < len; j++) ar[j] =  ~aa[j]; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported unary operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given unary operation for a Long cube.
     */
    private static Hypercube<Long> longUnaryOp(
        final Hypercube<Long> a,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        return unaryOp(a, new LongArrayHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given unary operation for a Long cube, putting the result into a
     * second.
     */
    private static Hypercube<Long> longUnaryOp(
        final Hypercube<Long> a,
        final Hypercube<Long> r,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longUnaryOpHelper(da, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longUnaryOpHelper(da, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case NEG:   r.setObjectAt(i, -va); break;
                    case ABS:   r.setObjectAt(i, Math.abs(va)); break;
                    case NOT:   r.setObjectAt(i,  ~va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported unary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longComparisonOp} that performs a comparison
     * operation on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void longComparisonOpHelper(
        final LongHypercube da,
        final LongHypercube db,
        final BooleanHypercube dr,
        final ComparisonOp     op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[]  aa = new long [STAGING_SIZE];
        final long[]  ab = new long [STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            switch (op) {
            case EQ: for (int j=0; j < len; j++) ar[j] = (aa[j] == ab[j]); break;
            case NE: for (int j=0; j < len; j++) ar[j] = (aa[j] != ab[j]); break;
            case LT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  < (ab[j])); break;
            case GT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  > (ab[j])); break;
            case LE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) <= (ab[j])); break;
            case GE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) >= (ab[j])); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported comparison operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given boolean comparison operation for two Long cubes.
     */
    private static Hypercube<Boolean> longComparisonOp(
        final Hypercube<Long> a,
        final Hypercube<Long> b,
        final ComparisonOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + b.toDescriptiveString()
            );
        }

        // Create the destination, a simple bitset one by default
        return comparisonOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given binary operation for two Long cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> longComparisonOp(
        final Hypercube<Long> a,
        final Hypercube<Long> b,
        final Hypercube<Boolean> r,
        final ComparisonOp       op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;
            final LongHypercube db = (LongHypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longComparisonOpHelper(da, db, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longComparisonOpHelper(da, db, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                final Long vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case EQ: r.setObjectAt(i,  va.equals(vb)); break;
                    case NE: r.setObjectAt(i, !va.equals(vb)); break;
                    case LT: r.setObjectAt(i, (va)  < (vb)); break;
                    case GT: r.setObjectAt(i, (va)  > (vb)); break;
                    case LE: r.setObjectAt(i, (va) <= (vb)); break;
                    case GE: r.setObjectAt(i, (va) >= (vb)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported comparison operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // All comparison operattions with a NaN operand evaluate
                    // to false, except for the inequality operation (!=).
                    r.setObjectAt(i, null);
                    switch (op) {
                    case NE: r.setObjectAt(i, true);  break;
                    default: r.setObjectAt(i, false); break;
                    }
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive operation for a long cube, for the
     * given axes.
     */
    private static BooleanHypercube longReductiveLogicOpByAxes(
        final Hypercube<Long> a,
        final int[] axes,
        final BooleanHypercube where,
        final ReductiveLogicOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[dstNDim];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final BooleanHypercube dst = new BooleanBitSetHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final boolean result = longReductiveLogicOp(a.slice(srcAccessors), whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index. If doing so would cause overflow then
                // we reset it to zero and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code longReductiveLogicOp} that performs a reductive
     * logical operation on a sub-array of the cube. The sub-array is specified
     * by parameters startIndex and endIndex, and the result is returned as a
     * boolean.
     */
    @SuppressWarnings("inline")
    private static boolean longReductiveLogicOpHelper(
        final LongHypercube da,
        final BooleanHypercube w,
        final ReductiveLogicOp op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final long[] aa = new long[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            if (w != null) {
                w.toFlattened(i, ww, 0, len);
            }

            switch (op) {
            case ANY: for (int j=0; j < len; j++) if (ww == null || ww[j]) r |= (aa[j] != 0); break;
            case ALL: for (int j=0; j < len; j++) if (ww == null || ww[j]) r &= (aa[j] != 0); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reductive logic operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given reductive logic operation for a Long cube.
     */
    private static boolean longReductiveLogicOp(
        final Hypercube<Long> a,
        final BooleanHypercube w,
        final ReductiveLogicOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final LongHypercube da = (LongHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = longReductiveLogicOpHelper(da, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final boolean[] ar = new boolean[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = longReductiveLogicOpHelper(da, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ANY: for (int j=0; j < NUM_THREADS; j++) r |= ar[j]; break;
                case ALL: for (int j=0; j < NUM_THREADS; j++) r &= ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported reductive logic operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Long va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case ANY: r |= (va != 0); break;
                    case ALL: r &= (va != 0); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported reductive logic operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // Note that NaN always evaluates to true.
                    switch (op) {
                    case ANY: r = true; break;
                    default:
                        // Nothing to do.
                    }
                }
            }
        }

        // Always return the result
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code longExtract} that performs an extract operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube, starting
     * from the given offset index value.
     */
    @SuppressWarnings("inline")
    private static void longExtractHelper(
        final BooleanHypercube dc,
        final LongHypercube da,
        final LongHypercube dr,
        final long startIndex,
        final long endIndex,
        final long offset
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean         [] ac = new boolean         [STAGING_SIZE];
        final long[] aa = new long[STAGING_SIZE];
        final long[] ar = new long[STAGING_SIZE];

        // Define an offset to track our progress on r
        long offsetR = offset;

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            dc.toFlattened(i, ac, 0, len);
            da.toFlattened(i, aa, 0, len);

            int k = 0;
            for (int j=0; j < len; j++) {
                if (Boolean.TRUE.equals(ac[j])) {
                    ar[k++] = aa[j];
                }
            }

            // Copy in
            dr.fromFlattened(ar, 0, offsetR, k);
            offsetR += k;
        }
    }

    /**
     * Handle the extract operation for a Long cube.
     */
    private static Hypercube<Long> longExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Long> a
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " + a.toDescriptiveString()
            );
        }

        // Create the destination, a simple Array one by default
        return longExtract(c, a, new LongArrayHypercube(Dimension.of(popcount(c))));
    }

    /**
     * Handle the extract operation for a Long cube, putting the
     * result into a third.
     */
    private static Hypercube<Long> longExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Long> a,
        final Hypercube<Long> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " +
                a.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube dc = (BooleanHypercube)c;
            final LongHypercube da = (LongHypercube)a;
            final LongHypercube dr = (LongHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                longExtractHelper(dc, da, dr, 0, a.getSize(), 0);
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // An offset to track where in r we should be, when processing
                // each bucket.
                long offset = 0;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final long offsetR = offset;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            longExtractHelper(dc, da, dr, startIndex, endIndex, offsetR);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });

                    // Update the offset value accordingly
                    offset += popcount(c, startIndex, endIndex);
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, j=0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean       vc = c.getObjectAt(i);
                final Long va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(vc)) {
                    r.setObjectAt(j++, va);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static FloatHypercube array(final FloatHypercube cube)
    {
        return cube.array();
    }

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static FloatHypercube array(final float[] array)
    {
        final FloatHypercube result =
            new FloatArrayHypercube(Dimension.of(array.length));
        result.fromFlattened(array);
        return result;
    }

    /**
     * Return a one dimensional cube with a range per the given value.
     */
    public static FloatHypercube arange(final float stop)
    {
        return arange(0, stop);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static FloatHypercube arange(
        final float start,
        final float stop
    )
    {
        return arange(start, stop, 1);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static FloatHypercube arange(
        final float start,
        final float stop,
        final float step
    )
    {
        if (step == 0) {
            throw new IllegalArgumentException("Given a step of zero");
        }
        if (step < 0 && start < stop) {
            throw new IllegalArgumentException(
                "Step was negative but start was before stop"
            );
        }
        if (step > 0 && start > stop) {
            throw new IllegalArgumentException(
                "Step was positive but start was after stop"
            );
        }
        final long length = (long)((stop - start) / step);
        final FloatHypercube cube = new FloatArrayHypercube(Dimension.of(length));
        for (long i=0; i < length; i++) {
            cube.setAt(i, (float)(start + step * i));
        }
        return cube;
    }

    /**
     * Handle a vector multiply, also known as a dot product.
     */
    public static float floatDotProd(
        final Hypercube<Float> a,
        final Hypercube<Float> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (a.getNDim() != 1 || !a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible or multi-dimensional cubes"
            );
        }

        try {
            final FloatHypercube da = (FloatHypercube)a;
            final FloatHypercube db = (FloatHypercube)b;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                final AtomicReference<Float> r = new AtomicReference<Float>();
                floatDotProdHelper(da, db, r, 0, a.getSize());
                return r.get();
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();
                @SuppressWarnings("unchecked") final AtomicReference<Float>[] r =
                    (AtomicReference<Float>[])new AtomicReference<?>[NUM_THREADS];
                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }
                    final AtomicReference<Float> rv = new AtomicReference<>();
                    r[j] = rv;

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatDotProdHelper(da, db, rv, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // And sum up the result
                float sum = 0;
                for (AtomicReference<Float> rv : r) {
                    sum += rv.get().floatValue();
                }
                return sum;
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Fall back to the simple version
            float sum = 0;
            for (long i = 0, sz = a.getSize(); i < sz; i++) {
                final Float va = a.getObjectAt(i);
                final Float vb = b.getObjectAt(i);
                if (va == null || vb == null) {
                    // For floats nulls are NaNs but they are zeroes for ints.
                    // Cheesy test to figure out which we are.
                    if (Double.isNaN(Float.NaN)) {
                        return Float.NaN;
                    }
                }
                else {
                    sum += va * vb;
                }
            }
            return sum;
        }
    }

    /**
     * Helper function for {@code floatDotProd} that performs the
     * dot product on two sub-arrays. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void floatDotProdHelper(
        final FloatHypercube da,
        final FloatHypercube db,
        final AtomicReference<Float> r,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final float[] ab = new float[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            // Compute the dot product
            float sum = 0;
            for (int j=0; j < len; j++) {
                sum += aa[j] * ab[j];
            }

            // And give it back via the atomic
            r.set(Float.valueOf(sum));
        }
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Float> floatMatMul(
        final Hypercube<Float> a,
        final Hypercube<Float> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[1].equals(bDims[0])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { aDims[0] };
                return floatMatMul(a, b, new FloatArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { bDims[0] };
                return floatMatMul(a, b, new FloatArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1]) &&
                aDims[1].equals(bDims[0]))
            {
               return floatMatMul(
                    a, b,
                    new FloatArrayHypercube(
                        new Dimension<?>[] { aDims[0], bDims[1] }
                    )
                );
            }
        }
        else if (a.getNDim() <= 1 && b.getNDim() <= 1) {
            // Nothing. We don't handle this case for matrix multiplication.
        }
        else if (a.getNDim() > 2 || b.getNDim() > 2) {
            // These need to be compatible in the corner dimensions like the 2D
            // case, and then other dimensions just need to match directly
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            final int nADim = aDims.length;
            final int nBDim = bDims.length;
            if ((bDims.length == 1 || aDims[nADim-1].equals(bDims[nBDim-2])) &&
                (aDims.length == 1 || aDims[nADim-2].equals(bDims[nBDim-1])))
            {
                final int maxNDim = Math.max(nADim, nBDim);
                final int minNDim = Math.min(nADim, nBDim);
                boolean matches = true;
                for (int i=0; matches && i < minNDim-2; i++) {
                    if (!aDims[i].equals(bDims[i])) {
                        matches = false;
                    }
                }
                if (matches) {
                    final Dimension<?>[] rDims;
                    final int offset;
                    if (aDims.length == 1 || bDims.length == 1) {
                        // We will lose a dimension when multiplying by a vector
                        rDims = new Dimension<?>[maxNDim-1];
                        rDims[rDims.length-1] =
                            (aDims.length == 1) ? bDims[bDims.length-2]
                                                : aDims[aDims.length-2];
                        offset = 1;
                    }
                    else {
                        rDims = new Dimension<?>[maxNDim];
                        rDims[rDims.length-1] = aDims[aDims.length-2];
                        rDims[rDims.length-2] = bDims[bDims.length-1];
                        offset = 0;
                    }
                    for (int i = 0; i < offset + rDims.length - 2; i++) {
                        rDims[i] = (aDims.length >= bDims.length) ? aDims[i] : bDims[i];
                    }
                    return floatMatMul(a, b, new FloatArrayHypercube(rDims));
                }
            }
        }

        // If we got here then we could not make the cubes match
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Float> floatMatMul(
        final Hypercube<Float> a,
        final Hypercube<Float> b,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Input cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }
        if (!a.getElementType().equals(r.getElementType())) {
            throw new NullPointerException(
                "Input and return cubes have different element types: " +
                a.getElementType() + " vs " + r.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            FloatHypercube dr = null;
            try {
                dr = (FloatHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, aDims[1].at(0) };
            if (a.slice(iSlice).matches(b) && a.slice(jSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        dr.setAt(i, floatDotProd(a.slice(iSlice), b));
                    }
                }
                else {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        r.setObjectAt(i, floatDotProd(a.slice(iSlice), b));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            FloatHypercube dr = null;
            try {
                dr = (FloatHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { bDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.matches(b.slice(iSlice)) && b.slice(iSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setAt(i, floatDotProd(a, b.slice(jSlice)));
                    }
                }
                else {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setObjectAt(i, floatDotProd(a, b.slice(jSlice)));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.slice(aSlice).matches(b.slice(bSlice))) {
                // A regular matrix multiply where we compute the dot product of
                // the row and column to get their intersection coordinate's
                // value.
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                try {
                    final FloatHypercube da = (FloatHypercube)a;
                    final FloatHypercube db = (FloatHypercube)b;
                    final FloatHypercube dr = (FloatHypercube)r;

                    // We copy out the column from 'b' for faster access if it's
                    // small enough to fit into an array. 2^30 doubles is 16GB
                    // for one column which is totally possible for a non-square
                    // matrix but, we hope, most matrices will not be quite that
                    // big. We could use an array of arrays to handle that case
                    // but this is slower for the general case.
                    if (bDims[0].length() <= 1<<30) {
                        // Hand off to the smarter method which will copy out
                        // the column and use multi-threading
                        floatMatMul2D(da, db, dr);
                    }
                    else {
                        // Where we start striding, see below.
                        ai[1] = bi[0] = 0;

                        // The stride through the flattened data, to walk a column
                        // in 'b'. We know that the format of the data is C-style in
                        // flattened form, so moving one row length in distance will
                        // step down one column index.
                        final long bs = bDims[1].length();

                        // Flipped the ordering of 'i' and 'j' since it's more cache
                        // efficient to copy out the column data (once) and then to
                        // stride through the rows each time.
                        da.preRead();
                        db.preRead();
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            for (long i=0; i < aDims[0].length(); i++) {
                                // We will stride through the two cubes pulling out
                                // the values for the sum directly, since we know
                                // their shape. This is much faster than going via
                                // the coordinate-based lookup. The stride in 'a' is
                                // 1 since it's walking along a row; in b it's the
                                // the row length, since it's walking along a column.
                                // Both will be the same number of steps so we only
                                // need to know when to stop walking in 'a'.
                                ai[0] = ri[0] = i;
                                long ao = da.toOffset(ai);
                                float sum = 0;
                                final long ae = ao + aDims[1].length();
                                for (long bo = db.toOffset(bi);
                                     ao < ae; ao++,
                                     bo += bs)
                                {
                                    sum += da.weakGetAt(ao) * db.weakGetAt(bo);
                                }
                                dr.set(sum, ri);
                            }
                        }
                    }
                }
                catch (ClassCastException e) {
                    // Need to be object-based
                    for (long i=0; i < aDims[0].length(); i++) {
                        ai[0] = ri[0] = i;
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            float sum = 0;
                            for (long k=0; k < bDims[0].length(); k++) {
                                ai[1] = bi[0] = k;
                                final Float va = a.getObj(ai);
                                final Float vb = b.getObj(bi);
                                if (va == null || vb == null) {
                                    // For floats nulls are NaNs but they are zeroes for ints.
                                    // Cheesy test to figure out which we are.
                                    if (Double.isNaN(Float.NaN)) {
                                        sum = Float.NaN;
                                        break;
                                    }
                                }
                                else {
                                    sum += va * vb;
                                }
                            }
                            r.setObj(sum, ri);
                        }
                    }
                }

                // And give it back
                return r;
            }
        }
        else if ((a.getNDim() == r.getNDim() || a.getNDim()-1 == r.getNDim()) &&
                 a.getNDim() > b.getNDim())
        {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                floatMatMul(a.slice(aSlice), b, r.slice(rSlice));
            }
            return r;
        }
        else if ((b.getNDim() == r.getNDim() || b.getNDim()-1 == r.getNDim()) &&
                 b.getNDim() > a.getNDim())
        {
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = bDims[0].length(); i < sz; i++) {
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                floatMatMul(a, b.slice(bSlice), r.slice(rSlice));
            }
            return r;
        }
        else if (a.getNDim() == b.getNDim() && b.getNDim() == r.getNDim()) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                floatMatMul(
                    a.slice(aSlice), b.slice(bSlice), r.slice(rSlice)
                );
            }
            return r;
        }

        // If we got here then we could not do anything
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given associative operation for a float cube, for the
     * given axes.
     */
    private static FloatHypercube floatAssociativeOpByAxes(
        final Hypercube<Float> a,
        final int[] axes,
        final Float initial,
        final BooleanHypercube where,
        final AssociativeOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[Math.max(1, dstNDim)];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final FloatHypercube dst = new FloatArrayHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final Float result =
                associativeOp(a.slice(srcAccessors), initial, whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index? If doing so would overflow the digit we
                // go back to zero for it and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code floatAssociativeOp} that performs an associative
     * operation on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is returned as a float.
     */
    @SuppressWarnings("inline")
    private static float floatAssociativeOpHelper(
        final FloatHypercube da,
        final Float    i,
        final BooleanHypercube w,
        final AssociativeOp    op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        float r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Float.MAX_VALUE; break;
            case MAX:    r = Float.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long ii = startIndex; ii < endIndex; ii += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - ii, STAGING_SIZE);

            // Copy out
            da.toFlattened(ii, aa, 0, len);
            if (w != null) {
                w.toFlattened(ii, ww, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD:    for (int j=0; j < len && !Float.isNaN(r); j++) if (ww == null || ww[j]) r += aa[j]; break;
            case MIN:    for (int j=0; j < len && !Float.isNaN(r); j++) if (ww == null || ww[j]) r  = (r < aa[j] ? r : aa[j]); break;
            case MAX:    for (int j=0; j < len && !Float.isNaN(r); j++) if (ww == null || ww[j]) r  = (r > aa[j] ? r : aa[j]); break;
            case NANADD: for (int j=0; j < len; j++) if (ww == null || ww[j]) r += (Float.isNaN(aa[j]) ? 0 : aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given associative operation for a float cube.
     */
    private static Float floatAssociativeOp(
        final Hypercube<Float> a,
        final Float i,
        final BooleanHypercube w,
        final AssociativeOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // If there is a where value then it should match in shape
        if (w != null && !a.matchesInShape(w)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        float r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Float.MAX_VALUE; break;
            case MAX:    r = Float.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = floatAssociativeOpHelper(da, i, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final float[] ar = new float[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = floatAssociativeOpHelper(da, null, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ADD:    for (int j=0; j < NUM_THREADS && !Float.isNaN(r); j++) r += ar[j];                     break;
                case MIN:    for (int j=0; j < NUM_THREADS && !Float.isNaN(r); j++) r  = ((r < ar[j]) ? r : ar[j]); break;
                case MAX:    for (int j=0; j < NUM_THREADS && !Float.isNaN(r); j++) r  = ((r > ar[j]) ? r : ar[j]); break;
                // The result of NaN functions on each thread can never be NaN.
                // So we can simply combine them with no NaN handling.
                case NANADD: for (int j=0; j < NUM_THREADS; j++) r += ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported associative operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            w.preRead();
            for (long ii = 0, size = a.getSize(); ii < size && !Float.isNaN(r); ii++) {
                // Handle any 'where' clause
                if (w != null && !w.weakGetAt(ii)) {
                    continue;
                }

                // Need to handle missing values
                final Float va = a.getObjectAt(ii);
                if (va != null) {
                    switch (op) {
                    case ADD:    r += va;                           break;
                    case MIN:    r  = (r < va           ? r : va);  break;
                    case MAX:    r  = (r > va           ? r : va);  break;
                    case NANADD: r += (Float.isNaN(va) ? 0 : va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported associative operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    switch (op) {
                    case ADD: r = Float.valueOf(Float.NaN); break;
                    case MIN: r = Float.valueOf(Float.NaN); break;
                    case MAX: r = Float.valueOf(Float.NaN); break;
                    default:
                        // Don't do anything for NaN functions
                    }
                }
            }
        }

        // Always return the result
        return Float.valueOf(r);
    }

    /**
     * Handle a 2D matrix multiply.
     */
    private static void floatMatMul2D(
        final FloatHypercube a,
        final FloatHypercube b,
        final FloatHypercube r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        final Dimension<?>[] aDims = a.getDimensions();
        final Dimension<?>[] bDims = b.getDimensions();

        // We will copy out the column from 'b' for faster access, if it's small
        // enough to fit into an array. If we got here then the caller should
        // have handled getting this right for us.
        if (bDims[0].length() > 1<<30) {
            throw new IllegalArgumentException(
                "Axis was too large: " + bDims[0].length()
            );
        }
        final float[] bcol = new float[(int)bDims[0].length()];

        // The stride through the flattened data, to walk a column
        // in 'b'. We know that the format of the data is C-style in
        // flattened form, so moving one row length in distance will
        // step down one column index.
        final long bs = bDims[1].length();

        // Flipped the ordering of 'i' and 'j' since it's more cache
        // efficient to copy out the column data (once) and then to
        // stride through the rows each time.
        for (long j=0; j < bDims[1].length(); j++) {
            long bco = b.toOffset(0, j);
            b.preRead();
            for (int i=0; i < bcol.length; i++, bco += bs) {
                bcol[i] = b.weakGetAt(bco);
            }

            // We will stride through the two cubes pulling out the values
            // for the sum directly, since we know their shape. This is much
            // faster than going via the coordinate-based lookup. The stride in
            // 'a' is 1 since it's walking along a row; in b it's the the row
            // length, since it's walking along a column. Both will be the same
            // number of steps so we only need to know when to stop walking in
            // 'a'.
            //
            // Only use multi-threading if it is enabled and if the cubes are
            // large enough to justify the overhead, noting that matmul is
            // an O(n^3) operation.
            final long numRows = aDims[0].length();
            if (ourExecutorService == null ||
                (numRows * numRows * numRows) < THREADING_THRESHOLD)
            {
                // Where we start striding, see below
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                ai[1] = bi[0] = 0;
                bi[1] = ri[1] = j;

                a.preRead();
                for (long i=0; i < numRows; i++) {
                    ai[0] = ri[0] = i;
                    long ao = a.toOffset(ai);
                    float sum = 0;
                    for (int bo=0 ; bo < bcol.length; ao++, bo++) {
                        sum += a.weakGetAt(ao) * bcol[bo];
                    }
                    r.set(sum, ri);
                }
            }
            else {
                // How many threads to use. This should not be more than there
                // are rows.
                final int numThreads = (int)Math.min(numRows, NUM_THREADS);

                // Initialize a countdown to wait for all threads to finish
                // processing.
                final CountDownLatch latch = new CountDownLatch(numThreads);

                // Bucket size for each thread, we use +1 to kinda ceil the
                // result which gives a somewhat better distribution. We do
                // this in such a way as to avoid double-rounding errors.
                final long threadRows = numRows / numThreads;
                final long bucket = threadRows +
                                  ((threadRows * numThreads == numRows) ? 0 : 1);
                for (int t=0; t < numThreads; t++) {
                    final long startIndex = bucket * t;
                    final long endIndex =
                        (t == numThreads-1) ? numRows
                                            : Math.min(bucket * (t+1), numRows);

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    final long jf = j; // <-- "final j"
                    ourExecutorService.submit(() -> {
                        try {
                            final long[] ai = new long[] { 0,  0 };
                            final long[] bi = new long[] { 0, jf };
                            final long[] ri = new long[] { 0, jf };
                            a.preRead();
                            for (long i = startIndex; i < endIndex; i++) {
                                ai[0] = ri[0] = i;
                                long ao = a.toOffset(ai);
                                float sum = 0;
                                for (int bo=0; bo < bcol.length; ao++, bo++) {
                                    sum += a.weakGetAt(ao) * bcol[bo];
                                }
                                r.set(sum, ri);
                            }
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Return a new float cube of given dimensions where all elements are
     * equal to a given float value.
     *
     * <p>This method is an alias of {@code broadcast()} and is equivalent to
     * {@code numpy.full()} method.
     *
     * @return The resulting float cube.
     */
    public static Hypercube<Float> full(
        final Dimension<?>[] dimensions,
        final float value
    )
    {
        return broadcast(dimensions, value);
    }

    /**
     * Return a new float cube of given dimensions where all elements are
     * equal to a given float value.
     *
     * @return The resulting float cube.
     */
    public static Hypercube<Float> broadcast(
        final Dimension<?>[] dimensions,
        final float value
    )
    {
        final FloatHypercube a = new FloatArrayHypercube(dimensions);
        a.fill(value);
        return a;
    }

    /**
     * Return a new 1-dimensional float cube of a given size where all
     * elements are equal to a given float value.
     *
     * <p>This is equivalent to calling {@code broadcast(Dimension.of(size), value)}.
     *
     * @return The resulting float cube.
     */
    public static Hypercube<Float> broadcast(
        final long size,
        final float value
    )
    {
        return broadcast(Dimension.of(size), value);
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatBinaryOp} that performs a binary operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void floatBinaryOpHelper(
        final FloatHypercube da,
        final FloatHypercube db,
        final FloatHypercube dr,
        final BooleanHypercube dw,
        final BinaryOp op,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final float[] ab = new float[STAGING_SIZE];
        final float[] ar = new float[STAGING_SIZE];
        final boolean[] aw = (dw == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);
            if (dw != null) {
                dw.toFlattened(i, aw, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] + ab[j];          break;
            case SUB: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] - ab[j];          break;
            case MUL: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] * ab[j];          break;
            case DIV: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] / ab[j];          break;
            case MOD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] % ab[j];          break;
            case MIN: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.min(aa[j], ab[j]); break;
            case MAX: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.max(aa[j], ab[j]); break;
            case POW: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = (float)Math.pow(aa[j], ab[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported binary operation: " + op
                );
            }

            // Copy in
            if (aw == null) {
                dr.fromFlattened(ar, 0, i, len);
            }
            else {
                for (int j=0; j < len; j++) {
                    if (aw[j]) {
                        dr.setAt(i + j, ar[j]);
                    }
                }
            }
        }
    }

    /**
     * Handle the given binary operation for two Float cubes.
     */
    private static Hypercube<Float> floatBinaryOp(
        final Hypercube<Float> a,
        final Hypercube<Float> b,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // Depending on which cube is a non-strict supercube of the other, create a simple
        // Array destination one as a copy of the appropriate argument
        if (a.submatches(b)) {
            return binaryOp(a, b, new FloatArrayHypercube(a.getDimensions()), dw, op);
        }
        if (b.submatches(a)) {
            return binaryOp(a, b, new FloatArrayHypercube(b.getDimensions()), dw, op);
        }

        // No match between the cubes
        throw new IllegalArgumentException(
            "Given incompatible cubes: " +
            a.toDescriptiveString() + " vs " + b.toDescriptiveString()
        );
    }

    /**
     * Handle the given binary operation for two Float cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Float> floatBinaryOp(
        final Hypercube<Float> a,
        final Hypercube<Float> b,
        final Hypercube<Float> r,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final FloatHypercube db = (FloatHypercube)b;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatBinaryOpHelper(da, db, dr, dw, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatBinaryOpHelper(da, db, dr, dw, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                final Float vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case ADD: r.setObjectAt(i, va + vb);          break;
                    case SUB: r.setObjectAt(i, va - vb);          break;
                    case MUL: r.setObjectAt(i, va * vb);          break;
                    case DIV: r.setObjectAt(i, va / vb);          break;
                    case MOD: r.setObjectAt(i, va % vb);          break;
                    case MIN: r.setObjectAt(i, Math.min(va, vb)); break;
                    case MAX: r.setObjectAt(i, Math.max(va, vb)); break;
                    case POW: r.setObjectAt(i, (float)Math.pow(va, vb)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported binary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatUnaryOp} that performs a unary operation
     * on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is put into a second cube.
     */
    @SuppressWarnings("inline")
    private static void floatUnaryOpHelper(
        final FloatHypercube da,
        final FloatHypercube dr,
        final UnaryOp op,
        final long    startIndex,
        final long    endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final float[] ar = new float[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case NEG:   for (int j=0; j < len; j++) ar[j] =         -aa[j];  break;
            case ABS:   for (int j=0; j < len; j++) ar[j] = Math.abs(aa[j]); break;
            case FLOOR: for (int j=0; j < len; j++) ar[j] = (float)Math.floor(aa[j]); break;
            case ROUND: for (int j=0; j < len; j++) ar[j] = (float)Math.round(aa[j]); break;
            case CEIL:  for (int j=0; j < len; j++) ar[j] = (float)Math.ceil (aa[j]); break;
            case COS:   for (int j=0; j < len; j++) ar[j] = (float)Math.cos  (aa[j]); break;
            case COSH:  for (int j=0; j < len; j++) ar[j] = (float)Math.cosh (aa[j]); break;
            case SIN:   for (int j=0; j < len; j++) ar[j] = (float)Math.sin  (aa[j]); break;
            case SINH:  for (int j=0; j < len; j++) ar[j] = (float)Math.sinh (aa[j]); break;
            case TAN:   for (int j=0; j < len; j++) ar[j] = (float)Math.tan  (aa[j]); break;
            case TANH:  for (int j=0; j < len; j++) ar[j] = (float)Math.tanh (aa[j]); break;
            case EXP:   for (int j=0; j < len; j++) ar[j] = (float)Math.exp  (aa[j]); break;
            case LOG:   for (int j=0; j < len; j++) ar[j] = (float)Math.log  (aa[j]); break;
            case LOG10: for (int j=0; j < len; j++) ar[j] = (float)Math.log10(aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported unary operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given unary operation for a Float cube.
     */
    private static Hypercube<Float> floatUnaryOp(
        final Hypercube<Float> a,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        return unaryOp(a, new FloatArrayHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given unary operation for a Float cube, putting the result into a
     * second.
     */
    private static Hypercube<Float> floatUnaryOp(
        final Hypercube<Float> a,
        final Hypercube<Float> r,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatUnaryOpHelper(da, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatUnaryOpHelper(da, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case NEG:   r.setObjectAt(i, -va); break;
                    case ABS:   r.setObjectAt(i, Math.abs(va)); break;
                    case FLOOR: r.setObjectAt(i, (float)Math.floor(va)); break;
                    case ROUND: r.setObjectAt(i, (float)Math.round(va)); break;
                    case CEIL:  r.setObjectAt(i, (float)Math.ceil (va)); break;
                    case COS:   r.setObjectAt(i, (float)Math.cos  (va)); break;
                    case COSH:  r.setObjectAt(i, (float)Math.cosh (va)); break;
                    case SIN:   r.setObjectAt(i, (float)Math.sin  (va)); break;
                    case SINH:  r.setObjectAt(i, (float)Math.sinh (va)); break;
                    case TAN:   r.setObjectAt(i, (float)Math.tan  (va)); break;
                    case TANH:  r.setObjectAt(i, (float)Math.tanh (va)); break;
                    case EXP:   r.setObjectAt(i, (float)Math.exp  (va)); break;
                    case LOG:   r.setObjectAt(i, (float)Math.log  (va)); break;
                    case LOG10: r.setObjectAt(i, (float)Math.log10(va)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported unary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatComparisonOp} that performs a comparison
     * operation on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void floatComparisonOpHelper(
        final FloatHypercube da,
        final FloatHypercube db,
        final BooleanHypercube dr,
        final ComparisonOp     op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[]  aa = new float [STAGING_SIZE];
        final float[]  ab = new float [STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            switch (op) {
            case EQ: for (int j=0; j < len; j++) ar[j] = (aa[j] == ab[j]); break;
            case NE: for (int j=0; j < len; j++) ar[j] = (aa[j] != ab[j]); break;
            case LT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  < (ab[j])); break;
            case GT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  > (ab[j])); break;
            case LE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) <= (ab[j])); break;
            case GE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) >= (ab[j])); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported comparison operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given boolean comparison operation for two Float cubes.
     */
    private static Hypercube<Boolean> floatComparisonOp(
        final Hypercube<Float> a,
        final Hypercube<Float> b,
        final ComparisonOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + b.toDescriptiveString()
            );
        }

        // Create the destination, a simple bitset one by default
        return comparisonOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given binary operation for two Float cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> floatComparisonOp(
        final Hypercube<Float> a,
        final Hypercube<Float> b,
        final Hypercube<Boolean> r,
        final ComparisonOp       op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;
            final FloatHypercube db = (FloatHypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatComparisonOpHelper(da, db, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatComparisonOpHelper(da, db, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                final Float vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case EQ: r.setObjectAt(i,  va.equals(vb)); break;
                    case NE: r.setObjectAt(i, !va.equals(vb)); break;
                    case LT: r.setObjectAt(i, (va)  < (vb)); break;
                    case GT: r.setObjectAt(i, (va)  > (vb)); break;
                    case LE: r.setObjectAt(i, (va) <= (vb)); break;
                    case GE: r.setObjectAt(i, (va) >= (vb)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported comparison operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // All comparison operattions with a NaN operand evaluate
                    // to false, except for the inequality operation (!=).
                    r.setObjectAt(i, null);
                    switch (op) {
                    case NE: r.setObjectAt(i, true);  break;
                    default: r.setObjectAt(i, false); break;
                    }
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive operation for a float cube, for the
     * given axes.
     */
    private static BooleanHypercube floatReductiveLogicOpByAxes(
        final Hypercube<Float> a,
        final int[] axes,
        final BooleanHypercube where,
        final ReductiveLogicOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[dstNDim];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final BooleanHypercube dst = new BooleanBitSetHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final boolean result = floatReductiveLogicOp(a.slice(srcAccessors), whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index. If doing so would cause overflow then
                // we reset it to zero and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code floatReductiveLogicOp} that performs a reductive
     * logical operation on a sub-array of the cube. The sub-array is specified
     * by parameters startIndex and endIndex, and the result is returned as a
     * boolean.
     */
    @SuppressWarnings("inline")
    private static boolean floatReductiveLogicOpHelper(
        final FloatHypercube da,
        final BooleanHypercube w,
        final ReductiveLogicOp op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final float[] aa = new float[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            if (w != null) {
                w.toFlattened(i, ww, 0, len);
            }

            switch (op) {
            case ANY: for (int j=0; j < len; j++) if (ww == null || ww[j]) r |= (aa[j] != 0); break;
            case ALL: for (int j=0; j < len; j++) if (ww == null || ww[j]) r &= (aa[j] != 0); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reductive logic operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given reductive logic operation for a Float cube.
     */
    private static boolean floatReductiveLogicOp(
        final Hypercube<Float> a,
        final BooleanHypercube w,
        final ReductiveLogicOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final FloatHypercube da = (FloatHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = floatReductiveLogicOpHelper(da, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final boolean[] ar = new boolean[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = floatReductiveLogicOpHelper(da, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ANY: for (int j=0; j < NUM_THREADS; j++) r |= ar[j]; break;
                case ALL: for (int j=0; j < NUM_THREADS; j++) r &= ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported reductive logic operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Float va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case ANY: r |= (va != 0); break;
                    case ALL: r &= (va != 0); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported reductive logic operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // Note that NaN always evaluates to true.
                    switch (op) {
                    case ANY: r = true; break;
                    default:
                        // Nothing to do.
                    }
                }
            }
        }

        // Always return the result
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code floatExtract} that performs an extract operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube, starting
     * from the given offset index value.
     */
    @SuppressWarnings("inline")
    private static void floatExtractHelper(
        final BooleanHypercube dc,
        final FloatHypercube da,
        final FloatHypercube dr,
        final long startIndex,
        final long endIndex,
        final long offset
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean         [] ac = new boolean         [STAGING_SIZE];
        final float[] aa = new float[STAGING_SIZE];
        final float[] ar = new float[STAGING_SIZE];

        // Define an offset to track our progress on r
        long offsetR = offset;

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            dc.toFlattened(i, ac, 0, len);
            da.toFlattened(i, aa, 0, len);

            int k = 0;
            for (int j=0; j < len; j++) {
                if (Boolean.TRUE.equals(ac[j])) {
                    ar[k++] = aa[j];
                }
            }

            // Copy in
            dr.fromFlattened(ar, 0, offsetR, k);
            offsetR += k;
        }
    }

    /**
     * Handle the extract operation for a Float cube.
     */
    private static Hypercube<Float> floatExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Float> a
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " + a.toDescriptiveString()
            );
        }

        // Create the destination, a simple Array one by default
        return floatExtract(c, a, new FloatArrayHypercube(Dimension.of(popcount(c))));
    }

    /**
     * Handle the extract operation for a Float cube, putting the
     * result into a third.
     */
    private static Hypercube<Float> floatExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Float> a,
        final Hypercube<Float> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " +
                a.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube dc = (BooleanHypercube)c;
            final FloatHypercube da = (FloatHypercube)a;
            final FloatHypercube dr = (FloatHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                floatExtractHelper(dc, da, dr, 0, a.getSize(), 0);
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // An offset to track where in r we should be, when processing
                // each bucket.
                long offset = 0;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final long offsetR = offset;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            floatExtractHelper(dc, da, dr, startIndex, endIndex, offsetR);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });

                    // Update the offset value accordingly
                    offset += popcount(c, startIndex, endIndex);
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, j=0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean       vc = c.getObjectAt(i);
                final Float va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(vc)) {
                    r.setObjectAt(j++, va);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static DoubleHypercube array(final DoubleHypercube cube)
    {
        return cube.array();
    }

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static DoubleHypercube array(final double[] array)
    {
        final DoubleHypercube result =
            new DoubleArrayHypercube(Dimension.of(array.length));
        result.fromFlattened(array);
        return result;
    }

    /**
     * Return a one dimensional cube with a range per the given value.
     */
    public static DoubleHypercube arange(final double stop)
    {
        return arange(0, stop);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static DoubleHypercube arange(
        final double start,
        final double stop
    )
    {
        return arange(start, stop, 1);
    }

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static DoubleHypercube arange(
        final double start,
        final double stop,
        final double step
    )
    {
        if (step == 0) {
            throw new IllegalArgumentException("Given a step of zero");
        }
        if (step < 0 && start < stop) {
            throw new IllegalArgumentException(
                "Step was negative but start was before stop"
            );
        }
        if (step > 0 && start > stop) {
            throw new IllegalArgumentException(
                "Step was positive but start was after stop"
            );
        }
        final long length = (long)((stop - start) / step);
        final DoubleHypercube cube = new DoubleArrayHypercube(Dimension.of(length));
        for (long i=0; i < length; i++) {
            cube.setAt(i, (double)(start + step * i));
        }
        return cube;
    }

    /**
     * Handle a vector multiply, also known as a dot product.
     */
    public static double doubleDotProd(
        final Hypercube<Double> a,
        final Hypercube<Double> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (a.getNDim() != 1 || !a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible or multi-dimensional cubes"
            );
        }

        try {
            final DoubleHypercube da = (DoubleHypercube)a;
            final DoubleHypercube db = (DoubleHypercube)b;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                final AtomicReference<Double> r = new AtomicReference<Double>();
                doubleDotProdHelper(da, db, r, 0, a.getSize());
                return r.get();
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();
                @SuppressWarnings("unchecked") final AtomicReference<Double>[] r =
                    (AtomicReference<Double>[])new AtomicReference<?>[NUM_THREADS];
                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }
                    final AtomicReference<Double> rv = new AtomicReference<>();
                    r[j] = rv;

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleDotProdHelper(da, db, rv, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // And sum up the result
                double sum = 0;
                for (AtomicReference<Double> rv : r) {
                    sum += rv.get().doubleValue();
                }
                return sum;
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Fall back to the simple version
            double sum = 0;
            for (long i = 0, sz = a.getSize(); i < sz; i++) {
                final Double va = a.getObjectAt(i);
                final Double vb = b.getObjectAt(i);
                if (va == null || vb == null) {
                    // For floats nulls are NaNs but they are zeroes for ints.
                    // Cheesy test to figure out which we are.
                    if (Double.isNaN(Double.NaN)) {
                        return Double.NaN;
                    }
                }
                else {
                    sum += va * vb;
                }
            }
            return sum;
        }
    }

    /**
     * Helper function for {@code doubleDotProd} that performs the
     * dot product on two sub-arrays. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void doubleDotProdHelper(
        final DoubleHypercube da,
        final DoubleHypercube db,
        final AtomicReference<Double> r,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final double[] ab = new double[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            // Compute the dot product
            double sum = 0;
            for (int j=0; j < len; j++) {
                sum += aa[j] * ab[j];
            }

            // And give it back via the atomic
            r.set(Double.valueOf(sum));
        }
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Double> doubleMatMul(
        final Hypercube<Double> a,
        final Hypercube<Double> b
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[1].equals(bDims[0])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { aDims[0] };
                return doubleMatMul(a, b, new DoubleArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1])) {
                final Dimension<?>[] rDims = new Dimension<?>[] { bDims[0] };
                return doubleMatMul(a, b, new DoubleArrayHypercube(rDims));
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            if (aDims[0].equals(bDims[1]) &&
                aDims[1].equals(bDims[0]))
            {
               return doubleMatMul(
                    a, b,
                    new DoubleArrayHypercube(
                        new Dimension<?>[] { aDims[0], bDims[1] }
                    )
                );
            }
        }
        else if (a.getNDim() <= 1 && b.getNDim() <= 1) {
            // Nothing. We don't handle this case for matrix multiplication.
        }
        else if (a.getNDim() > 2 || b.getNDim() > 2) {
            // These need to be compatible in the corner dimensions like the 2D
            // case, and then other dimensions just need to match directly
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension<?>[] bDims = b.getDimensions();
            final int nADim = aDims.length;
            final int nBDim = bDims.length;
            if ((bDims.length == 1 || aDims[nADim-1].equals(bDims[nBDim-2])) &&
                (aDims.length == 1 || aDims[nADim-2].equals(bDims[nBDim-1])))
            {
                final int maxNDim = Math.max(nADim, nBDim);
                final int minNDim = Math.min(nADim, nBDim);
                boolean matches = true;
                for (int i=0; matches && i < minNDim-2; i++) {
                    if (!aDims[i].equals(bDims[i])) {
                        matches = false;
                    }
                }
                if (matches) {
                    final Dimension<?>[] rDims;
                    final int offset;
                    if (aDims.length == 1 || bDims.length == 1) {
                        // We will lose a dimension when multiplying by a vector
                        rDims = new Dimension<?>[maxNDim-1];
                        rDims[rDims.length-1] =
                            (aDims.length == 1) ? bDims[bDims.length-2]
                                                : aDims[aDims.length-2];
                        offset = 1;
                    }
                    else {
                        rDims = new Dimension<?>[maxNDim];
                        rDims[rDims.length-1] = aDims[aDims.length-2];
                        rDims[rDims.length-2] = bDims[bDims.length-1];
                        offset = 0;
                    }
                    for (int i = 0; i < offset + rDims.length - 2; i++) {
                        rDims[i] = (aDims.length >= bDims.length) ? aDims[i] : bDims[i];
                    }
                    return doubleMatMul(a, b, new DoubleArrayHypercube(rDims));
                }
            }
        }

        // If we got here then we could not make the cubes match
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    /**
     * Handle a matrix multiply per {@code matmul()} semantics.
     */
    public static Hypercube<Double> doubleMatMul(
        final Hypercube<Double> a,
        final Hypercube<Double> b,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }
        if (!a.getElementType().equals(b.getElementType())) {
            throw new NullPointerException(
                "Input cubes have different element types: " +
                a.getElementType() + " vs " + b.getElementType()
            );
        }
        if (!a.getElementType().equals(r.getElementType())) {
            throw new NullPointerException(
                "Input and return cubes have different element types: " +
                a.getElementType() + " vs " + r.getElementType()
            );
        }

        // Shape is important for matrix operations. Hence some "duplicated"
        // code in the below.
        if (a.getNDim() == 2 && b.getNDim() == 1) {
            DoubleHypercube dr = null;
            try {
                dr = (DoubleHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, aDims[1].at(0) };
            if (a.slice(iSlice).matches(b) && a.slice(jSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        dr.setAt(i, doubleDotProd(a.slice(iSlice), b));
                    }
                }
                else {
                    for (long i=0; i < aDims[0].length(); i++) {
                        iSlice[0] = aDims[0].at(i);
                        r.setObjectAt(i, doubleDotProd(a.slice(iSlice), b));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 1 && b.getNDim() == 2) {
            DoubleHypercube dr = null;
            try {
                dr = (DoubleHypercube)r;
            }
            catch (ClassCastException e) {
                // Nothing
            }

            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] iSlice =
                new Dimension.Accessor<?>[] { bDims[0].at(0), null };
            final Dimension.Accessor<?>[] jSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.matches(b.slice(iSlice)) && b.slice(iSlice).matches(r)) {
                if (dr != null) {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setAt(i, doubleDotProd(a, b.slice(jSlice)));
                    }
                }
                else {
                    for (long i=0; i < bDims[1].length(); i++) {
                        jSlice[1] = bDims[1].at(i);
                        dr.setObjectAt(i, doubleDotProd(a, b.slice(jSlice)));
                    }
                }
                return r;
            }
        }
        else if (a.getNDim() == 2 && b.getNDim() == 2) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[] { aDims[0].at(0), null };
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[] { null, bDims[1].at(0) };
            if (a.slice(aSlice).matches(b.slice(bSlice))) {
                // A regular matrix multiply where we compute the dot product of
                // the row and column to get their intersection coordinate's
                // value.
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                try {
                    final DoubleHypercube da = (DoubleHypercube)a;
                    final DoubleHypercube db = (DoubleHypercube)b;
                    final DoubleHypercube dr = (DoubleHypercube)r;

                    // We copy out the column from 'b' for faster access if it's
                    // small enough to fit into an array. 2^30 doubles is 16GB
                    // for one column which is totally possible for a non-square
                    // matrix but, we hope, most matrices will not be quite that
                    // big. We could use an array of arrays to handle that case
                    // but this is slower for the general case.
                    if (bDims[0].length() <= 1<<30) {
                        // Hand off to the smarter method which will copy out
                        // the column and use multi-threading
                        doubleMatMul2D(da, db, dr);
                    }
                    else {
                        // Where we start striding, see below.
                        ai[1] = bi[0] = 0;

                        // The stride through the flattened data, to walk a column
                        // in 'b'. We know that the format of the data is C-style in
                        // flattened form, so moving one row length in distance will
                        // step down one column index.
                        final long bs = bDims[1].length();

                        // Flipped the ordering of 'i' and 'j' since it's more cache
                        // efficient to copy out the column data (once) and then to
                        // stride through the rows each time.
                        da.preRead();
                        db.preRead();
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            for (long i=0; i < aDims[0].length(); i++) {
                                // We will stride through the two cubes pulling out
                                // the values for the sum directly, since we know
                                // their shape. This is much faster than going via
                                // the coordinate-based lookup. The stride in 'a' is
                                // 1 since it's walking along a row; in b it's the
                                // the row length, since it's walking along a column.
                                // Both will be the same number of steps so we only
                                // need to know when to stop walking in 'a'.
                                ai[0] = ri[0] = i;
                                long ao = da.toOffset(ai);
                                double sum = 0;
                                final long ae = ao + aDims[1].length();
                                for (long bo = db.toOffset(bi);
                                     ao < ae; ao++,
                                     bo += bs)
                                {
                                    sum += da.weakGetAt(ao) * db.weakGetAt(bo);
                                }
                                dr.set(sum, ri);
                            }
                        }
                    }
                }
                catch (ClassCastException e) {
                    // Need to be object-based
                    for (long i=0; i < aDims[0].length(); i++) {
                        ai[0] = ri[0] = i;
                        for (long j=0; j < bDims[1].length(); j++) {
                            bi[1] = ri[1] = j;
                            double sum = 0;
                            for (long k=0; k < bDims[0].length(); k++) {
                                ai[1] = bi[0] = k;
                                final Double va = a.getObj(ai);
                                final Double vb = b.getObj(bi);
                                if (va == null || vb == null) {
                                    // For floats nulls are NaNs but they are zeroes for ints.
                                    // Cheesy test to figure out which we are.
                                    if (Double.isNaN(Double.NaN)) {
                                        sum = Double.NaN;
                                        break;
                                    }
                                }
                                else {
                                    sum += va * vb;
                                }
                            }
                            r.setObj(sum, ri);
                        }
                    }
                }

                // And give it back
                return r;
            }
        }
        else if ((a.getNDim() == r.getNDim() || a.getNDim()-1 == r.getNDim()) &&
                 a.getNDim() > b.getNDim())
        {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                doubleMatMul(a.slice(aSlice), b, r.slice(rSlice));
            }
            return r;
        }
        else if ((b.getNDim() == r.getNDim() || b.getNDim()-1 == r.getNDim()) &&
                 b.getNDim() > a.getNDim())
        {
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = bDims[0].length(); i < sz; i++) {
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                doubleMatMul(a, b.slice(bSlice), r.slice(rSlice));
            }
            return r;
        }
        else if (a.getNDim() == b.getNDim() && b.getNDim() == r.getNDim()) {
            final Dimension<?>[] aDims = a.getDimensions();
            final Dimension.Accessor<?>[] aSlice =
                new Dimension.Accessor<?>[aDims.length];
            aSlice[0] = aDims[0].at(0);
            final Dimension<?>[] bDims = b.getDimensions();
            final Dimension.Accessor<?>[] bSlice =
                new Dimension.Accessor<?>[bDims.length];
            bSlice[0] = bDims[0].at(0);
            final Dimension<?>[] rDims = r.getDimensions();
            final Dimension.Accessor<?>[] rSlice =
                new Dimension.Accessor<?>[rDims.length];
            for (long i = 0, sz = aDims[0].length(); i < sz; i++) {
                aSlice[0] = aDims[0].at(i);
                bSlice[0] = bDims[0].at(i);
                rSlice[0] = rDims[0].at(i);
                doubleMatMul(
                    a.slice(aSlice), b.slice(bSlice), r.slice(rSlice)
                );
            }
            return r;
        }

        // If we got here then we could not do anything
        throw new IllegalArgumentException(
            "Given incompatible (sub)cubes: " +
            Arrays.toString(a.getShape()) + " vs " +
            Arrays.toString(b.getShape()) + " "    +
            (Arrays.toString(a.getDimensions()) + " vs " +
             Arrays.toString(b.getDimensions()))
        );
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given associative operation for a double cube, for the
     * given axes.
     */
    private static DoubleHypercube doubleAssociativeOpByAxes(
        final Hypercube<Double> a,
        final int[] axes,
        final Double initial,
        final BooleanHypercube where,
        final AssociativeOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[Math.max(1, dstNDim)];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final DoubleHypercube dst = new DoubleArrayHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final Double result =
                associativeOp(a.slice(srcAccessors), initial, whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index? If doing so would overflow the digit we
                // go back to zero for it and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code doubleAssociativeOp} that performs an associative
     * operation on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is returned as a double.
     */
    @SuppressWarnings("inline")
    private static double doubleAssociativeOpHelper(
        final DoubleHypercube da,
        final Double    i,
        final BooleanHypercube w,
        final AssociativeOp    op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        double r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Double.MAX_VALUE; break;
            case MAX:    r = Double.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long ii = startIndex; ii < endIndex; ii += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - ii, STAGING_SIZE);

            // Copy out
            da.toFlattened(ii, aa, 0, len);
            if (w != null) {
                w.toFlattened(ii, ww, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD:    for (int j=0; j < len && !Double.isNaN(r); j++) if (ww == null || ww[j]) r += aa[j]; break;
            case MIN:    for (int j=0; j < len && !Double.isNaN(r); j++) if (ww == null || ww[j]) r  = (r < aa[j] ? r : aa[j]); break;
            case MAX:    for (int j=0; j < len && !Double.isNaN(r); j++) if (ww == null || ww[j]) r  = (r > aa[j] ? r : aa[j]); break;
            case NANADD: for (int j=0; j < len; j++) if (ww == null || ww[j]) r += (Double.isNaN(aa[j]) ? 0 : aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given associative operation for a double cube.
     */
    private static Double doubleAssociativeOp(
        final Hypercube<Double> a,
        final Double i,
        final BooleanHypercube w,
        final AssociativeOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // If there is a where value then it should match in shape
        if (w != null && !a.matchesInShape(w)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        double r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {
            r = i;
        }
        else {
            switch (op) {
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = Double.MAX_VALUE; break;
            case MAX:    r = Double.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = doubleAssociativeOpHelper(da, i, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final double[] ar = new double[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = doubleAssociativeOpHelper(da, null, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ADD:    for (int j=0; j < NUM_THREADS && !Double.isNaN(r); j++) r += ar[j];                     break;
                case MIN:    for (int j=0; j < NUM_THREADS && !Double.isNaN(r); j++) r  = ((r < ar[j]) ? r : ar[j]); break;
                case MAX:    for (int j=0; j < NUM_THREADS && !Double.isNaN(r); j++) r  = ((r > ar[j]) ? r : ar[j]); break;
                // The result of NaN functions on each thread can never be NaN.
                // So we can simply combine them with no NaN handling.
                case NANADD: for (int j=0; j < NUM_THREADS; j++) r += ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported associative operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            w.preRead();
            for (long ii = 0, size = a.getSize(); ii < size && !Double.isNaN(r); ii++) {
                // Handle any 'where' clause
                if (w != null && !w.weakGetAt(ii)) {
                    continue;
                }

                // Need to handle missing values
                final Double va = a.getObjectAt(ii);
                if (va != null) {
                    switch (op) {
                    case ADD:    r += va;                           break;
                    case MIN:    r  = (r < va           ? r : va);  break;
                    case MAX:    r  = (r > va           ? r : va);  break;
                    case NANADD: r += (Double.isNaN(va) ? 0 : va); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported associative operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    switch (op) {
                    case ADD: r = Double.valueOf(Double.NaN); break;
                    case MIN: r = Double.valueOf(Double.NaN); break;
                    case MAX: r = Double.valueOf(Double.NaN); break;
                    default:
                        // Don't do anything for NaN functions
                    }
                }
            }
        }

        // Always return the result
        return Double.valueOf(r);
    }

    /**
     * Handle a 2D matrix multiply.
     */
    private static void doubleMatMul2D(
        final DoubleHypercube a,
        final DoubleHypercube b,
        final DoubleHypercube r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        final Dimension<?>[] aDims = a.getDimensions();
        final Dimension<?>[] bDims = b.getDimensions();

        // We will copy out the column from 'b' for faster access, if it's small
        // enough to fit into an array. If we got here then the caller should
        // have handled getting this right for us.
        if (bDims[0].length() > 1<<30) {
            throw new IllegalArgumentException(
                "Axis was too large: " + bDims[0].length()
            );
        }
        final double[] bcol = new double[(int)bDims[0].length()];

        // The stride through the flattened data, to walk a column
        // in 'b'. We know that the format of the data is C-style in
        // flattened form, so moving one row length in distance will
        // step down one column index.
        final long bs = bDims[1].length();

        // Flipped the ordering of 'i' and 'j' since it's more cache
        // efficient to copy out the column data (once) and then to
        // stride through the rows each time.
        for (long j=0; j < bDims[1].length(); j++) {
            long bco = b.toOffset(0, j);
            b.preRead();
            for (int i=0; i < bcol.length; i++, bco += bs) {
                bcol[i] = b.weakGetAt(bco);
            }

            // We will stride through the two cubes pulling out the values
            // for the sum directly, since we know their shape. This is much
            // faster than going via the coordinate-based lookup. The stride in
            // 'a' is 1 since it's walking along a row; in b it's the the row
            // length, since it's walking along a column. Both will be the same
            // number of steps so we only need to know when to stop walking in
            // 'a'.
            //
            // Only use multi-threading if it is enabled and if the cubes are
            // large enough to justify the overhead, noting that matmul is
            // an O(n^3) operation.
            final long numRows = aDims[0].length();
            if (ourExecutorService == null ||
                (numRows * numRows * numRows) < THREADING_THRESHOLD)
            {
                // Where we start striding, see below
                final long[] ai = new long[2];
                final long[] bi = new long[2];
                final long[] ri = new long[2];
                ai[1] = bi[0] = 0;
                bi[1] = ri[1] = j;

                a.preRead();
                for (long i=0; i < numRows; i++) {
                    ai[0] = ri[0] = i;
                    long ao = a.toOffset(ai);
                    double sum = 0;
                    for (int bo=0 ; bo < bcol.length; ao++, bo++) {
                        sum += a.weakGetAt(ao) * bcol[bo];
                    }
                    r.set(sum, ri);
                }
            }
            else {
                // How many threads to use. This should not be more than there
                // are rows.
                final int numThreads = (int)Math.min(numRows, NUM_THREADS);

                // Initialize a countdown to wait for all threads to finish
                // processing.
                final CountDownLatch latch = new CountDownLatch(numThreads);

                // Bucket size for each thread, we use +1 to kinda ceil the
                // result which gives a somewhat better distribution. We do
                // this in such a way as to avoid double-rounding errors.
                final long threadRows = numRows / numThreads;
                final long bucket = threadRows +
                                  ((threadRows * numThreads == numRows) ? 0 : 1);
                for (int t=0; t < numThreads; t++) {
                    final long startIndex = bucket * t;
                    final long endIndex =
                        (t == numThreads-1) ? numRows
                                            : Math.min(bucket * (t+1), numRows);

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    final long jf = j; // <-- "final j"
                    ourExecutorService.submit(() -> {
                        try {
                            final long[] ai = new long[] { 0,  0 };
                            final long[] bi = new long[] { 0, jf };
                            final long[] ri = new long[] { 0, jf };
                            a.preRead();
                            for (long i = startIndex; i < endIndex; i++) {
                                ai[0] = ri[0] = i;
                                long ao = a.toOffset(ai);
                                double sum = 0;
                                for (int bo=0; bo < bcol.length; ao++, bo++) {
                                    sum += a.weakGetAt(ao) * bcol[bo];
                                }
                                r.set(sum, ri);
                            }
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Return a new double cube of given dimensions where all elements are
     * equal to a given double value.
     *
     * <p>This method is an alias of {@code broadcast()} and is equivalent to
     * {@code numpy.full()} method.
     *
     * @return The resulting double cube.
     */
    public static Hypercube<Double> full(
        final Dimension<?>[] dimensions,
        final double value
    )
    {
        return broadcast(dimensions, value);
    }

    /**
     * Return a new double cube of given dimensions where all elements are
     * equal to a given double value.
     *
     * @return The resulting double cube.
     */
    public static Hypercube<Double> broadcast(
        final Dimension<?>[] dimensions,
        final double value
    )
    {
        final DoubleHypercube a = new DoubleArrayHypercube(dimensions);
        a.fill(value);
        return a;
    }

    /**
     * Return a new 1-dimensional double cube of a given size where all
     * elements are equal to a given double value.
     *
     * <p>This is equivalent to calling {@code broadcast(Dimension.of(size), value)}.
     *
     * @return The resulting double cube.
     */
    public static Hypercube<Double> broadcast(
        final long size,
        final double value
    )
    {
        return broadcast(Dimension.of(size), value);
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleBinaryOp} that performs a binary operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void doubleBinaryOpHelper(
        final DoubleHypercube da,
        final DoubleHypercube db,
        final DoubleHypercube dr,
        final BooleanHypercube dw,
        final BinaryOp op,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final double[] ab = new double[STAGING_SIZE];
        final double[] ar = new double[STAGING_SIZE];
        final boolean[] aw = (dw == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);
            if (dw != null) {
                dw.toFlattened(i, aw, 0, len);
            }

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case ADD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] + ab[j];          break;
            case SUB: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] - ab[j];          break;
            case MUL: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] * ab[j];          break;
            case DIV: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] / ab[j];          break;
            case MOD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] % ab[j];          break;
            case MIN: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.min(aa[j], ab[j]); break;
            case MAX: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.max(aa[j], ab[j]); break;
            case POW: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = (double)Math.pow(aa[j], ab[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported binary operation: " + op
                );
            }

            // Copy in
            if (aw == null) {
                dr.fromFlattened(ar, 0, i, len);
            }
            else {
                for (int j=0; j < len; j++) {
                    if (aw[j]) {
                        dr.setAt(i + j, ar[j]);
                    }
                }
            }
        }
    }

    /**
     * Handle the given binary operation for two Double cubes.
     */
    private static Hypercube<Double> doubleBinaryOp(
        final Hypercube<Double> a,
        final Hypercube<Double> b,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }

        // Depending on which cube is a non-strict supercube of the other, create a simple
        // Array destination one as a copy of the appropriate argument
        if (a.submatches(b)) {
            return binaryOp(a, b, new DoubleArrayHypercube(a.getDimensions()), dw, op);
        }
        if (b.submatches(a)) {
            return binaryOp(a, b, new DoubleArrayHypercube(b.getDimensions()), dw, op);
        }

        // No match between the cubes
        throw new IllegalArgumentException(
            "Given incompatible cubes: " +
            a.toDescriptiveString() + " vs " + b.toDescriptiveString()
        );
    }

    /**
     * Handle the given binary operation for two Double cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Double> doubleBinaryOp(
        final Hypercube<Double> a,
        final Hypercube<Double> b,
        final Hypercube<Double> r,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final DoubleHypercube db = (DoubleHypercube)b;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleBinaryOpHelper(da, db, dr, dw, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleBinaryOpHelper(da, db, dr, dw, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                final Double vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case ADD: r.setObjectAt(i, va + vb);          break;
                    case SUB: r.setObjectAt(i, va - vb);          break;
                    case MUL: r.setObjectAt(i, va * vb);          break;
                    case DIV: r.setObjectAt(i, va / vb);          break;
                    case MOD: r.setObjectAt(i, va % vb);          break;
                    case MIN: r.setObjectAt(i, Math.min(va, vb)); break;
                    case MAX: r.setObjectAt(i, Math.max(va, vb)); break;
                    case POW: r.setObjectAt(i, (double)Math.pow(va, vb)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported binary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleUnaryOp} that performs a unary operation
     * on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is put into a second cube.
     */
    @SuppressWarnings("inline")
    private static void doubleUnaryOpHelper(
        final DoubleHypercube da,
        final DoubleHypercube dr,
        final UnaryOp op,
        final long    startIndex,
        final long    endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final double[] ar = new double[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {
            case NEG:   for (int j=0; j < len; j++) ar[j] =         -aa[j];  break;
            case ABS:   for (int j=0; j < len; j++) ar[j] = Math.abs(aa[j]); break;
            case FLOOR: for (int j=0; j < len; j++) ar[j] = (double)Math.floor(aa[j]); break;
            case ROUND: for (int j=0; j < len; j++) ar[j] = (double)Math.round(aa[j]); break;
            case CEIL:  for (int j=0; j < len; j++) ar[j] = (double)Math.ceil (aa[j]); break;
            case COS:   for (int j=0; j < len; j++) ar[j] = (double)Math.cos  (aa[j]); break;
            case COSH:  for (int j=0; j < len; j++) ar[j] = (double)Math.cosh (aa[j]); break;
            case SIN:   for (int j=0; j < len; j++) ar[j] = (double)Math.sin  (aa[j]); break;
            case SINH:  for (int j=0; j < len; j++) ar[j] = (double)Math.sinh (aa[j]); break;
            case TAN:   for (int j=0; j < len; j++) ar[j] = (double)Math.tan  (aa[j]); break;
            case TANH:  for (int j=0; j < len; j++) ar[j] = (double)Math.tanh (aa[j]); break;
            case EXP:   for (int j=0; j < len; j++) ar[j] = (double)Math.exp  (aa[j]); break;
            case LOG:   for (int j=0; j < len; j++) ar[j] = (double)Math.log  (aa[j]); break;
            case LOG10: for (int j=0; j < len; j++) ar[j] = (double)Math.log10(aa[j]); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported unary operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given unary operation for a Double cube.
     */
    private static Hypercube<Double> doubleUnaryOp(
        final Hypercube<Double> a,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        // Create the destination, a simple Array one by default
        return unaryOp(a, new DoubleArrayHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given unary operation for a Double cube, putting the result into a
     * second.
     */
    private static Hypercube<Double> doubleUnaryOp(
        final Hypercube<Double> a,
        final Hypercube<Double> r,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleUnaryOpHelper(da, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleUnaryOpHelper(da, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case NEG:   r.setObjectAt(i, -va); break;
                    case ABS:   r.setObjectAt(i, Math.abs(va)); break;
                    case FLOOR: r.setObjectAt(i, (double)Math.floor(va)); break;
                    case ROUND: r.setObjectAt(i, (double)Math.round(va)); break;
                    case CEIL:  r.setObjectAt(i, (double)Math.ceil (va)); break;
                    case COS:   r.setObjectAt(i, (double)Math.cos  (va)); break;
                    case COSH:  r.setObjectAt(i, (double)Math.cosh (va)); break;
                    case SIN:   r.setObjectAt(i, (double)Math.sin  (va)); break;
                    case SINH:  r.setObjectAt(i, (double)Math.sinh (va)); break;
                    case TAN:   r.setObjectAt(i, (double)Math.tan  (va)); break;
                    case TANH:  r.setObjectAt(i, (double)Math.tanh (va)); break;
                    case EXP:   r.setObjectAt(i, (double)Math.exp  (va)); break;
                    case LOG:   r.setObjectAt(i, (double)Math.log  (va)); break;
                    case LOG10: r.setObjectAt(i, (double)Math.log10(va)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported unary operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleComparisonOp} that performs a comparison
     * operation on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void doubleComparisonOpHelper(
        final DoubleHypercube da,
        final DoubleHypercube db,
        final BooleanHypercube dr,
        final ComparisonOp     op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[]  aa = new double [STAGING_SIZE];
        final double[]  ab = new double [STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            switch (op) {
            case EQ: for (int j=0; j < len; j++) ar[j] = (aa[j] == ab[j]); break;
            case NE: for (int j=0; j < len; j++) ar[j] = (aa[j] != ab[j]); break;
            case LT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  < (ab[j])); break;
            case GT: for (int j=0; j < len; j++) ar[j] = ((aa[j])  > (ab[j])); break;
            case LE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) <= (ab[j])); break;
            case GE: for (int j=0; j < len; j++) ar[j] = ((aa[j]) >= (ab[j])); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported comparison operation: " + op
                );
            }

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }
    }

    /**
     * Handle the given boolean comparison operation for two Double cubes.
     */
    private static Hypercube<Boolean> doubleComparisonOp(
        final Hypercube<Double> a,
        final Hypercube<Double> b,
        final ComparisonOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (!a.matches(b)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " + b.toDescriptiveString()
            );
        }

        // Create the destination, a simple bitset one by default
        return comparisonOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), op);
    }

    /**
     * Handle the given binary operation for two Double cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> doubleComparisonOp(
        final Hypercube<Double> a,
        final Hypercube<Double> b,
        final Hypercube<Boolean> r,
        final ComparisonOp       op
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (b == null) {
            throw new NullPointerException("Given a null cube, 'b'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matches(b) || !a.matchesInShape(r)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                a.toDescriptiveString() + " vs " +
                b.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;
            final DoubleHypercube db = (DoubleHypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleComparisonOpHelper(da, db, dr, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleComparisonOpHelper(da, db, dr, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                final Double vb = b.getObjectAt(i);
                if (va != null && vb != null) {
                    switch (op) {
                    case EQ: r.setObjectAt(i,  va.equals(vb)); break;
                    case NE: r.setObjectAt(i, !va.equals(vb)); break;
                    case LT: r.setObjectAt(i, (va)  < (vb)); break;
                    case GT: r.setObjectAt(i, (va)  > (vb)); break;
                    case LE: r.setObjectAt(i, (va) <= (vb)); break;
                    case GE: r.setObjectAt(i, (va) >= (vb)); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported comparison operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // All comparison operattions with a NaN operand evaluate
                    // to false, except for the inequality operation (!=).
                    r.setObjectAt(i, null);
                    switch (op) {
                    case NE: r.setObjectAt(i, true);  break;
                    default: r.setObjectAt(i, false); break;
                    }
                }
            }
        }

        // Always give back the resultant one
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive operation for a double cube, for the
     * given axes.
     */
    private static BooleanHypercube doubleReductiveLogicOpByAxes(
        final Hypercube<Double> a,
        final int[] axes,
        final BooleanHypercube where,
        final ReductiveLogicOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {
            if (axis >= a.getNDim()) {
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }
            if (axesSet.get(axis)) {
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }
            axesSet.set(axis);
        }

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {
            // Sliced out dims
            dstDims = new Dimension<?>[dstNDim];
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    dstDims[j++] = srcDims[i];
                }
            }
        }
        else {
            // Singleton cube
            dstDims = Dimension.of(1);
        }

        // Where we will put the result
        final BooleanHypercube dst = new BooleanBitSetHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0; i < srcAccessors.length; i++) {
            if (axesSet.get(i)) {
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }
        }

        // Incrementally walk all the non-axes dimensions
        while (true) {
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {
                if (!axesSet.get(i)) {
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }
            }

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final boolean result = doubleReductiveLogicOp(a.slice(srcAccessors), whereSlice, op);
            if (dstNDim > 0) {
                // Subcube
                dst.setObj(result, dstAccessors);
            }
            else {
                // Singleton
                dst.setObjectAt(0, result);
            }

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {
                // Skip over indices which were slicing
                if (axesSet.get(i)) {
                    continue;
                }

                // Increment this index. If doing so would cause overflow then
                // we reset it to zero and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {
                    indices[i]++;
                    carry = false;
                }
                else {
                    indices[i] = 0;
                }
            }

            // If we overflowed then we're done
            if (carry) {
                break;
            }
        }

        // Give back the result
        return dst;
    }

    /**
     * Helper function for {@code doubleReductiveLogicOp} that performs a reductive
     * logical operation on a sub-array of the cube. The sub-array is specified
     * by parameters startIndex and endIndex, and the result is returned as a
     * boolean.
     */
    @SuppressWarnings("inline")
    private static boolean doubleReductiveLogicOpHelper(
        final DoubleHypercube da,
        final BooleanHypercube w,
        final ReductiveLogicOp op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {
        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final double[] aa = new double[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            if (w != null) {
                w.toFlattened(i, ww, 0, len);
            }

            switch (op) {
            case ANY: for (int j=0; j < len; j++) if (ww == null || ww[j]) r |= (aa[j] != 0); break;
            case ALL: for (int j=0; j < len; j++) if (ww == null || ww[j]) r &= (aa[j] != 0); break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reductive logic operation: " + op
                );
            }
        }

        return r;
    }

    /**
     * Handle the given reductive logic operation for a Double cube.
     */
    private static boolean doubleReductiveLogicOp(
        final Hypercube<Double> a,
        final BooleanHypercube w,
        final ReductiveLogicOp op
    ) throws NullPointerException
    {
        // Null checks first
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }

        boolean r;

        // Initialize the return value according to the operation
        switch (op) {
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final DoubleHypercube da = (DoubleHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                r = doubleReductiveLogicOpHelper(da, w, op, 0, a.getSize());
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final boolean[] ar = new boolean[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            ar[idx] = doubleReductiveLogicOpHelper(da, w, op, startIndex, endIndex);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }

                // Combine the result from all threads into one
                switch (op) {
                case ANY: for (int j=0; j < NUM_THREADS; j++) r |= ar[j]; break;
                case ALL: for (int j=0; j < NUM_THREADS; j++) r &= ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported reductive logic operation: " + op
                    );
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Double va = a.getObjectAt(i);
                if (va != null) {
                    switch (op) {
                    case ANY: r |= (va != 0); break;
                    case ALL: r &= (va != 0); break;
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported reductive logic operation: " + op
                        );
                    }
                }
                else {
                    // Mimic NaN semantics
                    // Note that NaN always evaluates to true.
                    switch (op) {
                    case ANY: r = true; break;
                    default:
                        // Nothing to do.
                    }
                }
            }
        }

        // Always return the result
        return r;
    }

    // -------------------------------------------------------------------------

    /**
     * Helper function for {@code doubleExtract} that performs an extract operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube, starting
     * from the given offset index value.
     */
    @SuppressWarnings("inline")
    private static void doubleExtractHelper(
        final BooleanHypercube dc,
        final DoubleHypercube da,
        final DoubleHypercube dr,
        final long startIndex,
        final long endIndex,
        final long offset
    )
    {
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean         [] ac = new boolean         [STAGING_SIZE];
        final double[] aa = new double[STAGING_SIZE];
        final double[] ar = new double[STAGING_SIZE];

        // Define an offset to track our progress on r
        long offsetR = offset;

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            dc.toFlattened(i, ac, 0, len);
            da.toFlattened(i, aa, 0, len);

            int k = 0;
            for (int j=0; j < len; j++) {
                if (Boolean.TRUE.equals(ac[j])) {
                    ar[k++] = aa[j];
                }
            }

            // Copy in
            dr.fromFlattened(ar, 0, offsetR, k);
            offsetR += k;
        }
    }

    /**
     * Handle the extract operation for a Double cube.
     */
    private static Hypercube<Double> doubleExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Double> a
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Checks
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " + a.toDescriptiveString()
            );
        }

        // Create the destination, a simple Array one by default
        return doubleExtract(c, a, new DoubleArrayHypercube(Dimension.of(popcount(c))));
    }

    /**
     * Handle the extract operation for a Double cube, putting the
     * result into a third.
     */
    private static Hypercube<Double> doubleExtract(
        final Hypercube<Boolean> c,
        final Hypercube<Double> a,
        final Hypercube<Double> r
    ) throws IllegalArgumentException,
             NullPointerException
    {
        // Null checks first
        if (c == null) {
            throw new NullPointerException("Given a null cube, 'c'");
        }
        if (a == null) {
            throw new NullPointerException("Given a null cube, 'a'");
        }
        if (r == null) {
            throw new NullPointerException("Given a null cube, 'r'");
        }

        // Compatibility checks
        if (!a.matchesInShape(c)) {
            throw new IllegalArgumentException(
                "Given incompatible cubes: " +
                c.toDescriptiveString() + " vs " +
                a.toDescriptiveString() + " vs " +
                r.toDescriptiveString()
            );
        }

        // Try to do this natively but fall back to the non-native version
        try {
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube dc = (BooleanHypercube)c;
            final DoubleHypercube da = (DoubleHypercube)a;
            final DoubleHypercube dr = (DoubleHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {
                doubleExtractHelper(dc, da, dr, 0, a.getSize(), 0);
            }
            else {
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // An offset to track where in r we should be, when processing
                // each bucket.
                long offset = 0;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final long offsetR = offset;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {
                        latch.countDown();
                        continue;
                    }

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {
                        try {
                            doubleExtractHelper(dc, da, dr, startIndex, endIndex, offsetR);
                        }
                        catch (Exception e) {
                            exception.set(e);
                        }
                        finally {
                            latch.countDown();
                        }
                    });

                    // Update the offset value accordingly
                    offset += popcount(c, startIndex, endIndex);
                }

                // Wait here for all threads to conclude
                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        // Nothing, we will just go around again
                    }
                }

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {
                    throw exception.get();
                }
            }
        }
        catch (Exception e) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(
                    "Falling back to the naive version due to exception: " + e
                );
            }

            // Just do a linear waltz
            for (long i = 0, j=0, size = a.getSize(); i < size; i++) {
                // Need to handle missing values
                final Boolean       vc = c.getObjectAt(i);
                final Double va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(vc)) {
                    r.setObjectAt(j++, va);
                }
            }
        }

        // Always give back the resultant one
        return r;
    }
}

 // [[[end]]] (checksum: 706f122e0c6be3c756958f90a57aa972)
