"""
Cog code to generate cube math library.

This code can be used to create different implementations of cube math, by
specifying the underlying implementation of operations.

For a list of supported data types, refer to ``DTYPES`` below.
"""

import numpy
import params

_CUBE_MATH_IMPL = {
    # A dictionary of required keyword arguments for formatting the cog code.
    # This dictionary should always include 'class_name'.
    'KWARGS': {
        'class_name': 'CubeMath'
    },

    'DTYPES': [
        numpy.bool_,
        numpy.int32,
        numpy.int64,
        numpy.float32,
        numpy.float64
    ],

    'IMPORTS': '''\
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
''',

    'HEADER': '''\

/**
 * Math operations on {{@link Hypercube}} instances.
 *
 * <p>This class provides a subset of Python's various {{@code numpy}} methods
 * as well as similar utility functions. Some methods also support a subset of
 * the corresponding kwargs. It is not current feature-complete with respect to
 * the {{@code numpy}} module and does not aim to better it in performance.
 */
public class {class_name}
{{
''',

    'ADDITIONAL_DECLARATIONS': '''''',

    'COMMON': '''\
    /**
     * The number of elements to stage when processing operations in bulk.
     *
     * <p>This number does not have to be very large for us to get the benefit of
     * loop unrolling. This parameter can be set by passing the Java property
     * {{@code com.deshaw.hypercube.cubemath.stagingSize}}. Otherwise, a default
     * value is set to be fairly small.
     */
    private static final int STAGING_SIZE;

    /**
     * The cube size threshold at which {class_name} will use multithreading.
     *
     * <p>Multithreading has an overhead cost that is only worth bearing when processing
     * a large number of operations. This threshold can be set by passing the Java
     * property {{@code com.deshaw.hypercube.cubemath.threadingThreshold}}. Otherwise,
     * a default threshold value is set to be fairly large. By passing 0, {class_name}
     * will always use multithreaded processing, if possible.
     */
    private static final long THREADING_THRESHOLD;

    /**
     * The number of threads used for multithreaded processing.
     *
     * <p>The number of threads can be set by passing the Java property
     * {{@code com.deshaw.hypercube.cubemath.numThreads}}. Otherwise, the library
     * defaults to using a fairly small number of threads. By passing 0 or 1,
     * {class_name} will not use multithreaded processing.
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
    {{
        ADD, SUB, MUL, DIV, MOD, POW,
        MIN, MAX,
        AND, OR, XOR;
    }}

    /**
     * The 1-ary operations which we can do.
     */
    private enum UnaryOp
    {{
        NEG, ABS,
        FLOOR, ROUND, CEIL,
        COS, COSH, SIN, SINH, TAN, TANH,
        EXP, LOG, LOG10,
        NOT;
    }}

    /**
     * The associative operations which we can do.
     */
    private enum AssociativeOp
    {{
        ADD, MIN, MAX, NANADD;
    }}

    /**
     * The boolean comparison operations which we can do.
     */
    private enum ComparisonOp
    {{
        EQ, NE, LT, GT, LE, GE;
    }}

    /**
     * The reductive logic operations which we can do.
     */
    private enum ReductiveLogicOp
    {{
        ANY, ALL;
    }}

    static {{
        // Try setting {class_name} parameters through Java properties and raise an
        // exception if bad values are passed. In case no Java properties are
        // available for a parameter, a default value is used (refer to individual
        // parameter descriptions for more detail).
        /*scope*/ {{
            final String propName = "com.deshaw.hypercube.cubemath.stagingSize";
            try {{
                STAGING_SIZE = Integer.parseInt(System.getProperty(propName, "128"));
                if (STAGING_SIZE < 1) {{
                    throw new RuntimeException("Bad value for " + propName + ": " +
                                               propName + " has to be a positive integer, " +
                                               "but " + STAGING_SIZE + " was given.");
                }}
            }}
            catch (NumberFormatException e) {{
                throw new RuntimeException("Bad value for " + propName, e);
            }}
        }}
        /*scope*/ {{
            final String propName = "com.deshaw.hypercube.cubemath.threadingThreshold";
            try {{
                THREADING_THRESHOLD = Integer.parseInt(System.getProperty(propName, "131072"));
                if (THREADING_THRESHOLD < 0) {{
                    throw new RuntimeException("Bad value for " + propName + ": " +
                                               propName + " has to be a non-negative integer, " +
                                               "but " + THREADING_THRESHOLD + " was given.");
                }}
            }}
            catch (NumberFormatException e) {{
                throw new RuntimeException("Bad value for " + propName, e);
            }}
        }}
        /*scope*/ {{
            final String propName = "com.deshaw.hypercube.cubemath.numThreads";
            try {{
                NUM_THREADS = Integer.parseInt(System.getProperty(propName, "4"));
                if (NUM_THREADS < 0) {{
                    throw new RuntimeException("Bad value for " + propName + ": " +
                                               propName + " has to be a non-negative integer, " +
                                               "but " + NUM_THREADS + " was given.");
                }}
            }}
            catch (NumberFormatException e) {{
                throw new RuntimeException("Bad value for " + propName, e);
            }}
        }}

        // Only initialize an executor service if NUM_THREADS > 1.
        if (NUM_THREADS > 1) {{
            // Initialize ourExecutorService according to the NUM_THREADS variable
            // and add a hook to shutdown all threads when the main thread terminates.
            ourExecutorService = Executors.newFixedThreadPool(NUM_THREADS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {{
                ourExecutorService.shutdown();
                try {{
                    ourExecutorService.awaitTermination(5, TimeUnit.SECONDS);
                }}
                catch (InterruptedException e) {{
                    // No need to do anything since we are shutting down.
                }}
            }}));
        }}
        else {{
            ourExecutorService = null;
        }}
    }}

    /**
     * Utility class only, never created.
     */
    private {class_name}()
    {{
        // Nothing
    }}

    // -------------------------------------------------------------------------

    /**
     * Convert the given value to a byte, throwing if truncation occurs.
     */
    /*package*/ static byte strictByte(final long value)
        throws ClassCastException
    {{
        final byte v = (byte)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a byte"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a short, throwing if truncation occurs.
     */
    /*package*/ static short strictShort(final long value)
        throws ClassCastException
    {{
        final short v = (short)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a short"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a int, throwing if truncation occurs.
     */
    /*package*/ static int strictInteger(final long value)
        throws ClassCastException
    {{
        final int v = (int)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a int"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a long, throwing if truncation occurs.
     */
    /*package*/ static long strictLong(final long value)
        throws ClassCastException
    {{
        // NOP
        return value;
    }}

    /**
     * Convert the given value to a byte, throwing if truncation occurs.
     */
    /*package*/ static byte strictByte(final double value)
        throws ClassCastException
    {{
        final byte v = (byte)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a byte"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a short, throwing if truncation occurs.
     */
    /*package*/ static short strictShort(final double value)
        throws ClassCastException
    {{
        final short v = (short)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a short"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a int, throwing if truncation occurs.
     */
    /*package*/ static int strictInteger(final double value)
        throws ClassCastException
    {{
        final int v = (int)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a int"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a long, throwing if truncation occurs.
     */
    /*package*/ static long strictLong(final double value)
        throws ClassCastException
    {{
        final long v = (long)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a long"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a byte, throwing if truncation occurs.
     */
    /*package*/ static byte strictByte(final Number value)
        throws ClassCastException
    {{
        if (value == null) {{
            throw new NullPointerException("Given a null value");
        }}
        return ((value instanceof Double) || (value instanceof Float))
            ? strictByte(value.doubleValue()) :
              strictByte(value.longValue());
    }}

    /**
     * Convert the given value to a short, throwing if truncation occurs.
     */
    /*package*/ static short strictShort(final Number value)
        throws ClassCastException
    {{
        if (value == null) {{
            throw new NullPointerException("Given a null value");
        }}
        return ((value instanceof Double) || (value instanceof Float))
            ? strictShort(value.doubleValue()) :
              strictShort(value.longValue());
    }}

    /**
     * Convert the given value to a int, throwing if truncation occurs.
     */
    /*package*/ static int strictInteger(final Number value)
        throws ClassCastException
    {{
        if (value == null) {{
            throw new NullPointerException("Given a null value");
        }}
        return ((value instanceof Double) || (value instanceof Float))
            ? strictInteger(value.doubleValue()) :
              strictInteger(value.longValue());
    }}

    /**
     * Convert the given value to a long, throwing if truncation occurs.
     */
    /*package*/ static long strictLong(final Number value)
        throws ClassCastException
    {{
        if (value == null) {{
            throw new NullPointerException("Given a null value");
        }}
        return ((value instanceof Double) || (value instanceof Float))
            ? strictLong(value.doubleValue()) :
              strictLong(value.longValue());
    }}

    /**
     * Convert the given value to a float, throwing if truncation occurs.
     */
    /*package*/ static float strictFloat(final long value)
        throws ClassCastException
    {{
        final float v = (float)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a float"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a double, throwing if truncation occurs.
     */
    /*package*/ static double strictDouble(final long value)
        throws ClassCastException
    {{
        final double v = (double)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a double"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a float, throwing if truncation occurs.
     */
    /*package*/ static float strictFloat(final double value)
        throws ClassCastException
    {{
        final float v = (float)value;
        if (value != v) {{
            throw new ClassCastException(
                "Cannot convert " + value + " to a float"
            );
        }}
        return v;
    }}

    /**
     * Convert the given value to a double, throwing if truncation occurs.
     */
    /*package*/ static double strictDouble(final double value)
        throws ClassCastException
    {{
        // NOP
        return value;
    }}

    /**
     * Convert the given value to a float, throwing if truncation occurs.
     */
    /*package*/ static float strictFloat(final Number value)
        throws ClassCastException
    {{
        if (value == null) {{
            throw new NullPointerException("Given a null value");
        }}
        return ((value instanceof Double) || (value instanceof Float))
            ? strictFloat(value.doubleValue()) :
              strictFloat(value.longValue());
    }}

    /**
     * Convert the given value to a double, throwing if truncation occurs.
     */
    /*package*/ static double strictDouble(final Number value)
        throws ClassCastException
    {{
        if (value == null) {{
            throw new NullPointerException("Given a null value");
        }}
        return ((value instanceof Double) || (value instanceof Float))
            ? strictDouble(value.doubleValue()) :
              strictDouble(value.longValue());
    }}

    // -------------------------------------------------------------------------

    /**
     * Return an empty cube of the given dimensions and type.
     *
     * @param shape   The shape of the cube to return.
     * @param kwargs  <ul>
     *                  <li>{@code dtype='float'} -- The type for the cube.</li>
     *                </ul>
     *
     * @throws NullPointerException          If any given cube was {{@code null}}.
     */
    @Kwargs(value="dtype")
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static Hypercube<?> empty(final Object             shape,
                                     final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        if (shape == null) {{
            throw new NullPointerException("Given a null shape");
        }}
        final Dimension<?>[] dims;
        if (shape instanceof Dimension[]) {{
            dims = (Dimension<?>[])shape;
        }}
        else {{
            dims = Dimension.of(KwargUtil.toLongArray(shape));
        }}

        final Object dtype = (kwargs == null) ? "float64" : kwargs.get("dtype");
        final DType.Type type = KwargUtil.toDTypeType(dtype);
        final Class<?> klass = type.objectClass;

        // See if we can do it, leveraging our already existing casting methods
        if (klass.equals(Double.class)) {{
            return new DoubleArrayHypercube(dims);
        }}
        else if (klass.equals(Float.class)) {{
            return new FloatArrayHypercube(dims);
        }}
        else if (klass.equals(Integer.class)) {{
            return new IntegerArrayHypercube(dims);
        }}
        else if (klass.equals(Long.class)) {{
            return new LongArrayHypercube(dims);
        }}
        else if (klass.equals(Boolean.class)) {{
            return new BooleanBitSetHypercube(dims);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " + dtype
            );
        }}
    }}

    /**
     * Return an empty cube of the given dimensions and type full of zeroes.
     *
     * @param shape   The shape of the cube to return.
     * @param kwargs  <ul>
     *                  <li>{@code dtype='float'} -- The type for the cube.</li>
     *                </ul>
     *
     * @throws NullPointerException          If any given cube was {{@code null}}.
     */
    @Kwargs(value="dtype")
    @GenericReturnType
    public static Hypercube<?> zeros(final Object             shape,
                                     final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        if (shape == null) {{
            throw new NullPointerException("Given a null shape");
        }}
        final Dimension<?>[] dims;
        if (shape instanceof Dimension[]) {{
            dims = (Dimension<?>[])shape;
        }}
        else {{
            dims = Dimension.of(KwargUtil.toLongArray(shape));
        }}

        final Object dtype = (kwargs == null) ? "float64" : kwargs.get("dtype");
        final DType.Type type = KwargUtil.toDTypeType(dtype);
        final Class<?> klass = type.objectClass;

        // See if we can do it, leveraging our already existing casting methods
        if (klass.equals(Double.class)) {{
            return full(dims, (double)0);
        }}
        else if (klass.equals(Float.class)) {{
            return full(dims, (float)0);
        }}
        else if (klass.equals(Integer.class)) {{
            return full(dims, (int)0);
        }}
        else if (klass.equals(Long.class)) {{
            return full(dims, (long)0);
        }}
        else if (klass.equals(Boolean.class)) {{
            return full(dims, false);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " + dtype
            );
        }}
    }}

    /**
     * Return an empty cube of the given dimensions and type full of ones.
     *
     * @param shape   The shape of the cube to return.
     * @param kwargs  <ul>
     *                  <li>{@code dtype='float'} -- The type for the cube.</li>
     *                </ul>
     *
     * @throws NullPointerException          If any given cube was {{@code null}}.
     */
    @Kwargs(value="dtype")
    @GenericReturnType
    public static Hypercube<?> ones(final Object             shape,
                                    final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        if (shape == null) {{
            throw new NullPointerException("Given a null shape");
        }}
        final Dimension<?>[] dims;
        if (shape instanceof Dimension[]) {{
            dims = (Dimension<?>[])shape;
        }}
        else {{
            dims = Dimension.of(KwargUtil.toLongArray(shape));
        }}

        final Object dtype = (kwargs == null) ? "float64" : kwargs.get("dtype");
        final DType.Type type = KwargUtil.toDTypeType(dtype);
        final Class<?> klass = type.objectClass;

        // See if we can do it, leveraging our already existing casting methods
        if (klass.equals(Double.class)) {{
            return full(dims, (double)1);
        }}
        else if (klass.equals(Float.class)) {{
            return full(dims, (float)1);
        }}
        else if (klass.equals(Integer.class)) {{
            return full(dims, (int)1);
        }}
        else if (klass.equals(Long.class)) {{
            return full(dims, (long)1);
        }}
        else if (klass.equals(Boolean.class)) {{
            return full(dims, true);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " + dtype
            );
        }}
    }}

    /**
     * Return a new copy of a given cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {{@code null}}.
     */
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> copy(final Hypercube<T> a)
        throws NullPointerException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // See if we can do it, leveraging our already existing casting methods
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)toDoubleHypercube((Hypercube<Double>)a);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)toFloatHypercube((Hypercube<Float>)a);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)toIntegerHypercube((Hypercube<Integer>)a);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)toLongHypercube((Hypercube<Long>)a);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)toBooleanHypercube((Hypercube<Boolean>)a);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

    /**
     * Copy a given cube, putting the result into a second.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     */
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> copy(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}
        if (!a.matches(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleToDoubleHypercube((Hypercube<Double>)a,
                                                         (Hypercube<Double>)r);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatToFloatHypercube((Hypercube<Float>)a,
                                                       (Hypercube<Float>)r);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intToIntegerHypercube((Hypercube<Integer>)a,
                                                       (Hypercube<Integer>)r);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longToLongHypercube((Hypercube<Long>)a,
                                                     (Hypercube<Long>)r);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)booleanToBooleanHypercube((Hypercube<Boolean>)a,
                                                           (Hypercube<Boolean>)r);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to copy cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

    // -------------------------------------------------------------------------

    /**
     * Roll a hypercube's elements along a given axis.
     *
     * @param shift   The (signed) number of elements to roll this cube by or,
     *                if an array, in each axis.
     * @param kwargs  <ul>
     *                  <li>{@code axis=0} -- The axis to roll or, if {{@code shift}}
     *                      is an array, a matching array of axes.</li>
     *                </ul>
     *
     * <p>This follows {{@code numpy.roll()}}'s semantics.
     *
     * @return The rolled cube.
     *
     * @throws IllegalArgumentException If the shift was not valid.
     * @throws NullPointerException     If the given cube was {{@code null}}.
     */
    @Kwargs(value="axis")
    @SuppressWarnings("unchecked")
    @GenericReturnType
    public static <T> Hypercube<T> roll(final Hypercube<T> a,
                                        final Object shift,
                                        final Map<String,Object> kwargs)
        throws IllegalArgumentException,
               NullPointerException
    {{
        if (shift instanceof Number) {{
            return a.roll(((Number)shift).longValue(), kwargs);
        }}
        else if (shift instanceof byte[]) {{
            final byte[] shifts = (byte[])shift;
            final long[] longs  = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {{
                longs[i] = shifts[i];
            }}
            return a.roll(longs, kwargs);
        }}
        else if (shift instanceof short[]) {{
            final short[] shifts = (short[])shift;
            final long[] longs   = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {{
                longs[i] = shifts[i];
            }}
            return a.roll(longs, kwargs);
        }}
        else if (shift instanceof int[]) {{
            final int[]  shifts = (int[])shift;
            final long[] longs  = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {{
                longs[i] = shifts[i];
            }}
            return a.roll(longs, kwargs);
        }}
        else if (shift instanceof long[]) {{
            return a.roll((long[])shift, kwargs);
        }}
        else if (shift instanceof Object[] &&
                 ((Object[])shift).length > 0 &&
                 ((Object[])shift)[0] instanceof Number)
        {{
            final Object[] shifts = (Object[])shift;
            final long[]   longs  = new long[shifts.length];
            for (int i=0; i < shifts.length; i++) {{
                longs[i] = ((Number)(shifts[i])).longValue();
            }}
            return a.roll(longs, kwargs);
        }}
        else {{
            throw new IllegalArgumentException("Unhandle shift type: " + shift);
        }}
    }}

    // -------------------------------------------------------------------------

    /**
     * Whether two cubes are equal or not.
     *
     * @return The resulting count.
     *
     * @throws NullPointerException If the given cube was {{@code null}}.
     */
    public static boolean arrayEquals(final Hypercube<?> a, final Hypercube<?> b)
    {{
        return (a != null && a.equals(b));
    }}

    /**
     * Count the number of {{@code true}} (populated) values in a boolean hypercube.{popcount_extra_javadoc}
     *
     * @return The resulting count.
     *
     * @throws NullPointerException If the given cube was {{@code null}}.
     */
    public static long popcount(final Hypercube<Boolean> a)
        throws NullPointerException
    {{
        return popcount(a, 0L, a.getSize());
    }}

    // -------------------------------------------------------------------------

    /**
     * Pairwise-add two cubes together.{add_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.ADD);
    }}

    /**
     * Pairwise-add two cubes together, putting the result into a third.{add_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.ADD);
    }}

    /**
     * Element-wise add a value to a cube.{add_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.ADD);
    }}

    /**
     * Element-wise add a value to a cube, putting the result into a second cube.{add_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.ADD);
    }}

    /**
     * Element-wise add a value to a cube.{add_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> add(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.ADD);
    }}

    /**
     * Element-wise add a value to a cube, putting the result into a second cube.{add_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.ADD);
    }}

    /**
     * Pairwise-subtract one cube from another.{subtract_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final Hypercube<T> a,
                                            final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.SUB);
    }}

    /**
     * Pairwise-subtract one cube from another, putting the result into a third.{subtract_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.SUB);
    }}

    /**
     * Element-wise subtract a value from a cube.{subtract_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final Hypercube<T> a,
                                            final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.SUB);
    }}

    /**
     * Element-wise subtract a value from a cube, putting the result into a second cube.{subtract_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.SUB);
    }}

    /**
     * Element-wise subtract a cube from a value.{subtract_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> subtract(final T            a,
                                            final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.SUB);
    }}

    /**
     * Element-wise subtract a cube from a value, putting the result into a second cube.{subtract_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.SUB);
    }}

    /**
     * Pairwise-multiply two cubes together.{multiply_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final Hypercube<T> a,
                                            final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.MUL);
    }}

    /**
     * Pairwise-multiply two cubes together, putting the result into a third.{multiply_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.MUL);
    }}

    /**
     * Element-wise multiply a value and a cube together.{multiply_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final Hypercube<T> a,
                                            final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MUL);
    }}

    /**
     * Element-wise multiply a value and a cube together, putting the result
     * into a second cube.{multiply_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MUL);
    }}

    /**
     * Element-wise multiply a value and a cube together.{multiply_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> multiply(final T            a,
                                            final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MUL);
    }}

    /**
     * Element-wise multiply a value and a cube together, putting the result
     * into a second cube.{multiply_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MUL);
    }}

    /**
     * Pairwise-divide one cube by another.{divide_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final Hypercube<T> a,
                                          final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.DIV);
    }}

    /**
     * Pairwise-divide one cube by another, putting the result into a third.{divide_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.DIV);
    }}

    /**
     * Element-wise divide a cube by a value.{divide_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final Hypercube<T> a,
                                          final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.DIV);
    }}

    /**
     * Element-wise divide a cube by a value, putting the result into a second cube.{divide_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.DIV);
    }}

    /**
     * Element-wise divide a value by a cube.{divide_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> divide(final T            a,
                                          final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.DIV);
    }}

    /**
     * Element-wise divide a value by a cube, putting the result into a second cube.{divide_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.DIV);
    }}

    /**
     * Pairwise-modulo one cube by another.{mod_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.MOD);
    }}

    /**
     * Pairwise-modulo one cube by another, putting the result into a third.{mod_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.MOD);
    }}

    /**
     * Element-wise module a cube by a value.{mod_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MOD);
    }}

    /**
     * Element-wise module a cube by a value, putting the result into a second cube.{mod_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MOD);
    }}

    /**
     * Element-wise module a value by a cube.{mod_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> mod(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MOD);
    }}

    /**
     * Element-wise module a value by a cube, putting the result into a second cube.{mod_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MOD);
    }}

    /**
     * Perform the pairwise power operation on the elements of two cubes.{power_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final Hypercube<T> a,
                                         final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.POW);
    }}

    /**
     * Perform the pairwise power operation on the elements of two cubes,
     * putting the result into a third.{power_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.POW);
    }}

    /**
     * Perform the element-wise power operation on a cube and a value.{power_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final Hypercube<T> a,
                                         final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.POW);
    }}

    /**
     * Perform the element-wise power operation on a cube and a value, putting
     * the result into a second cube.{power_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.POW);
    }}

    /**
     * Perform the element-wise power operation on a value and a cube.{power_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> power(final T            a,
                                         final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.POW);
    }}

    /**
     * Perform the element-wise power operation on a value and a cube, putting
     * the result into a second cube.{power_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.POW);
    }}

    /**
     * Pairwise compare two cubes and return a new cube containing the element-wise
     * minima.{minimum_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final Hypercube<T> a,
                                           final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.MIN);
    }}

    /**
     * Pairwise compare two cubes and put the result of the element-wise minima
     * into a third.{minimum_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.MIN);
    }}

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise minima.{minimum_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final Hypercube<T> a,
                                           final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MIN);
    }}

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise minima into a second cube.{minimum_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MIN);
    }}

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise minima.{minimum_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> minimum(final T            a,
                                           final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MIN);
    }}

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise minima into a second cube.{minimum_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MIN);
    }}

    /**
     * Pairwise compare two cubes and return a new cube containing the element-wise
     * maxima.{maximum_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final Hypercube<T> a,
                                           final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.MAX);
    }}

    /**
     * Pairwise compare two cubes and put the result of the element-wise maxima
     * into a third.{maximum_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.MAX);
    }}

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise maxima.{maximum_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final Hypercube<T> a,
                                           final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.MAX);
    }}

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise maxima into a second cube.{maximum_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.MAX);
    }}

    /**
     * Element-wise compare a cube with a value and return a new cube containing
     * the element-wise maxima.{maximum_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> maximum(final T            a,
                                           final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.MAX);
    }}

    /**
     * Element-wise compare a cube with a value and put the result of the
     * element-wise maxima into a second cube.{maximum_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.MAX);
    }}

    // -------------------------------------------------------------------------

    /**
     * Negate all elements of a given cube.{negative_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> negative(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.NEG);
    }}

    /**
     * Negate all elements of a given cube, putting the result into a second cube.{negative_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> negative(final Hypercube<T> a,
                                            final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.NEG);
    }}

    /**
     * Apply an absolute-value operation on all elements of a given cube.{abs_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> abs(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.ABS);
    }}

    /**
     * Apply an absolute-value operation on all elements of a given cube,
     * putting the result into a second cube.{abs_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> abs(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.ABS);
    }}

    /**
     * Apply a floor operation on all elements of a given cube.{floor_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> floor(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.FLOOR);
    }}

    /**
     * Apply a floor operation on all elements of a given cube, putting the
     * result into a second cube.{floor_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> floor(final Hypercube<T> a,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.FLOOR);
    }}

    /**
     * Round all the elements of a given cube to the nearest whole number.{round_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> round(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.ROUND);
    }}

    /**
     * Round all the elements of a given cube to the nearest whole number,
     * putting the result into a second cube.{round_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> round(final Hypercube<T> a,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.ROUND);
    }}

    /**
     * Apply a ceil operation on all elements of a given cube.{ceil_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> ceil(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.CEIL);
    }}

    /**
     * Apply a ceil operation on all elements of a given cube, putting the
     * result into a second cube.{ceil_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> ceil(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.CEIL);
    }}

    /**
     * Apply a cosine operation on all elements of a given cube.{cos_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cos(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.COS);
    }}

    /**
     * Apply a cosine operation on all elements of a given cube, putting the
     * result into a second cube.{cos_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cos(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.COS);
    }}

    /**
     * Apply a sine operation on all elements of a given cube.{sin_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sin(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.SIN);
    }}

    /**
     * Apply a sine operation on all elements of a given cube, putting the result
     * into a second cube.{sin_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sin(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.SIN);
    }}

    /**
     * Apply a tangent operation on all elements of a given cube.{tan_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tan(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.TAN);
    }}

    /**
     * Apply a tangent operation on all elements of a given cube, putting the
     * result into a second cube.{tan_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tan(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.TAN);
    }}

    /**
     * Apply an exponential operation on all elements of a given cube.{exp_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> exp(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.EXP);
    }}

    /**
     * Apply an exponential operation on all elements of a given cube, putting
     * the result into a second cube.{exp_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> exp(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.EXP);
    }}

    /**
     * Apply a natural logarithm operation on all elements of a given cube.{log_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.LOG);
    }}

    /**
     * Apply a natural logarithm operation on all elements of a given cube,
     * putting the result into a second cube.{log_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.LOG);
    }}

    /**
     * Apply a logarithm operation in base 10 on all elements of a given cube.{log10_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log10(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.LOG10);
    }}

    /**
     * Apply a logarithm operation in base 10 on all elements of a given cube,
     * putting the result into a second cube.{log10_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> log10(final Hypercube<T> a,
                                         final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.LOG10);
    }}

    /**
     * Apply a hyperbolic cosine operation on all elements of a given cube.{cosh_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cosh(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.COSH);
    }}

    /**
     * Apply a hyperbolic cosine operation on all elements of a given cube,
     * putting the result into a second cube.{cosh_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> cosh(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.COSH);
    }}

    /**
     * Apply a hyperbolic sine operation on all elements of a given cube.{sinh_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sinh(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.SINH);
    }}

    /**
     * Apply a hyperbolic sine operation on all elements of a given cube,
     * putting the result into a second cube.{sinh_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> sinh(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.SINH);
    }}

    /**
     * Apply a hyperbolic tangent operation on all elements of a given cube.{tanh_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tanh(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.TANH);
    }}

    /**
     * Apply a hyperbolic tangent operation on all elements of a given cube,
     * putting the result into a second cube.{tanh_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant
     *         values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> tanh(final Hypercube<T> a,
                                        final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.TANH);
    }}

    // -------------------------------------------------------------------------

    /**
     * Sum all the values of a cube.{sum_extra_javadoc}
     *
     * @return The resulting sum, which will be {{@code NaN}} if any of the values
     *         were {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T sum0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, null, null, AssociativeOp.ADD);
    }}

    /**
     * Sum all the values of a cube.{sum_extra_javadoc}
     *
     * @return The resulting sum, which will be {{@code NaN}} if any of the values
     *         were {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T sum0d(final Hypercube<T> a,
                              final T initial,
                              final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, initial, where, AssociativeOp.ADD);
    }}

    /**
     * Returns the minimum value of a cube.{min_extra_javadoc}
     *
     * @return The resulting minimum, which will be {{@code NaN}} if any of the
     *         values were {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T min0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, null, null, AssociativeOp.MIN);
    }}

    /**
     * Returns the minimum value of a cube.{min_extra_javadoc}
     *
     * @return The resulting minimum, which will be {{@code NaN}} if any of the
     *         values were {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T min0d(final Hypercube<T> a,
                              final T initial,
                              final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, initial, where, AssociativeOp.MIN);
    }}

    /**
     * Returns the maximum value of a cube.{max_extra_javadoc}
     *
     * @return The resulting maximum, which will be {{@code NaN}} if any of the
     *         values were {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T max0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, null, null, AssociativeOp.MAX);
    }}

    /**
     * Returns the maximum value of a cube.{max_extra_javadoc}
     *
     * @return The resulting maximum, which will be {{@code NaN}} if any of the
     *         values were {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T max0d(final Hypercube<T> a,
                              final T initial,
                              final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, initial, where, AssociativeOp.MAX);
    }}

    /**
     * Sum all the values of a cube, ignoring floating point {{@code NaN}} values
     * where applicable.{nansum_extra_javadoc}
     *
     * @return The resulting sum, which will be zero if all the values were
     *         {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T nansum0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, null, null, AssociativeOp.NANADD);
    }}

    /**
     * Sum all the values of a cube, ignoring floating point {{@code NaN}} values
     * where applicable.{nansum_extra_javadoc}
     *
     * @return The resulting sum, which will be zero if all the values were
     *         {{@code NaN}}s.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> T nansum0d(final Hypercube<T> a,
                                 final T initial,
                                 final BooleanHypercube where)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return associativeOp(a, initial, where, AssociativeOp.NANADD);
    }}

    /**
     * Sum all the values of a cube, along the given axes.{sum_extra_javadoc}
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.ADD);
    }}

    /**
     * Returns the minimum value of a cube, along the given axes.{min_extra_javadoc}
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.MIN);
    }}

    /**
     * Returns the maximum value of a cube, along the given axes.{max_extra_javadoc}
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.MAX);
    }}

    /**
     * Sum all the values of a cube, ignoring floating point {{@code NaN}} values
     * where applicable, along the given axes.{nansum_extra_javadoc}
     *
     * @return A cube containing the result.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        return associativeOpByAxes(a, axes, initial, where, AssociativeOp.NANADD);
    }}

    /**
     * Sum all the values of a cube, potentially along the given axes.{sum_extra_javadoc}
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
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
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }}
            }}
        }}
        if (axes == null) {{
            return associativeOp(a, initial, where, AssociativeOp.ADD);
        }}
        else {{
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.ADD);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }}
    }}

    /**
     * Returns the minimum value of a cube, potentially along the given axes.{min_extra_javadoc}
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
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
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }}
            }}
        }}
        if (axes == null) {{
            return associativeOp(a, initial, where, AssociativeOp.MIN);
        }}
        else {{
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.MIN);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }}
    }}

    /**
     * Returns the maximum value of a cube, potentially along the given axes.{max_extra_javadoc}
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
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
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }}
            }}
        }}
        if (axes == null) {{
            return associativeOp(a, initial, where, AssociativeOp.MAX);
        }}
        else {{
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.MAX);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }}
    }}

    /**
     * Sum all the values of a cube, ignoring floating point {{@code NaN}} values
     * where applicable, potentially along the given axes.{nansum_extra_javadoc}
     *
     * @return A cube containing the result, or a single value.
     *
     * @throws IllegalArgumentException      If the arguments were bad.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        int[] axes = null;
        T initial = null;
        BooleanHypercube where = null;
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
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
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }}
            }}
        }}
        if (axes == null) {{
            return associativeOp(a, initial, where, AssociativeOp.NANADD);
        }}
        else {{
            final Hypercube<T> result =
                associativeOpByAxes(a, axes, initial, where, AssociativeOp.NANADD);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }}
    }}

    // -------------------------------------------------------------------------

    /**
     * Perform the pairwise logic or bitwise AND operation on the elements of two cubes.{and_extra_javadoc}
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.AND);
    }}

    /**
     * Perform the pairwise logic or bitwise AND operation on the elements of two cubes,
     * putting the result into a third.{and_extra_javadoc}
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.AND);
    }}

    /**
     * Element-wise AND a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.AND);
    }}

    /**
     * Element-wise AND a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> and(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.AND);
    }}

    /**
     * Element-wise AND a value to a cube, putting the result into a second cube.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.AND);
    }}

    /**
     * Element-wise AND a value to a cube, putting the result into a second cube.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.AND);
    }}

    /**
     * Perform the pairwise logic or bitwise OR operation on the elements of two cubes.
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final Hypercube<T> a,
                                      final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.OR);
    }}

    /**
     * Perform the pairwise logic or bitwise OR operation on the elements of two cubes,
     * putting the result into a third.{or_extra_javadoc}
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.OR);
    }}

    /**
     * Element-wise OR a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final Hypercube<T> a,
                                      final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.OR);
    }}

    /**
     * Element-wise OR a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> or(final T            a,
                                      final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.OR);
    }}

    /**
     * Element-wise or a value to a cube, putting the result into a second cube.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.OR);
    }}

    /**
     * Element-wise or a value to a cube, putting the result into a second cube.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.OR);
    }}

    /**
     * Perform the pairwise logic or bitwise XOR operation on the elements of two cubes.
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final Hypercube<T> a,
                                       final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, b, null, BinaryOp.XOR);
    }}

    /**
     * Perform the pairwise logic or bitwise XOR operation on the elements of two cubes,
     * putting the result into a third.{xor_extra_javadoc}
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, b, r, null, BinaryOp.XOR);
    }}

    /**
     * Element-wise xor a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final Hypercube<T> a,
                                       final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(a, singleValuedCube(a, b), null, BinaryOp.XOR);
    }}

    /**
     * Element-wise xor a value to a cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @GenericReturnType
    public static <T> Hypercube<T> xor(final T            a,
                                       final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return binaryOp(singleValuedCube(b, a), b, null, BinaryOp.XOR);
    }}

    /**
     * Element-wise xor a value to a cube, putting the result into a second cube.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(a, singleValuedCube(a, b), r, null, BinaryOp.XOR);
    }}

    /**
     * Element-wise xor a value to a cube, putting the result into a second cube.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in
     *         it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
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
    {{
        return binaryOp(singleValuedCube(b, a), b, r, null, BinaryOp.XOR);
    }}

    // -------------------------------------------------------------------------

    /**
     * Perform the element-wise logic or bitwise NOT operation on the elements of a cube.{not_extra_javadoc}
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> not(final Hypercube<T> a)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, UnaryOp.NOT);
    }}

    /**
     * Perform the element-wise logic or bitwise NOT operation on the elements of
     * a cube, putting the result into a second cube.{not_extra_javadoc}
     *
     * <p>{class_name} will use logical operation for boolean cubes and bitwise
     * operation for integer cubes.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @GenericReturnType
    public static <T> Hypercube<T> not(final Hypercube<T> a,
                                       final Hypercube<T> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return unaryOp(a, r, UnaryOp.NOT);
    }}

    // -------------------------------------------------------------------------

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of an element-wise equality operation.{equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T> a,
                                               final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, ComparisonOp.EQ);
    }}

    /**
     * Pairwise compare two cubes and put the result of an element-wise
     * equality operation into a third (boolean) cube.{equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T>       a,
                                               final Hypercube<T>       b,
                                               final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, r, ComparisonOp.EQ);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise equality operation.{equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T> a,
                                               final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.EQ);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * equality operation into a second (boolean) cube.{equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final Hypercube<T>       a,
                                               final T                  b,
                                               final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.EQ);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise equality operation.{equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> equal(final T            a,
                                               final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.EQ);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * equality operation into a second (boolean) cube.{equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> equal(final T                  a,
                                               final Hypercube<T>       b,
                                               final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.EQ);
    }}

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of an element-wise inequality operation.{not_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T> a,
                                                  final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, ComparisonOp.NE);
    }}

    /**
     * Pairwise compare two cubes and put the result of an element-wise
     * inequality operation into a third (boolean) cube.{not_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T>       a,
                                                  final Hypercube<T>       b,
                                                  final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, r, ComparisonOp.NE);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise inequality operation.{not_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T> a,
                                                  final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.NE);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * inequality operation into a second (boolean) cube.{not_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final Hypercube<T>       a,
                                                  final T                  b,
                                                  final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.NE);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise inequality operation.{not_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> notEqual(final T            a,
                                                  final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.NE);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * inequality operation into a second (boolean) cube.{not_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> notEqual(final T                  a,
                                                  final Hypercube<T>       b,
                                                  final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.NE);
    }}

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of an element-wise less-than operation.{less_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T> a,
                                              final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, ComparisonOp.LT);
    }}

    /**
     * Pairwise compare two cubes and put the result of an element-wise
     * less-than operation into a third (boolean) cube.{less_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T>       a,
                                              final Hypercube<T>       b,
                                              final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, r, ComparisonOp.LT);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than operation.{less_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T> a,
                                              final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.LT);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than operation into a second (boolean) cube.{less_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final Hypercube<T>       a,
                                              final T                  b,
                                              final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.LT);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than operation.{less_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> less(final T            a,
                                              final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.LT);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than operation into a second (boolean) cube.{less_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> less(final T                  a,
                                              final Hypercube<T>       b,
                                              final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.LT);
    }}

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of the element-wise greater-than operation.{greater_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T> a,
                                                 final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, ComparisonOp.GT);
    }}

    /**
     * Pairwise compare two cubes and put the result of the element-wise
     * greater-than operation into a third (boolean) cube.{greater_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T>       a,
                                                 final Hypercube<T>       b,
                                                 final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, r, ComparisonOp.GT);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than operation.{greater_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T> a,
                                                 final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.GT);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than operation into a second (boolean) cube.{greater_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final Hypercube<T>       a,
                                                 final T                  b,
                                                 final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.GT);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than operation.{greater_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greater(final T            a,
                                                 final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.GT);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than operation into a second (boolean) cube.{greater_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greater(final T                  a,
                                                 final Hypercube<T>       b,
                                                 final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.GT);
    }}

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of the element-wise less-than-or-equal-to operation.{less_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T> a,
                                                   final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, ComparisonOp.LE);
    }}

    /**
     * Pairwise compare two cubes and put the result of the element-wise
     * less-than-or-equal-to operation into a third (boolean) cube.{less_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T>       a,
                                                   final Hypercube<T>       b,
                                                   final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, r, ComparisonOp.LE);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than-or-equal-to operation.{less_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T> a,
                                                   final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.LE);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than-or-equal-to operation into a second (boolean) cube.{less_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final Hypercube<T>       a,
                                                   final T                  b,
                                                   final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.LE);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise less-than-or-equal-to operation.{less_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> lessEqual(final T            a,
                                                   final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.LE);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * less-than-or-equal-to operation into a second (boolean) cube.{less_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> lessEqual(final T                  a,
                                                   final Hypercube<T>       b,
                                                   final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.LE);
    }}

    /**
     * Pairwise compare two cubes and return a new boolean cube containing the
     * result of the element-wise greater-than-or-equal-to operation.{greater_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T> a,
                                                      final Hypercube<T> b)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, ComparisonOp.GE);
    }}

    /**
     * Pairwise compare two cubes and put the result of the element-wise
     * greater-than-or-equal-to operation into a third (boolean) cube.{greater_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T>       a,
                                                      final Hypercube<T>       b,
                                                      final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, b, r, ComparisonOp.GE);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than-or-equal-to operation.{greater_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T> a,
                                                      final T            b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), ComparisonOp.GE);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than-or-equal-to operation into a second (boolean) cube.{greater_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final Hypercube<T>       a,
                                                      final T                  b,
                                                      final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(a, singleValuedCube(a, b), r, ComparisonOp.GE);
    }}

    /**
     * Element-wise compare a cube with a value and return a new boolean cube
     * containing the result of an element-wise greater-than-or-equal-to operation.{greater_equal_extra_javadoc}
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final T            a,
                                                      final Hypercube<T> b)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, ComparisonOp.GE);
    }}

    /**
     * Pairwise compare a cube with a value and put the result of an element-wise
     * greater-than-or-equal-to operation into a second (boolean) cube.{greater_equal_extra_javadoc}
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<Boolean> greaterEqual(final T                  a,
                                                      final Hypercube<T>       b,
                                                      final Hypercube<Boolean> r)
        throws IllegalArgumentException,
               NullPointerException,
               UnsupportedOperationException
    {{
        return comparisonOp(singleValuedCube(b, a), b, r, ComparisonOp.GE);
    }}

    // -------------------------------------------------------------------------

    /**
     * Returns whether any elements of the given cube evaluate to {{@code true}}.{any_extra_javadoc}
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> boolean any0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return reductiveLogicOp(a, null, ReductiveLogicOp.ANY);
    }}

    /**
     * Returns whether all elements of the given cube evaluate to {{@code true}}.{all_extra_javadoc}
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    public static <T> boolean all0d(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        return reductiveLogicOp(a, null, ReductiveLogicOp.ALL);
    }}

    /**
     * Returns whether any elements of the given cube evaluate to {{@code true}}.{any_extra_javadoc}
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,out,keepdims,where")
    @GenericReturnType
    public static <T> Object any(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        int[] axes = null;
        BooleanHypercube where = null;
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                case "initial":
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }}
            }}
        }}
        if (axes == null) {{
            return reductiveLogicOp(a, null, ReductiveLogicOp.ANY);
        }}
        else {{
            final BooleanHypercube result = reductiveLogicOpByAxes(a, axes, where, ReductiveLogicOp.ANY);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }}
    }}

    /**
     * Returns whether all elements of the given cube evaluate to {{@code true}}.{all_extra_javadoc}
     *
     * @return The resulting boolean.
     *
     * @throws NullPointerException          If the given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cube.
     */
    @Kwargs("axis,out,keepdims,where")
    @GenericReturnType
    public static <T> Object all(final Hypercube<T> a,
                                 final Map<String,Object> kwargs)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        int[] axes = null;
        BooleanHypercube where = null;
        if (kwargs != null) {{
            for (Map.Entry<String,Object> entry : kwargs.entrySet()) {{
                switch (entry.getKey()) {{
                case "axis":
                    axes = KwargUtil.toIntArray(entry.getValue());
                    break;

                case "where":
                    where = KwargUtil.toBooleanHypercube(entry.getValue());
                    break;

                case "out":
                case "initial":
                    if (entry.getValue() != null) {{
                        throw new UnsupportedOperationException(
                            "Unhandled kwarg: " + entry
                        );
                    }}
                    break;

                default:
                    throw new UnsupportedOperationException(
                        "Unhandled kwarg: " + entry
                    );
                }}
            }}
        }}
        if (axes == null) {{
            return reductiveLogicOp(a, null, ReductiveLogicOp.ALL);
        }}
        else {{
            final BooleanHypercube result = reductiveLogicOpByAxes(a, axes, where, ReductiveLogicOp.ALL);
            return result.isSingleton() ? result.getObjectAt(0) : result;
        }}
    }}

    // -------------------------------------------------------------------------

    /**
     * Return the elements of a cube that satisfy some condition, provided as
     * a boolean cube. Note that this operations is always flattening, meaning
     * that the resulting cube will always be 1-dimensional (this is equivalent
     * to {{@code numpy.extract}}'s behavior), e.g: <pre>
     *
     *   [[1, 2, 3],               [[False, False, False],
     *    [4, 5, 6],  selected by   [False, True,  True ],  ==> [5, 6, 7, 8, 9]
     *    [7, 8, 9]]                [True,  True,  True ]]
     *
     * </pre>{extract_extra_javadoc}
     *
     * @params c A cube describing the condition. If an element of c is set to
     *           {{@code True}}, the corresponding element of the cube is extracted.
     * @params a The cube to extract from.
     *
     * @return The resulting cube containing only the elements where the
     *         corresponding element in the condition evaluate to {{@code True}}.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If the given cube was {{@code null}}.
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
    {{
        // Checks
        if (c == null) {{
            throw new NullPointerException("Given a null cube, 'c'");
        }}
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (!a.matchesInShape(c)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleExtract(c, (Hypercube<Double>)a);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatExtract(c, (Hypercube<Float>)a);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intExtract(c, (Hypercube<Integer>)a);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longExtract(c, (Hypercube<Long>)a);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)booleanExtract(c, (Hypercube<Boolean>)a);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform extract operation on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (!a.matches(b)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}
        if (w != null && !a.matchesInShape(w)) {{
            throw new IllegalArgumentException("Given an incompatible 'w' cube");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleBinaryOp((Hypercube<Double>)a,
                                                (Hypercube<Double>)b,
                                                w,
                                                op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatBinaryOp((Hypercube<Float>)a,
                                               (Hypercube<Float>)b,
                                               w,
                                               op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intBinaryOp((Hypercube<Integer>)a,
                                             (Hypercube<Integer>)b,
                                             w,
                                             op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longBinaryOp((Hypercube<Long>)a,
                                              (Hypercube<Long>)b,
                                              w,
                                              op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)booleanBinaryOp((Hypercube<Boolean>)a,
                                                 (Hypercube<Boolean>)b,
                                                 w,
                                                 op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform binary " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}
        if (!a.matches(b) || !a.matches(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}
        if (w != null && !a.matchesInShape(w)) {{
            throw new IllegalArgumentException("Given an incompatible 'w' cube");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleBinaryOp((Hypercube<Double>)a,
                                                (Hypercube<Double>)b,
                                                (Hypercube<Double>)r,
                                                w,
                                                op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatBinaryOp((Hypercube<Float>)a,
                                               (Hypercube<Float>)b,
                                               (Hypercube<Float>)r,
                                               w,
                                               op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intBinaryOp((Hypercube<Integer>)a,
                                             (Hypercube<Integer>)b,
                                             (Hypercube<Integer>)r,
                                             w,
                                             op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longBinaryOp((Hypercube<Long>)a,
                                              (Hypercube<Long>)b,
                                              (Hypercube<Long>)r,
                                              w,
                                              op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)booleanBinaryOp((Hypercube<Boolean>)a,
                                                 (Hypercube<Boolean>)b,
                                                 (Hypercube<Boolean>)r,
                                                 w,
                                                 op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform binary " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleUnaryOp((Hypercube<Double>)a, op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatUnaryOp((Hypercube<Float>)a, op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intUnaryOp((Hypercube<Integer>)a, op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longUnaryOp((Hypercube<Long>)a, op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)booleanUnaryOp((Hypercube<Boolean>)a, op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform unary " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}
        if (!a.matches(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleUnaryOp((Hypercube<Double>)a,
                                               (Hypercube<Double>)r,
                                               op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatUnaryOp((Hypercube<Float>)a,
                                              (Hypercube<Float>)r,
                                              op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intUnaryOp((Hypercube<Integer>)a,
                                            (Hypercube<Integer>)r,
                                            op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longUnaryOp((Hypercube<Long>)a,
                                             (Hypercube<Long>)r,
                                             op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)booleanUnaryOp((Hypercube<Boolean>)a,
                                                (Hypercube<Boolean>)r,
                                                op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform unary " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // See if we can do it
        final Object o = i;
        if (a.getElementType().equals(Double.class)) {{
            return (T)doubleAssociativeOp((Hypercube<Double>)a, (Double)o, w, op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (T)floatAssociativeOp((Hypercube<Float>)a, (Float)o, w, op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (T)intAssociativeOp((Hypercube<Integer>)a, (Integer)o, w, op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (T)longAssociativeOp((Hypercube<Long>)a, (Long)o, w, op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform associative " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (c == null) {{
            throw new NullPointerException("Given a null cube");
        }}

        // See if we can do it
        final Object o = i;
        if (c.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)doubleAssociativeOpByAxes((Hypercube<Double>)c, a, (Double)o, w, op);
        }}
        else if (c.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)floatAssociativeOpByAxes((Hypercube<Float>)c, a, (Float)o, w, op);
        }}
        else if (c.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)intAssociativeOpByAxes((Hypercube<Integer>)c, a, (Integer)o, w, op);
        }}
        else if (c.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)longAssociativeOpByAxes((Hypercube<Long>)c, a, (Long)o, w, op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform associative " + op + " " +
                "on a cube with element type " +
                c.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (!a.matches(b)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return doubleComparisonOp((Hypercube<Double>)a,
                                      (Hypercube<Double>)b,
                                      op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return floatComparisonOp((Hypercube<Float>)a,
                                     (Hypercube<Float>)b,
                                     op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return intComparisonOp((Hypercube<Integer>)a,
                                   (Hypercube<Integer>)b,
                                   op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return longComparisonOp((Hypercube<Long>)a,
                                    (Hypercube<Long>)b,
                                    op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return booleanComparisonOp((Hypercube<Boolean>)a,
                                       (Hypercube<Boolean>)b,
                                       op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform comparison " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}
        if (!a.matches(b) || !a.matchesInShape(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return doubleComparisonOp((Hypercube<Double>)a,
                                      (Hypercube<Double>)b,
                                      r,
                                      op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return floatComparisonOp((Hypercube<Float>)a,
                                     (Hypercube<Float>)b,
                                     r,
                                     op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return intComparisonOp((Hypercube<Integer>)a,
                                       (Hypercube<Integer>)b,
                                       r,
                                       op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return longComparisonOp((Hypercube<Long>)a,
                                    (Hypercube<Long>)b,
                                    r,
                                    op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return booleanComparisonOp((Hypercube<Boolean>)a,
                                       (Hypercube<Boolean>)b,
                                       r,
                                       op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform comparison " + op + " on cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return doubleReductiveLogicOp ((Hypercube<Double>) a, w, op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return floatReductiveLogicOp  ((Hypercube<Float>)  a, w, op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return intReductiveLogicOp    ((Hypercube<Integer>)a, w, op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return longReductiveLogicOp   ((Hypercube<Long>)   a, w, op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return booleanReductiveLogicOp((Hypercube<Boolean>)a, w, op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform reductive logical " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return doubleReductiveLogicOpByAxes ((Hypercube<Double>) a, axes, where, op);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return floatReductiveLogicOpByAxes  ((Hypercube<Float>)  a, axes, where, op);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return intReductiveLogicOpByAxes    ((Hypercube<Integer>)a, axes, where, op);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return longReductiveLogicOpByAxes   ((Hypercube<Long>)   a, axes, where, op);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return booleanReductiveLogicOpByAxes((Hypercube<Boolean>)a, axes, where, op);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to perform reductive logical " + op + " on a cube with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}

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
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null value b");
        }}

        // See if we can do it. We handle casting a little specially here since
        // we could be called from the Python side, where the Ts will become
        // Objects owing to type erasure. The strictFoo() calls will be a NOP
        // for the Java code.
        if (a.getElementType().equals(Double.class)) {{
            return (Hypercube<T>)new DoubleSingleValueHypercube(a.getDimensions(),
                                                                strictDouble((Number)b));
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return (Hypercube<T>)new FloatSingleValueHypercube(a.getDimensions(),
                                                               strictFloat((Number)b));
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return (Hypercube<T>)new IntegerSingleValueHypercube(a.getDimensions(),
                                                                 strictInteger((Number)b));
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return (Hypercube<T>)new LongSingleValueHypercube(a.getDimensions(),
                                                              strictLong((Number)b));
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return (Hypercube<T>)new BooleanSingleValueHypercube(a.getDimensions(),
                                                                 (boolean)b);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to broadcast cubes with element type " +
                a.getElementType().getSimpleName()
            );
        }}
    }}
''',

    'NUMERIC': '''\

    // -------------------------------------------------------------------------

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static {object_type}Hypercube array(final {object_type}Hypercube cube)
    {{
        return cube.array();
    }}

    /**
     * Return a one dimensional cube as a copy of the given array.
     */
    public static {object_type}Hypercube array(final {primitive_type}[] array)
    {{
        final {object_type}Hypercube result =
            new {object_type}{default_impl}Hypercube(Dimension.of(array.length));
        result.fromFlattened(array);
        return result;
    }}

    /**
     * Return a one dimensional cube with a range per the given value.
     */
    public static {object_type}Hypercube arange(final {primitive_type} stop)
    {{
        return arange(0, stop);
    }}

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static {object_type}Hypercube arange(
        final {primitive_type} start,
        final {primitive_type} stop
    )
    {{
        return arange(start, stop, 1);
    }}

    /**
     * Return a one dimensional cube with a range per the given values.
     */
    public static {object_type}Hypercube arange(
        final {primitive_type} start,
        final {primitive_type} stop,
        final {primitive_type} step
    )
    {{
        if (step == 0) {{
            throw new IllegalArgumentException("Given a step of zero");
        }}
        if (step < 0 && start < stop) {{
            throw new IllegalArgumentException(
                "Step was negative but start was before stop"
            );
        }}
        if (step > 0 && start > stop) {{
            throw new IllegalArgumentException(
                "Step was positive but start was after stop"
            );
        }}
        final long length = (long)((stop - start) / step);
        final {object_type}Hypercube cube = new {object_type}{default_impl}Hypercube(Dimension.of(length));
        for (long i=0; i < length; i++) {{
            cube.setAt(i, ({primitive_type})(start + step * i));
        }}
        return cube;
    }}

    // -------------------------------------------------------------------------

    /**
     * Handle the given associative operation for a {primitive_type} cube, for the
     * given axes.
     */
    private static {object_type}Hypercube {primitive_type}AssociativeOpByAxes(
        final Hypercube<{object_type}> a,
        final int[] axes,
        final {object_type} initial,
        final BooleanHypercube where,
        final AssociativeOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {{
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {{
            if (axis >= a.getNDim()) {{
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }}
            if (axesSet.get(axis)) {{
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }}
            axesSet.set(axis);
        }}

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {{
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }}

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {{
            // Sliced out dims
            dstDims = new Dimension<?>[Math.max(1, dstNDim)];
            for (int i=0, j=0; i < a.getNDim(); i++) {{
                if (!axesSet.get(i)) {{
                    dstDims[j++] = srcDims[i];
                }}
            }}
        }}
        else {{
            // Singleton cube
            dstDims = Dimension.of(1);
        }}

        // Where we will put the result
        final {object_type}Hypercube dst = new {object_type}ArrayHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0, j=0; i < srcAccessors.length; i++) {{
            if (axesSet.get(i)) {{
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }}
        }}

        // Incrementally walk all the non-axes dimensions
        while (true) {{
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {{
                if (!axesSet.get(i)) {{
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }}
            }}

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final {object_type} result =
                associativeOp(a.slice(srcAccessors), initial, whereSlice, op);
            if (dstNDim > 0) {{
                // Subcube
                dst.setObj(result, dstAccessors);
            }}
            else {{
                // Singleton
                dst.setObjectAt(0, result);
            }}

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {{
                // Skip over indices which were slicing
                if (axesSet.get(i)) {{
                    continue;
                }}

                // Increment this index? If doing so would overflow the digit we
                // go back to zero for it and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {{
                    indices[i]++;
                    carry = false;
                }}
                else {{
                    indices[i] = 0;
                }}
            }}

            // If we overflowed then we're done
            if (carry) {{
                break;
            }}
        }}

        // Give back the result
        return dst;
    }}

    /**
     * Helper function for {{@code {primitive_type}AssociativeOp}} that performs an associative
     * operation on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is returned as a {primitive_type}.
     */
    @SuppressWarnings("inline")
    private static {primitive_type} {primitive_type}AssociativeOpHelper(
        final {object_type}Hypercube da,
        final {object_type}    i,
        final BooleanHypercube w,
        final AssociativeOp    op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {{
        {primitive_type} r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {{
            r = i;
        }}
        else {{
            switch (op) {{
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = {object_type}.MAX_VALUE; break;
            case MAX:    r = {object_type}.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }}
        }}

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final {primitive_type}[] aa = new {primitive_type}[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long ii = startIndex; ii < endIndex; ii += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - ii, STAGING_SIZE);

            // Copy out
            da.toFlattened(ii, aa, 0, len);
            if (w != null) {{
                w.toFlattened(ii, ww, 0, len);
            }}

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {{
{associative_ops}\
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }}
        }}

        return r;
    }}

    /**
     * Handle the given associative operation for a {primitive_type} cube.
     */
    private static {object_type} {primitive_type}AssociativeOp(
        final Hypercube<{object_type}> a,
        final {object_type} i,
        final BooleanHypercube w,
        final AssociativeOp op
    ) throws NullPointerException
    {{
        // Null checks first
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // If there is a where value then it should match in shape
        if (w != null && !a.matchesInShape(w)) {{
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }}

        {primitive_type} r;

        // Initialize the return value, possibly according to the operation
        if (i != null) {{
            r = i;
        }}
        else {{
            switch (op) {{
            case ADD:    r = 0; break;
            case NANADD: r = 0; break;
            case MIN:    r = {object_type}.MAX_VALUE; break;
            case MAX:    r = {object_type}.MIN_VALUE; break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported associative operation: " + op
                );
            }}
        }}

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final {object_type}Hypercube da = ({object_type}Hypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                r = {primitive_type}AssociativeOpHelper(da, i, w, op, 0, a.getSize());
            }}
            else {{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final {primitive_type}[] ar = new {primitive_type}[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            ar[idx] = {primitive_type}AssociativeOpHelper(da, null, w, op, startIndex, endIndex);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}

                // Combine the result from all threads into one
                switch (op) {{
                case ADD:    for (int j=0; j < NUM_THREADS{nan_check_for_r}; j++) r += ar[j];                     break;
                case MIN:    for (int j=0; j < NUM_THREADS{nan_check_for_r}; j++) r  = ((r < ar[j]) ? r : ar[j]); break;
                case MAX:    for (int j=0; j < NUM_THREADS{nan_check_for_r}; j++) r  = ((r > ar[j]) ? r : ar[j]); break;
                // The result of NaN functions on each thread can never be NaN.
                // So we can simply combine them with no NaN handling.
                case NANADD: for (int j=0; j < NUM_THREADS; j++) r += ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported associative operation: " + op
                    );
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long ii = 0, size = a.getSize(); ii < size{nan_check_for_r}; ii++) {{
                // Handle any 'where' clause
                if (w != null && !w.getAt(ii)) {{
                    continue;
                }}

                // Need to handle missing values
                final {object_type} va = a.getObjectAt(ii);
                if (va != null) {{
                    switch (op) {{
{associative_ops_naive}\
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported associative operation: " + op
                        );
                    }}
                }}
                else {{
                    // Mimic NaN semantics
                    switch (op) {{
                    case ADD: r = {object_type}.valueOf({primitive_from_null}); break;
                    case MIN: r = {object_type}.valueOf({primitive_from_null}); break;
                    case MAX: r = {object_type}.valueOf({primitive_from_null}); break;
                    default:
                        // Don't do anything for NaN functions
                    }}
                }}
            }}
        }}

        // Always return the result
        return {object_type}.valueOf(r);
    }}
''',

    'ALL': '''\

    // -------------------------------------------------------------------------

    /**
     * Return a new {primitive_type} cube of given dimensions where all elements are
     * equal to a given {primitive_type} value.
     *
     * <p>This method is an alias of {{@code broadcast()}} and is equivalent to
     * {{@code numpy.full()}} method.
     *
     * @return The resulting {primitive_type} cube.
     */
    public static Hypercube<{object_type}> full(
        final Dimension<?>[] dimensions,
        final {primitive_type} value
    )
    {{
        return broadcast(dimensions, value);
    }}

    /**
     * Return a new {primitive_type} cube of given dimensions where all elements are
     * equal to a given {primitive_type} value.
     *
     * @return The resulting {primitive_type} cube.
     */
    public static Hypercube<{object_type}> broadcast(
        final Dimension<?>[] dimensions,
        final {primitive_type} value
    )
    {{
        final {object_type}Hypercube a = new {object_type}{default_impl}Hypercube(dimensions);
        a.fill(value);
        return a;
    }}

    /**
     * Return a new 1-dimensional {primitive_type} cube of a given size where all
     * elements are equal to a given {primitive_type} value.
     *
     * <p>This is equivalent to calling {{@code broadcast(Dimension.of(size), value)}}.
     *
     * @return The resulting {primitive_type} cube.
     */
    public static Hypercube<{object_type}> broadcast(
        final long size,
        final {primitive_type} value
    )
    {{
        return broadcast(Dimension.of(size), value);
    }}

    // -------------------------------------------------------------------------

    /**
     * Helper function for {{@code {primitive_type}BinaryOp}} that performs a binary operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void {primitive_type}BinaryOpHelper(
        final {object_type}Hypercube da,
        final {object_type}Hypercube db,
        final {object_type}Hypercube dr,
        final BooleanHypercube dw,
        final BinaryOp op,
        final long     startIndex,
        final long     endIndex
    ) throws UnsupportedOperationException
    {{
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final {primitive_type}[] aa = new {primitive_type}[STAGING_SIZE];
        final {primitive_type}[] ab = new {primitive_type}[STAGING_SIZE];
        final {primitive_type}[] ar = new {primitive_type}[STAGING_SIZE];
        final boolean[] aw = (dw == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);
            if (dw != null) {{
                dw.toFlattened(i, aw, 0, len);
            }}

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {{
{binary_numeric_ops}\
{binary_logic_ops}\
            default:
                throw new UnsupportedOperationException(
                    "Unsupported binary operation: " + op
                );
            }}

            // Copy in
            if (aw == null) {{
                dr.fromFlattened(ar, 0, i, len);
            }}
            else {{
                for (int j=0; j < len; j++) {{
                    if (aw[j]) {{
                        dr.setAt(i + j, ar[j]);
                    }}
                }}
            }}
        }}
    }}

    /**
     * Handle the given binary operation for two {object_type} cubes.
     */
    private static Hypercube<{object_type}> {primitive_type}BinaryOp(
        final Hypercube<{object_type}> a,
        final Hypercube<{object_type}> b,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (!a.matches(b)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Create the destination, a simple {default_impl} one by default
        return binaryOp(a, b, new {object_type}{default_impl}Hypercube(a.getDimensions()), dw, op);
    }}

    /**
     * Handle the given binary operation for two {object_type} cubes together, putting the
     * result into a third.
     */
    private static Hypercube<{object_type}> {primitive_type}BinaryOp(
        final Hypercube<{object_type}> a,
        final Hypercube<{object_type}> b,
        final Hypercube<{object_type}> r,
        final BooleanHypercube dw,
        final BinaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Null checks first
        if (a == null) {{
            throw new NullPointerException("Given a null cube 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}

        // Compatibility checks
        if (!a.matches(b) || !a.matches(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final {object_type}Hypercube da = ({object_type}Hypercube)a;
            final {object_type}Hypercube db = ({object_type}Hypercube)b;
            final {object_type}Hypercube dr = ({object_type}Hypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                {primitive_type}BinaryOpHelper(da, db, dr, dw, op, 0, a.getSize());
            }}
            else {{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            {primitive_type}BinaryOpHelper(da, db, dr, dw, op, startIndex, endIndex);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {{
                // Need to handle missing values
                final {object_type} va = a.getObjectAt(i);
                final {object_type} vb = b.getObjectAt(i);
                if (va != null && vb != null) {{
                    switch (op) {{
{binary_numeric_ops_naive}\
{binary_logic_ops_naive}\
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported binary operation: " + op
                        );
                    }}
                }}
                else {{
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }}
            }}
        }}

        // Always give back the resultant one
        return r;
    }}

    // -------------------------------------------------------------------------

    /**
     * Helper function for {{@code {primitive_type}UnaryOp}} that performs a unary operation
     * on a sub-array of the cube. The sub-array is specified by parameters
     * startIndex and endIndex, and the result is put into a second cube.
     */
    @SuppressWarnings("inline")
    private static void {primitive_type}UnaryOpHelper(
        final {object_type}Hypercube da,
        final {object_type}Hypercube dr,
        final UnaryOp op,
        final long    startIndex,
        final long    endIndex
    ) throws UnsupportedOperationException
    {{
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final {primitive_type}[] aa = new {primitive_type}[STAGING_SIZE];
        final {primitive_type}[] ar = new {primitive_type}[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            // For operations with no special NaN handling, missing values will
            // automatically propagate into NaNs so we can ignore them.
            switch (op) {{
{unary_numeric_ops}\
{unary_real_ops}\
{unary_logic_ops}\
            default:
                throw new UnsupportedOperationException(
                    "Unsupported unary operation: " + op
                );
            }}

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }}
    }}

    /**
     * Handle the given unary operation for a {object_type} cube.
     */
    private static Hypercube<{object_type}> {primitive_type}UnaryOp(
        final Hypercube<{object_type}> a,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // Create the destination, a simple {default_impl} one by default
        return unaryOp(a, new {object_type}{default_impl}Hypercube(a.getDimensions()), op);
    }}

    /**
     * Handle the given unary operation for a {object_type} cube, putting the result into a
     * second.
     */
    private static Hypercube<{object_type}> {primitive_type}UnaryOp(
        final Hypercube<{object_type}> a,
        final Hypercube<{object_type}> r,
        final UnaryOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Null checks first
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}

        // Compatibility checks
        if (!a.matches(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final {object_type}Hypercube da = ({object_type}Hypercube)a;
            final {object_type}Hypercube dr = ({object_type}Hypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                {primitive_type}UnaryOpHelper(da, dr, op, 0, a.getSize());
            }}
            else {{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            {primitive_type}UnaryOpHelper(da, dr, op, startIndex, endIndex);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {{
                // Need to handle missing values
                final {object_type} va = a.getObjectAt(i);
                if (va != null) {{
                    switch (op) {{
{unary_numeric_ops_naive}\
{unary_real_ops_naive}\
{unary_logic_ops_naive}\
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported unary operation: " + op
                        );
                    }}
                }}
                else {{
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }}
            }}
        }}

        // Always give back the resultant one
        return r;
    }}

    // -------------------------------------------------------------------------

    /**
     * Helper function for {{@code {primitive_type}ComparisonOp}} that performs a comparison
     * operation on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube.
     */
    @SuppressWarnings("inline")
    private static void {primitive_type}ComparisonOpHelper(
        final {object_type}Hypercube da,
        final {object_type}Hypercube db,
        final BooleanHypercube dr,
        final ComparisonOp     op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {{
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final {primitive_type}[]  aa = new {primitive_type} [STAGING_SIZE];
        final {primitive_type}[]  ab = new {primitive_type} [STAGING_SIZE];
        final boolean[] ar = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            db.toFlattened(i, ab, 0, len);

            switch (op) {{
{comparison_ops}\
            default:
                throw new UnsupportedOperationException(
                    "Unsupported comparison operation: " + op
                );
            }}

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }}
    }}

    /**
     * Handle the given boolean comparison operation for two {object_type} cubes.
     */
    private static Hypercube<Boolean> {primitive_type}ComparisonOp(
        final Hypercube<{object_type}> a,
        final Hypercube<{object_type}> b,
        final ComparisonOp op
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (!a.matches(b)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Create the destination, a simple bitset one by default
        return comparisonOp(a, b, new BooleanBitSetHypercube(a.getDimensions()), op);
    }}

    /**
     * Handle the given binary operation for two {object_type} cubes together, putting the
     * result into a third.
     */
    private static Hypercube<Boolean> {primitive_type}ComparisonOp(
        final Hypercube<{object_type}> a,
        final Hypercube<{object_type}> b,
        final Hypercube<Boolean> r,
        final ComparisonOp       op
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Null checks first
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (b == null) {{
            throw new NullPointerException("Given a null cube, 'b'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}

        // Compatibility checks
        if (!a.matches(b) || !a.matchesInShape(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final {object_type}Hypercube da = ({object_type}Hypercube)a;
            final {object_type}Hypercube db = ({object_type}Hypercube)b;
            final BooleanHypercube dr = (BooleanHypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                {primitive_type}ComparisonOpHelper(da, db, dr, op, 0, a.getSize());
            }}
            else {{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            {primitive_type}ComparisonOpHelper(da, db, dr, op, startIndex, endIndex);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {{
                // Need to handle missing values
                final {object_type} va = a.getObjectAt(i);
                final {object_type} vb = b.getObjectAt(i);
                if (va != null && vb != null) {{
                    switch (op) {{
{comparison_ops_naive}\
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported comparison operation: " + op
                        );
                    }}
                }}
                else {{
                    // Mimic NaN semantics
                    // All comparison operattions with a NaN operand evaluate
                    // to false, except for the inequality operation (!=).
                    r.setObjectAt(i, null);
                    switch (op) {{
                    case NE: r.setObjectAt(i, true);  break;
                    default: r.setObjectAt(i, false); break;
                    }}
                }}
            }}
        }}

        // Always give back the resultant one
        return r;
    }}

    // -------------------------------------------------------------------------

    /**
     * Handle the given reductive operation for a {primitive_type} cube, for the
     * given axes.
     */
    private static BooleanHypercube {primitive_type}ReductiveLogicOpByAxes(
        final Hypercube<{object_type}> a,
        final int[] axes,
        final BooleanHypercube where,
        final ReductiveLogicOp op
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {{
        // Figure out what axes we are working over, and if they are good
        final BitSet axesSet = new BitSet(a.getNDim());
        for (int axis : axes) {{
            if (axis >= a.getNDim()) {{
                throw new IllegalArgumentException(
                    "Axis " + axis + " not in given cube"
                );
            }}
            if (axesSet.get(axis)) {{
                throw new IllegalArgumentException(
                    "Duplicate axis " + axis + " in " + Arrays.toString(axes)
                );
            }}
            axesSet.set(axis);
        }}

        // If there is a where value then it should match in shape
        if (where != null && !a.matchesInShape(where)) {{
            // TODO implement broadcasting
            throw new IllegalArgumentException(
                "Source cube and where cube don't match in shape"
            );
        }}

        // Create the destination axes by copying out the remaining (non-sliced)
        // axes from the source cube
        final Dimension<?>[] srcDims = a.getDimensions();
        final int            dstNDim = a.getNDim() - axesSet.cardinality();
        final Dimension<?>[] dstDims;
        if (dstNDim > 0) {{
            // Sliced out dims
            dstDims = new Dimension<?>[dstNDim];
            for (int i=0, j=0; i < a.getNDim(); i++) {{
                if (!axesSet.get(i)) {{
                    dstDims[j++] = srcDims[i];
                }}
            }}
        }}
        else {{
            // Singleton cube
            dstDims = Dimension.of(1);
        }}

        // Where we will put the result
        final BooleanHypercube dst = new BooleanBitSetHypercube(dstDims);

        // Walk all the non-axes values, using the axes values as slices
        final long[] indices = new long[a.getNDim()];
        final Dimension.Accessor<?>  [] srcAccessors = new Dimension.Accessor  <?>[a.getNDim()];
        final Dimension.Coordinate<?>[] dstAccessors = new Dimension.Coordinate<?>[dstNDim];
        for (int i=0, j=0; i < srcAccessors.length; i++) {{
            if (axesSet.get(i)) {{
                srcAccessors[i] = a.dim(i).slice(0, a.length(i));
            }}
        }}

        // Incrementally walk all the non-axes dimensions
        while (true) {{
            // Create the accessors
            for (int i=0, j=0; i < a.getNDim(); i++) {{
                if (!axesSet.get(i)) {{
                    srcAccessors[i] = dstAccessors[j++] = a.dim(i).at(indices[i]);
                }}
            }}

            // Compute and assign
            final BooleanHypercube whereSlice =
                (where == null) ? null : where.slice(srcAccessors);
            final boolean result = {primitive_type}ReductiveLogicOp(a.slice(srcAccessors), whereSlice, op);
            if (dstNDim > 0) {{
                // Subcube
                dst.setObj(result, dstAccessors);
            }}
            else {{
                // Singleton
                dst.setObjectAt(0, result);
            }}

            // Move on one, we use a ripple adder to increment the indices,
            // stopping when the carry bit overflows. (The indices are
            // effectively a variable-base number which we're stepping, until it
            // loops back through zero.)
            boolean carry = true;
            for (int i=0; i < indices.length && carry; i++) {{
                // Skip over indices which were slicing
                if (axesSet.get(i)) {{
                    continue;
                }}

                // Increment this index. If doing so would cause overflow then
                // we reset it to zero and move onto the next one, taking the
                // carry bit with us.
                if (indices[i] < a.length(i)-1) {{
                    indices[i]++;
                    carry = false;
                }}
                else {{
                    indices[i] = 0;
                }}
            }}

            // If we overflowed then we're done
            if (carry) {{
                break;
            }}
        }}

        // Give back the result
        return dst;
    }}

    /**
     * Helper function for {{@code {primitive_type}ReductiveLogicOp}} that performs a reductive
     * logical operation on a sub-array of the cube. The sub-array is specified
     * by parameters startIndex and endIndex, and the result is returned as a
     * boolean.
     */
    @SuppressWarnings("inline")
    private static boolean {primitive_type}ReductiveLogicOpHelper(
        final {object_type}Hypercube da,
        final BooleanHypercube w,
        final ReductiveLogicOp op,
        final long             startIndex,
        final long             endIndex
    ) throws UnsupportedOperationException
    {{
        boolean r;

        // Initialize the return value according to the operation
        switch (op) {{
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }}

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final {primitive_type}[] aa = new {primitive_type}[STAGING_SIZE];
        final boolean[]          ww = (w == null) ? null : new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);
            if (w != null) {{
                w.toFlattened(i, ww, 0, len);
            }}

            switch (op) {{
{reductive_logic_ops}\
            default:
                throw new UnsupportedOperationException(
                    "Unsupported reductive logic operation: " + op
                );
            }}
        }}

        return r;
    }}

    /**
     * Handle the given reductive logic operation for a {object_type} cube.
     */
    private static boolean {primitive_type}ReductiveLogicOp(
        final Hypercube<{object_type}> a,
        final BooleanHypercube w,
        final ReductiveLogicOp op
    ) throws NullPointerException
    {{
        // Null checks first
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        boolean r;

        // Initialize the return value according to the operation
        switch (op) {{
        case ANY: r = false; break;
        case ALL: r = true;  break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported reductive logic operation: " + op
            );
        }}

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final {object_type}Hypercube da = ({object_type}Hypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                r = {primitive_type}ReductiveLogicOpHelper(da, w, op, 0, a.getSize());
            }}
            else {{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;
                final boolean[] ar = new boolean[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            ar[idx] = {primitive_type}ReductiveLogicOpHelper(da, w, op, startIndex, endIndex);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}

                // Combine the result from all threads into one
                switch (op) {{
                case ANY: for (int j=0; j < NUM_THREADS; j++) r |= ar[j]; break;
                case ALL: for (int j=0; j < NUM_THREADS; j++) r &= ar[j]; break;
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported reductive logic operation: " + op
                    );
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {{
                // Need to handle missing values
                final {object_type} va = a.getObjectAt(i);
                if (va != null) {{
                    switch (op) {{
{reductive_logic_ops_naive}\
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported reductive logic operation: " + op
                        );
                    }}
                }}
                else {{
                    // Mimic NaN semantics
                    // Note that NaN always evaluates to true.
                    switch (op) {{
                    case ANY: r = true; break;
                    default:
                        // Nothing to do.
                    }}
                }}
            }}
        }}

        // Always return the result
        return r;
    }}

    // -------------------------------------------------------------------------

    /**
     * Helper function for {{@code {primitive_type}Extract}} that performs an extract operation
     * on a sub-array of the cubes. The sub-arrays are specified by parameters
     * startIndex and endIndex, and the result is put into a third cube, starting
     * from the given offset index value.
     */
    @SuppressWarnings("inline")
    private static void {primitive_type}ExtractHelper(
        final BooleanHypercube dc,
        final {object_type}Hypercube da,
        final {object_type}Hypercube dr,
        final long startIndex,
        final long endIndex,
        final long offset
    )
    {{
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean         [] ac = new boolean         [STAGING_SIZE];
        final {primitive_type}[] aa = new {primitive_type}[STAGING_SIZE];
        final {primitive_type}[] ar = new {primitive_type}[STAGING_SIZE];

        // Define an offset to track our progress on r
        long offsetR = offset;

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            dc.toFlattened(i, ac, 0, len);
            da.toFlattened(i, aa, 0, len);

{extract}\

            // Copy in
            dr.fromFlattened(ar, 0, offsetR, k);
            offsetR += k;
        }}
    }}

    /**
     * Handle the extract operation for a {object_type} cube.
     */
    private static Hypercube<{object_type}> {primitive_type}Extract(
        final Hypercube<Boolean> c,
        final Hypercube<{object_type}> a
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Checks
        if (c == null) {{
            throw new NullPointerException("Given a null cube, 'c'");
        }}
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (!a.matchesInShape(c)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Create the destination, a simple {default_impl} one by default
        return {primitive_type}Extract(c, a, new {object_type}{default_impl}Hypercube(Dimension.of(popcount(c))));
    }}

    /**
     * Handle the extract operation for a {object_type} cube, putting the
     * result into a third.
     */
    private static Hypercube<{object_type}> {primitive_type}Extract(
        final Hypercube<Boolean> c,
        final Hypercube<{object_type}> a,
        final Hypercube<{object_type}> r
    ) throws IllegalArgumentException,
             NullPointerException
    {{
        // Null checks first
        if (c == null) {{
            throw new NullPointerException("Given a null cube, 'c'");
        }}
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}

        // Compatibility checks
        if (!a.matchesInShape(c)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube dc = (BooleanHypercube)c;
            final {object_type}Hypercube da = ({object_type}Hypercube)a;
            final {object_type}Hypercube dr = ({object_type}Hypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                {primitive_type}ExtractHelper(dc, da, dr, 0, a.getSize(), 0);
            }}
            else {{
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

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());
                    final long offsetR = offset;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            {primitive_type}ExtractHelper(dc, da, dr, startIndex, endIndex, offsetR);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});

                    // Update the offset value accordingly
                    offset += popcount(c, startIndex, endIndex);
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long i = 0, j=0, size = a.getSize(); i < size; i++) {{
                // Need to handle missing values
                final Boolean       vc = c.getObjectAt(i);
                final {object_type} va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(vc)) {{
                    r.setObjectAt(j++, va);
                }}
            }}
        }}

        // Always give back the resultant one
        return r;
    }}
''',

    'BOOLEAN': '''\

    // -------------------------------------------------------------------------

    /**
     * Helper function for {{@code popcount}} that counts the number of {{@code true}}
     * values in a sub-array of a boolean cube. The sub-arrays are specified by
     * parameters startIndex and endIndex, and the result is returned as a long.
     */
    @SuppressWarnings("inline")
    private static long popcountHelper(final BooleanHypercube da,
                                       final long             startIndex,
                                       final long             endIndex)
    {{
        // Initialize the return value
        long r = 0;

        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final boolean[] aa = new boolean[STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

{popcount}\
        }}

        return r;
    }}

    /**
     * Handle the given reductive logic operation for a sub-array of a boolean
     * cube. The sub-arrays are specified by parameters startIndex and endIndex,
     * and the result is returned as a long.
     */
    private static long popcount(final Hypercube<Boolean> a,
                                 final long               start,
                                 final long               end)
        throws NullPointerException
    {{
        // Null checks first
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // Initialize the return value
        long r = 0;

        // Try to do this natively but fall back to the non-native version
        try {{
            // Natively first. We will use bulk operations to unroll the loop
            final BooleanHypercube da = (BooleanHypercube)a;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{
                r = popcountHelper(da, start, end);
            }}
            else {{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = ((end - start) / NUM_THREADS / 32 + 1) * 32;
                final long[] ar   = new long[NUM_THREADS];

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{
                    final long startIndex = start + bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? end
                                             : Math.min(start + bucket * (j+1), end);
                    final int idx = j;

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{
                        continue;
                    }}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{
                        try {{
                            ar[idx] = popcountHelper(da, startIndex, endIndex);
                        }}
                        catch (Exception e) {{
                            exception.set(e);
                        }}
                        finally {{
                            latch.countDown();
                        }}
                    }});
                }}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{
                    throw exception.get();
                }}

                // Combine the result from all threads into one
                for (int j=0; j < NUM_THREADS; j++) {{
                    r += ar[j];
                }}
            }}
        }}
        catch (Exception e) {{
            // Just do a linear waltz
            for (long i = start; i < end; i++) {{
                // No need to handle missing values separately
                final Boolean va = a.getObjectAt(i);
                if (Boolean.TRUE.equals(va)) {{
                    r ++;
                }}
            }}
        }}

        // Always return the result
        return r;
    }}
''',

    'CLASS': '''\
    /**
     * An immutable single-valued {primitive_type} cube representation, that is
     * internally useful for cube-with-value operations.
     */
    private static class {object_type}SingleValueHypercube
        extends Abstract{object_type}Hypercube
    {{
        // This cube's single value.
        final {primitive_type} singleValue;
        /**
         * Constructor.
         *
         * @param dimensions  The dimension of this cube.
         * @param v           The single value of this cube.
         */
        public {object_type}SingleValueHypercube(final Dimension<?>[] dimensions, final {primitive_type} v)
        {{
            super(dimensions);
            singleValue = v;
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public void toFlattened(final long srcPos,
                                final {primitive_type}[] dst,
                                final int  dstPos,
                                final int  length)
        {{
            Arrays.fill(dst, dstPos, dstPos + length, singleValue);
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public {object_type} getObjectAt(final long index)
        {{
            return singleValue;
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public void setObjectAt(final long index, final {object_type} v)
            throws UnsupportedOperationException
        {{
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public {primitive_type} getAt(final long index)
        {{
            return singleValue;
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public void setAt(final long index, final {primitive_type} v)
            throws UnsupportedOperationException
        {{
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public {primitive_type} get(final long... indices)
        {{
            return singleValue;
        }}

        /**
         * {{@inheritDoc}}
         */
        @Override
        public void set(final {primitive_type} v, final long... indices)
            throws UnsupportedOperationException
        {{
            throw new UnsupportedOperationException(
                "Mutator methods should never be used"
            );
        }}
    }}

    // -------------------------------------------------------------------------

''',

    'CAST': '''\

    // -------------------------------------------------------------------------

    /**
     * Cast a given cube to a {object_type} cube, returning the new cube.
     *
     * @return A cube with the resultant values in it.
     *
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    public static <T> Hypercube<{object_type}> to{object_type}Hypercube(final Hypercube<T> a)
        throws NullPointerException,
               UnsupportedOperationException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}

        // Create the destination, a simple {default_impl} one by default
        try {{
            return to{object_type}Hypercube(
                a,
                new {object_type}{default_impl}Hypercube(a.getDimensions())
            );
        }}
        catch (ClassCastException e) {{
            return to{object_type}Hypercube(a, new {object_type}{default_impl}Hypercube(a.getDimensions()));
        }}
    }}

    /**
     * Cast a given cube to a {object_type} cube, putting the result into a second.
     *
     * @return The given {{@code r}} cube, which will have the resultant values in it.
     *
     * @throws IllegalArgumentException      If the cubes are not compatible.
     * @throws NullPointerException          If any given cube was {{@code null}}.
     * @throws UnsupportedOperationException If the operation was not supported
     *                                       for the given cubes.
     */
    @SuppressWarnings("unchecked")
    public static <T> Hypercube<{object_type}> to{object_type}Hypercube(
        final Hypercube<T> a,
        final Hypercube<{object_type}> r
    ) throws IllegalArgumentException,
             NullPointerException,
             UnsupportedOperationException
    {{
        // Checks
        if (a == null) {{
            throw new NullPointerException("Given a null cube, 'a'");
        }}
        if (r == null) {{
            throw new NullPointerException("Given a null cube, 'r'");
        }}
        if (!a.matchesInShape(r)) {{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}

        // See if we can do it
        if (a.getElementType().equals(Double.class)) {{
            return doubleTo{object_type}Hypercube((Hypercube<Double>)a, r);
        }}
        else if (a.getElementType().equals(Float.class)) {{
            return floatTo{object_type}Hypercube((Hypercube<Float>)a, r);
        }}
        else if (a.getElementType().equals(Integer.class)) {{
            return intTo{object_type}Hypercube((Hypercube<Integer>)a, r);
        }}
        else if (a.getElementType().equals(Long.class)) {{
            return longTo{object_type}Hypercube((Hypercube<Long>)a, r);
        }}
        else if (a.getElementType().equals(Boolean.class)) {{
            return booleanTo{object_type}Hypercube((Hypercube<Boolean>)a, r);
        }}
        else {{
            throw new UnsupportedOperationException(
                "Don't know how to cast cube with element type " +
                a.getElementType().getSimpleName() +
                "to a {object_type} cube"
            );
        }}
    }}
''',

    # The following helper cog code uses quadruple braces, as it is double-cogged,
    # meaning that it will first be formatted with the arguments of the type we
    # are "casting to" (first-layer arguments), and then formatted again with
    # the arguments of the type we are "casting from" (second-layer arguments).
    # This is done to simplify and reduce code, using smarter cogging.
    # As a result of this, first-layer arguments are in a single bracket, second-
    # layer arguments in double brackets, and finally actual brackets in the code
    # are replaced with quadruple brackets.
    'CAST_HELPER': '''\

    // -------------------------------------------------------------------------

    /**
     * Helper function for {{{{@code {{primitive_type}}To{object_type}Hypercube}}}} that casts
     * a sub-array of the given {{object_type}} cube to a {object_type} cube. The
     * sub-arrays are specified by parameters startIndex and endIndex, and the
     * result is put into the second cube.
     */
    @SuppressWarnings("inline")
    private static void {{primitive_type}}To{object_type}HypercubeHelper(
        final {{object_type}}Hypercube da,
        final {object_type}Hypercube dr,
        final long startIndex,
        final long endIndex
    )
    {{{{
        // Stage in here. These arrays don't need to be huge in order to get
        // the benefit of the loop unrolling.
        final {{primitive_type}}[] aa = new {{primitive_type}}[STAGING_SIZE];
        final {primitive_type}  [] ar = new {primitive_type}  [STAGING_SIZE];

        // Now walk and do the op on the chunk
        for (long i = startIndex; i < endIndex; i += STAGING_SIZE) {{{{
            // How much to copy for this round. This will be 'STAGING_SIZE' until
            // we hit the end.
            final int len = (int)Math.min(endIndex - i, STAGING_SIZE);

            // Copy out
            da.toFlattened(i, aa, 0, len);

            for (int j=0; j < len; j++) {{{{
                ar[j] = ({primitive_type})(aa[j]{{conversion}});
            }}}}

            // Copy in
            dr.fromFlattened(ar, 0, i, len);
        }}}}
    }}}}

    /**
     * Cast the given {{object_type}} cube to a {object_type} cube, putting the
     * result into the given {{{{@code r}}}} cube.
     */
    private static Hypercube<{object_type}> {{primitive_type}}To{object_type}Hypercube(
        final Hypercube<{{object_type}}> a,
        final Hypercube<{object_type}> r
    ) throws IllegalArgumentException,
             NullPointerException
    {{{{
        // Null checks first
        if (a == null) {{{{
            throw new NullPointerException("Given a null cube, 'a'");
        }}}}
        if (r == null) {{{{
            throw new NullPointerException("Given a null cube, 'r'");
        }}}}

        // Compatibility checks
        if (!a.matchesInShape(r)) {{{{
            throw new IllegalArgumentException("Given incompatible cubes");
        }}}}

        // Try to do this natively but fall back to the non-native version
        try {{{{
            // Natively first. We will use bulk operations to unroll the loop
            final {{object_type}}Hypercube da = ({{object_type}}Hypercube)a;
            final {object_type}Hypercube dr = ({object_type}Hypercube)r;

            // Only use multithreading if it is enabled and if the
            // cubes are large enough to justify the overhead.
            if (ourExecutorService == null || a.getSize() < THREADING_THRESHOLD) {{{{
                {{primitive_type}}To{object_type}HypercubeHelper(da, dr, 0, a.getSize());
            }}}}
            else {{{{
                // Initialize a countdown to wait for all threads to finish processing.
                final CountDownLatch latch = new CountDownLatch(NUM_THREADS);

                // Bucket size for each thread.
                // Make sure to be a multiple of 32 for cache efficiency.
                final long bucket = (a.getSize() / NUM_THREADS / 32 + 1) * 32;

                // Atomic exception reference for handling a thrown exception in helper threads
                final AtomicReference<Exception> exception = new AtomicReference<>();

                for (int j=0; j < NUM_THREADS; j++) {{{{
                    final long startIndex = bucket * j;
                    final long endIndex =
                        (j == NUM_THREADS-1) ? a.getSize()
                                             : Math.min(bucket * (j+1), a.getSize());

                    // Redundancy to avoid submitting empty tasks
                    if (startIndex == endIndex) {{{{
                        continue;
                    }}}}

                    // Submit this subtask to the thread pool
                    ourExecutorService.submit(() -> {{{{
                        try {{{{
                            {{primitive_type}}To{object_type}HypercubeHelper(da, dr, startIndex, endIndex);
                        }}}}
                        catch (Exception e) {{{{
                            exception.set(e);
                        }}}}
                        finally {{{{
                            latch.countDown();
                        }}}}
                    }}}});
                }}}}

                // Wait here for all threads to conclude
                latch.await();

                // Propagate a runtime exception, if any.
                if (exception.get() != null) {{{{
                    throw exception.get();
                }}}}
            }}}}
        }}}}
        catch (Exception e) {{{{
            // Just do a linear waltz
            for (long i = 0, size = a.getSize(); i < size; i++) {{{{
                // Need to handle missing values
                final {{object_type}} va = a.getObjectAt(i);
                if (va != null) {{{{
                    r.setObjectAt(i, ({primitive_type})(va.{{primitive_type}}Value(){{conversion}}));
                }}}}
                else {{{{
                    // Mimic NaN semantics
                    r.setObjectAt(i, null);
                }}}}
            }}}}
        }}}}

        // Always give back the resultant one
        return r;
    }}}}
''',

    ################################################################################
    # Make definitions for handling different operations below. This is particularly
    # useful if we decide to change the logic for handling an operation, without
    # needing to modify the main code (for example to create a vectorized version
    # of CubeMath).
    'BINARY_NUMERIC_OPS': '''\
            case ADD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] + ab[j];          break;
            case SUB: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] - ab[j];          break;
            case MUL: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] * ab[j];          break;
            case DIV: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] / ab[j];          break;
            case MOD: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] % ab[j];          break;
            case MIN: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.min(aa[j], ab[j]); break;
            case MAX: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = Math.max(aa[j], ab[j]); break;
            case POW: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = {cast}Math.pow(aa[j], ab[j]); break;
''',

    'BINARY_NUMERIC_OPS_NAIVE': '''\
                    case ADD: r.setObjectAt(i, va + vb);          break;
                    case SUB: r.setObjectAt(i, va - vb);          break;
                    case MUL: r.setObjectAt(i, va * vb);          break;
                    case DIV: r.setObjectAt(i, va / vb);          break;
                    case MOD: r.setObjectAt(i, va % vb);          break;
                    case MIN: r.setObjectAt(i, Math.min(va, vb)); break;
                    case MAX: r.setObjectAt(i, Math.max(va, vb)); break;
                    case POW: r.setObjectAt(i, {cast}Math.pow(va, vb)); break;
''',

    # For better readability, we are using additional spaces below to align the
    # code with other unary operations in the final cogged code.
    'UNARY_NUMERIC_OPS': '''\
            case NEG:   for (int j=0; j < len; j++) ar[j] =         -aa[j];  break;
            case ABS:   for (int j=0; j < len; j++) ar[j] = Math.abs(aa[j]); break;
''',

    # Once again, we are using additional spaces below to align the code with
    # other unary operations in the final cogged code, for better readability.
    'UNARY_NUMERIC_OPS_NAIVE': '''\
                    case NEG:   r.setObjectAt(i, -va); break;
                    case ABS:   r.setObjectAt(i, Math.abs(va)); break;
''',

    'BINARY_LOGIC_OPS': '''\
            case AND: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] & ab[j]; break;
            case OR:  for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] | ab[j]; break;
            case XOR: for (int j=0; j < len; j++) if (aw == null || aw[j]) ar[j] = aa[j] ^ ab[j]; break;
''',

    'BINARY_LOGIC_OPS_NAIVE': '''\
                    case AND: r.setObjectAt(i, va & vb); break;
                    case OR:  r.setObjectAt(i, va | vb); break;
                    case XOR: r.setObjectAt(i, va ^ vb); break;
''',

    # Once again, we are using additional spaces below to align the code with
    # other unary operations in the final cogged code, for better readability.
    'UNARY_LOGIC_OPS': '''\
            case NOT:   for (int j=0; j < len; j++) ar[j] =  {not_operation}aa[j]; break;
''',

    # Once again, we are using additional spaces below to align the code with
    # other unary operations in the final cogged code, for better readability.
    'UNARY_LOGIC_OPS_NAIVE': '''\
                    case NOT:   r.setObjectAt(i,  {not_operation}va); break;
''',

    'UNARY_REAL_OPS': '''\
            case FLOOR: for (int j=0; j < len; j++) ar[j] = {cast}Math.floor(aa[j]); break;
            case ROUND: for (int j=0; j < len; j++) ar[j] = {cast}Math.round(aa[j]); break;
            case CEIL:  for (int j=0; j < len; j++) ar[j] = {cast}Math.ceil (aa[j]); break;
            case COS:   for (int j=0; j < len; j++) ar[j] = {cast}Math.cos  (aa[j]); break;
            case COSH:  for (int j=0; j < len; j++) ar[j] = {cast}Math.cosh (aa[j]); break;
            case SIN:   for (int j=0; j < len; j++) ar[j] = {cast}Math.sin  (aa[j]); break;
            case SINH:  for (int j=0; j < len; j++) ar[j] = {cast}Math.sinh (aa[j]); break;
            case TAN:   for (int j=0; j < len; j++) ar[j] = {cast}Math.tan  (aa[j]); break;
            case TANH:  for (int j=0; j < len; j++) ar[j] = {cast}Math.tanh (aa[j]); break;
            case EXP:   for (int j=0; j < len; j++) ar[j] = {cast}Math.exp  (aa[j]); break;
            case LOG:   for (int j=0; j < len; j++) ar[j] = {cast}Math.log  (aa[j]); break;
            case LOG10: for (int j=0; j < len; j++) ar[j] = {cast}Math.log10(aa[j]); break;
''',

    'UNARY_REAL_OPS_NAIVE': '''\
                    case FLOOR: r.setObjectAt(i, {cast}Math.floor(va)); break;
                    case ROUND: r.setObjectAt(i, {cast}Math.round(va)); break;
                    case CEIL:  r.setObjectAt(i, {cast}Math.ceil (va)); break;
                    case COS:   r.setObjectAt(i, {cast}Math.cos  (va)); break;
                    case COSH:  r.setObjectAt(i, {cast}Math.cosh (va)); break;
                    case SIN:   r.setObjectAt(i, {cast}Math.sin  (va)); break;
                    case SINH:  r.setObjectAt(i, {cast}Math.sinh (va)); break;
                    case TAN:   r.setObjectAt(i, {cast}Math.tan  (va)); break;
                    case TANH:  r.setObjectAt(i, {cast}Math.tanh (va)); break;
                    case EXP:   r.setObjectAt(i, {cast}Math.exp  (va)); break;
                    case LOG:   r.setObjectAt(i, {cast}Math.log  (va)); break;
                    case LOG10: r.setObjectAt(i, {cast}Math.log10(va)); break;
''',

    'ASSOCIATIVE_OPS': '''\
            case ADD:    for (int j=0; j < len{nan_check_for_r}; j++) if (ww == null || ww[j]) r += aa[j]; break;
            case MIN:    for (int j=0; j < len{nan_check_for_r}; j++) if (ww == null || ww[j]) r  = (r < aa[j] ? r : aa[j]); break;
            case MAX:    for (int j=0; j < len{nan_check_for_r}; j++) if (ww == null || ww[j]) r  = (r > aa[j] ? r : aa[j]); break;
            case NANADD: for (int j=0; j < len; j++) if (ww == null || ww[j]) r += ({floating_object_type}.isNaN(aa[j]) ? 0 : aa[j]); break;
''',

    'ASSOCIATIVE_OPS_NAIVE': '''\
                    case ADD:    r += va;                           break;
                    case MIN:    r  = (r < va           ? r : va);  break;
                    case MAX:    r  = (r > va           ? r : va);  break;
                    case NANADD: r += ({floating_object_type}.isNaN(va) ? 0 : va); break;
''',

    'COMPARISON_OPS': '''\
            case EQ: for (int j=0; j < len; j++) ar[j] = (aa[j] == ab[j]); break;
            case NE: for (int j=0; j < len; j++) ar[j] = (aa[j] != ab[j]); break;
            case LT: for (int j=0; j < len; j++) ar[j] = ((aa[j]{numeric_conversion})  < (ab[j]{numeric_conversion})); break;
            case GT: for (int j=0; j < len; j++) ar[j] = ((aa[j]{numeric_conversion})  > (ab[j]{numeric_conversion})); break;
            case LE: for (int j=0; j < len; j++) ar[j] = ((aa[j]{numeric_conversion}) <= (ab[j]{numeric_conversion})); break;
            case GE: for (int j=0; j < len; j++) ar[j] = ((aa[j]{numeric_conversion}) >= (ab[j]{numeric_conversion})); break;
''',

    'COMPARISON_OPS_NAIVE': '''\
                    case EQ: r.setObjectAt(i,  va.equals(vb)); break;
                    case NE: r.setObjectAt(i, !va.equals(vb)); break;
                    case LT: r.setObjectAt(i, (va{numeric_conversion})  < (vb{numeric_conversion})); break;
                    case GT: r.setObjectAt(i, (va{numeric_conversion})  > (vb{numeric_conversion})); break;
                    case LE: r.setObjectAt(i, (va{numeric_conversion}) <= (vb{numeric_conversion})); break;
                    case GE: r.setObjectAt(i, (va{numeric_conversion}) >= (vb{numeric_conversion})); break;
''',

    'REDUCTIVE_LOGIC_OPS': '''\
            case ANY: for (int j=0; j < len; j++) if (ww == null || ww[j]) r |= (aa[j]{boolean_conversion}); break;
            case ALL: for (int j=0; j < len; j++) if (ww == null || ww[j]) r &= (aa[j]{boolean_conversion}); break;
''',

    'REDUCTIVE_LOGIC_OPS_NAIVE': '''\
                    case ANY: r |= (va{boolean_conversion}); break;
                    case ALL: r &= (va{boolean_conversion}); break;
''',

    'EXTRACT': '''\
            int k = 0;
            for (int j=0; j < len; j++) {{
                if (Boolean.TRUE.equals(ac[j])) {{
                    ar[k++] = aa[j];
                }}
            }}
''',

    'POPCOUNT': '''\
            for (int j=0; j < len; j++) {{
                r += (aa[j] ? 1 : 0);
            }}
'''
}

class DefaultMap(dict):
    """
    An alternative dictionary that returns the empty string whenever a key does
    not exist. This is useful when formatting the cog code and including additional
    keys only for specific data types or implementations.
    """
    def __missing__(self, key):
        return ''


def _get_kwargs_for_dtype(dtype, get_kwargs, impl):
    """
    Return the necessary keyword arguments for generating cogged code for a given
    dtype, given an implementation and a helper kwargs generator function.

    :param dtype: The dtype to generate kwargs for.
    :type dtype: type, Required

    :param get_kwargs: A helper function to initialize kwargs for a dtype.
    :type get_kwargs: method, Required

    :param impl: The implementation we are using (refer to ``generate()``'s pydoc).
    :type impl: dictionary, Required
    """
    # Include all general implementation-specific kwargs, as well as dtype-specific ones.
    kwargs = DefaultMap(impl['KWARGS'] | get_kwargs(dtype))

    if dtype == numpy.bool_:
        # Special numeric handling for booleans
        kwargs['numeric_conversion']   = ' ? 1 : 0'
        kwargs['not_operation']        = '!'
    elif dtype == numpy.int32:
        kwargs['not_operation']        = '~'
        # Special boolean handling for other dtypes
        kwargs['boolean_conversion']   = ' != 0'
        # Integer values need a floating-point object for NaN handling.
        kwargs['floating_object_type'] = 'Float'
    elif dtype == numpy.int64:
        kwargs['not_operation']        = '~'
        kwargs['boolean_conversion']   = ' != 0'
        kwargs['floating_object_type'] = 'Double'
    elif dtype == numpy.float32:
        kwargs['boolean_conversion']   = ' != 0'
        kwargs['floating_object_type'] = 'Float'
        # For floating-point values, we need a condition to check for NaNs.
        kwargs['nan_check_for_r']      = ' && !Float.isNaN(r)'
    elif dtype == numpy.float64:
        kwargs['boolean_conversion']   = ' != 0'
        kwargs['floating_object_type'] = 'Double'
        kwargs['nan_check_for_r']      = ' && !Double.isNaN(r)'
    else:
        raise Exception(f'The given dtype {dtype} is unsupported')

    # Some operations like BinaryOp.POW need to recast the result back to its
    # original dtype.
    kwargs['cast'] = f'({kwargs["primitive_type"]})'

    # Some operations are only supported for numeric types. Add those here.
    if dtype != numpy.bool_:
        kwargs['binary_numeric_ops']       = impl['BINARY_NUMERIC_OPS']      .format_map(kwargs)
        kwargs['binary_numeric_ops_naive'] = impl['BINARY_NUMERIC_OPS_NAIVE'].format_map(kwargs)
        kwargs['unary_numeric_ops']        = impl['UNARY_NUMERIC_OPS']       .format_map(kwargs)
        kwargs['unary_numeric_ops_naive']  = impl['UNARY_NUMERIC_OPS_NAIVE'] .format_map(kwargs)

    # Some operations are only supported for booleans, integers and longs.
    # Add those here.
    if dtype in [numpy.bool_, numpy.int32, numpy.int64]:
        kwargs['binary_logic_ops']       = impl['BINARY_LOGIC_OPS']      .format_map(kwargs)
        kwargs['binary_logic_ops_naive'] = impl['BINARY_LOGIC_OPS_NAIVE'].format_map(kwargs)
        kwargs['unary_logic_ops']        = impl['UNARY_LOGIC_OPS']       .format_map(kwargs)
        kwargs['unary_logic_ops_naive']  = impl['UNARY_LOGIC_OPS_NAIVE'] .format_map(kwargs)

    # Some operations are only supported for real numbers. Add those here.
    if dtype in [numpy.float32, numpy.float64]:
        kwargs['unary_real_ops']       = impl['UNARY_REAL_OPS']      .format_map(kwargs)
        kwargs['unary_real_ops_naive'] = impl['UNARY_REAL_OPS_NAIVE'].format_map(kwargs)

    # Some operations are handled the same, regardless of dtype. Add those here.
    kwargs['associative_ops']            = impl['ASSOCIATIVE_OPS']          .format_map(kwargs)
    kwargs['associative_ops_naive']      = impl['ASSOCIATIVE_OPS_NAIVE']    .format_map(kwargs)
    kwargs['comparison_ops']             = impl['COMPARISON_OPS']           .format_map(kwargs)
    kwargs['comparison_ops_naive']       = impl['COMPARISON_OPS_NAIVE']     .format_map(kwargs)
    kwargs['reductive_logic_ops']        = impl['REDUCTIVE_LOGIC_OPS']      .format_map(kwargs)
    kwargs['reductive_logic_ops_naive']  = impl['REDUCTIVE_LOGIC_OPS_NAIVE'].format_map(kwargs)
    kwargs['extract']                    = impl['EXTRACT']                  .format_map(kwargs)
    kwargs['popcount']                   = impl['POPCOUNT']                 .format_map(kwargs)

    # Returned the augmented kwargs for the dtype
    return kwargs


def generate(get_kwargs=params.get_kwargs, implementation={}):
    """
    Generate a cube math library using the provided implementation.
    This way, we can easily generate different implementations of CubeMath as
    needed (i.e., a vectorized version) by just specifying alternative
    implementations for select parts.

    :param get_kwargs: A function that given a dtype, provides all the necessary
                       keyword arguments related to that dtype to properly format
                       (cog) the given implementation. Naturally, this function
                       needs to accept all the specified dtypes by the implementation.
                       By default, ``params.get_kwargs`` is used.
    :type get_kwargs: method, Optional

    :param implementation: A dictionary containing implementation details for
                           cogging. This can be used to overwrite implementations
                           for specific parts of the code. Wherever a key is not
                           specified, then the default ``_CUBE_MATH_IMPLEMENTATION``
                           is used instead. The full list of accepted parameters
                           is detailed below:
    :type implementation: dictionary, Optional

    Common Code:
        * KWARGS
        * DTYPES
        * IMPORTS
        * HEADER
        * EXTRA_DECLARATIONS
        * COMMON
        * NUMERIC
        * ALL
        * BOOLEAN
        * CLASS
        * CAST
        * CAST_HELPER

    Operations:
        * BINARY_NUMERIC_OPS
        * BINARY_NUMERIC_OPS_NAIVE
        * UNARY_NUMERIC_OPS
        * UNARY_NUMERIC_OPS_NAIVE
        * BINARY_LOGIC_OPS
        * BINARY_LOGIC_OPS_NAIVE
        * UNARY_LOGIC_OPS
        * UNARY_LOGIC_OPS_NAIVE
        * UNARY_REAL_OPS
        * UNARY_REAL_OPS_NAIVE
        * ASSOCIATIVE_OPS
        * ASSOCIATIVE_OPS_NAIVE
        * COMPARISON_OPS
        * COMPARISON_OPS_NAIVE
        * REDUCTIVE_LOGIC_OPS
        * REDUCTIVE_LOGIC_OPS_NAIVE
        * EXTRACT
        * POPCOUNT
    """
    # Fill in the gaps of the given implementations with the default one,
    # wherever applicable.
    impl = _CUBE_MATH_IMPL | implementation

    # This KWARGS dictionary in the implementation should always include
    # 'class_name'. Raise an error if no class name was provided.
    if 'class_name' not in impl['KWARGS']:
        raise Exception("'class_name' was not provided to cog code")

    # All supported dtypes
    dtypes = impl['DTYPES']

    # Initiate the source code with the imports and the 'header' portion of the code
    _CUBE_MATH = impl['IMPORTS'] + \
                 impl['HEADER'] .format_map(DefaultMap(impl['KWARGS']))

    # Add all class definitions at the top
    for dtype in dtypes:
        kwargs = _get_kwargs_for_dtype(dtype, get_kwargs, impl)
        _CUBE_MATH  += impl['CLASS'].format_map(kwargs)

    # Add the common declaration of the source code (all shared method definitions)
    # Also, add any additional declarations required by alternate implementations
    _CUBE_MATH += impl['ADDITIONAL_DECLARATIONS'] + \
                  impl['COMMON']                 .format_map(DefaultMap(impl['KWARGS']))

    # Now add all the casting-related methods.
    # Developer note: Here we are allowing casting a cube to the same dtype it
    #                 is. The reason for this is that all hypercubes should support
    #                 casting to our supported primitive dtypes and we don't want
    #                 to inhibit this feature. This is also particularly useful for
    #                 us as a helper method to copy primitive cubes.
    for cast_to in dtypes: # dtype to cast to
        to_kwargs = _get_kwargs_for_dtype(cast_to, get_kwargs, impl)

        # Add the common methods
        _CUBE_MATH += impl['CAST']       .format(**to_kwargs)
        CAST_TMP    = impl['CAST_HELPER'].format(**to_kwargs)
        for cast_from in dtypes: # dtype to cast from
            from_kwargs = _get_kwargs_for_dtype(cast_from, get_kwargs, impl)

            # Converting to and from booleans has its own special logic
            if (cast_to     == numpy.bool_ and cast_from != numpy.bool_):
                from_kwargs['conversion'] = ' != 0'
            elif (cast_from == numpy.bool_ and cast_to   != numpy.bool_):
                from_kwargs['conversion'] = ' ? 1 : 0'

            # Add the casting methods
            _CUBE_MATH += CAST_TMP.format_map(from_kwargs)


    # Generate the code for the given dtype
    for dtype in dtypes:
        kwargs = _get_kwargs_for_dtype(dtype, get_kwargs, impl)

        # Since numeric operations are not supported for booleans, we handle
        # them separately.
        if dtype == numpy.bool_:
            _CUBE_MATH += impl['BOOLEAN'].format_map(kwargs) + \
                          impl['ALL']    .format_map(kwargs)
        # Otherwise we can cog all the operations for the given dtype.
        else:
            _CUBE_MATH += impl['NUMERIC'].format_map(kwargs) + \
                          impl['ALL']    .format_map(kwargs)

    # Don't forget the enclosing curly bracket
    _CUBE_MATH += '}\n'
    return _CUBE_MATH
