package com.deshaw.pjrmi;

// We use cog.py to provide the different versions of the various methods. In
// order to update this file you need to do:
//   cog.py -rc AsType.java
//
// See:  https://nedbatchelder.com/code/cog
//       https://www.python.org/about/success/cog/

/**
 * Some methods for type "hinting", to be used when going from the Python to the
 * Java side. This works by effectively telling the Python side what type we
 * want and letting it figure out how to convert what it has into that desired
 * type. This can be useful when heavily overloaded methods yield ambiguous or
 * undesired binding semantics.
 *
 * <p>E.g.<pre>
 *   AsType = class_for_name('com.deshaw.pjrmi.AsType')
 *   Arrays = class_for_name('java.util.Arrays')
 *   ints = tuple(range(10))
 *   Arrays.toString(AsType.int1d(ints))
 * </pre>
 */
public class AsType
{
    /**
     * Private CTOR to prevent instantiation.
     */
    private AsType()
    {
    }

    // [[[cog
    //     import cog
    //     PRIMITIVE_NUMBERS = ('byte', 'short', 'int', 'long', 'float', 'double')
    //     BOXES = {'char'   : 'Character',
    //              'byte'   : 'Byte',
    //              'short'  : 'Short',
    //              'int'    : 'Integer',
    //              'long'   : 'Long',
    //              'float'  : 'Float',
    //              'double' : 'Double'}
    //
    //     # Functions to convert to the different types. Some of these are
    //     # identity functions but help with type disambiguation when going
    //     # from Python to Java.
    //     #
    //     # While some of these methods exist elsewhere, in other forms, it's
    //     # convenient to have them all in one place when it comes to using
    //     # them on the Python side.
    //
    //     as_type = '''\
    //        /**
    //         * A function for converting a primitive to the {{@link Object}}-based
    //         * version of itself.
    //         *
    //         * <p>This is semantically {{@code {boxed_type}.valueOf(v)}}.
    //         *
    //         * @param v The value to box.
    //         *
    //         * @return the boxed value.
    //         */
    //        public static {boxed_type} as{boxed_type}(final {type} v)
    //        {{
    //            return v;
    //        }}
    //     '''
    //
    //     identity = '''\
    //        /**
    //         * An identity function for converting a {ndim}-dimension {type} value to itself.
    //         *
    //         * @param v The value to return.
    //         *
    //         * @return the value given.
    //         */
    //        public static {type}{dims} {ltype}{ndim}d(final {type}{dims} v)
    //        {{
    //            return v;
    //        }}
    //     '''
    //
    //     # ------------------------------------------------------------------
    //
    //     #
    //     for (type, boxed_type) in BOXES.items():
    //         cog.outl(as_type.format(type      =type,
    //                                 boxed_type=boxed_type))
    //
    //     # Do this for all the primitive numbers, and also for Object and
    //     # String, which also can suffer from binding abiguities.
    //     for type in (PRIMITIVE_NUMBERS + ('Object', 'String')):
    //         for ndim in range(1, 6):
    //             cog.outl(identity.format(type=type,
    //                                      ltype=type.lower(),
    //                                      ndim=ndim,
    //                                      dims=''.join(['[]'] * ndim)))
    // ]]]
    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Character.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Character asCharacter(final char v)
    {
        return v;
    }

    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Byte.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Byte asByte(final byte v)
    {
        return v;
    }

    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Short.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Short asShort(final short v)
    {
        return v;
    }

    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Integer.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Integer asInteger(final int v)
    {
        return v;
    }

    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Long.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Long asLong(final long v)
    {
        return v;
    }

    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Float.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Float asFloat(final float v)
    {
        return v;
    }

    /**
     * A function for converting a primitive to the {@link Object}-based
     * version of itself.
     *
     * <p>This is semantically {@code Double.valueOf(v)}.
     *
     * @param v The value to box.
     *
     * @return the boxed value.
     */
    public static Double asDouble(final double v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension byte value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static byte[] byte1d(final byte[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension byte value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static byte[][] byte2d(final byte[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension byte value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static byte[][][] byte3d(final byte[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension byte value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static byte[][][][] byte4d(final byte[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension byte value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static byte[][][][][] byte5d(final byte[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension short value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static short[] short1d(final short[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension short value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static short[][] short2d(final short[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension short value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static short[][][] short3d(final short[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension short value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static short[][][][] short4d(final short[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension short value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static short[][][][][] short5d(final short[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension int value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static int[] int1d(final int[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension int value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static int[][] int2d(final int[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension int value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static int[][][] int3d(final int[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension int value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static int[][][][] int4d(final int[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension int value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static int[][][][][] int5d(final int[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension long value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static long[] long1d(final long[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension long value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static long[][] long2d(final long[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension long value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static long[][][] long3d(final long[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension long value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static long[][][][] long4d(final long[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension long value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static long[][][][][] long5d(final long[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension float value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static float[] float1d(final float[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension float value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static float[][] float2d(final float[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension float value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static float[][][] float3d(final float[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension float value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static float[][][][] float4d(final float[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension float value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static float[][][][][] float5d(final float[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension double value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static double[] double1d(final double[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension double value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static double[][] double2d(final double[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension double value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static double[][][] double3d(final double[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension double value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static double[][][][] double4d(final double[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension double value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static double[][][][][] double5d(final double[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension Object value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static Object[] object1d(final Object[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension Object value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static Object[][] object2d(final Object[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension Object value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static Object[][][] object3d(final Object[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension Object value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static Object[][][][] object4d(final Object[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension Object value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static Object[][][][][] object5d(final Object[][][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 1-dimension String value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static String[] string1d(final String[] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 2-dimension String value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static String[][] string2d(final String[][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 3-dimension String value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static String[][][] string3d(final String[][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 4-dimension String value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static String[][][][] string4d(final String[][][][] v)
    {
        return v;
    }

    /**
     * An identity function for converting a 5-dimension String value to itself.
     *
     * @param v The value to return.
     *
     * @return the value given.
     */
    public static String[][][][][] string5d(final String[][][][][] v)
    {
        return v;
    }

    // [[[end]]] (checksum: 645ac2cb137f0df1c7d65bdc8fe4cf39)
}
