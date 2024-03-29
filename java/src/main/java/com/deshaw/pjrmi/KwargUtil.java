package com.deshaw.pjrmi;

import com.deshaw.hypercube.BooleanHypercube;
import com.deshaw.hypercube.Boolean1dWrappingHypercube;
import com.deshaw.hypercube.Hypercube;
import com.deshaw.python.DType;

import java.util.Arrays;
import java.util.List;

/**
 * KwargUtil provides utility functions for helping with kwargs.
 */
public class KwargUtil
{
    /**
     * Turn the given (kwarg) object into a value of the given class, if
     * possible.
     *
     * @return the cube, or {@code null} if given null.
     *
     * @throws IllegalArgumentException if the conversion is not possible.
     */
    @SuppressWarnings("unchecked")
    public static <T> T toClassValue(final Object   kwarg,
                                     final Class<T> klass)
        throws IllegalArgumentException
    {
        // Null breeds null
        if (kwarg == null) {
            return null;
        }

        // Handle a missing class
        if (klass == null) {
            throw new IllegalArgumentException("Given a null class");
        }

        // See if direct casting is possible
        try {
            return klass.cast(kwarg);
        }
        catch (ClassCastException e) {
            // Nothing; best effort
        }

        // Do things by hand

        // Convert to numbers
        if (kwarg instanceof Number) {
            final Number number = (Number)kwarg;
            if (klass == Byte   .class) return (T)(Object)number.byteValue();
            if (klass == Double .class) return (T)(Object)number.doubleValue();
            if (klass == Float  .class) return (T)(Object)number.floatValue();
            if (klass == Integer.class) return (T)(Object)number.intValue();
            if (klass == Short  .class) return (T)(Object)number.shortValue();
            if (klass == Long   .class) return (T)(Object)number.longValue();
        }

        // Turn things into Strings. This should really be made a little
        // cleverer so as to handle arrays etc. at some point.
        if (klass == String.class) {
            return (T)(Object)kwarg.toString();
        }

        // If we got here then we couldn't do it
        throw new IllegalArgumentException(
            "Could not convert " + kwarg + " to a " + klass.getSimpleName()
        );
    }

    /**
     * Turn the given value into a {@link DType.Type}.
     */
    @SuppressWarnings("unchecked")
    public static DType.Type toDTypeType(Object kwarg)
        throws IllegalArgumentException
    {
        if (kwarg == null) {
            kwarg = "float";
        }
        if (kwarg.equals("float")) {
            kwarg = "float64";
        }
        if (!(kwarg instanceof String)) {
            kwarg = kwarg.toString();
        }
        final DType.Type type = DType.Type.byName((String)kwarg);
        if (type == null) {
            throw new IllegalArgumentException("No kwarg for '" + kwarg + "'");
        }
        return type;
    }

    /**
     * Turn the given (kwarg) object into a {@code int[]}, if possible.
     *
     * @return the integer array, or {@code null} if given null.
     *
     * @throws IllegalArgumentException if the conversion is not possible.
     */
    @SuppressWarnings("unchecked")
    public static int[] toIntArray(final Object kwarg)
        throws IllegalArgumentException
    {
        // Handle these via a direct cast
        if (kwarg == null || kwarg instanceof int[]) {
            return (int[])kwarg;
        }

        // Go via longs for everything else, to save on repeated logic
        final long[] array;
        try {
            array = toLongArray(kwarg);;
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Could not convert " + kwarg + " to an int[]",
                e
            );
        }

        // And copy over
        final int[] result = new int[array.length];
        for (int i=0; i < array.length; i++) {
            result[i] = (int)array[i];
            if (result[i] != array[i]) {
                throw new IllegalArgumentException(
                    "Loss of precision when converting " +
                    array[i] + " to an int in " +
                    Arrays.toString(array)
                );
            }
        }
        return result;
    }

    /**
     * Turn the given (kwarg) object into a {@code long[]}, if possible.
     *
     * @return the long array, or {@code null} if given null.
     *
     * @throws IllegalArgumentException if the conversion is not possible.
     */
    @SuppressWarnings("unchecked")
    public static long[] toLongArray(final Object kwarg)
        throws IllegalArgumentException
    {
        // Null breeds null
        if (kwarg == null) {
            return null;
        }

        // Turn a single number into an array
        else if (kwarg instanceof Number) {
            final Number num = (Number)kwarg;
            final long   i   = num.longValue();
            if (i != num.doubleValue()) {
                throw new IllegalArgumentException(
                    "Loss of precision when converting " +
                    num + " to a long"
                );
            }
            return new long[] { i };
        }

        // Primitive arrays we can mostly convert by copying
        else if (kwarg instanceof byte[]) {
            final byte[] array  = (byte[])kwarg;
            final long[] result = new long[array.length];
            for (int i=0; i < array.length; i++) {
                result[i] = array[i];
            }
            return result;
        }
        else if (kwarg instanceof short[]) {
            final short[] array  = (short[])kwarg;
            final long[]  result = new long[array.length];
            for (int i=0; i < array.length; i++) {
                result[i] = array[i];
            }
            return result;
        }
        else if (kwarg instanceof int[]) {
            final int[]  array  = (int[])kwarg;
            final long[] result = new long[array.length];
            for (int i=0; i < array.length; i++) {
                result[i] = array[i];
            }
            return result;
        }
        else if (kwarg instanceof long[]) {
            return (long[])kwarg;
        }
        else if (kwarg instanceof double[]) {
            final double[] array  = (double[])kwarg;
            final long[]   result = new long[array.length];
            for (int i=0; i < array.length; i++) {
                result[i] = (long)array[i];
                if (result[i] != array[i]) {
                    throw new IllegalArgumentException(
                        "Loss of precision when converting " +
                        array[i] + " to a long in " +
                        Arrays.toString(array)
                    );
                }
            }
            return result;
        }
        else if (kwarg instanceof float[]) {
            final float[] array  = (float[])kwarg;
            final long[]  result = new long[array.length];
            for (int i=0; i < array.length; i++) {
                result[i] = (long)array[i];
                if (result[i] != array[i]) {
                    throw new IllegalArgumentException(
                        "Loss of precision when converting " +
                        array[i] + " to a long in" +
                        Arrays.toString(array)
                    );
                }
            }
            return result;
        }

        // Unbox object types
        else if (kwarg instanceof List) {
            try {
                // Try converting it into a List of Numbers, with the assumption
                // that we have what we expect. We'll handle cast errors in the
                // catch.
                final List<Number> list  = (List<Number>)kwarg;
                final long[]       array = new long[list.size()];
                for (int i=0; i < list.size(); i++) {
                    final Number num = list.get(i);
                    array[i] = num.longValue();
                    if (array[i] != num.doubleValue()) {
                        throw new IllegalArgumentException(
                            "Loss of precision when converting " +
                            num + " to a long in " + list
                        );
                    }
                }
                return array;
            }
            catch (ClassCastException e) {
                // Throw an error otherwise
                throw new IllegalArgumentException(
                    "Could not convert " + kwarg + " to a long[]",
                    e
                );
            }
        }
        else if (kwarg instanceof Object[]) {
            try {
                // Try converting it into a longeger array, with the same cast
                // handling as above
                final Object[] objs = (Object[])kwarg;
                final long[] array = new long[objs.length];
                for (int i=0; i < objs.length; i++) {
                    final Number num = (Number)objs[i];
                    array[i] = num.longValue();
                    if (array[i] != num.longValue() ||
                        array[i] != num.doubleValue())
                    {
                        throw new IllegalArgumentException(
                            "Loss of precision when converting " +
                            num + " to a long in " + Arrays.toString(objs)
                        );
                    }
                }
                return array;
            }
            catch (ClassCastException e) {
                // Throw an error otherwise
                throw new IllegalArgumentException(
                    "Could not convert " + kwarg + " to a long[]",
                    e
                );
            }
        }
        else {
            // Don't know how to do this
            throw new IllegalArgumentException(
                "Could not convert " + kwarg + " to a long[]"
            );
        }
    }

    /**
     * Turn the given (kwarg) object into a {@link BooleanHypercube}, if possible.
     *
     * @return the cube, or {@code null} if given null.
     *
     * @throws IllegalArgumentException if the conversion is not possible.
     */
    public static BooleanHypercube toBooleanHypercube(final Object kwarg)
        throws IllegalArgumentException
    {
        // Null breeds null
        if (kwarg == null) {
            return null;
        }

        // Handle some primitives
        if (kwarg instanceof Boolean) {
            return new Boolean1dWrappingHypercube(
                new boolean[]{((Boolean)kwarg).booleanValue()}
            );
        }
        if (kwarg instanceof Number) {
            return new Boolean1dWrappingHypercube(
                new boolean[]{((Number)kwarg).byteValue() != 0}
            );
        }

        // Now assume it's a cube
        BooleanHypercube result = null;
        try {
            result = BooleanHypercube.asBooleanHypercube((Hypercube<?>)kwarg);
        }
        catch (ClassCastException e) {
            // Nothing, checked below
        }

        // If the result was null then the conversion failed
        if (result == null) {
            throw new IllegalArgumentException(
                "Could not convert value to a BooleanHypercube: " + kwarg
            );
        }

        // Otherwise it must have worked
        return result;
    }
}
