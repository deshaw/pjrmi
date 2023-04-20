package com.deshaw.pjrmi.test;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Helper classes used in python PJRmi test cases.
 */
public class PJRmiTestHelpers
{
    /**
     * Base class for the overridden methods unit test.
     */
    public static class OverriddenMethodsBase
    {
        public CharSequence getValue()
        {
            return "BASE";
        }
    }

    /**
     * Subclass for the overridden methods unit test.
     */
    public static class OverriddenMethodsDerived
        extends OverriddenMethodsBase
    {
        @Override
        public String getValue()
        {
            return "DERIVED";
        }
    }

    /**
     * An interface which redefines methods in Object.
     */
    public static interface ObjectInterface
    {
        /**
         * Redefine {@code hashCode()}.
         */
        public int hashCode();
    }

    // ----------------------------------------------------------------------

    // Misc helper methods. Some of these may look pointless, but they do have a
    // point; honest!

    /**
     * A method which takes a boolean[] and returns its length.
     *
     * @param array  The arrat.
     *
     * @return the length.
     */
    public static int booleanArrayLength(final boolean[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a boolean[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static boolean[] booleanArrayIdentity(final boolean[] array)
    {
        return array;
    }

    /**
     * A method which takes a byte[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static int byteArrayLength(final byte[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a byte[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static byte[] byteArrayIdentity(final byte[] array)
    {
        return array;
    }

    /**
     * A method which takes a short[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the length.
     */
    public static int shortArrayLength(final short[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a short[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static short[] shortArrayIdentity(final short[] array)
    {
        return array;
    }

    /**
     * A method which takes a int[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the length.
     */
    public static int intArrayLength(final int[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a int[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static int[] intArrayIdentity(final int[] array)
    {
        return array;
    }

    /**
     * A method which takes a long[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the length.
     */
    public static int longArrayLength(final long[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a long[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static long[] longArrayIdentity(final long[] array)
    {
        return array;
    }

    /**
     * A method which takes a float[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the length.
     */
    public static int floatArrayLength(final float[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a float[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static float[] floatArrayIdentity(final float[] array)
    {
        return array;
    }

    /**
     * A method which takes a double[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the length.
     */
    public static int doubleArrayLength(final double[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a double[] and returns it.
     *
     * @param array  The array.
     *
     * @return the array
     */
    public static double[] doubleArrayIdentity(final double[] array)
    {
        return array;
    }

    /**
     * A method which takes an Object, casts it to the appropriate array, and
     * returns its length.
     *
     * @param array  The array.
     *
     * @return the length.

     */
    public static int objectLength(final Object array)
    {
        if (array instanceof boolean[]) {
            return ((boolean[])array).length;
        }
        else if (array instanceof byte[]) {
            return ((byte[])array).length;
        }
        else if (array instanceof short[]) {
            return ((short[])array).length;
        }
        else if (array instanceof int[]) {
            return ((int[])array).length;
        }
        else if (array instanceof long[]) {
            return ((long[])array).length;
        }
        else if (array instanceof float[]) {
            return ((float[])array).length;
        }
        else if (array instanceof double[]) {
            return ((double[])array).length;
        }
        else if (array instanceof String[]) {
            return ((String[])array).length;
        }
        else {
            return -1;
        }
    }

    /**
     * A method which takes an Object[] and returns its length.
     *
     * @param array  The array.
     *
     * @return the length.
     */
    public static int objectArrayLength(final Object[] array)
    {
        return array.length;
    }

    /**
     * A method which takes a Collection and returns its size.
     *
     * @param collection  The collection.
     *
     * @return the size.
     */
    public static int collectionSize(final Collection<?> collection)
    {
        return collection.size();
    }

    /**
     * A method returning an {@link ObjectInterface}.
     *
     * @return the interface instance.
     */
    public static ObjectInterface getObjectInterface()
    {
        final class Result implements ObjectInterface { }
        return new Result();
    }

    // ----------------------------------------------------------------------

    // Method required for Python-side testing.

    /**
     * Method used to check if correct parameter names are received by python
     * side. Please don't change this method's name or parameter names as we
     * have tests that rely directly upon these values.
     *
     * @param num1  The first number.
     * @param num2  The second number.
     */
    public static int add(int num1, int num2)
    {
        return num1 + num2;
    }

    // ----------------------------------------------------------------------

    // Methods for the binding precedence test

    /**
     * A root class.
     */
    public static class A
    {
        // Nothing
    }

    /**
     * Subclass of {@link A}.
     */
    public static class B
        extends A
    {
        // Nothing
    }

    /**
     * Subclass of {@link B}.
     */
    public static class C
        extends B
    {
        // Nothing
    }

    /**
     * A class with Methods.
     */
    public static class PrecedenceMethods
    {
        public final String ctor;

        // Constructors, which also have ambiuity etc.
        public PrecedenceMethods()         { ctor =  "v"; }
        public PrecedenceMethods(A x)      { ctor =  "a"; }
        public PrecedenceMethods(B x)      { ctor =  "b"; }
        public PrecedenceMethods(C x)      { ctor =  "c"; }
        public PrecedenceMethods(A x, A y) { ctor = "aa"; }
        public PrecedenceMethods(B x, A y) { ctor = "ba"; }
        public PrecedenceMethods(A x, B y) { ctor = "ab"; }
        public PrecedenceMethods(float  x) { ctor =  "f"; }
        public PrecedenceMethods(double x) { ctor =  "d"; }
        public PrecedenceMethods(short  x) { ctor =  "s"; }
        public PrecedenceMethods(int    x) { ctor =  "i"; }
        public PrecedenceMethods(long   x) { ctor =  "l"; }

        // Least to most specific
        public CharSequence f(A      x) { return "cs_f_a"; }
        public CharSequence f(B      x) { return "cs_f_b"; }
        public CharSequence f(C      x) { return "cs_f_c"; }
        public CharSequence f(float  x) { return "cs_f_f"; }
        public CharSequence f(double x) { return "cs_f_d"; }
        public CharSequence f(short  x) { return "cs_f_s"; }
        public CharSequence f(int    x) { return "cs_f_i"; }
        public CharSequence f(long   x) { return "cs_f_l"; }

        // Most to least specific
        public CharSequence g(C      x) { return "cs_g_c"; }
        public CharSequence g(B      x) { return "cs_g_b"; }
        public CharSequence g(A      x) { return "cs_g_a"; }
        public CharSequence g(double x) { return "cs_g_d"; }
        public CharSequence g(float  x) { return "cs_g_f"; }
        public CharSequence g(long   x) { return "cs_g_l"; }
        public CharSequence g(int    x) { return "cs_g_i"; }
        public CharSequence g(short  x) { return "cs_g_s"; }

        // Least to most specific
        public CharSequence f(A x, A y) { return "cs_f_aa"; }
        public CharSequence f(B x, A y) { return "cs_f_ba"; }
        public CharSequence f(A x, B y) { return "cs_f_ab"; }

        // Methods with what looks like a circular hierachy, but are actually
        // all incomparable
        public CharSequence f(Integer x, String  y, Number  z) { return "cs_f_isn"; }
        public CharSequence f(Number  x, Integer y, String  z) { return "cs_f_nis"; }
        public CharSequence f(String  x, Number  y, Integer z) { return "cs_f_sni"; }

        // Ditto, but with inheritance between all the arguments
        public CharSequence f(Integer x, Object  y, Number  z) { return "cs_f_ion"; }
        public CharSequence f(Number  x, Integer y, Object  z) { return "cs_f_nio"; }
        public CharSequence f(Object  x, Number  y, Integer z) { return "cs_f_oni"; }

        // Methods which should not, and should, enforce strict typing owing to
        // whether they are overloaded or not
        public CharSequence ff(int   i) { return "cs_ff_i"; }
        public CharSequence gg(short i) { return "cs_gg_s"; }
        public CharSequence gg(int   i) { return "cs_gg_i"; }
    }

    /**
     * A class with more methods, some of which override those in the parent
     * class.
     */
    public static class MorePrecedenceMethods
        extends PrecedenceMethods
    {
        // Methods where the first two are incomparable but the last one is more
        // specific than both. Order is important here since the Python pjrmi
        // unittest code relies on it.
        public CharSequence f(String x, Object y) { return "cs_f_so"; }
        public CharSequence f(Object x, String y) { return "cs_f_os"; }
        public CharSequence f(String x, String y) { return "cs_f_ss"; }

        // Overloaded return type
        public String f(A x, A y) { return "s_f_aa"; }
    }

    /**
     * An iterator that yields 1, 2, 3.
     */
    public static class OneTwoThreeIterator
        implements Iterator<Integer>
    {
        private static class CustomNoSuchElementException
            extends NoSuchElementException
        {
        }

        private int myCounter = 0;

        @Override
        public boolean hasNext()
        {
            return myCounter < 3;
        }

        @Override
        public Integer next()
        {
            if (hasNext()) {
                myCounter++;
                return myCounter;
            }
            else {
                // Make sure we're able to handle classes extending
                // NoSuchElementException as well.
                throw new CustomNoSuchElementException();
            }
        }
    }

    /**
     * An iterator that throws after yielding 1, 2.
     */
    public static class OneTwoThrowIterator
        implements Iterator<Integer>
    {
        private int myCounter = 0;

        @Override
        public boolean hasNext()
        {
            return true;
        }

        @Override
        public Integer next()
        {
            myCounter++;
            if (myCounter < 3) {
                return myCounter;
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
    }
}
