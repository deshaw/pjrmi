package com.deshaw.python;

import com.deshaw.python.DType;
import com.deshaw.python.NumpyArray;
import com.deshaw.python.PythonPickle;
import com.deshaw.python.PythonUnpickle;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verify the operation of the Python picking code.
 */
public class PickleTest
{
    /*
     * Empty arrays
     */
    private static final int[]    EMPTY_INT    = new int   [0];
    private static final long[]   EMPTY_LONG   = new long  [0];
    private static final double[] EMPTY_DOUBLE = new double[0];


    /**
     * Objects of various primitive types.
     */
    private static final Object[] OBJECTS = {
        null,
        true, false,
        -100.0f, 0.0f, 100.0f
        -1000.0, 0.0, 1000.0,
        -10000000, 10000000,
        -10000000000L, 10000000000L,
        "hello", "world", ""
    };

    // ----------------------------------------------------------------------

    /**
     * Test pickling and unpickling of primitives.
     */
    @Test
    public void testPrimitives()
        throws Exception
    {
        for (Object object : OBJECTS) {
            doInOut(object);
        }
    }

    /**
     * Test arrays.
     */
    @Test
    public void testArrays()
        throws Exception
    {
        doInOut(new int[]    {     -10000000,     10000000 });
        doInOut(new long[]   { -10000000000L, 10000000000L });
        doInOut(new double[] {       -1000.0,       1000.0 });
    }

    /**
     * Test a map.
     */
    @Test
    public void testMap()
        throws Exception
    {
        final Map<Object,Object> map = new HashMap<>();
        for (Object object : OBJECTS) {
            if (object != null) {
                map.put(object, object);
            }
        }
        doInOut(map);
    }

    // ----------------------------------------------------------------------

    /**
     * Actually test some pickling.
     */
    private void doInOut(Object in)
        throws Exception
    {
        final PythonPickle pickle = new PythonPickle();
        final Object out = PythonUnpickle.loadPickle(pickle.toByteArray(in));
        Assertions.assertTrue(equals(in, out),
                              "IN[" + describe(in) + "] != OUT[" + describe(out) + "]");
    }

    /**
     * Whether two things are equal.
     */
    private boolean equals(final Object in, final Object out)
    {
        // Null check
        if (in == null && out == null) {
            return true;
        }
        if (in == null && out != null ||
            in != null && out == null)
        {
            return false;
        }

        // Unwrap arrays
        final boolean isArray = in.getClass().isArray();
        if (isArray) {
            // Should be a numpy array
            if (!(out instanceof NumpyArray)) {
                return false;
            }
            final NumpyArray array = (NumpyArray)out;

            // Check size
            final int length = array.size();
            if (length != Array.getLength(in)) {
                return false;
            }

            if (in.getClass().equals(EMPTY_INT.getClass())) {
                if (DType.Type.INT32 != array.dtype().type()) {
                    return false;
                }
                for (int i=0; i < length; i++) {
                    if (array.getInt(i) != Array.getInt(in, i)) {
                        return false;
                    }
                }
                return true;
            }
            else if (in.getClass().equals(EMPTY_LONG.getClass())) {
                if (DType.Type.INT64 != array.dtype().type()) {
                    return false;
                }
                for (int i=0; i < length; i++) {
                    if (array.getLong(i) != Array.getLong(in, i)) {
                        return false;
                    }
                }
                return true;
            }
            else if (in.getClass().equals(EMPTY_DOUBLE.getClass())) {
                if (DType.Type.FLOAT64 != array.dtype().type()) {
                    return false;
                }
                for (int i=0; i < length; i++) {
                    if (array.getDouble(i) != Array.getDouble(in, i)) {
                        return false;
                    }
                }
                return true;
            }
            else {
                // Unhandled
                return false;
            }
        }

        // Compare
        return Objects.equals(in, out);
    }

    /**
     * Describe an object.
     */
    private String describe(final Object object)
    {
        final StringBuilder sb = new StringBuilder();
        if (object == null) {
            sb.append("null");
        }
        else if (object.getClass().isArray()) {
            final int length = Array.getLength(object);
            sb.append('[');
            for (int i=0; i < length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(Array.get(object, i));
            }
            sb.append(']');
        }
        else if (object instanceof CharSequence) {
            sb.append('"').append(object).append('"');
        }
        else {
            sb.append(String.valueOf(object));
        }
        if (object != null) {
            sb.append(" <");
            sb.append(object.getClass());
            sb.append('>');
        }
        return sb.toString();
    }
}
