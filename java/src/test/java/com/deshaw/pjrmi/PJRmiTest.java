package com.deshaw.pjrmi;

import com.deshaw.pjrmi.PythonFunction;
import com.deshaw.pjrmi.PythonMinion;
import com.deshaw.pjrmi.PythonMinionProvider;
import com.deshaw.pjrmi.PythonObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests for PJRmi.
 */
public class PJRmiTest
{
    private static final PythonMinion PYTHON;
    static {
        try {
            PYTHON = PythonMinionProvider.spawn(true);
            PYTHON.exec("import pjrmi");
            PYTHON.exec("from numpy import int8");
        }
        catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Test eval()
     */
    @Test
    public void testPythonEval()
        throws Throwable
    {
        assertEquals(Byte.valueOf((byte)2), PYTHON.eval("1 + 1"));
        assertEquals(Byte.valueOf((byte)5), PYTHON.eval("len('hello')"));
    }

    /**
     * Test invoke().
     */
    @Test
    public void testPythonInvoke()
        throws Throwable
    {
        assertEquals(Byte.valueOf((byte)5), PYTHON.invoke("len", "hello"));
    }

    /**
     * Test setGlobalVariable().
     */
    @Test
    public void testSetGlobal()
        throws Throwable
    {
        // Put our thread object into Python as a global and attempt to call a
        // method on it to test that it's there okay
        final Thread thread = Thread.currentThread();
        PYTHON.setGlobalVariable("global_variable", thread);
        assertEquals(thread.getName(), PYTHON.eval("global_variable.getName()"));
    }

    /**
     * Test GC under the minion.
     */
    @Test
    public void testMinionGC()
        throws Throwable
    {
        // Make a function which will create a large number of Objects, and
        // Strings, and let the Java side know that they have been GC'd. This
        // checks that EMPTY_ACK messages are correctly handled in the minion.
        //
        // The underlying Python code handles GC collections on a 100ms cycle
        // and batches them in 100s right now. Using a 1000 objects and a 0.1ms
        // sleep should be enough to trigger the issue, and give us some wiggle
        // room.
        PYTHON.exec("import gc\n");
        PYTHON.exec("def to_string(o):\n" +
                    "    return 'toString=' + o.toString()\n");
        for (int i=0; i < 1000; i++) {
            PYTHON.invoke("to_string", String.class, new Object());
            PYTHON.invoke("gc.collect");
            try { Thread.sleep(0, 100); }
            catch (InterruptedException e) { /* Nothing */ }
        }
    }

    /**
     * Test PythonObject.
     */
    @Test
    public void testPythonObject()
        throws Throwable
    {
        // Create a Python class "C"
        final String klass = ("class C(object):\n" +
                              "    def __init__(self, i):\n" +
                              "        self.i = i\n" +
                              "    def f(self):\n" +
                              "        return 'FOO'\n" +
                              "    def g(self):\n" +
                              "        raise ValueError('BAR')\n" +
                              "    def h(self, lst):\n" +
                              "        lst.get(0)\n" +
                              "    def c(self, i):\n" +
                              "        return C(self.i + i)");
        PYTHON.exec(klass);

        // Get an instance of the class "C"
        final PythonObject c = PYTHON.getObject("C(int8(1))");

        // Try explicit and implicit marshalling with getattr
        assertEquals(Boolean.TRUE,               c.getattr(Boolean.class, "i"));
        assertEquals(Byte   .valueOf((byte)  1), c.getattr(Byte   .class, "i"));
        assertEquals(Short  .valueOf((short) 1), c.getattr(Short  .class, "i"));
        assertEquals(Integer.valueOf((int)   1), c.getattr(Integer.class, "i"));
        assertEquals(Long   .valueOf((long)  1), c.getattr(Long   .class, "i"));
        assertEquals(Float  .valueOf((float) 1), c.getattr(Float  .class, "i"));
        assertEquals(Double .valueOf((double)1), c.getattr(Double .class, "i"));
        assertEquals("1",                        c.getattr(Object .class, "i").toString());

        // Invoke a method to get back another instance of C, and test similarly
        PythonObject cc = c.invoke(PythonObject.class, "c", 1);
        assertEquals(Integer.valueOf(2), cc.getattr(Integer.class, "i"));

        // Look for a missing field
        try {
            c.getattr(Integer.class, "j");
            fail();
        }
        catch (NoSuchFieldException e) {
            // pass
        }

        // Get a method handle and invoke it to get back a value
        @SuppressWarnings("unchecked")
        final PythonFunction<String> f1 = c.getattr(PythonFunction.class, "f");
        assertEquals("FOO", f1.invoke());

        // And again, in a different way
        final PythonFunction<String> f2 = c.getMethod(String.class, "f");
        assertEquals("FOO", f2.invoke());

        // Get a method handle and invoke it to raise an exception
        @SuppressWarnings("unchecked")
        final PythonFunction<String> g = c.getattr(PythonFunction.class, "g");
        try {
            g.invoke();
            fail();
        }
        catch (Exception e) {
            // pass
        }

        // Get another method handle and invoke it to raise a Java exception
        @SuppressWarnings("unchecked")
        final PythonFunction<String> h = c.getattr(PythonFunction.class, "h");
        try {
            h.invoke(Collections.emptyList());
            fail();
        }
        catch (Exception e) {
            // pass
        }

        // Test that we can construct an object by passing Java args.
        PythonObject c2 = PYTHON.invokeAndGetObject("C", 3);
        assertEquals(Integer.valueOf(3), c2.getattr(Integer.class, "i"));
    }

    /**
     * Test PythonObject.
     */
    @Test
    public void testPythonMethods()
        throws Throwable
    {
        // Get the object wrapper
        final PythonObject dict = PYTHON.getObject("dict()", "MyDictionary");

        // Get some functional aliases; same method but different ways of
        // invoking it
        final BiConsumer<String,String> setitemBC = dict.getBiConsumer("__setitem__");
        final PythonFunction<Object>    setitemM  = dict.getMethod(Object.class,
                                                                   "__setitem__");

        // Insert values using the methods
        setitemBC.accept("hello", "world");
        setitemM .invoke("dict",  dict);

        // Get the stringified version of the dict; the entry ordering here is
        // abritrary so we test both
        final String str = dict.invoke(String.class, "__str__");
        assertTrue("{u'dict': {...}, u'hello': u'world'}".equals(str)
                || "{u'hello': u'world', u'dict': {...}}".equals(str)
                || "{'dict': {...}, 'hello': 'world'}".equals(str)
                || "{'hello': 'world', 'dict': {...}}".equals(str),
                              "Unexpected output: \"" + str + "\"");
    }

    /**
     * Test PythonObject as a Proxy. Tests calling methods which given back
     * values.
     */
    @Test
    public void testPythonProxy1()
        throws Throwable
    {
        // Create a Python class "CharSequence"
        final String klass = ("class CharSequence(pjrmi.JavaProxyBase):\n" +
                              "    def __init__(self, s):\n" +
                              "        self.s = s\n" +
                              "    def charAt(self, i):\n" +
                              "        return self.s[i]\n" +
                              "    def length(self):\n" +
                              "        return len(self.s)\n" +
                              "    def subSequence(self, s, e):\n" +
                              "        return self.s[s:e]\n" +
                              "    def toString(self):\n" +
                              "        return self.s");
        PYTHON.exec(klass);

        // And get an instance of it, masqueraiding as a Java CharSequence
        final CharSequence s1 = "0123456789";
        final CharSequence s2 = PYTHON.getObject("CharSequence('" + s1 + "')")
                                      .asProxy(CharSequence.class);
        assertEquals(s1.length(), s2.length());
        assertEquals(s1.toString(), s2.toString());
        for (int i=0; i < s1.length(); i++) {
            assertEquals(s1.charAt(i), s2.charAt(i));
        }
        assertEquals(s1.subSequence(3, 6), s2.subSequence(3, 6));
    }

    /**
     * Test PythonObject as a Proxy. Makes sure that calling methods which
     * "return" void works as well as the "native" toString() method.
     */
    @Test
    public void testPythonProxy2()
        throws Throwable
    {
        // Create a Python class "Runnable"
        final String klass = ("class Runnable(pjrmi.JavaProxyBase):\n" +
                              "    def __init__(self):\n" +
                              "        pass\n" +
                              "    def run(self):\n" +
                              "        pass\n");
        PYTHON.exec(klass);

        // Just make sure we can call it
        final Runnable r = PYTHON.getObject("Runnable()")
                                 .asProxy(Runnable.class);
        r.run();

        // And ensure that the native toString() method doesn't explode
        r.toString();
    }

    /**
     * Tests array method return types from Python back to Java.
     */
    @Test
    public void testArrayMethodReturnTypes()
        throws Throwable
    {
        // Create some methods which return back various different types of
        // array data.
        final String methodDefs = (
            "import numpy\n" +
            "def getByteArray():\n" +
            "    return numpy.array([1,2,3], dtype='int8')\n" +
            "def getShortArray():\n" +
            "    return numpy.array([1,2,3], dtype='int16')\n" +
            "def getIntArray():\n" +
            "    return numpy.array([1,2,3], dtype='int32')\n" +
            "def getLongArray():\n" +
            "    return numpy.array([1,2,3], dtype='int64')\n" +
            "def getFloatArray():\n" +
            "    return numpy.array([1,2,3], dtype='float32')\n" +
            "def getDoubleArray():\n" +
            "    return numpy.array([1,2,3], dtype='float64')\n" +
            "def getBooleanArray():\n" +
            "    return numpy.array([False,True], dtype='bool')\n" +
            "def getStringArray():\n" +
            "    return numpy.array(['abc', 'def'])\n" +
            "def getMixedMap():\n" +
            "    return {\n" +
            "        'bytes'  : getByteArray  (),\n" +
            "        'ints'   : getIntArray   (),\n" +
            "        'strings': getStringArray(),\n" +
            "    }\n"
        );

        PYTHON.exec(methodDefs);

        // Test casting of the return type.
        assertTrue(Arrays.equals(new    byte[] { 1, 2, 3      }, (byte   []) PYTHON.invoke("getByteArray"   )));
        assertTrue(Arrays.equals(new   short[] { 1, 2, 3      }, (short  []) PYTHON.invoke("getShortArray"  )));
        assertTrue(Arrays.equals(new     int[] { 1, 2, 3      }, (int    []) PYTHON.invoke("getIntArray"    )));
        assertTrue(Arrays.equals(new    long[] { 1, 2, 3      }, (long   []) PYTHON.invoke("getLongArray"   )));
        assertTrue(Arrays.equals(new   float[] { 1, 2, 3      }, (float  []) PYTHON.invoke("getFloatArray"  )));
        assertTrue(Arrays.equals(new  double[] { 1, 2, 3      }, (double []) PYTHON.invoke("getDoubleArray" )));
        assertTrue(Arrays.equals(new boolean[] { false, true  }, (boolean[]) PYTHON.invoke("getBooleanArray")));
        assertTrue(Arrays.equals(new  String[] { "abc", "def" }, (String []) PYTHON.invoke("getStringArray" )));

        // Test explicitly providing the return type to Python.
        assertTrue(Arrays.equals(new    byte[] { 1, 2, 3      }, PYTHON.invoke("getByteArray"   , byte   [].class)));
        assertTrue(Arrays.equals(new   short[] { 1, 2, 3      }, PYTHON.invoke("getShortArray"  , short  [].class)));
        assertTrue(Arrays.equals(new     int[] { 1, 2, 3      }, PYTHON.invoke("getIntArray"    , int    [].class)));
        assertTrue(Arrays.equals(new    long[] { 1, 2, 3      }, PYTHON.invoke("getLongArray"   , long   [].class)));
        assertTrue(Arrays.equals(new   float[] { 1, 2, 3      }, PYTHON.invoke("getFloatArray"  , float  [].class)));
        assertTrue(Arrays.equals(new  double[] { 1, 2, 3      }, PYTHON.invoke("getDoubleArray" , double [].class)));
        assertTrue(Arrays.equals(new boolean[] { false, true  }, PYTHON.invoke("getBooleanArray", boolean[].class)));
        assertTrue(Arrays.equals(new  String[] { "abc", "def" }, PYTHON.invoke("getStringArray" , String [].class)));

        // Test some cases which would fail.
        assertFalse(Arrays.equals(new    byte[] { 3, 2, 1      }, PYTHON.invoke("getByteArray"   , byte   [].class)));
        assertFalse(Arrays.equals(new boolean[] { true, false  }, PYTHON.invoke("getBooleanArray", boolean[].class)));
        assertFalse(Arrays.equals(new  String[] { null, "def"  }, PYTHON.invoke("getStringArray" , String [].class)));
        try {
            short[] result = (short[]) PYTHON.invoke("getByteArray");

            // We should never get here.
            fail("byte[] cast to short[]");
        }
        catch (ClassCastException e) {
            // This is expected.
        }

        // Test that arrays in a more complex type like a map are returned with
        // the correct type.
        Map<?,?> result = PYTHON.invoke("getMixedMap", Map.class);
        assertTrue(Arrays.equals(new   byte[] { 1, 2, 3      }, (byte  []) result.get("bytes"  )));
        assertTrue(Arrays.equals(new   int [] { 1, 2, 3      }, (int   []) result.get("ints"   )));
        assertTrue(Arrays.equals(new String[] { "abc", "def" }, (String[]) result.get("strings")));
    }

    /**
     * Tests native array handling from Java to Python and vice versa.
     */
    @Test
    public void testNativeArray()
        throws Throwable
    {
        // Create some methods which take various different types of array data.
        final String methodDefs = (
            "import numpy\n" +
            "def getArrayLength(array):\n" +
            "    return(len(array))\n" +
            "def getNumpyBooleanEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='bool')\n" +
            "def getNumpyByteEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='int8')\n" +
            "def getNumpyShortEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='int16')\n" +
            "def getNumpyIntEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='int32')\n" +
            "def getNumpyLongEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='int64')\n" +
            "def getNumpyFloatEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='float32')\n" +
            "def getNumpyDoubleEquivalent(array):\n" +
            "    return numpy.asarray(array, dtype='float64')\n" +
            "def populateMap(java_map):\n" +
            "    # To match the Java arrays in the later code\n" +
            "    for dtype in ('int8', 'int16', 'int32', 'int64',\n" +
            "                  'float32', 'float64'):\n" +
            "        java_map.put(dtype, numpy.arange(1, 4, dtype=dtype))\n"
        );

        PYTHON.exec(methodDefs);

        // Some primitive Java arrays we'll use for the tests
        boolean[] java_empty_array   = new boolean[] {};
        boolean[] java_boolean_array = new boolean[] {true, false};
        byte   [] java_byte_array    = new byte   [] {1, 2, 3};
        short  [] java_short_array   = new short  [] {1, 2, 3};
        int    [] java_int_array     = new int    [] {1, 2, 3};
        long   [] java_long_array    = new long   [] {1, 2, 3};
        float  [] java_float_array   = new float  [] {1, 2, 3};
        double [] java_double_array  = new double [] {1, 2, 3};

        // Test that input type of primitive Java array are cast
        // appropriately in Python.
        assertEquals(Integer.valueOf(0),
                     PYTHON.invoke("getArrayLength", int.class, java_empty_array));
        assertEquals(Integer.valueOf(2),
                     PYTHON.invoke("getArrayLength", int.class, java_boolean_array));
        assertEquals(Integer.valueOf(3),
                     PYTHON.invoke("getArrayLength", int.class, java_byte_array));
        assertEquals(Integer.valueOf(3),
                     PYTHON.invoke("getArrayLength", int.class, java_short_array));
        assertEquals(Integer.valueOf(3),
                     PYTHON.invoke("getArrayLength", int.class, java_int_array));
        assertEquals(Integer.valueOf(3),
                     PYTHON.invoke("getArrayLength", int.class, java_long_array));
        assertEquals(Integer.valueOf(3),
                     PYTHON.invoke("getArrayLength", int.class, java_float_array));
        assertEquals(Integer.valueOf(3),
                     PYTHON.invoke("getArrayLength", int.class, java_double_array));

        // Test that input type of primitive Java array are cast and used
        // appropriately in Python. We cast the returned value to check
        // for equality.
        assertTrue(Arrays.equals(         java_empty_array,   (boolean[]) PYTHON.invoke
            ("getNumpyBooleanEquivalent", java_empty_array)));
        assertTrue(Arrays.equals(         java_boolean_array, (boolean[]) PYTHON.invoke
            ("getNumpyBooleanEquivalent", java_boolean_array)));
        assertTrue(Arrays.equals(         java_byte_array,    (byte   []) PYTHON.invoke
            ("getNumpyByteEquivalent",    java_byte_array)));
        assertTrue(Arrays.equals(         java_short_array,   (short  []) PYTHON.invoke
            ("getNumpyShortEquivalent",   java_byte_array)));
        assertTrue(Arrays.equals(         java_int_array,     (int    []) PYTHON.invoke
            ("getNumpyIntEquivalent",     java_byte_array)));
        assertTrue(Arrays.equals(         java_long_array,    (long   []) PYTHON.invoke
            ("getNumpyLongEquivalent",    java_byte_array)));
        assertTrue(Arrays.equals(         java_float_array,   (float  []) PYTHON.invoke
            ("getNumpyFloatEquivalent",   java_byte_array)));
        assertTrue(Arrays.equals(         java_double_array,  (double []) PYTHON.invoke
            ("getNumpyDoubleEquivalent",  java_double_array)));

        // Now test stuffing values into a Map
        final Map<String,Object> map = new HashMap<>();
        PYTHON.invoke("populateMap", map);
        assertTrue(Arrays.equals(java_byte_array,   (byte   [])map.get("int8"   )));
        assertTrue(Arrays.equals(java_short_array,  (short  [])map.get("int16"  )));
        assertTrue(Arrays.equals(java_int_array,    (int    [])map.get("int32"  )));
        assertTrue(Arrays.equals(java_long_array,   (long   [])map.get("int64"  )));
        assertTrue(Arrays.equals(java_float_array,  (float  [])map.get("float32")));
        assertTrue(Arrays.equals(java_double_array, (double [])map.get("float64")));
    }
}
