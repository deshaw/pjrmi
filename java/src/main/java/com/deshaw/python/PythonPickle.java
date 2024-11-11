package com.deshaw.python;

import com.deshaw.util.ByteList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.RandomAccess;

/**
 * Serialization of basic Java objects into a format compatible with Python's
 * pickle protocol. This should allow objects to be marshalled from Java into
 * Python.
 *
 * <p>This class is not thread-safe.
 */
public class PythonPickle
{
    // Keep in sync with pickle.Pickler._BATCHSIZE. This is how many elements
    // batch_list/dict() pumps out before doing APPENDS/SETITEMS. Nothing will
    // break if this gets out of sync with pickle.py, but it's unclear that
    // would help anything either.
    private static final int  BATCHSIZE = 1000;
    private static final byte MARK_V    = Operations.MARK.code;

    // ----------------------------------------------------------------------

    /**
     * We buffer up everything in here for dumping.
     */
    private final ByteArrayOutputStream myStream = new ByteArrayOutputStream();

    /**
     * Used to provide a handle on objects which we have already stored (so that
     * we don't duplicate them in the result).
     */
    private final IdentityHashMap<Object,Integer> myMemo = new IdentityHashMap<>();

    // Scratch space
    private final ByteBuffer myTwoByteBuffer   = ByteBuffer.allocate(2);
    private final ByteBuffer myFourByteBuffer  = ByteBuffer.allocate(4);
    private final ByteBuffer myEightByteBuffer = ByteBuffer.allocate(8);
    private final ByteList   myByteList        = new ByteList();

    // ----------------------------------------------------------------------

    /**
     * Dump an object out to a given stream.
     */
    public void toStream(Object o, OutputStream stream)
        throws IOException
    {
        // Might be better to use the stream directly, rather than staging
        // locally.
        toPickle(o);
        myStream.writeTo(stream);
    }

    /**
     * Dump to a byte-array.
     */
    public byte[] toByteArray(Object o)
    {
        toPickle(o);
        return myStream.toByteArray();
    }

    /**
     * Pickle an arbitrary object which isn't handled by default.
     *
     * <p>Subclasses can override this to extend the class's behaviour.
     *
     * @throws UnsupportedOperationException if the object could not be pickled.
     */
    protected void saveObject(Object o)
        throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException(
            "Cannot pickle " + o.getClass().getCanonicalName()
        );
    }

    // ----------------------------------------------------------------------------
    // Methods which subclasses might need to extend funnctionality

    /**
     * Get back the reference to a previously saved object.
     */
    protected final void get(Object o)
    {
        writeOpcodeForValue(Operations.BINGET,
                            Operations.LONG_BINGET,
                            myMemo.get(o));
    }

    /**
     * Save a reference to an object.
     */
    protected final void put(Object o)
    {
        // 1-indexed; see comment about positve vs. non-negative in Python's
        // C pickle code
        final int n = myMemo.size() + 1;
        myMemo.put(o, n);
        writeOpcodeForValue(Operations.BINPUT,
                            Operations.LONG_BINPUT,
                            n);
    }

    /**
     * Write out an opcode, depending on the size of the 'n' value we are
     * encoding (i.e. if it fits in a byte).
     */
    protected final void writeOpcodeForValue(Operations op1, Operations op5, int n)
    {
        if (n < 256) {
            write(op1);
            write((byte) n);
        }
        else {
            write(op5);
            // The pickle protocol saves this in little-endian format.
            writeLittleEndianInt(n);
        }
    }

    /**
     * Dump out a string as ASCII.
     */
    protected final void writeAscii(String s)
    {
        write(s.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Write out a byte.
     */
    protected final void write(Operations op)
    {
        myStream.write(op.code);
    }

    /**
     * Write out a byte.
     */
    protected final void write(byte i)
    {
        myStream.write(i);
    }

    /**
     * Write out a char.
     */
    protected final void write(char c)
    {
        myStream.write(c);
    }

    /**
     * Write out an int, in little-endian format.
     */
    protected final void writeLittleEndianInt(final int n)
    {
        write(myFourByteBuffer.order(ByteOrder.LITTLE_ENDIAN).putInt(0, n));
    }

    /**
     * Write out the contents of a ByteBuffer.
     */
    protected final void write(ByteBuffer b)
    {
        write(b.array());
    }

    /**
     * Write out the contents of a byte array.
     */
    protected final void write(byte[] array)
    {
        myStream.write(array, 0, array.length);
    }

    /**
     * Dump out a 32bit float.
     */
    protected final void saveFloat(float o)
    {
        myByteList.clear();
        myByteList.append(Float.toString(o).getBytes());

        write(Operations.FLOAT);
        write(myByteList.toArray());
        write((byte) '\n');
    }

    /**
     * Dump out a 64bit double.
     */
    protected final void saveFloat(double o)
    {
        write(Operations.BINFLOAT);
        // The pickle protocol saves Python floats in big-endian format.
        write(myEightByteBuffer.order(ByteOrder.BIG_ENDIAN).putDouble(0, o));
    }

    /**
     * Write a 32-or-less bit integer.
     */
    protected final void saveInteger(int o)
    {
        // The pickle protocol saves Python integers in little-endian format.
        final byte[] a =
            myFourByteBuffer.order(ByteOrder.LITTLE_ENDIAN).putInt(0, o)
                            .array();
        if (a[2] == 0 && a[3] == 0) {
            if (a[1] == 0) {
                // BININT1 is for integers [0, 256), not [-128, 128).
                write(Operations.BININT1);
                write(a[0]);
                return;
            }
            // BININT2 is for integers [256, 65536), not [-32768, 32768).
            write(Operations.BININT2);
            write(a[0]);
            write(a[1]);
            return;
        }
        write(Operations.BININT);
        write(a);
    }

    /**
     * Write a 64-or-less bit integer.
     */
    protected final void saveInteger(long o)
    {
        if (o <= Integer.MAX_VALUE && o >= Integer.MIN_VALUE) {
            saveInteger((int) o);
        }
        else {
            write(Operations.LONG1);
            write((byte)8);
            write(myEightByteBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(0, o));
        }
    }

    /**
     * Write out a string as real unicode.
     */
    protected final void saveUnicode(String o)
    {
        final byte[] b;
        b = o.getBytes(StandardCharsets.UTF_8);
        write(Operations.BINUNICODE);
        // Pickle protocol is always little-endian
        writeLittleEndianInt(b.length);
        write(b);
        put(o);
    }

    // ----------------------------------------------------------------------
    // Serializing objects intended to be unpickled as numpy arrays
    //
    // Instead of serializing array-like objects exactly the way numpy
    // arrays are serialized, we simply serialize them to be unpickled
    // as numpy arrays.  The simplest way to do this is to use
    // numpy.frombuffer().  We write out opcodes to build the
    // following stack:
    //
    //     [..., numpy.frombuffer, binary_data_string, dtype_string]
    //
    // and then call TUPLE2 and REDUCE to get:
    //
    //     [..., numpy.frombuffer(binary_data_string, dtype_string)]
    //
    // https://numpy.org/doc/stable/reference/generated/numpy.frombuffer.html

    /**
     * Save the Python function module.name.  We use this function
     * with the REDUCE opcode to build Python objects when unpickling.
     */
    protected final void saveGlobal(String module, String name)
    {
        write(Operations.GLOBAL);
        writeAscii(module);
        writeAscii("\n");
        writeAscii(name);
        writeAscii("\n");
    }

    /**
     * Save a boolean array as a numpy array.
     */
    protected final void saveNumpyBooleanArray(boolean[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader((long) n);

        for (boolean d : o) {
            write((byte) (d?1:0));
        }

        addNumpyArrayEnding(DType.Type.BOOLEAN, o);
    }

    /**
     * Save a byte array as a numpy array.
     */
    protected final void saveNumpyByteArray(byte[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader((long) n);

        for (byte d : o) {
            write(d);
        }

        addNumpyArrayEnding(DType.Type.INT8, o);
    }

    /**
     * Save a char array as a numpy array.
     */
    protected final void saveNumpyCharArray(char[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader((long) n);

        for (char c : o) {
            write(c);
        }

        addNumpyArrayEnding(DType.Type.CHAR, o);
    }

    /**
     * Save a ByteList as a numpy array.
     */
    protected final void saveNumpyByteArray(ByteList o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.size();
        writeBinStringHeader((long) n);

        for (int i=0; i < n; ++i) {
            write(o.get(i));
        }

        addNumpyArrayEnding(DType.Type.INT8, o);
    }

    /**
     * Save a short array as a numpy array.
     */
    protected final void saveNumpyShortArray(short[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader(2 * (long) n);
        myTwoByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (short d : o) {
            write(myTwoByteBuffer.putShort(0, d));
        }

        addNumpyArrayEnding(DType.Type.INT16, o);
    }

    /**
     * Save an int array as a numpy array.
     */
    protected final void saveNumpyIntArray(int[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader(4 * (long) n);
        myFourByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (int d : o) {
            write(myFourByteBuffer.putInt(0, d));
        }

        addNumpyArrayEnding(DType.Type.INT32, o);
    }

    /**
     * Save a long array as a numpy array.
     */
    protected final void saveNumpyLongArray(long[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader(8 * (long) n);
        myEightByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (long d : o) {
            write(myEightByteBuffer.putLong(0, d));
        }

        addNumpyArrayEnding(DType.Type.INT64, o);
    }

    /**
     * Save a float array as a numpy array.
     */
    protected final void saveNumpyFloatArray(float[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader(4 * (long) n);
        myFourByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (float f : o) {
            write(myFourByteBuffer.putFloat(0, f));
        }

        addNumpyArrayEnding(DType.Type.FLOAT32, o);
    }

    /**
     * Save a double array as a numpy array.
     */
    protected final void saveNumpyDoubleArray(double[] o)
    {
        saveGlobal("numpy", "frombuffer");
        final int n = o.length;
        writeBinStringHeader(8 * (long) n);
        myEightByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (double d : o) {
            write(myEightByteBuffer.putDouble(0, d));
        }

        addNumpyArrayEnding(DType.Type.FLOAT64, o);
    }

    // ----------------------------------------------------------------------------

    /**
     * Actually do the dump.
     */
    private void toPickle(Object o)
    {
        myStream.reset();

        write(Operations.PROTO);
        write((byte) 2);
        save(o);
        write(Operations.STOP);

        myMemo.clear();
    }

    /**
     * Pickle an arbitrary object.
     */
    @SuppressWarnings("unchecked")
    private void save(Object o)
        throws UnsupportedOperationException
    {
        if (o == null) {
            write(Operations.NONE);
        }
        else if (myMemo.containsKey(o)) {
            get(o);
        }
        else if (o instanceof Boolean) {
            write(((Boolean) o) ? Operations.NEWTRUE : Operations.NEWFALSE);
        }
        else if (o instanceof Float) {
            saveFloat((Float) o);
        }
        else if (o instanceof Double) {
            saveFloat((Double) o);
        }
        else if (o instanceof Byte) {
            saveInteger(((Byte) o).intValue());
        }
        else if (o instanceof Short) {
            saveInteger(((Short) o).intValue());
        }
        else if (o instanceof Integer) {
            saveInteger((Integer) o);
        }
        else if (o instanceof Long) {
            saveInteger((Long) o);
        }
        else if (o instanceof String) {
            saveUnicode((String) o);
        }
        else if (o instanceof boolean[]) {
            saveNumpyBooleanArray((boolean[]) o);
        }
        else if (o instanceof char[]) {
            saveNumpyCharArray((char[]) o);
        }
        else if (o instanceof byte[]) {
            saveNumpyByteArray((byte[]) o);
        }
        else if (o instanceof short[]) {
            saveNumpyShortArray((short[]) o);
        }
        else if (o instanceof int[]) {
            saveNumpyIntArray((int[]) o);
        }
        else if (o instanceof long[]) {
            saveNumpyLongArray((long[]) o);
        }
        else if (o instanceof float[]) {
            saveNumpyFloatArray((float[]) o);
        }
        else if (o instanceof double[]) {
            saveNumpyDoubleArray((double[]) o);
        }
        else if (o instanceof List) {
            saveList((List<Object>) o);
        }
        else if (o instanceof Map) {
            saveDict((Map<Object,Object>) o);
        }
        else if (o instanceof Collection) {
            saveCollection((Collection<Object>) o);
        }
        else if (o.getClass().isArray()) {
            saveList(Arrays.asList((Object[]) o));
        }
        else if (o instanceof DType) {
            saveDType((DType) o);
        }
        else {
            try {
                saveObject(o);
            }
            catch (UnsupportedOperationException e) {
                // Last so that we handle all specific iterable types correctly,
                // including in saveObject()'s handling
                if (o instanceof Iterable) {
                    saveIterable((Iterable<Object>) o);
                }
                else {
                    throw e;
                }
            }
        }
    }

    /**
     * Write out the header for a binary "string" of data.
     */
    private void writeBinStringHeader(long n)
    {
        if (n < 256) {
            write(Operations.SHORT_BINSTRING);
            write((byte) n);
        }
        else if (n <= Integer.MAX_VALUE) {
            write(Operations.BINSTRING);
            // Pickle protocol is always little-endian
            writeLittleEndianInt((int) n);
        }
        else {
            throw new UnsupportedOperationException("String length of " + n + " is too large");
        }
    }

    /**
     * The string for which {@code numpy.dtype(...)} returns the desired dtype.
     */
    private String dtypeDescr(final DType.Type type)
    {
        if (type == null) {
            throw new NullPointerException("Null dtype");
        }

        switch (type) {
        case BOOLEAN: return "|b1";
        case CHAR:    return "<S1";
        case INT8:    return "<i1";
        case INT16:   return "<i2";
        case INT32:   return "<i4";
        case INT64:   return "<i8";
        case FLOAT32: return "<f4";
        case FLOAT64: return "<f8";
        default: throw new IllegalArgumentException("Unhandled type: " + type);
        }
    }

    /**
     * Add the suffix of a serialized numpy array
     *
     * @param dtype type of the numpy array
     * @param o the array (or list) being serialized
     */
    private void addNumpyArrayEnding(DType.Type dtype, Object o)
    {
        final String descr = dtypeDescr(dtype);
        writeBinStringHeader(descr.length());
        writeAscii(descr);
        write(Operations.TUPLE2);
        write(Operations.REDUCE);
        put(o);
    }

    /**
     * Save a DType.
     */
    protected void saveDType(DType x)
    {
        saveGlobal("numpy", "dtype");
        saveUnicode(dtypeDescr(x.type()));
        write(Operations.TUPLE1);
        write(Operations.REDUCE);
    }

    /**
     * Save a Collection of arbitrary Objects as a tuple.
     */
    protected void saveCollection(Collection<?> x)
    {
        // Tuples over 3 elements in size need a "mark" to look back to
        if (x.size() > 3) {
            write(Operations.MARK);
        }

        // Save all the elements
        for (Object o : x) {
            save(o);
        }

        // And say what we sent
        switch (x.size()) {
        case 0:  write(Operations.EMPTY_TUPLE); break;
        case 1:  write(Operations.TUPLE1);      break;
        case 2:  write(Operations.TUPLE2);      break;
        case 3:  write(Operations.TUPLE3);      break;
        default: write(Operations.TUPLE);       break;
        }

        put(x);
    }

    /**
     * Save an Iterable of arbitrary Objects as a tuple.
     */
    protected void saveIterable(Iterable<?> x)
    {
        // We don't know how big the iterable is so we'll just treat it as a
        // tuple by dropping a mark and saving all the objects
        write(Operations.MARK);

        // Save all the elements
        for (Object o : x) {
            save(o);
        }

        // And say what we sent
        write(Operations.TUPLE);

        put(x);
    }

    /**
     * Save a list of arbitrary objects.
     */
    protected void saveList(List<Object> x)
    {
        // Two implementations here. For RandomAccess lists it's faster to do
        // explicit get methods. For other ones iteration is faster.
        if (x instanceof RandomAccess) {
            write(Operations.EMPTY_LIST);
            put(x);
            for (int i=0; i < x.size(); i++) {
                final Object first = x.get(i);
                if (++i >= x.size()) {
                    save(first);
                    write(Operations.APPEND);
                    break;
                }
                final Object second = x.get(i);
                write(MARK_V);
                save(first);
                save(second);
                int left = BATCHSIZE - 2;
                while (left > 0 && ++i < x.size()) {
                    final Object item = x.get(i);
                    save(item);
                    left -= 1;
                }
                write(Operations.APPENDS);
            }
        }
        else {
            write(Operations.EMPTY_LIST);
            put(x);
            final Iterator<Object> items = x.iterator();
            while (true) {
                if (!items.hasNext())
                    break;
                final Object first = items.next();
                if (!items.hasNext()) {
                    save(first);
                    write(Operations.APPEND);
                    break;
                }
                final Object second = items.next();
                write(MARK_V);
                save(first);
                save(second);
                int left = BATCHSIZE - 2;
                while (left > 0 && items.hasNext()) {
                    final Object item = items.next();
                    save(item);
                    left -= 1;
                }
                write(Operations.APPENDS);
            }
        }
    }

    /**
     * Save a map of arbitrary objects as a dict.
     */
    protected void saveDict(Map<Object,Object> x)
    {
        write(Operations.EMPTY_DICT);
        put(x);
        final Iterator<Entry<Object,Object>> items = x.entrySet().iterator();
        while (true) {
            if (!items.hasNext())
                break;
            final Entry<Object,Object> first = items.next();
            if (!items.hasNext()) {
                save(first.getKey());
                save(first.getValue());
                write(Operations.SETITEM);
                break;
            }
            final Entry<Object,Object> second = items.next();
            write(MARK_V);
            save(first.getKey());
            save(first.getValue());
            save(second.getKey());
            save(second.getValue());
            int left = BATCHSIZE - 2;
            while (left > 0 && items.hasNext()) {
                final Entry<Object,Object> item = items.next();
                save(item.getKey());
                save(item.getValue());
                left -= 1;
            }
            write(Operations.SETITEMS);
        }
    }
}
