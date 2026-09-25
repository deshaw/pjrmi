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

    /**
     * The pickle protocol version which we open a stream with.
     */
    private static final byte DEFAULT_PROTOCOL = 2;

    /**
     * The pickle protocol version which first has an opcode carrying a 64bit
     * length, which is needed for anything longer than
     * {@code Integer.MAX_VALUE}.
     */
    private static final byte LONG_LENGTH_PROTOCOL = 4;

    /**
     * Where the protocol version byte sits in the stream; it follows the
     * {@code PROTO} opcode which opens every pickle.
     */
    private static final long PROTOCOL_OFFSET = 1;

    /**
     * The name of the {@link #MAX_RETAINED_CAPACITY} property.
     */
    public static final String MAX_RETAINED_CAPACITY_PROPERTY =
        "com.deshaw.python.maxRetainedCapacity";

    /**
     * The largest buffer which we will keep between pickles.
     *
     * <p>Emptying the buffer holds on to whatever it grew to, so without this
     * a single huge pickle would pin its space for as long as the instance
     * lives, and instances tend to be long-lived thread-local ones.
     *
     * <p>Sized at a sixteenth of a 1GB heap, which is the smallest this is
     * normally run in. A pickling thread therefore retains at most a sixteenth
     * of such a heap, which stays far above any everyday pickle, so the common
     * case never reallocates.
     *
     * <p>Override it at startup with {@code -D} and
     * {@link #MAX_RETAINED_CAPACITY_PROPERTY}: lower it if you pickle from
     * many threads in a small heap, raise it if your pickles are routinely
     * larger and you would rather keep the space than regrow the buffer each
     * time.
     *
     * <p>Note the {@code L} suffix on the default. Without it a value of a
     * gigabyte or more would be computed in {@code int} arithmetic and wrap,
     * which for {@code 4 * 1024 * 1024 * 1024} lands on exactly zero and would
     * silently throw the buffer away on every pickle.
     *
     * <p>A value which is set but is not a positive number stops the process,
     * rather than being defaulted over. Setting this is a deliberate act, so
     * running on with a different value than was asked for would leave the
     * process misconfigured with nothing to draw attention to it.
     */
    private static final long MAX_RETAINED_CAPACITY;

    static {
        final long   dflt     = 64L * 1024 * 1024;
        final String property =
            System.getProperty(MAX_RETAINED_CAPACITY_PROPERTY);
        if (property == null) {
            MAX_RETAINED_CAPACITY = dflt;
        }
        else {
            final long value;
            try {
                value = Long.parseLong(property.trim());
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    MAX_RETAINED_CAPACITY_PROPERTY + "=\"" + property +
                    "\" is not a number",
                    e
                );
            }
            if (value <= 0) {
                throw new IllegalArgumentException(
                    MAX_RETAINED_CAPACITY_PROPERTY + "=\"" + property +
                    "\" must be positive"
                );
            }
            MAX_RETAINED_CAPACITY = value;
        }
    }

    /**
     * How large a buffer to start with, both for a new instance and for the
     * replacement of one which outgrew {@link #MAX_RETAINED_CAPACITY}. Enough
     * for an everyday pickle to be built without regrowing.
     */
    private static final long INITIAL_BUFFER_CAPACITY = 1024L * 1024;

    // ----------------------------------------------------------------------

    /**
     * We buffer up everything in here for dumping.
     *
     * <p>This is a {@link ByteList}, and not a {@link ByteArrayOutputStream},
     * so that a pickle may be larger than {@code Integer.MAX_VALUE} bytes. It
     * is replaced, rather than emptied, once it grows past
     * {@link #MAX_RETAINED_CAPACITY}.
     */
    private ByteList myBuffer = new ByteList(INITIAL_BUFFER_CAPACITY);

    /**
     * Used to provide a handle on objects which we have already stored (so that
     * we don't duplicate them in the result).
     */
    private final IdentityHashMap<Object,Integer> myMemo = new IdentityHashMap<>();

    /**
     * The protocol version which the pickle being written actually needs. This
     * starts out as the default and is raised by anything which uses a later
     * opcode; see {@link #toPickle(Object)}.
     */
    private byte myProtocol = DEFAULT_PROTOCOL;

    // Scratch space
    private final ByteBuffer myTwoByteBuffer   = ByteBuffer.allocate(2);
    private final ByteBuffer myFourByteBuffer  = ByteBuffer.allocate(4);
    private final ByteBuffer myEightByteBuffer = ByteBuffer.allocate(8);
    private final ByteList   myByteList        = new ByteList();

    // ----------------------------------------------------------------------

    /**
     * The largest buffer which an instance keeps between pickles, in bytes.
     *
     * <p>This is the resolved value of
     * {@link #MAX_RETAINED_CAPACITY_PROPERTY}.
     *
     * @return the size.
     */
    public static long getMaxRetainedCapacity()
    {
        return MAX_RETAINED_CAPACITY;
    }

    /**
     * Dump an object out to a given stream.
     *
     * <p>Unlike {@link #toByteArray(Object)} this has no size limit, since
     * nothing is ever materialised as a single {@code byte[]}.
     *
     * @param o       The object to pickle.
     * @param stream  The stream to write the pickle to.
     *
     * @throws IOException                   if writing to the stream failed.
     * @throws UnsupportedOperationException if the object could not be pickled.
     */
    public void toStream(Object o, OutputStream stream)
        throws IOException,
               UnsupportedOperationException
    {
        // Might be better to use the stream directly, rather than staging
        // locally.
        toPickle(o);
        myBuffer.writeTo(stream);
    }

    /**
     * Dump to a {@link ByteList}.
     *
     * @param o  The object to pickle.
     *
     * @return the pickled form of the given object. Do not mutate this
     *         result. Subsequent calls to pickling methods on this instance
     *         will invalidate the result.
     *
     * @throws UnsupportedOperationException if the object could not be pickled.
     */
    public ByteList toByteList(Object o)
        throws UnsupportedOperationException
    {
        toPickle(o);
        return myBuffer;
    }

    /**
     * Dump to a byte-array.
     *
     * @param o  The object to pickle.
     *
     * @return the pickled form of the given object. This is always a copy, so
     *         it stays valid after later calls on this instance; use
     *         {@link #toByteList(Object)} if you would rather not pay for the
     *         copy and can respect that method's lifetime rule.
     *
     * @throws UnsupportedOperationException if the object could not be
     *                                       pickled, or if the pickle was
     *                                       larger than a {@code byte[]} can
     *                                       hold; use
     *                                       {@link #toByteList(Object)} or
     *                                       {@link #toStream(Object,OutputStream)}
     *                                       for one that big.
     */
    public byte[] toByteArray(Object o)
        throws UnsupportedOperationException
    {
        toPickle(o);

        // This method hands back an array which outlives the next pickle, so
        // it must not be one myBuffer still holds. toArray() gives back its
        // own sub-array when the pickle fills it exactly, and a fresh copy
        // otherwise; only the first case still owes us one. Copying in both
        // would mean two passes over a large pickle to no end.
        final byte[] array = myBuffer.toArray();
        return (array.length > 0 && array.length == myBuffer.capacity())
               ? array.clone()
               : array;
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
        myBuffer.add(op.code);
    }

    /**
     * Write out a byte.
     */
    protected final void write(byte i)
    {
        myBuffer.add(i);
    }

    /**
     * Write out a char.
     */
    protected final void write(char c)
    {
        myBuffer.add((byte)c);
    }

    /**
     * Write out an int, in little-endian format.
     */
    protected final void writeLittleEndianInt(final int n)
    {
        write(myFourByteBuffer.order(ByteOrder.LITTLE_ENDIAN).putInt(0, n));
    }

    /**
     * Write out a long, in little-endian format.
     */
    protected final void writeLittleEndianLong(final long n)
    {
        write(myEightByteBuffer.order(ByteOrder.LITTLE_ENDIAN).putLong(0, n));
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
        myBuffer.append(array);
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
        final long n = o.size();
        writeBinStringHeader(n);

        // Copy it in one go. Going element by element would be billions of
        // bounds-checked calls for the sizes this now has to handle.
        myBuffer.addAll(o);

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
        // Give back the space taken by an outsized pickle, rather than holding
        // on to it for the life of this instance. The replacement starts at a
        // sensible working size, rather than at zero, so that the next pickle
        // does not have to grow from DEFAULT_INITIAL_CAPACITY a step at a time.
        if (myBuffer.capacity() > MAX_RETAINED_CAPACITY) {
            myBuffer = new ByteList(INITIAL_BUFFER_CAPACITY);
        }
        else {
            myBuffer.clear();
        }
        myProtocol = DEFAULT_PROTOCOL;

        // The memo is emptied at the end, and not up here with the rest of the
        // per-pickle state, so that a finished pickle stops holding a reference
        // to every object it saw. That makes emptying it conditional on getting
        // to the end, but that won't happen if save() throws for an object it
        // cannot pickle. If that occurs, then entries added by the current
        // pickle would still be in the memo for the next one. Those stale
        // entries will cause the next invocation to produce a pickle which only
        // fails when someone comes to read it.
        try {
            write(Operations.PROTO);
            write(DEFAULT_PROTOCOL);
            save(o);
            write(Operations.STOP);

            // We can only know which protocol version the pickle needs once it
            // has been written, since that depends on the opcodes which the
            // data turned out to require. Anything which needed a later one
            // than we opened with will have raised it, so go back and correct
            // the header. We keep declaring the oldest version we can so that
            // readers which only handle the older protocol carry on working.
            if (myProtocol != DEFAULT_PROTOCOL) {
                myBuffer.set(PROTOCOL_OFFSET, myProtocol);
            }
        }
        finally {
            myMemo.clear();
        }
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
     *
     * <p>The opcode used depends on how long the data is. Anything longer than
     * {@code Integer.MAX_VALUE} needs {@code BINBYTES8}, which arrived in
     * protocol 4, so writing one of those raises the protocol version that the
     * stream declares.
     *
     * @param n  The number of bytes which will follow the header.
     *
     * @throws IllegalArgumentException if the given length was negative.
     */
    private void writeBinStringHeader(final long n)
        throws IllegalArgumentException
    {
        if (n < 0) {
            throw new IllegalArgumentException(
                "Negative string length: " + n
            );
        }
        else if (n < 256) {
            write(Operations.SHORT_BINSTRING);
            write((byte) n);
        }
        else if (n <= Integer.MAX_VALUE) {
            write(Operations.BINSTRING);
            // Pickle protocol is always little-endian
            writeLittleEndianInt((int) n);
        }
        else {
            // Note that this yields a bytes object on the Python side, where
            // BINSTRING yields whatever the unpickler's encoding says. Both
            // give bytes for an unpickler using encoding='bytes', which is
            // what reading these back needs; see _handle_pickle_bytes() and
            // _read_argument() in python/pjrmi/__init__.py, which must keep
            // passing that.
            myProtocol = LONG_LENGTH_PROTOCOL;
            write(Operations.BINBYTES8);
            // Pickle protocol is always little-endian
            writeLittleEndianLong(n);
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
