package com.deshaw.python;

import com.deshaw.util.StringUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.math.BigInteger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unpickler for Python binary pickle files.
 *
 * <p>This unpickler will probably require more work to handle general pickle
 * files. In particular, only operations necessary for decoding tuples, lists,
 * dictionaries, and numeric numpy arrays encoded using protocol version 2 are
 * supported.
 *
 * <p>Things that won't work:
 * <ul>
 *   <li>Some protocol 0 opcodes.
 *   <li>Numpy arrays of types other than {@code int1}, ..., {@code int64},
 *       {@code float32}, and {@code float64}. That includes string arrays,
 *       recarrays, etc.
 *   <li>Generic Python objects. You can, however, use
 *       {@link #registerGlobal(String, String, Global)} to add support for
 *       specific types, which is how dtypes and numpy arrays are implemented.
 * </ul>
 *
 * <p>Signedness of numpy integers is ignored.
 */
public class PythonUnpickle
{
    /**
     * {@link ArrayList} with a bulk removal operation made public.
     */
    private static class ShrinkableList<T>
        extends ArrayList<T>
    {
        /**
         * Constructs an empty list.
         */
        public ShrinkableList()
        {
            super();
        }

        /**
         * Constructs an empty list with a specified initial capacity.
         */
        public ShrinkableList(int initialCapacity)
        {
            super(initialCapacity);
        }

        /**
         * Constructs a list containing the elements of the specified
         * collection, in the order they are returned by the collection's
         * iterator.
         */
        public ShrinkableList(Collection<? extends T> c)
        {
            super(c);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void removeRange(int fromIndex, int toIndex)
        {
            super.removeRange(fromIndex, toIndex);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Instance of an object that may be initialized by the
     * {@code BUILD} opcode.
     */
    private static interface Instance
    {
        /**
         * Python's {@code __setstate__} method. Typically, the
         * {@code state} parameter will contain a tuple (unmodifiable
         * list) with object-specific contents.
         */
        public void setState(Object state)
            throws MalformedPickleException;
    }

    /**
     * Callable global object recognized by the pickle framework.
     */
    private static interface Global
    {
        public Object call(Object c)
            throws MalformedPickleException;
    }

    /**
     * Factory class for use by the pickle framework to reconstruct
     * proper numpy arrays.
     */
    private static class NumpyCoreMultiarrayReconstruct
        implements Global
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(final Object c)
            throws MalformedPickleException
        {
            // args is a 3-tuple of arguments to pass to this constructor
            // function (type(self), (0,), self.dtypechar).
            return new UnpickleableNumpyArray();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "numpy.core.multiarray._reconstruct()";
        }
    }

    /**
     * Factory class for use by the pickle framework to reconstruct
     * numpy arrays from byte buffers.
     */
    private static class NumpyFrombuffer
        implements Global
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(final Object c)
            throws MalformedPickleException
        {
            // Args are a tuple of (data, dtype)
            try {
                return new NumpyFrombufferArray((List)c);
            }
            catch (ClassCastException e) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to numpy.core.multiarray scalar: " +
                        "expecting 2-tuple (dtype, data), got " + c
                );
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "numpy.frombuffer()";
        }
    }

    /**
     * Factory class for use by the pickle framework to reconstruct
     * numpy scalar arrays. Python treats these as scalars so we match
     * these semantics in Java.
     */
    private static class NumpyCoreMultiarrayScalar
        implements Global
    {
        /**
         * Shape to pass to {@code NumpyArray} constructor.
         */
        private static final int[] SCALAR_ARRAY_SHAPE = { 1 };

        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(final Object c)
            throws MalformedPickleException
        {
            // Parse and return a scalar
            final List tuple = (List) c;
            if (tuple.size() != 2) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to numpy.core.multiarray scalar: " +
                        "expecting 2-tuple (dtype, data), got " + c
                );
            }

            // Use NumpyArray to do the actual parsing
            final DType dtype = (DType) tuple.get(0);
            final BinString rawData = (BinString) tuple.get(1);
            final NumpyArray dummyArray =
                new NumpyArray(dtype, false, SCALAR_ARRAY_SHAPE, rawData.data());

            // Always reconstruct scalars as either longs or doubles
            switch (dtype.type()) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return dummyArray.getLong(0);

            case FLOAT32:
            case FLOAT64:
                return dummyArray.getDouble(0);

            default:
                throw new MalformedPickleException("Can't handle " + dtype);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "numpy.core.multiarray.scalar()";
        }
    }

    /**
     * Type marker.
     */
    private static class NDArrayType
        implements Global
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(final Object c)
            throws MalformedPickleException
        {
            throw new UnsupportedOperationException("NDArrayType is not callable");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "numpy.core.multiarray.ndarray";
        }
    }

    /**
     * Factory to register with the pickle framework.
     */
    private static class DTypeFactory
        implements Global
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(Object c)
            throws MalformedPickleException
        {
            if (!(c instanceof List)) {
                throw new MalformedPickleException(
                    "Argument was not a List: " + c
                );
            }

            final List t = (List) c;
            final String dtype = String.valueOf(t.get(0));
            return new UnpickleableDType(dtype);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "numpy.dtype";
        }
    }

    /**
     * Factory to register with the pickle framework.
     */
    private static class Encoder
        implements Global
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(Object c)
            throws MalformedPickleException
        {
            if (!(c instanceof List)) {
                throw new MalformedPickleException(
                    "Argument was not a List: " +
                    (c != null ?
                        "type = " + c.getClass().getName() + ", value = " :
                        "") +
                    c
                );
            }

            final List t = (List) c;
            if (t.size() != 2) {
                throw new MalformedPickleException(
                    "Expected 2 arguments to encode, but got " +
                    t.size() + ": " + t
                );
            }

            final String encodingName = String.valueOf(t.get(1));
            if (!encodingName.equals("latin1")) {
                throw new MalformedPickleException(
                    "Unsupported encoding.  Expected 'latin1', but got '" +
                    encodingName + "'"
                );
            }

            // We're being handed a string where each character corresponds to
            // one byte and the value of the byte is the code point of the
            // character. The code point at each location was the value of the
            // byte in the raw data, so we know it can fit in a byte and we
            // assert this to be true.
            final String s = String.valueOf(t.get(0));
            final byte[] bytes = new byte[s.length()];
            for (int i = 0; i < s.length(); i++) {
                int codePoint = s.codePointAt(i);
                if (codePoint < 0 || codePoint >= 256) {
                    throw new MalformedPickleException(
                        "Invalid byte data passed to " +
                        "_codecs.encode: " + codePoint +
                        " is outside range [0,255]."
                    );
                }
                bytes[i] = (byte) codePoint;
            }

            return new BinString(ByteBuffer.wrap(bytes));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "_codecs.encode";
        }
    }

    /**
     * Factory to register with the pickle framework.
     */
    private static class BytesPlaceholder
        implements Global
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public Object call(Object c)
            throws MalformedPickleException
        {
            if (!(c instanceof List)) {
                throw new MalformedPickleException(
                    "Argument was not a List: " +
                    (c != null ?
                        "type = " + c.getClass().getName() + ", value = " :
                        "") +
                    c
                );
            }

            List t = (List) c;
            if (t.size() != 0) {
                throw new MalformedPickleException(
                    "Expected 0 arguments to bytes, but got " +
                    t.size() + ": " + t
                );
            }

            // Return a zero-byte BinString corresponding to this
            // empty indicator of bytes.
            return new BinString(ByteBuffer.allocate(0));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "__builtin__.bytes";
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * A version of the NumpyArray for unpickling.
     */
    private static class UnpickleableNumpyArray
        extends NumpyArray
        implements Instance
    {
        /**
         * Constructor
         */
        public UnpickleableNumpyArray()
        {
            super();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setState(Object args)
            throws MalformedPickleException
        {
            List tuple = (List) args;
            if (tuple.size() == 5) {
                int version = ((Number) tuple.get(0)).intValue();
                if (version < 0 || version > 1) {
                    throw new MalformedPickleException(
                        "Unsupported numpy array pickle version " + version
                    );
                }
                tuple = tuple.subList(1, tuple.size());
            }

            if (tuple.size() != 4) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to ndarray.__setstate__: " +
                    "expecting 4-tuple (shape, dtype, isFortran, data), got " +
                    render(tuple)
                );
            }

            try {
                // Tuple arguments
                final List shape = (List) tuple.get(0);
                final int[] shapeArray = new int[shape.size()];
                for (int i = 0; i < shape.size(); i++) {
                    long size = ((Number) shape.get(i)).longValue();
                    if (size < 0 || size > Integer.MAX_VALUE) {
                        throw new MalformedPickleException(
                            "Bad array size, " + size + ", in " + render(tuple)
                        );
                    }
                    shapeArray[i] = (int)size;
                }

                final DType dtype = (DType) tuple.get(1);

                final boolean isFortran =
                    (tuple.get(2) instanceof Number)
                        ? (((Number)tuple.get(2)).intValue() != 0)
                        : (Boolean) tuple.get(2);

                final ByteBuffer data;
                if (tuple.get(3) instanceof BinString) {
                    data = ((BinString) tuple.get(3)).data();
                }
                else {
                    data = ByteBuffer.wrap(((String)tuple.get(3)).getBytes());
                }

                initArray(dtype, isFortran, shapeArray, null, data);
            }
            catch (ClassCastException e) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to ndarray.__setstate__: " +
                    "expecting (shape, dtype, isFortran, data), got " +
                    render(tuple),
                    e
                );
            }
            catch (NullPointerException e) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to ndarray.__setstate__: " +
                    "nulls not allowed in (shape, dtype, isFortran, data), got " +
                    render(tuple),
                    e
                );
            }
        }
    }

    /**
     * Unpickle a basic numpy array with the frombuffer method.
     */
    private static class NumpyFrombufferArray
        extends NumpyArray
    {
        /**
         * Constructor
         */
        public NumpyFrombufferArray(List tuple)
            throws MalformedPickleException
        {
            if (tuple.size() != 2) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to numpy.frombuffer: " +
                    "expecting 2-tuple (data, dtype), got " + tuple
                );
            }

            try {
                // Tuple arguments
                final DType     dtype   = new DType((BinString)tuple.get(1));
                final BinString rawData = (BinString) tuple.get(0);
                final int[]     shape   = { rawData.length() / dtype.size() };
                initArray(dtype, false, shape, null, rawData.data());
            }
            catch (ClassCastException e) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to numpy.frombuffer: " +
                    "expecting (data, dtype), got " + tuple,
                    e
                );
            }
            catch (NullPointerException e) {
                throw new MalformedPickleException(
                    "Invalid arguments passed to numpy.frombuffer: " +
                    "nulls not allowed in (data, dtype), got " + tuple
                );
            }
        }
    }

    /**
     * A version of the DType which supports unpickling.
     */
    private static class UnpickleableDType
        extends    DType
        implements Instance
    {
        /**
         * Constructor.
         */
        public UnpickleableDType(String dtype)
            throws IllegalArgumentException
        {
            super(dtype);
        }

        /**
         * Unpickling support.
         */
        @Override
        public void setState(Object state)
        {
            // The __reduce__() method returns a 3-tuple consisting of (callable object,
            // args, state), where the callable object is numpy.core.multiarray.dtype and
            // args is (typestring, 0, 1) unless the data-type inherits from void (or
            // is user-defined) in which case args is (typeobj, 0, 1).
            // The state is an 8-tuple with (version, endian, self.subdtype, self.names,
            // self.fields, self.itemsize, self.alignment, self.flags).
            // The self.itemsize and self.alignment entries are both -1 if the data-type
            // object is built-in and not flexible (because they are fixed on creation).
            // The setstate method takes the saved state and updates the data-type.
            String endianness = String.valueOf(((List) state).get(1));
            setEndianness(endianness.equals(">"));
        }
    }

    // ----------------------------------------------------------------------

    /**
     * Global objects that are going to be recognized by the
     * unpickler by their module and name. New global objects
     * may be added by {@link #registerGlobal(String, String, Global)}.
     */
    private static final Map<String,Map<String,Global>> GLOBALS = new HashMap<>();
    static
    {
        // Breaking the 80col convention for readability
        registerGlobal("numpy.core.multiarray", "_reconstruct", new NumpyCoreMultiarrayReconstruct());
        registerGlobal("numpy.core.multiarray", "scalar",       new NumpyCoreMultiarrayScalar());
        registerGlobal("numpy",                 "ndarray",      new NDArrayType());
        registerGlobal("numpy",                 "dtype",        new DTypeFactory());
        registerGlobal("numpy",                 "frombuffer",   new NumpyFrombuffer());
        registerGlobal("_codecs",               "encode",       new Encoder());
        registerGlobal("__builtin__",           "bytes",        new BytesPlaceholder());
    }

    // ----------------------------------------------------------------------

    /**
     * Unique marker object.
     */
    private static final Object MARK =
        new Object() {
            @Override public String toString() {
                return "<MARK>";
            }
        };

    /**
     * Stream where we read pickle data from.
     */
    private final InputStream myFp;

    /**
     * Object stack.
     */
    private final ShrinkableList<Object> myStack = new ShrinkableList<>();

    /**
     * Memo (objects indexed by integers).
     */
    private final Map<Integer,Object> myMemo = new HashMap<>();

    // ----------------------------------------------------------------------

    /**
     * Helper method to unpickle a single object from an array of raw bytes.
     *
     * @param bytes  The byte array to load from.
     *
     * @return the object.
     *
     * @throws MalformedPickleException if the byte array could not be decoded.
     * @throws IOException if the byte array could not be read.
     */
    public static Object loadPickle(final byte[] bytes)
        throws MalformedPickleException,
               IOException
    {
        return loadPickle(new ByteArrayInputStream(bytes));
    }

    /**
     * Helper method to unpickle a single object from a stream.
     *
     * @param fp  The stream to load from.
     *
     * @return the object.
     *
     * @throws MalformedPickleException if the stream could not be decoded.
     * @throws IOException if the stream could not be read.
     */
    public static Object loadPickle(final InputStream fp)
        throws MalformedPickleException,
               IOException
    {
        // We use a buffered input stream because gzip'd streams tend to
        // interact badly with loading owing to the way in which they are read
        // in. This seems to be especially pathological for Python3 pickled
        // data.
        return (fp instanceof BufferedInputStream)
            ? new PythonUnpickle(fp).loadPickle()
            : loadPickle(new BufferedInputStream(fp));
    }

    /**
     * Register a global name to be recognized by the unpickler.
     */
    private static void registerGlobal(String module, String name, Global f)
    {
        GLOBALS.computeIfAbsent(
            module,
            k -> new HashMap<>()
        ).put(name, f);
    }

    /**
     * Unwind a collection as a String, with special handling of CharSequences
     * (since they might be Pythonic data).
     */
    private static String render(final Collection<?> collection)
    {
        // What we'll build up with
        final StringBuilder sb = new StringBuilder();
        sb.append('[');

        // Print all the elements, with some special handling
        boolean first = true;
        for (Object element : collection) {
            // Separator?
            if (!first) {
                sb.append(", ");
            }
            else {
                first = false;
            }

            // What we'll render
            String value = String.valueOf(element);

            // Handle strings specially
            if (element instanceof CharSequence) {
                sb.append('"');

                // Handle the fact that strings might be data
                boolean truncated = false;
                if (value.length() > 1000) {
                    value = value.substring(0, 1000);
                    truncated = true;
                }
                for (int j=0; j < value.length(); j++) {
                    char c = value.charAt(j);
                    if (' ' <= c && c <= '~') {
                        sb.append(c);
                    }
                    else {
                        sb.append('\\').append("0x");
                        StringUtil.appendHexByte(sb, (byte)c);
                    }
                }

                if (truncated) {
                    sb.append("...");
                }
                sb.append('"');
            }
            else if (element instanceof Collection) {
                sb.append(render((Collection<?>)element));
            }
            else {
                sb.append(value);
            }
        }

        sb.append(']');

        // And give it back
        return sb.toString();
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     */
    public PythonUnpickle(InputStream fp)
    {
        myFp = fp;
    }

    /**
     * Unpickle an object from the stream.
     */
    @SuppressWarnings({ "unchecked" })
    public Object loadPickle()
        throws IOException,
               MalformedPickleException
    {
        while (true) {
            byte code = (byte)read();
            try {
                Operations op = Operations.valueOf(code);
                switch (op) {
                case STOP:
                    if (myStack.size() != 1) {
                        if (myStack.isEmpty()) {
                            throw new MalformedPickleException(
                                "No objects on the stack when STOP is encountered"
                            );
                        }
                        else {
                            throw new MalformedPickleException(
                                "More than one object on the stack " +
                                "when STOP is encountered: " + myStack.size()
                            );
                        }
                    }
                    return pop();

                case GLOBAL:
                    String module = readline();
                    String name = readline();
                    Global f = GLOBALS.getOrDefault(module, Collections.emptyMap())
                                      .get(name);
                    if (f == null) {
                        throw new MalformedPickleException(
                            "Global " + module + "." + name + " is not supported"
                        );
                    }
                    myStack.add(f);
                    break;

                // Memo and mark operations

                case PUT: {
                    String repr = readline();
                    try {
                        myMemo.put(Integer.parseInt(repr), peek());
                    }
                    catch (NumberFormatException e) {
                        throw new MalformedPickleException(
                            "Could not parse int \"" + repr + "\" for PUT",
                            e
                        );
                    }
                    break;
                }

                case BINPUT:
                    myMemo.put(read(), peek());
                    break;

                case LONG_BINPUT:
                    myMemo.put(readInt32(), peek());
                    break;

                case GET: {
                    String repr = readline();
                    try {
                        memoGet(Integer.parseInt(repr));
                    }
                    catch (NumberFormatException e) {
                        throw new MalformedPickleException(
                            "Could not parse int \"" + repr + "\" for GET",
                            e
                        );
                    }
                    break;
                }

                case BINGET:
                    memoGet(read());
                    break;

                case LONG_BINGET:
                    // Untested
                    memoGet(readInt32());
                    break;

                case MARK:
                    myStack.add(MARK);
                    break;

                // Integers

                case INT:
                    myStack.add(Long.parseLong(readline()));
                    break;

                case LONG1: {
                    int c = (int)(read() & 0xff);
                    if (c != 8) {
                        throw new MalformedPickleException(
                            "Unsupported LONG1 size " + c
                        );
                    }
                    long a = ((long) readInt32() & 0xffffffffL);
                    long b = ((long) readInt32() & 0xffffffffL);
                    myStack.add(a + (b << 32));
                }   break;

                case BININT:
                    myStack.add(readInt32());
                    break;

                case BININT1:
                    myStack.add(read());
                    break;

                case BININT2:
                    myStack.add(read() + 256 * read());
                    break;

                // Dicts

                case EMPTY_DICT:
                    myStack.add(new Dict());
                    break;

                case DICT: {
                    int k = marker();
                    Map dict = new Dict();
                    for (int idx = k + 1; idx < myStack.size(); idx += 2) {
                        dict.put(keyType(myStack.get(idx)), myStack.get(idx + 1));
                    }
                    myStack.removeRange(k, myStack.size());
                    myStack.add(dict);
                    break;
                }

                case SETITEM: {
                    // Untested
                    Object v = pop();
                    Object k = pop();
                    Object top = peek();
                    if (!(top instanceof Dict)) {
                        throw new MalformedPickleException(
                            "Not a dict on top of the stack in SETITEM: " + top
                        );
                    }
                    ((Dict) top).put(keyType(k), v);
                    break;
                }

                case SETITEMS: {
                    int k = marker();
                    if (k < 1) {
                        throw new MalformedPickleException(
                            "No dict to add to in SETITEMS"
                        );
                    }

                    Object top = myStack.get(k - 1);
                    if (!(top instanceof Dict)) {
                        throw new MalformedPickleException(
                            "Not a dict on top of the stack in SETITEMS: " + top
                        );
                    }

                    Dict dict = (Dict) top;
                    for (int i = k + 1; i < myStack.size(); i += 2) {
                        dict.put(keyType(myStack.get(i)), myStack.get(i + 1));
                    }
                    myStack.removeRange(k, myStack.size());
                    break;
                }

                // Tuples

                case TUPLE: {
                    int k = marker();
                    List<Object> tuple = new ArrayList<>(
                        myStack.subList(k + 1, myStack.size())
                    );
                    myStack.removeRange(k, myStack.size());
                    myStack.add(Collections.unmodifiableList(tuple));
                    break;
                }

                case EMPTY_TUPLE:
                    myStack.add(Collections.emptyList());
                    break;

                case TUPLE1:
                    myStack.add(Collections.singletonList(pop()));
                    break;

                case TUPLE2: {
                    Object i2 = pop();
                    Object i1 = pop();
                    myStack.add(Arrays.asList(i1, i2));
                    break;
                }

                case TUPLE3: {
                    Object i3 = pop();
                    Object i2 = pop();
                    Object i1 = pop();
                    myStack.add(Arrays.asList(i1, i2, i3));
                    break;
                }

                // Lists

                case EMPTY_LIST:
                    myStack.add(new ArrayList<>());
                    break;

                case LIST: {
                    int k = marker();
                    List<Object> list = new ArrayList<>(
                        myStack.subList(k + 1, myStack.size())
                    );
                    myStack.removeRange(k, myStack.size());
                    myStack.add(list);
                    break;
                }

                case APPEND: {
                    Object v = pop();
                    Object top = peek();
                    if (!(top instanceof List)) {
                        throw new MalformedPickleException(
                            "Not a list on top of the stack in APPEND: " + top
                        );
                    }
                    ((List) top).add(v);
                    break;
                }

                case APPENDS: {
                    int k = marker();
                    if (k < 1) {
                        throw new MalformedPickleException(
                            "No list to add to in APPENDS"
                        );
                    }

                    Object top = myStack.get(k - 1);
                    if (!(top instanceof List)) {
                        throw new MalformedPickleException(
                            "Not a list on top of the stack in APPENDS: " + top
                        );
                    }
                    List list = (List) top;
                    for (int i = k + 1; i < myStack.size(); i++) {
                        list.add(myStack.get(i));
                    }
                    myStack.removeRange(k, myStack.size());
                    break;
                }

                // Strings

                case STRING:
                    myStack.add(readline());
                    break;

                case BINSTRING:
                    myStack.add(new BinString(readBytes(readInt32())));
                    break;

                case SHORT_BINSTRING:
                    myStack.add(new BinString(readBytes(read())));
                    break;

                case BINUNICODE: {
                    int length = readInt32();
                    final byte[] b = new byte[length];
                    for (int i=0; i < b.length; i++) {
                        b[i] = (byte)read();
                    }
                    myStack.add(new String(b, StandardCharsets.UTF_8));
                    break;
                }

                // Objects

                case REDUCE:
                    Object args = pop();
                    Object func = pop();
                    if (!(func instanceof Global)) {
                        throw new MalformedPickleException(
                            "Argument " +
                            ((func == null) ? "<null>"
                                            : "of type " + func.getClass()) +
                            " to REDUCE is not a function"
                        );
                    }
                    myStack.add(((Global) func).call(args));
                    break;

                case BUILD:
                    Object state = pop();
                    Object inst = peek();
                    if (!(inst instanceof Instance)) {
                        throw new MalformedPickleException(
                            "Argument " +
                            ((inst == null) ? "<null>"
                                            : "of type " + inst.getClass()) +
                            " to BUILD is not an instance"
                        );
                    }
                    ((Instance) inst).setState(state);
                    break;

                case NONE:
                    myStack.add(null);
                    break;

                case NEWTRUE:
                    myStack.add(true);
                    break;

                case NEWFALSE:
                    myStack.add(false);
                    break;

                case PROTO:
                    int version = read();
                    if (version < 0 || version > 2) {
                        throw new MalformedPickleException(
                            "Unsupported pickle version " + version
                        );
                    }
                    break;

                case POP:
                    pop();
                    break;

                case POP_MARK: {
                    int k = marker();
                    myStack.removeRange(k, myStack.size());
                    break;
                }

                case DUP:
                    myStack.add(peek());
                    break;

                case FLOAT:
                    myStack.add(Float.parseFloat(readline()));
                    break;

                case BINFLOAT: {
                    long a = ((long) readInt32() & 0xffffffffL);
                    long b = ((long) readInt32() & 0xffffffffL);
                    long bits = Long.reverseBytes(a + (b << 32));
                    myStack.add(Double.longBitsToDouble(bits));
                    break;
                }

                case LONG:
                    myStack.add(new BigInteger(readline()));
                    break;

                default:
                    throw new MalformedPickleException(
                        "Unsupported operation " + Operations.valueOf(code)
                    );
                }
            }
            catch (NumberFormatException e) {
                throw new MalformedPickleException(
                    "Malformed number while handling opcode " +
                    Operations.valueOf(code),
                    e
                );
            }
            catch (IllegalArgumentException e) {
                throw new MalformedPickleException(
                    "Could not handle opcode " + (int)code
                );
            }
            catch (ClassCastException e) {
                throw new MalformedPickleException(
                    "Elements on the stack are unsuitable to opcode " +
                    Operations.valueOf(code),
                    e
                );
            }
        }
    }

    /**
     * Convert {@code BinString} objects to strings for dictionary keys.
     */
    private Object keyType(Object o)
    {
        return (o instanceof BinString) ? String.valueOf(o) : o;
    }

    // Implementation

    /**
     * Return the index of the marker on the stack.
     */
    private int marker() throws MalformedPickleException
    {
        for (int i = myStack.size(); i-- > 0;) {
            if (myStack.get(i) == MARK) {
                return i;
            }
        }
        throw new MalformedPickleException("No MARK on the stack");
    }

    /**
     * Retrieve a memo object by its key.
     */
    private void memoGet(int key) throws MalformedPickleException
    {
        if (!myMemo.containsKey(key)) {
            throw new MalformedPickleException(
                "GET key " + key + " missing from the memo"
            );
        }
        myStack.add(myMemo.get(key));
    }

    /**
     * Read a single byte from the stream.
     */
    private int read()
        throws IOException
    {
        int c = myFp.read();
        if (c == -1) {
            throw new EOFException();
        }
        return c;
    }

    /**
     * Read a 32-bit integer from the stream.
     */
    private int readInt32()
        throws IOException
    {
        return read() + 256 * read() + 65536 * read() + 16777216 * read();
    }

    /**
     * Read a given number of bytes from the stream and return
     * a byte buffer.
     */
    private ByteBuffer readBytes(int length)
        throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate(length);
        for (int read = 0; read < length;) {
            int bytesRead = myFp.read(buf.array(), read, length - read);
            if (bytesRead == -1) {
                throw new EOFException();
            }
            read += bytesRead;
        }
        buf.limit(length);
        return buf;
    }

    /**
     * Read a newline ({@code\n})-terminated line from the stream. Does not do
     * any additional parsing.
     */
    private String readline()
        throws IOException
    {
        int c;
        final StringBuilder sb = new StringBuilder(1024 * 1024); // might be big!
        while ((c = read()) != '\n') {
            sb.append((char) c);
        }
        return sb.toString();
    }

    /**
     * Returns the top element on the stack.
     */
    private Object peek()
        throws MalformedPickleException
    {
        if (myStack.isEmpty()) {
            throw new MalformedPickleException(
                "No objects on the stack during peek()"
            );
        }
        return myStack.get(myStack.size() - 1);
    }

    /**
     * Pop the top element from the stack.
     */
    private Object pop()
        throws MalformedPickleException
    {
        if (myStack.isEmpty()) {
            throw new MalformedPickleException(
                "No objects on the stack during pop()"
            );
        }
        return myStack.remove(myStack.size() - 1);
    }
}
