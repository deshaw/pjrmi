package com.deshaw.python;

import java.util.List;

/**
 * Encapsulation of a single-element numpy dtype object.
 *
 * @see <a href="http://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html">Data type objects</a>
 */
public class DType
{
    /**
     * Element types we know about.
     */
    public enum Type
    {
        // The type numbers can be found by looking at the Python dtype.num. The
        // Character as a bytes isn't quite right, but it's close enough for now.
        /** A bool.    */ BOOLEAN("bool",    boolean.class, Boolean  .class,             1,  0),
        /** A bytes.   */ CHAR   ("bytes",   char   .class, Character.class,             1, 18),
        /** A int8.    */ INT8   ("int8",    byte   .class, Byte     .class, Byte.   BYTES,  1),
        /** A int16.   */ INT16  ("int16",   short  .class, Short    .class, Short.  BYTES,  3),
        /** A int32.   */ INT32  ("int32",   int    .class, Integer  .class, Integer.BYTES,  5),
        /** A int64.   */ INT64  ("int64",   long   .class, Long     .class, Long.   BYTES,  7),
        /** A float32. */ FLOAT32("float32", float  .class, Float    .class, Float.  BYTES, 11),
        /** A float64. */ FLOAT64("float64", double .class, Double   .class, Double. BYTES, 12);

        // name() is a enum method so we use 'dtypename' as the member name
        /** See numpy. */ public final String   dtypename;
        /** See numpy. */ public final byte     alignment;
        /** See numpy. */ public final byte     num;
        /** Primitive. */ public final Class<?> primitiveClass;
        /** Object.    */ public final Class<?> objectClass;

        /**
         * Get the Type for the given dtype name, if any.
         */
        public static Type byName(final String name)
        {
            for (Type type : values()) {
                if (type.dtypename.equals(name)) {
                    return type;
                }
            }
            return null;
        }

        /**
         * Constructor.
         *
         * @param name      The name.
         * @param alignment The memory alignment.
         * @param num       The numpy number.
         */
        private Type(String name, Class<?> pCls, Class<?> oCls, int alignment, int num)
        {
            this.dtypename      = name;
            this.alignment      = (byte)alignment;
            this.num            = (byte)num;
            this.primitiveClass = pCls;
            this.objectClass    = oCls;
        }
    }

    // ----------------------------------------------------------------------

    // Python-like values and methods

    /** Empty int[] */
    private static int[] EMPTY_INT_ARRAY = new int[0];

    /** See numpy.ndarray. */ public final byte               alignment;
    /** See numpy.ndarray. */ public final DType              base;
    /** See numpy.ndarray. */ public final char               byteorder;
    /** See numpy.ndarray. */ public final List<List<String>> descr;
    /** See numpy.ndarray. */ public final long               flags;
    /** See numpy.ndarray. */ public final Object             fields;
    /** See numpy.ndarray. */ public final boolean            hasobject;
    /** See numpy.ndarray. */ public final boolean            isalignedstruct;
    /** See numpy.ndarray. */ public final byte               isbuiltin;
    /** See numpy.ndarray. */ public final boolean            isnative;
    /** See numpy.ndarray. */ public final int                itemsize;
    /** See numpy.ndarray. */ public final char               kind;
    /** See numpy.ndarray. */ public final Object             metadata;
    /** See numpy.ndarray. */ public final String             name;
    /** See numpy.ndarray. */ public final List<String>       names;
    /** See numpy.ndarray. */ public final byte               ndim;
    /** See numpy.ndarray. */ public final byte               num;
    /** See numpy.ndarray. */ public final int[]              shape;
    /** See numpy.ndarray. */ public final String             str;
    /** See numpy.ndarray. */ public final DType              subdtype;

    /**
     * See numpy.ndarray.
     *
     * @return this dtype.
     */
    public final DType newbyteorder()
    {
        return this;
    }

    /**
     * See numpy.ndarray.
     *
     * @param s  Ignored.
     *
     * @return this dtype.
     */
    public final DType newbyteorder(String s)
    {
        return this;
    }

    // ----------------------------------------------------------------------

    /**
     * Element type char (as given).
     */
    private final char myTypeChar;

    /**
     * Element size.
     */
    private final int mySize;

    /**
     * Element type.
     */
    private final Type myType;

    /**
     * Whether the data is structured.
     */
    private boolean myIsStructured;

    /**
     * Whether the data is big- or little-endian.
     */
    private boolean myIsBigEndian;

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     *
     * @param dtype  The {@code str} value of the dtype.
     */
    public DType(final CharSequence dtype)
        throws IllegalArgumentException
    {
        // Sanitise input
        if (dtype == null) {
            throw new NullPointerException("Given a null dtype");
        }

        // Defaults
        myIsStructured = false;
        myIsBigEndian  = false;

        // Shred the type string. It possibly has an endian char, followed by a
        // type char and a size char.
        int start = 0;
        while (start < dtype.length()) {
            // Get the char and step on
            final char c = dtype.charAt(start++);
            if (c == '\'') {
                // Ignore leading 's
            }
            else if (c == '|') {
                myIsStructured = true;
            }
            else if (c == '<') {
                myIsBigEndian = false;
            }
            else if (c == '>') {
                myIsBigEndian = true;
            }
            else {
                // Step back and we're done
                start--;
                break;
            }
        }

        // Figure out the end (inclusive)
        int end = dtype.length() - 1;
        if (end > 0 && dtype.charAt(end) == '\'') {
            // Ignore trailing 's
            end--;
        }

        // The type is one char now, at the start
        myTypeChar = dtype.charAt(start);

        // The size is the rest
        int size = 0;
        for (int i = start + 1; i <= end; i++) {
            size = 10 * size + (dtype.charAt(i) - '0');
        }
        mySize = size;

        // Whether we recognise it as an integer; shorthand for a test below
        final boolean isInteger = (myTypeChar == 'b' ||
                                   myTypeChar == 'i' ||
                                   myTypeChar == 'u');

        if (myIsStructured && myTypeChar == 'b' && mySize == 1) {
            myType = Type.BOOLEAN;
        }
        else if (myTypeChar == 'S' && mySize == 1) {
            myType = Type.CHAR;
        }
        else if (isInteger && mySize == 1) {
            myType = Type.INT8;
        }
        else if (isInteger && mySize == 2) {
            myType = Type.INT16;
        }
        else if (isInteger && mySize == 4) {
            myType = Type.INT32;
        }
        else if (isInteger && mySize == 8) {
            myType = Type.INT64;
        }
        else if (myTypeChar == 'f' && mySize == 4) {
            myType = Type.FLOAT32;
        }
        else if (myTypeChar == 'f' && mySize == 8) {
            myType = Type.FLOAT64;
        }
        else {
            throw new IllegalArgumentException(
                "Unsupported dtype \"" + dtype + "\""
            );
        }

        // dtype values
        alignment       = myType.alignment;
        byteorder       ='=';
        base            = this;
        str             = dtype.toString();
        descr           = List.of(List.of("", str));
        flags           = 0;
        fields          = null;
        hasobject       = false;
        isalignedstruct = false;
        isbuiltin       = 1;
        isnative        = true;
        itemsize        = mySize;
        kind            = myTypeChar;
        metadata        = null;
        name            = myType.dtypename;
        names           = null;
        ndim            = 0;
        num             = myType.num;
        shape           = EMPTY_INT_ARRAY;
        subdtype        = null;
    }

    /**
     * Element size.
     *
     * @return the size of the type, in bytes.
     */
    public int size()
    {
        return mySize;
    }

    /**
     * Element type.
     *
     * @return the dtype's type.
     */
    public Type type()
    {
        return myType;
    }

    /**
     * Endianness.
     *
     * @return whether the type is big endian, or not.
     */
    public boolean bigEndian()
    {
        return myIsBigEndian;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();

        sb.append("dtype('");

        if (myIsStructured) {
            sb.append('|');
        }
        sb.append(myIsBigEndian ? '>' : '<');
        sb.append(myTypeChar);
        sb.append(mySize);

        sb.append("')");

        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DType)) {
            return false;
        }

        DType dtype = (DType) o;
        return myIsBigEndian  == dtype.myIsBigEndian  &&
               myIsStructured == dtype.myIsStructured &&
               myType         == dtype.myType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return 2 * myType.hashCode() +
               (myIsBigEndian  ? 1 : 0) +
               (myIsStructured ? 1 : 0);
    }

    /**
     * Set the {@code endian}ness of this type.
     *
     * @param isBigEndian  The new endianess.
     */
    protected void setEndianness(final boolean isBigEndian)
    {
        myIsBigEndian = isBigEndian;
    }
}
