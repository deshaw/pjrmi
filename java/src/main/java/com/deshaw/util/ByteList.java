package com.deshaw.util;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Arrays;

/**
 * A resizable array implementation of a list of {@code byte}s.
 *
 * <p>The capacity of this list implementation goes beyond the common
 * {@link Integer#MAX_VALUE} limit, making memory the constraining factor.
 *
 * <p>This can be treated as a {@link CharSequence} for ASCII and Latin1
 * (i.e. byte-based) character sets. It won't work for UTF as-is. The
 * {@link CharSequence} methods are inherently limited to
 * {@code Integer.MAX_VALUE} elements: {@link #length()} throws an
 * {@link UnsupportedOperationException} for a list longer than that, and
 * {@link #charAt(int)} cannot be asked about anything beyond it, since it
 * takes an {@code int}. {@link #toString()} is the exception: it
 * truncates rather than throwing, since it is called implicitly from places
 * which cannot deal with a failure.
 */
public class ByteList
    implements CharSequence
{
    // ----------------------------------------------------------------------

    /**
     * Used when the initial capacity isn't specified in the constructor.
     * Makes it public so people know the default value.
     *
     * <p>It's important for this value to be greater than zero for
     * some implementations to work properly.
     */
    public static final int DEFAULT_INITIAL_CAPACITY = 10;

    /**
     * The number of low bits of an index which give the offset of an element
     * within its sub-array. The remaining bits give which sub-array it is in.
     *
     * <p>See {@code VeryLongArray} in this package, which chunks its storage
     * the same way; the two should be changed together if the scheme changes.
     */
    private static final int SUB_ARRAY_SHIFT = 30;

    /**
     * The length of each sub-array. Should be 2^30.
     *
     * <p>Package-private so that the tests ({@code ByteListTest} and
     * {@code ByteListStressMain}) can place their probes exactly on the seam;
     * nothing outside this package should care where it falls.
     */
    /*package*/ static final long SUB_ARRAY_SIZE = (1L << SUB_ARRAY_SHIFT);

    /**
     * The mask which turns an index into an offset within its sub-array.
     * Should be 2^30-1.
     */
    private static final long SUB_ARRAY_MASK = SUB_ARRAY_SIZE - 1;

    /**
     * The largest capacity which this list can be given. This is a structural
     * limit, being the point at which the number of sub-arrays no longer fits
     * into an {@code int}; allocation will fail long before it is reached.
     */
    private static final long MAX_CAPACITY = SUB_ARRAY_SIZE * Integer.MAX_VALUE;

    /**
     * The largest {@code byte[]} which we will attempt to allocate. Some VMs
     * reserve a few header words and so refuse anything larger than this.
     */
    private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

    /**
     * The most elements which {@link #toString()} will render before it
     * truncates and reports a count of what it left out.
     *
     * <p>Sized so that a truncated rendering is still large enough to identify
     * what the list holds, while staying small enough to drop into a log line
     * or a debugger without materialising the whole list as a {@link String}.
     */
    private static final int MAX_RENDERED_ELEMENTS = 1024;

    /**
     * An empty {@code byte[]}.
     */
    private static final byte[] EMPTY_ARRAY = new byte[0];

    /**
     * An empty {@code byte[][]}.
     */
    private static final byte[][] EMPTY_ARRAYS = new byte[0][];

    /**
     * The sub-arrays into which the elements of this list are stored. Every
     * sub-array is exactly {@code SUB_ARRAY_SIZE} elements long, save for the
     * last one, which may be shorter (but is never empty). The index
     * arithmetic depends on that being true.
     */
    private byte[][] myData;

    /**
     * The first sub-array, or an empty array if there are none yet. Caching it
     * means that a list which fits inside a single sub-array, which is the
     * common case, costs one dereference to read from.
     */
    private byte[] myData0;

    /**
     * The total length of all the sub-arrays. Kept as a field, rather than
     * recomputed from {@link #myData}, because it is tested on every append;
     * walking to the last sub-array to measure it costs several dependent
     * loads. Only ever assigned where {@link #myData} is.
     */
    private long myCapacity;

    /**
     * The size of the list (the number of elements it contains).
     */
    private long mySize;

    /**
     * Cache of the toString() result. This is to save repeated copies of the
     * ByteList's contents.
     */
    private String myToString;

    // ----------------------------------------------------------------------

    /**
     * Constructs an empty list with zero initial capacity (but which will jump
     * to the default initial capacity when something gets added).
     *
     * @see #DEFAULT_INITIAL_CAPACITY
     */
    public ByteList()
    {
        myData     = EMPTY_ARRAYS;
        myData0    = EMPTY_ARRAY;
        myCapacity = 0;
        mySize     = 0;
        myToString = null;
    }

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param  initialCapacity the initial capacity of the list
     *
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative, or larger than this list
     *                                  can ever hold.
     */
    public ByteList(final long initialCapacity)
        throws IllegalArgumentException
    {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal capacity: " +
                                               initialCapacity);
        }
        if (initialCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException(
                "A capacity of " + initialCapacity + " is more than the " +
                "maximum of " + MAX_CAPACITY + " which this list can hold"
            );
        }

        // Allocate exactly what was asked for. Going via ensureCapacity() would
        // round a small request up to the default initial capacity, which is
        // the right thing when growing but not when the caller has said what
        // they want.
        if (initialCapacity == 0) {
            myData  = EMPTY_ARRAYS;
            myData0 = EMPTY_ARRAY;
        }
        else {
            final int numArrays = numArraysFor(initialCapacity);
            myData = new byte[numArrays][];
            for (int i=0; i < numArrays; i++) {
                myData[i] =
                    new byte[(int)arrayLength(i, numArrays, initialCapacity)];
            }
            myData0 = myData[0];
        }

        myCapacity = initialCapacity;
        mySize     = 0;
        myToString = null;

        checkCapacity();
    }

    /**
     * Increases the capacity of this instance, if necessary, to ensure that it
     * can hold at least the number of elements specified by the minimum
     * capacity argument. Does nothing if the current capacity is already
     * greater than or equal to the minimum capacity argument.
     *
     * <p>The instance may grow as a result.
     *
     * <p>Note this should be the only place where the sub-arrays are grown.
     * Aside from the constructor, which allocates exactly what it was asked
     * for, other methods shouldn't create or grow them directly and should
     * invoke this method to do it for them.
     *
     * @param  minCapacity the desired minimum capacity
     *
     * @throws IllegalArgumentException if the specified minimal capacity is
     *                                  negative, or is more than this list can
     *                                  ever hold.
     */
    public void ensureCapacity(final long minCapacity)
        throws IllegalArgumentException
    {
        if (minCapacity < 0) {
            throw new IllegalArgumentException(
                "Negative minCapacity is not allowed: " + minCapacity
            );
        }
        if (minCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException(
                "A capacity of " + minCapacity + " is more than the maximum " +
                "of " + MAX_CAPACITY + " which this list can hold"
            );
        }

        // Check if nothing to do.
        if (minCapacity <= myCapacity) {
            return;
        }

        // How many sub-arrays it takes to hold minCapacity elements. Since
        // minCapacity is strictly positive by this point, this is always at
        // least one.
        final int numArrays = numArraysFor(minCapacity);

        // Make room for all of them in one go. Extending by a sub-array at a
        // time would be quadratic in their number.
        //
        // When a sub-array has to be added this works on a local, and only
        // publishes it once everything has been allocated. Allocating can fail,
        // with an OutOfMemoryError, and assigning to myData first would leave
        // this list holding null sub-arrays, and a myData0 which no longer
        // matched myData, for the rest of its life. Since a list like this one
        // is likely to be reused rather than discarded, that would turn one
        // failed allocation into a permanently broken instance.
        //
        // When the count is unchanged there is nothing to publish early: only
        // the last sub-array can need growing, since every earlier one is
        // already full length, so a failure there leaves myData as it was.
        final byte[][] data = (numArrays > myData.length)
            ? Arrays.copyOf(myData, numArrays)
            : myData;

        // Now create or grow each sub-array. All but the last one have to be
        // full length since that's what the index arithmetic assumes.
        for (int i=0; i < numArrays; i++) {
            final long   needed    = arrayLength(i, numArrays, minCapacity);
            final byte[] array     = data[i];
            final int    oldLength = (array == null) ? 0 : array.length;

            // Check if nothing to do for this one.
            if (oldLength >= needed) {
                continue;
            }

            // The growth factor is 1.5, kept in the x + (x >>> 1) form rather
            // than spelled x * 3 / 2. The latter overflows an int once a
            // sub-array passes Integer.MAX_VALUE / 3, and sub-arrays grow half
            // as large again as that before they are full. This is int
            // arithmetic, widened only on assignment, and it is safe only
            // because SUB_ARRAY_SIZE is 2^30: raising SUB_ARRAY_SHIFT means
            // revisiting it.
            long newLength = oldLength + (oldLength >>> 1);

            // Note we may start with a zero-length sub-array, which is valid.
            if (newLength < DEFAULT_INITIAL_CAPACITY) {
                newLength = DEFAULT_INITIAL_CAPACITY;
            }

            // A large request is used as-is, so that a single-sub-array list
            // comes out at exactly its requested length. That is what lets
            // toArray() hand back the sub-array itself rather than a copy.
            if (newLength < needed) {
                newLength = needed;
            }

            // And we can never exceed the sub-array limit.
            if (newLength > SUB_ARRAY_SIZE) {
                newLength = SUB_ARRAY_SIZE;
            }

            data[i] = (array == null) ? new byte[(int)newLength]
                                      : Arrays.copyOf(array, (int)newLength);
        }

        // Everything is allocated, so it's safe to switch over to it now
        myData     = data;
        myData0    = data[0];
        myCapacity = (((long)(data.length - 1)) << SUB_ARRAY_SHIFT) +
                     data[data.length - 1].length;

        // That capacity is computed from the length of the last sub-array and
        // the assumption that every earlier one is full length, so it is only
        // right if the loop above held to that. Everything downstream trusts
        // it, so it is worth the walk to know.
        checkCapacity();
    }

    /**
     * Get the size of the list.
     *
     * @return the size.
     */
    public long size()
    {
        return mySize;
    }

    /**
     * Get the capacity of the list.
     *
     * @return the capacity.
     */
    public long capacity()
    {
        return myCapacity;
    }

    /**
     * Set the size of this list to be the given value.
     *
     * @param newSize  The new size of the list.
     *
     * @throws IllegalArgumentException if the given size was negative, or was
     *                                  more than this list can ever hold.
     */
    public void setSize(final long newSize)
        throws IllegalArgumentException
    {
        if (newSize < 0) {
            throw new IllegalArgumentException("Negative size");
        }

        // Make sure there is room and set it. This change will also invalidate
        // the cached toString() value.
        ensureCapacity(newSize);
        mySize     = newSize;
        myToString = null;
    }

    /**
     * Get the element value at the given index.
     *
     * @param index  The index to get the byte from.
     *
     * @return the byte at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     * @throws IllegalStateException     if the sub-arrays did not hold to this
     *                                   class's invariants.
     */
    public byte get(final long index)
        throws IndexOutOfBoundsException,
               IllegalStateException
    {
        // The fast path is a read from within the first sub-array, which is
        // the common case. Anything else, including a bad index, is handled
        // the long way around, where checkRange() says what was wrong with it.
        if (index >= 0 && index < mySize && index < myData0.length) {
            return myData0[(int)index];
        }
        else {
            checkRange(index, 1);
            return arrayForNoCheck(index)[(int)(index & SUB_ARRAY_MASK)];
        }
    }

    /**
     * Set the element value at the given index.
     *
     * @param index    The index to set the byte at.
     * @param element  The value to set it to.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     * @throws IllegalStateException     if the sub-arrays did not hold to this
     *                                   class's invariants.
     */
    public void set(final long index, final byte element)
        throws IndexOutOfBoundsException,
               IllegalStateException
    {
        // The fast path is a write to within the first sub-array, which is
        // the common case. Anything else, including a bad index, is handled
        // the long way around, where checkRange() says what was wrong with it.
        if (index >= 0 && index < mySize && index < myData0.length) {
            myData0[(int)index] = element;
        }
        else {
            checkRange(index, 1);
            arrayForNoCheck(index)[(int)(index & SUB_ARRAY_MASK)] = element;
        }
        myToString = null;
    }

    /**
     * Get the {@code boolean} value at the given index, encoded as a byte where
     * any non-zero value is {@code true} and zero is {@code false}.
     *
     * @param index  The index to get the byte from.
     *
     * @return the byte at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public boolean getBoolean(final long index)
        throws IndexOutOfBoundsException
    {
        return (get(index) != 0);
    }

    /**
     * Get the {@code byte} value at the given index. This is equivalent to
     * {@link #get(long)}.
     *
     * @param index  The index to get the byte from.
     *
     * @return the byte at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public byte getByte(final long index)
        throws IndexOutOfBoundsException
    {
        return get(index);
    }

    /**
     * Get the {@code short} value at the given index, assuming a big-endian
     * format.
     *
     * @param index  The index to get the short from.
     *
     * @return the short at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public short getShort(final long index)
        throws IndexOutOfBoundsException
    {
        return (short)getBigEndian(index, Short.BYTES);
    }

    /**
     * Get the {@code int} value at the given index, assuming a big-endian
     * format.
     *
     * @param index  The index to get the int from.
     *
     * @return the int at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public int getInt(final long index)
        throws IndexOutOfBoundsException
    {
        return (int)getBigEndian(index, Integer.BYTES);
    }

    /**
     * Get the {@code long} value at the given index, assuming a big-endian
     * format.
     *
     * @param index  The index to get the long from.
     *
     * @return the long at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public long getLong(final long index)
        throws IndexOutOfBoundsException
    {
        return getBigEndian(index, Long.BYTES);
    }

    /**
     * Get the {@code float} value at the given index, assuming a big-endian
     * format.
     *
     * @param index  The index to get the float from.
     *
     * @return the float at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public float getFloat(final long index)
        throws IndexOutOfBoundsException
    {
        return Float.intBitsToFloat(getInt(index));
    }

    /**
     * Get the {@code double} value at the given index, assuming a big-endian
     * format.
     *
     * @param index  The index to get the double from.
     *
     * @return the double at the given index.
     *
     * @throws IndexOutOfBoundsException if, surprise, the given index was out
     *                                   of bounds.
     */
    public double getDouble(final long index)
        throws IndexOutOfBoundsException
    {
        return Double.longBitsToDouble(getLong(index));
    }

    /**
     * Give back (a possible) copy of the data held by this class. Mutating the
     * results of this method may result in undefined behaviour.
     *
     * <p>The length of the returned array is always this list's size.
     *
     * @return the array of this list's contents.
     *
     * @throws IllegalStateException         if the sub-arrays did not hold to
     *                                       this class's invariants.
     * @throws UnsupportedOperationException if this list holds more elements
     *                                       than a {@code byte[]} can.
     */
    public byte[] toArray()
        throws IllegalStateException,
               UnsupportedOperationException
    {
        if (mySize == 0) {
            return EMPTY_ARRAY;
        }
        else if (myData.length == 1 && myData0.length == mySize) {
            return myData0;
        }
        else if (mySize > MAX_ARRAY_LENGTH) {
            throw new UnsupportedOperationException(
                "A list of " + mySize + " elements is too large to render as " +
                "a byte[]; the maximum is " + MAX_ARRAY_LENGTH
            );
        }
        else {
            final byte[] result = new byte[(int)mySize];
            copyToNoCheck(0, result, 0, (int)mySize);
            return result;
        }
    }

    /**
     * Write the contents of this list to the given stream, in order.
     *
     * <p>Unlike {@link #toArray()} this has no size limit, since nothing is
     * ever materialised as a single {@code byte[]}.
     *
     * @param out  The stream to write to.
     *
     * @throws IOException           if the write failed.
     * @throws IllegalStateException if the sub-arrays did not hold to this
     *                               class's invariants.
     */
    public void writeTo(final OutputStream out)
        throws IOException,
               IllegalStateException
    {
        // Walk the sub-arrays, handing over as much of each one as belongs to
        // the list. Only the last one is ever partly used, but a list which
        // has been shrunk may not reach the last one at all.
        long remaining = mySize;
        for (int i=0; remaining > 0; i++) {
            // Both of these can only happen if the sub-arrays don't look like
            // we say they do. The bound is tested before the dereference since
            // running off the end of myData is the likelier malformation of the
            // two, and an ArrayIndexOutOfBoundsException from here would say
            // much less about what went wrong.
            checkWalk(i < myData.length, mySize - remaining);
            final byte[] array = myData[i];
            final int    count = (int)Math.min(remaining, array.length);
            checkWalk(count > 0, mySize - remaining);

            out.write(array, 0, count);
            remaining -= count;
        }
    }

    /**
     * Copy a run of this list's elements into the given array.
     *
     * <p>This is how to pull a block of bytes out of a list: it hands the work
     * to {@link System#arraycopy} for each sub-array which the run touches,
     * rather than making the caller loop over {@link #get(long)} and pay a
     * bounds check per byte.
     *
     * @param srcPos  The index of the first element to copy.
     * @param dst     The array to copy into.
     * @param dstPos  The index in the destination to copy to.
     * @param len     The number of elements to copy.
     *
     * @throws IndexOutOfBoundsException if any of the indices were negative, or
     *                                   the run ran off the end of either this
     *                                   list or the destination.
     * @throws IllegalStateException     if the sub-arrays did not hold to this
     *                                   class's invariants.
     */
    public void copyTo(final long srcPos,
                       final byte[] dst,
                       final int dstPos,
                       final int len)
        throws IndexOutOfBoundsException,
               IllegalStateException
    {
        // Note the range tests are phrased as subtractions, since the
        // corresponding sums can overflow.
        if (srcPos < 0 || dstPos < 0 || len < 0 ||
            len > mySize - srcPos || len > dst.length - dstPos)
        {
            throw new IndexOutOfBoundsException(
                "srcPos " + srcPos + ", dstPos " + dstPos + ", len " + len +
                ", size " + mySize + ", dst.length " + dst.length
            );
        }

        copyToNoCheck(srcPos, dst, dstPos, len);
    }

    /**
     * Appends the specified element to the end of this list.
     *
     * @param element  The value to append.
     *
     * @return {@code true} always.
     */
    public boolean add(final byte element)
    {
        // Yes, this check is performed in ensureCapacity() but we short-circuit
        // it here to avoid the overhead of the method call (it does make a
        // difference!).
        if (mySize >= myCapacity) {
            ensureCapacity(mySize + 1);
        }

        // Append the byte, which will also invalidate the cached toString()
        // value. A list which fits inside a single sub-array is the common case
        // and costs no more here than it did when this class held just the one
        // array.
        if (mySize < myData0.length) {
            myData0[(int)mySize] = element;
        }
        else {
            final byte[] array = myData[(int)(mySize >>> SUB_ARRAY_SHIFT)];
            array[(int)(mySize & SUB_ARRAY_MASK)] = element;
        }
        mySize++;
        myToString = null;

        return true;
    }

    /**
     * Appends all of the elements in the specified list to the end of this
     * list.
     *
     * <p>Appending a list to itself is well defined and doubles its contents.
     *
     * @param list  The list containing elements to be added to this list
     *
     * @return {@code true} if this list changed as a result of the call
     */
    public boolean addAll(final ByteList list)
    {
        // Take the size before we do anything else. The given list may be this
        // one, in which case it will be growing as we copy out of it.
        final long numNew = list.size();

        // Check if nothing to do.
        if (numNew == 0) {
            return false;
        }
        else {
            appendFrom(list, 0, numNew);
            return true;
        }
    }

    /**
     * Appends the elements of the {@code data} array argument to this list.
     *
     * <p>The elements of the array argument are appended, in order, to the
     * contents of this list. The length of this list increases by the length of
     * the argument.
     *
     * @param data  The data to be appended.
     *
     * @return a reference to this object.
     */
    public ByteList append(final byte[] data)
    {
        return appendNoCheck(data, 0, data.length);
    }

    /**
     * Appends the elements of a subarray of the {@code data} array argument to
     * this list.
     *
     * <p>Elements of the array {@code data}, starting at index {@code offset},
     * are appended, in order, to the contents of this list. The length of this
     * list increases by the value of {@code len}.
     *
     * @param  data   the data to be appended
     * @param  offset the index of the first element to append
     * @param  len    the number of elements to append
     *
     * @return a reference to this object
     *
     * @throws IndexOutOfBoundsException if {@code offset} is negative, or
     *                                   {@code len} is negative, or
     *                                   {@code len} is larger than
     *                                   {@code data.length - offset}.
     */
    public ByteList append(final byte[] data,
                           final int offset,
                           final int len)
        throws IndexOutOfBoundsException
    {
        // Note the last test is phrased as a subtraction, not as
        // offset + len > data.length, since that sum can overflow.
        if (offset < 0 || len < 0 || len > data.length - offset) {
            throw new IndexOutOfBoundsException(
                "offset " + offset + ", len " + len + ", data.len " +
                data.length
            );
        }

        return appendNoCheck(data, offset, len);
    }

    /**
     * Remove all the entries from this list.
     *
     * <p>The capacity is unchanged; the space stays with this list, ready to be
     * reused. Discard the list itself to give that space back.
     */
    public void clear()
    {
        mySize     = 0;
        myToString = null;
    }

    /**
     * {@inheritDoc}
     *
     * <p>At most {@link #MAX_RENDERED_ELEMENTS} elements are rendered; anything
     * beyond that is reported as a trailing count. A list holding a
     * multi-gigabyte payload is not something anyone wants materialised as a
     * {@link String}, and this is called implicitly by string concatenation,
     * loggers and debuggers, where throwing or allocating gigabytes is the
     * wrong answer.
     */
    @Override
    public String toString()
    {
        // Need to compute the cached value?
        if (myToString == null) {
            // We don't necessarily know the encoding of this ByteList, as
            // someone might have subclassed it to understand UTF etc. As such,
            // it's safest to go via charAt() here, which works on a per-char
            // basis.
            final int count = (int)Math.min(mySize, MAX_RENDERED_ELEMENTS);
            final StringBuilder sb = new StringBuilder(count + 32);
            for (int i=0; i < count; i++) {
                sb.append(charAt(i));
            }
            if (mySize > count) {
                sb.append("...<").append(mySize - count)
                  .append(" more bytes>");
            }
            myToString = sb.toString();
        }
        return myToString;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The return type is narrowed to {@link ByteList}, so that which
     * overload the caller happened to bind to does not change the static type
     * of what they get back.
     *
     * @see #subSequence(long,long)
     */
    @Override
    public ByteList subSequence(final int start, final int end)
        throws IndexOutOfBoundsException
    {
        return subSequence((long)start, (long)end);
    }

    /**
     * Get a list containing the elements of this one between the given start
     * index, inclusive, and the given end index, exclusive.
     *
     * @param start  The index of the first element to take.
     * @param end    The index after the last element to take.
     *
     * @return the sublist.
     *
     * @throws IndexOutOfBoundsException if either index was negative, or the
     *                                   start was after the end, or the end
     *                                   was beyond the end of this list.
     */
    public ByteList subSequence(final long start, final long end)
        throws IndexOutOfBoundsException
    {
        if (start < 0 || end < 0 || start > end || end > mySize) {
            throw new IndexOutOfBoundsException(
                "start " + start + ", end " + end + ", size " + mySize
            );
        }

        final long     len  = end - start;
        final ByteList list = new ByteList(len);

        list.appendFrom(this, start, len);

        return list;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This is equivalent to size().
     *
     * @throws UnsupportedOperationException if this list holds more elements
     *                                       than a {@link CharSequence} can.
     *
     * @see #size()
     */
    @Override
    public int length()
        throws UnsupportedOperationException
    {
        if (mySize > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                "A list of " + mySize + " elements is too long to be handled " +
                "as a CharSequence"
            );
        }
        return (int)mySize;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This is equivalent to get(long).
     *
     * @throws IndexOutOfBoundsException {@inheritDoc}
     * @see    #get(long)
     */
    @Override
    public char charAt(final int index)
        throws IndexOutOfBoundsException
    {
        return (char) get(index);
    }

    // ----------------------------------------------------------------------

    /**
     * How many sub-arrays it takes to hold the given number of elements.
     *
     * @param capacity  The number of elements to hold.
     *
     * @return the number of sub-arrays.
     */
    private static int numArraysFor(final long capacity)
    {
        return (int)((capacity + SUB_ARRAY_MASK) >>> SUB_ARRAY_SHIFT);
    }

    /**
     * How long a given sub-array has to be, in a list of a given capacity.
     * Every sub-array is full length save for the last one, which holds
     * whatever is left over.
     *
     * @param arrayIdx   Which sub-array.
     * @param numArrays  How many sub-arrays there are in total.
     * @param capacity   The capacity which they hold between them.
     *
     * @return the length of that sub-array.
     */
    private static long arrayLength(final int arrayIdx,
                                    final int numArrays,
                                    final long capacity)
    {
        return (arrayIdx + 1 < numArrays)
            ? SUB_ARRAY_SIZE
            : capacity - (((long)arrayIdx) << SUB_ARRAY_SHIFT);
    }

    /**
     * Guard for the loops which walk the sub-arrays. A walk which does not
     * advance would spin forever, and one which runs off the end of
     * {@link #myData} would throw something which said nothing useful; both
     * mean the sub-arrays no longer look the way this class says they do.
     *
     * @param ok     Whether the walk may proceed.
     * @param index  The element index which the walk had reached, for the
     *               message. Always a logical index into the list, never an
     *               offset within a sub-array or a sub-array number, so that
     *               the four walks all report in the same units.
     *
     * @throws IllegalStateException if {@code ok} was {@code false}.
     */
    private void checkWalk(final boolean ok, final long index)
        throws IllegalStateException
    {
        if (!ok) {
            throw new IllegalStateException(
                "Malformed sub-arrays at index " + index + " in a list of " +
                "size " + mySize + " and capacity " + myCapacity
            );
        }
    }

    /**
     * Check that the sub-arrays are as this class says they are: that they all
     * exist, that all but the last are exactly {@link #SUB_ARRAY_SIZE} long,
     * that they are as long in total as {@link #myCapacity} claims, and that
     * between them they are at least as long as the list's size.
     *
     * @throws IllegalStateException if they were not.
     */
    private void checkCapacity()
        throws IllegalStateException
    {
        // Total up what is really there. The lengths are what the index
        // arithmetic will meet, so this trusts nothing but the arrays.
        long total = 0;
        for (int i=0; i < myData.length; i++) {
            final byte[] array = myData[i];
            checkWalk(array != null, total);
            if (i + 1 == myData.length) {
                // The last one holds whatever is left over, which is never
                // nothing: an empty one would leave the capacity looking like
                // it needed one sub-array fewer than the list actually has.
                checkWalk(array.length > 0, total);
            }
            else {
                // And every earlier one is full length, since that is what the
                // index arithmetic assumes
                checkWalk(array.length == SUB_ARRAY_SIZE, total);
            }
            total += array.length;
        }

        // And it has to be what we say it is, and enough to hold what we say
        // we hold
        checkWalk(total == myCapacity, total);
        checkWalk(total >= mySize,     total);
    }

    /**
     * Check that a run of elements of the given length, starting at the given
     * index, lies within this list.
     *
     * <p>This is the one place where an element access is bounds-checked, so
     * that every accessor agrees on where the list ends. The accessors' fast
     * paths test the same thing inline and hand anything they turn away to
     * this method, which is what decides whether it was really out of bounds
     * and says so.
     *
     * <p>The bound is the size, and not the capacity. The sub-arrays behind a
     * list which has been shrunk, or cleared and refilled, still hold what was
     * put in them before; a list which is reused for one message after another
     * would otherwise hand back the tail of the previous one instead of
     * refusing the read.
     *
     * @param index  The index of the first element of the run.
     * @param count  The number of elements in the run, which is at least one.
     *
     * @throws IndexOutOfBoundsException if the run was not wholly within this
     *                                   list.
     */
    private void checkRange(final long index, final int count)
        throws IndexOutOfBoundsException
    {
        // The test for a negative index is required, and not merely for the
        // sake of a better message. Shifting an index down gives its sub-array
        // number, but that number needs 34 bits and only the low 32 of them
        // survive a cast to an int. Without this, an index like Long.MIN_VALUE
        // would address element zero instead of being rejected. Testing the
        // top end against the size deals with the same problem there, since a
        // size can never exceed MAX_CAPACITY: an index such as 2^62 is
        // positive, and would truncate to zero, but no size can admit it.
        //
        // The upper test is phrased as a subtraction since index + count can
        // overflow.
        if (index < 0 || count > mySize - index) {
            final String what =
                (count == 1) ? ("Index " + index)
                             : ("A run of " + count + " elements at index " +
                                index);
            throw new IndexOutOfBoundsException(
                what + " is out of bounds for a list of size " + mySize
            );
        }
    }

    /**
     * Get the sub-array which holds the element at the given index.
     *
     * @param index  The index of the element, which must already have been
     *               checked by {@link #checkRange(long,int)}.
     *
     * @return the sub-array holding it.
     *
     * @throws IllegalStateException if the sub-arrays did not hold to this
     *                               class's invariants.
     */
    private byte[] arrayForNoCheck(final long index)
        throws IllegalStateException
    {
        // A checked index lies within the size, and the sub-arrays are as long
        // as the size says they are, so both of these hold for any index which
        // reaches here. They are tested rather than assumed because the whole
        // of the bounds check now happens before this point: nothing else
        // looks at the length of the sub-array being addressed, so if the
        // sub-arrays are shorter than the size claims then a checked index
        // would read off the end of one of them. Failing that way says the
        // list is malformed, which is true, rather than blaming the index,
        // which is not.
        //
        // Note that the sub-array number is kept as a long until after it has
        // been tested. Casting first would let a number needing more than 32
        // bits truncate and quietly address the wrong sub-array.
        final long arrayIdx = (index >>> SUB_ARRAY_SHIFT);
        checkWalk(arrayIdx < myData.length, index);
        final byte[] array = myData[(int)arrayIdx];
        checkWalk((index & SUB_ARRAY_MASK) < array.length, index);
        return array;
    }

    /**
     * Get the sub-array holding a run of elements which starts at the given
     * index, if the whole run happens to be within a single sub-array.
     *
     * @param index  The index of the first element of the run, which must
     *               already have been checked by
     *               {@link #checkRange(long,int)}.
     * @param count  The number of elements in the run.
     *
     * @return the sub-array holding the whole run, or {@code null} if the run
     *         is split across two sub-arrays.
     *
     * @throws IllegalStateException if the sub-arrays did not hold to this
     *                               class's invariants.
     */
    private byte[] arrayForRunNoCheck(final long index, final int count)
        throws IllegalStateException
    {
        final byte[] array = arrayForNoCheck(index);
        return ((index & SUB_ARRAY_MASK) + count <= array.length) ? array
                                                                  : null;
    }

    /**
     * Get a big-endian value of the given width, at the given index.
     *
     * <p>The three widths share one body since only the number of bytes
     * differs; the callers narrow the result. Since {@code count} is a compile
     * time constant at each call site the loop unrolls away.
     *
     * @param index  The index of the first byte of the value.
     * @param count  The number of bytes in the value.
     *
     * @return the value, in the low {@code count} bytes of the result.
     *
     * @throws IndexOutOfBoundsException if the run was out of bounds.
     */
    private long getBigEndian(final long index, final int count)
        throws IndexOutOfBoundsException
    {
        long value = 0;

        // The fast path is a run which lies inside the first sub-array, which
        // is the common case. Phrased as subtractions since index + count
        // could overflow. This mirrors what get() and set() do.
        if (index >= 0 &&
            index <= mySize - count &&
            index <= myData0.length - count)
        {
            final int offset = (int)index;
            for (int i=0; i < count; i++) {
                value = (value << 8) | (((long)myData0[offset + i]) & 0xffL);
            }
            return value;
        }

        // Otherwise check the run, then see whether it at least sits inside a
        // single sub-array, and fall back to going byte by byte if it
        // straddles two.
        checkRange(index, count);
        final byte[] array = arrayForRunNoCheck(index, count);
        if (array != null) {
            final int offset = (int)(index & SUB_ARRAY_MASK);
            for (int i=0; i < count; i++) {
                value = (value << 8) | (((long)array[offset + i]) & 0xffL);
            }
        }
        else {
            for (int i=0; i < count; i++) {
                value = (value << 8) | (((long)get(index + i)) & 0xffL);
            }
        }
        return value;
    }

    /**
     * Copy a run of this list's elements into the given array, without doing
     * any boundary checking. The caller is responsible for ensuring that the
     * run is within this list's capacity and that the destination has room for
     * it.
     *
     * @param srcPos  The index of the first element to copy.
     * @param dst     The array to copy into.
     * @param dstPos  The index in the destination to copy to.
     * @param len     The number of elements to copy.
     *
     * @throws IllegalStateException if the sub-arrays did not hold to this
     *                               class's invariants.
     */
    private void copyToNoCheck(final long srcPos,
                               final byte[] dst,
                               final int dstPos,
                               final int len)
        throws IllegalStateException
    {
        // Walk the sub-arrays, copying as much as we can out of each one. Note
        // that len is an int and sub-arrays are 2^30 elements long, so a run
        // can span three of them.
        long src = srcPos;
        int  off = dstPos;
        int  rem = len;
        while (rem > 0) {
            final byte[] array = myData[(int)(src >>> SUB_ARRAY_SHIFT)];
            final int    col   = (int)(src & SUB_ARRAY_MASK);
            final int    count = Math.min(rem, array.length - col);
            checkWalk(count > 0, src);

            System.arraycopy(array, col, dst, off, count);
            src += count;
            off += count;
            rem -= count;
        }
    }

    /**
     * Appends a run of another list's elements to this one.
     *
     * <p>The other list may be this one; the run is fully reserved before
     * anything is copied, which means the source is never reallocated part-way
     * through and the two ranges can never overlap.
     *
     * @param list  The list to copy from.
     * @param from  The index of the first element to copy.
     * @param len   The number of elements to copy.
     *
     * @throws IllegalStateException if the sub-arrays did not hold to this
     *                               class's invariants.
     */
    private void appendFrom(final ByteList list,
                            final long from,
                            final long len)
        throws IllegalStateException
    {
        // Check if nothing to do.
        if (len == 0) {
            return;
        }

        // All the room which we need, in one go
        ensureCapacity(mySize + len);

        // Walk both sets of sub-arrays, copying as much as we can each time
        long src = from;
        long dst = mySize;
        long rem = len;
        while (rem > 0) {
            final byte[] srcArray = list.myData[(int)(src >>> SUB_ARRAY_SHIFT)];
            final byte[] dstArray =      myData[(int)(dst >>> SUB_ARRAY_SHIFT)];
            final int    srcCol   = (int)(src & SUB_ARRAY_MASK);
            final int    dstCol   = (int)(dst & SUB_ARRAY_MASK);
            final long   room     = Math.min(srcArray.length - srcCol,
                                             dstArray.length - dstCol);
            final int    count    = (int)Math.min(rem, room);
            // Report where we had got to in this list, not in the one being
            // read, since that is what the message's size and capacity describe
            checkWalk(count > 0, dst);

            System.arraycopy(srcArray, srcCol, dstArray, dstCol, count);
            src += count;
            dst += count;
            rem -= count;
        }

        mySize     = dst;
        myToString = null;
    }

    /**
     * Appends the elements of a subarray of the {@code data} array argument to
     * this list without doing any boundary checking.
     *
     * <p>Elements of the array {@code data}, starting at index {@code offset},
     * are appended, in order, to the contents of this list. The length of this
     * list increases by the value of {@code len}.
     *
     * @param  data   the data to be appended
     * @param  offset the index of the first element to append
     * @param  len    the number of elements to append
     *
     * @return A reference to this object.
     *
     * @throws IllegalStateException if the sub-arrays did not hold to this
     *                               class's invariants.
     */
    private ByteList appendNoCheck(final byte[] data,
                                   final int offset,
                                   final int len)
        throws IllegalStateException
    {
        // You must make sure the following is NOT true before invoking this
        // method:
        //   offset < 0 || len < 0 || len > data.length - offset

        // Check if nothing to do.
        if (len == 0) {
            return this;
        }

        // Ensure we have space, append the bytes, and update meta-data etc.
        ensureCapacity(mySize + len);

        // Walk the sub-arrays, filling as much of each one as we can. Note
        // that len is an int and sub-arrays are 2^30 elements long, so the
        // data can span three of them.
        long pos = mySize;
        int  off = offset;
        int  rem = len;
        while (rem > 0) {
            final byte[] array = myData[(int)(pos >>> SUB_ARRAY_SHIFT)];
            final int    col   = (int)(pos & SUB_ARRAY_MASK);
            final int    count = Math.min(rem, array.length - col);
            checkWalk(count > 0, pos);

            System.arraycopy(data, off, array, col, count);
            pos += count;
            off += count;
            rem -= count;
        }

        mySize     = pos;
        myToString = null;

        return this;
    }
}
