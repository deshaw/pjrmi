package com.deshaw.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.nio.ReadOnlyBufferException;

import java.util.Arrays;

/**
 * A resizable array implementation of a list of {@code byte}s.
 *
 * <p>This can be treated as a {@link CharSequence} for ASCII and Latin1
 * (i.e. byte-based) character sets. It won't work for UTF as-is.
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
     * An empty {@code byte[]}.
     */
    private static final byte[] EMPTY_ARRAY = new byte[0];

    /**
     * The array buffer into which the elements of the ArrayList are stored.
     * The capacity of the ArrayList is the length of this array buffer.
     */
    private byte[] myData;

    /**
     * The size of the list (the number of elements it contains).
     */
    private int mySize;

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
        myData     = EMPTY_ARRAY;
        mySize     = 0;
        myToString = null;
    }

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param  initialCapacity the initial capacity of the list
     *
     * @throws IllegalArgumentException if the specified initial capacity
     *                                  is negative.
     */
    public ByteList(int initialCapacity)
        throws IllegalArgumentException
    {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal capacity: " +
                                               initialCapacity);
        }

        myData     = new byte[initialCapacity];
        mySize     = 0;
        myToString = null;
    }

    /**
     * Increases the capacity of this instance, if necessary, to ensure that it
     * can hold at least the number of elements specified by the minimum
     * capacity argument. Does nothing if the current capacity is either
     * {@code Integer.MAX_VALUE} or already greater than or equal to the minimum
     * capacity argument.
     *
     * <p>The instance may grow as a result. Care has been take to make sure
     * this list doesn't contain more than {@code Integer.MAX_VALUE} elements.
     *
     * <p>Note this should be the single place where the array is directly
     * grown.  Other methods shouldn't grow the array directly and should
     * invoke this method to do so.
     *
     * @param  minCapacity the desired minimum capacity
     *
     * @throws IllegalArgumentException if the specified minimal capacity
     *                                  is negative.  This may be caused by
     *                                  integer addition overflow, which means
     *                                  we are trying to store more than
     *                                  {@code Integer.MAX_VALUE} number of
     *                                  elements, which is impossible to handle.
     */
    public void ensureCapacity(int minCapacity)
        throws IllegalArgumentException
    {
        if (minCapacity < 0) {
            throw new IllegalArgumentException(
                "Negative minCapacity is not allowed: " + minCapacity +
                ".  This might be caused by trying to store more than " +
                "Integer.MAX_VALUE number of elements, " +
                "which is impossible to handle.");
        }

        final int oldCapacity = myData.length;

        // Check if nothing to do.
        if (oldCapacity >= minCapacity || oldCapacity == Integer.MAX_VALUE) {
            return;
        }

        // Let's grow the array.  Note we may start with a 0 capacity,
        // which is valid.  The growth factor is 1.5.
        int newCapacity = oldCapacity == 0 ?
            DEFAULT_INITIAL_CAPACITY : (oldCapacity * 3 / 2);

        // Integer overflow from growing?  Use Integer.MAX_VALUE if so.
        if (newCapacity <= 0) {
            newCapacity = Integer.MAX_VALUE;
        }

        // In case of rounding problems when oldCapacity is 1.  Note when
        // we get here, oldCapacity should be < Integer.MAX_VALUE, so we
        // won't get integer overflow again by adding one.
        if (newCapacity <= oldCapacity) {
            newCapacity = oldCapacity + 1;
        }

        // The minCapacity may be large.
        if (newCapacity < minCapacity) {
            newCapacity = minCapacity;
        }

        // Actually grow the array buffer while keeping the data.
        myData = Arrays.copyOf(myData, newCapacity);
    }

    /**
     * Get the size of the list.
     *
     * @return the size.
     */
    public int size()
    {
        return mySize;
    }

    /**
     * Get the capacity of the list.
     *
     * @return the capacity.
     */
    public int capacity()
    {
        return myData.length;
    }

    /**
     * Set the size of this list to be the given value.
     *
     * @param newSize  The new size of the list.
     *
     * @throws IllegalArgumentException if the given size was negative.
     */
    public void setSize(final int newSize)
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
     */
    public byte get(final int index)
        throws IndexOutOfBoundsException
    {
        if (index >= mySize) {
            throw new IndexOutOfBoundsException(index);
        }

        // This will throw exception on negative index.
        return myData[index];
    }

    /**
     * Get the element value at the given index without checking against the
     * list size.
     *
     * @param index  The index to get the byte from.
     *
     * @return the byte at the given index.
     *
     * @throws IndexOutOfBoundsException if the given index was out of the
     *                                   bounds of the underlying array.
     */
    public byte getNoCheck(final int index)
        throws IndexOutOfBoundsException
    {
        return myData[index];
    }

    /**
     * Give back (a possible) copy of the data held by this class. Mutating the
     * results of this method may result in undefined behaviour.
     *
     * @return the array of this list's contents.
     */
    public byte[] toArray()
    {
        if (mySize == 0) {
            return EMPTY_ARRAY;
        }
        else if (myData.length == mySize) {
            return myData;
        }
        else {
            return Arrays.copyOf(myData, mySize);
        }
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
        // difference!)
        if (mySize >= myData.length) {
            ensureCapacity(mySize + 1);
        }

        // Append the byte, which will also invalidate cached toString() value
        myData[mySize++] = element;
        myToString = null;

        return true;
    }

    /**
     * Appends all of the elements in the specified list to the end of this
     * list.  The behavior of this operation is undefined if the specified
     * list is modified while the operation is in progress.  (Note that
     * this will occur if the specified list is this list, and it's
     * nonempty.)
     *
     * @param list  The list containing elements to be added to this list
     *
     * @return {@code true} if this list changed as a result of the call
     */
    public boolean addAll(final ByteList list)
    {
        final int numNew = list.size();

        // Check if nothing to do.
        if (numNew == 0) {
            return false;
        }
        else {
            appendNoCheck(list.myData, 0, list.mySize);
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
     *                                   {@code offset + len} is larger than
     *                                   {@code data.length}.
     */
    public ByteList append(final byte[] data,
                           final int offset,
                           final int len)
        throws IndexOutOfBoundsException
    {
        if (offset < 0 || len < 0 || offset + len > data.length) {
            throw new IndexOutOfBoundsException(
                "offset " + offset + ", len " + len + ", data.len " +
                data.length
            );
        }

        return appendNoCheck(data, offset, len);
    }

    /**
     * Remove all the entries from this list.
     */
    public void clear()
    {
        mySize     = 0;
        myToString = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        // Need to compute the cached value?
        if (myToString == null) {
            // We don't necessarily know the encoding of this ByteList, as
            // someone might have subclassed it to understand UTF etc. As such,
            // it's safest to go via a StringBuilder here, which will append on
            // a per-char basis.
            myToString = new StringBuilder(this).toString();
        }
        return myToString;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CharSequence subSequence(int start, int end)
    {
        if (start < 0 || end < 0 || start > end || end > mySize) {
            throw new IndexOutOfBoundsException(
                "start " + start + ", end " + end + ", size " + mySize
            );
        }

        final int len = end - start;

        final ByteList list = new ByteList(len);

        list.append(myData, start, len);

        return list;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This is equivalent to size().
     *
     * @see #size()
     */
    @Override
    public int length()
    {
        return size();
    }

    /**
     * {@inheritDoc}
     *
     * <p>This is equivalent to get(int).
     *
     * @throws IndexOutOfBoundsException {@inheritDoc}
     * @see    #get(int)
     */
    @Override
    public char charAt(int index)
    {
        return (char) get(index);
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
     * @throws IndexOutOfBoundsException if {@code offset} is negative, or
     *                                   {@code len} is negative, or
     *                                   {@code offset + len} is larger than
     *                                   {@code data.length}.
     */
    private ByteList appendNoCheck(final byte[] data,
                                   final int offset,
                                   final int len)
    {
        // You must make sure the following is NOT true before invoking this
        // method:
        //   offset < 0 || len < 0 || offset + len > data.length

        // Check if nothing to do.
        if (len == 0) {
            return this;
        }

        // Ensure we have space, append the bytes, and update meta-data etc.
        ensureCapacity(mySize + len);
        System.arraycopy(data, offset, myData, mySize, len);
        mySize += len;
        myToString = null;

        return this;
    }
}
