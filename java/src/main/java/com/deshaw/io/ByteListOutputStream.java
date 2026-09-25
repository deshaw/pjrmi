package com.deshaw.io;

import com.deshaw.util.ByteList;

import java.io.OutputStream;

/**
 * An {@link OutputStream} which appends everything written to it to a
 * {@link ByteList}.
 *
 * <p>This is the {@link ByteList} analogue of
 * {@link java.io.ByteArrayOutputStream}, and exists because a
 * {@link java.io.ByteArrayOutputStream} cannot hold more than
 * {@code Integer.MAX_VALUE} bytes. Wrapping one of these in a
 * {@link java.io.DataOutputStream} gives a way to build a buffer of more than
 * two gigabytes.
 *
 * <p>Nothing here is synchronized, since a {@link ByteList} isn't either. An
 * instance should be written to by one thread at a time.
 */
public class ByteListOutputStream
    extends OutputStream
{
    /**
     * The list which we append everything to.
     */
    private final ByteList myList;

    /**
     * Constructor.
     *
     * @param list  The list to append to. This is not copied; writes go
     *              directly into it and the caller is free to read it as they
     *              go.
     *
     * @throws NullPointerException if the given list was {@code null}.
     */
    public ByteListOutputStream(final ByteList list)
        throws NullPointerException
    {
        if (list == null) {
            throw new NullPointerException("Given a null list");
        }
        myList = list;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Only the low eight bits of the given value are written, per the
     * {@link OutputStream} contract.
     */
    @Override
    public void write(final int b)
    {
        myList.add((byte)b);
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@link OutputStream} does not declare this, but the underlying
     * {@link ByteList} will dereference the array, so it is named here rather
     * than left to be discovered.
     *
     * @throws NullPointerException if the given array was {@code null}.
     */
    @Override
    public void write(final byte[] b)
        throws NullPointerException
    {
        myList.append(b);
    }

    /**
     * {@inheritDoc}
     *
     * <p>As with {@link #write(byte[])}, the unchecked exceptions which the
     * underlying {@link ByteList} can raise are named rather than left to the
     * inherited contract, which does not mention them.
     *
     * @throws IndexOutOfBoundsException if {@code off} is negative, or
     *                                   {@code len} is negative, or
     *                                   {@code len} is larger than
     *                                   {@code b.length - off}.
     * @throws NullPointerException      if the given array was {@code null}.
     */
    @Override
    public void write(final byte[] b, final int off, final int len)
        throws IndexOutOfBoundsException,
               NullPointerException
    {
        myList.append(b, off, len);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This does nothing; there is nowhere for the data to be flushed to.
     */
    @Override
    public void flush()
    {
        // Nothing to do
    }

    /**
     * {@inheritDoc}
     *
     * <p>This does nothing; the list remains usable after closing.
     */
    @Override
    public void close()
    {
        // Nothing to do
    }
}
