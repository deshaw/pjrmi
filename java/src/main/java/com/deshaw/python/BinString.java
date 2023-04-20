package com.deshaw.python;

import com.deshaw.util.ThreadLocalStringBuilder;

import java.nio.ByteBuffer;

/**
 * ASCII string that wraps around {@link ByteBuffer}s.
 */
public class BinString
    implements CharSequence
{
    /**
     * Thread local string builders.
     */
    private static final ThreadLocalStringBuilder ourThreadLocalStringBuilder =
        new ThreadLocalStringBuilder(64);

    /**
     * Byte buffer containing the data. May be heap-backed or direct.
     */
    private final ByteBuffer myData;

    /**
     * Constructor.
     *
     * @param data  The {@link ByteBuffer} to copy from.
     */
    public BinString(ByteBuffer data)
    {
        this(data, data.position(), data.remaining());
    }

    /**
     * Constructor.
     *
     * @param data    The data to copy from.
     * @param offset  Where to start in the buffer.
     * @param length  How many bytes to wrap from {@code offset}.
     */
    public BinString(ByteBuffer data, int offset, int length)
    {
        assert offset + length <= data.capacity();

        ByteBuffer bufferCopy = data.duplicate();
        bufferCopy.limit(offset + length).position(offset);
        myData = bufferCopy.slice();
    }

    /**
     * Underlying byte buffer.
     *
     * @return The buffer backing this instance.
     */
    public ByteBuffer data()
    {
        return myData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int length()
    {
        return myData.remaining();
    }

    /**
     * Returns the {@code byte} value at a specified index.
     *
     * @param index  The index to get the value at.
     *
     * @return the {@code byte} at that index.
     */
    public byte byteAt(int index)
    {
        return myData.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public char charAt(int index)
    {
        return (char) byteAt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CharSequence subSequence(int start, int end)
    {
        return new BinString(myData, start, end);
    }

    /**
     * Compares this object for <i>contents</i> equality with another
     * {@link CharSequence}. This makes the implementation non-symmetric,
     * but more useful in practice.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CharSequence)) {
            return false;
        }

        CharSequence rhs = (CharSequence) o;
        if (rhs.length() != length()) {
            return false;
        }
        for (int i = 0, length = length(); i < length; i++) {
            if (charAt(i) != rhs.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        // Should match String.hashCode()'s API definition
        int result = 0;
        for (int i = 0, length = length(); i < length; i++) {
            result = 31*result + charAt(i);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return ourThreadLocalStringBuilder.get().append(this).toString();
    }
}
