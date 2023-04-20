package com.deshaw.util;

/**
 * Provides thread local {@link StringBuilder}s.
 */
public class ThreadLocalStringBuilder
    extends ThreadLocal<StringBuilder>
{
    /**
     * The initial size for the {@link StringBuilder} buffer.
     */
    private final int mySize;

    /**
     * CTOR
     *
     * @param size The initial size for the created
     *             {@link StringBuilder}'s buffer.
     */
    public ThreadLocalStringBuilder(int size)
    {
        mySize = size;
    }

    /**
     * Returns an empty thread local {@link StringBuilder}.
     */
    @Override
    public StringBuilder get()
    {
        StringBuilder sb = super.get();
        sb.setLength(0);
        return sb;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected StringBuilder initialValue()
    {
        return new StringBuilder(mySize);
    }
}
