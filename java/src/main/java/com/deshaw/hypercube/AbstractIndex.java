package com.deshaw.hypercube;

import com.deshaw.util.LongBitSet;

import java.util.Objects;

/**
 * The base class of many {@link Index} implementations.
 */
public abstract class AbstractIndex<T>
    implements Index<T>
{
    /**
     * The name of the index, if any.
     */
    private final String myName;

    /**
     * Constructor.
     */
    public AbstractIndex(final String name)
    {
        myName = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName()
    {
        return myName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Index<T> mask(final LongBitSet mask)
    {
        return new MaskedIndex<T>(this, mask);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method does not compare the name since that doesn't have
     * implications for the semantics of the index operation.
     */
    @Override
    public boolean equals(final Object o)
    {
        if (o == this) {
            return true;
        }
        else if (o instanceof Index) {
            final Index<?> that = (Index<?>)o;
            return (this.size() == that.size());
        }
        else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        if (getName() != null) {
            return getName();
        }
        else {
            return getClass().getSimpleName() + "{" + size() + "}";
        }
    }

    /**
     * Whether all the keys in this index match those in another one.
     */
    protected boolean keysEquals(final Index<?> that)
    {
        if (that == null) {
            return false;
        }
        final long size = that.size();
        if (size != size()) {
            return false;
        }
        for (long i=0; i < size; i++) {
            if (!Objects.equals(this.keyOf(i), that.keyOf(i))) {
                return false;
            }
        }
        return true;
    }
}
