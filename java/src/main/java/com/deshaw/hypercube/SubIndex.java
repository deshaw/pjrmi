package com.deshaw.hypercube;

import java.util.Objects;

/**
 * An index which is a sub-range of another index.
 */
public class SubIndex<T>
    extends AbstractIndex<T>
{
    /**
     * The Index which we are mapping.
     */
    private final Index<T> myIndex;

    /**
     * The starting offset of the mapping, inclusive.
     */
    private final long myStart;

    /**
     * The ending index of the mapping, exclusive.
     */
    private final long myEnd;

    /**
     * Constructor.
     *
     * <p>Note {@code start} does not have to be smaller than {@code end}. If
     * the range is reversed then the look-ups will also be reversed
     * accordingly. The {@code start} and {@code end} value may not be equal,
     * however, since that would define a range of length zero.
     *
     * @param name   The name of this index, if any.
     * @param index  The {@link Index} to wrap.
     * @param start  The starting offset into {@code index}, inclusive.
     * @param end    The ending offset into {@code index}, exclusive.
     *
     * @throws NullPointerException     If {@code index} was null.
     * @throws IllegalArgumentException If {@code start} or {@code end} were
     *                                  outside the bounds of {@code index},
     *                                  or were equal..
     */
    public SubIndex(final String   name,
                    final Index<T> index,
                    final long     start,
                    final long     end)
        throws IllegalArgumentException,
               NullPointerException
    {
        super(name);

        // Checks
        if (index == null) {
            throw new NullPointerException("Given a null index");
        }
        final long size = index.size();
        if (start < 0 || start >= size) {
            throw new IllegalArgumentException(
                "Start, " + start + ", was outside the bounds of " +
                "the given index [0.." + size + ")"
            );
        }
        // Note that end is exclusive, hence the test is different
        if (end < -1 || end > size) {
            throw new IllegalArgumentException(
                "End, " + end + ", was outside the bounds of " +
                "the given index [0.." + size + ")"
            );
        }
        if (start == end) {
            throw new IllegalArgumentException(
                "Start and end define a zero-length range: " +
                "[" + start + ".." + end + ")"
            );
        }

        // And assign
        myIndex = index;
        myStart = start;
        myEnd   = end;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long indexOf(final T key)
    {
        // See what the wrapped index says. If this returns -1 then it
        // will be our of bounds in the below tests.
        final long index = myIndex.indexOf(key);

        // If we are reversed then we need to handle things differently
        if (myStart < myEnd) {
            // Bounds check
            if (index < myStart || index >= myEnd ) {
                return index;
            }
            else {
                return index - myStart;
            }
        }
        else {
            // Bounds check
            if (index <= myEnd || index > myStart ) {
                return index;
            }
            else {
                return myStart - index;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T keyOf(final long index)
    {
        // If our range is reversed then we go in the other direction
        return (myStart < myEnd) ? myIndex.keyOf(myStart + index)
                                 : myIndex.keyOf(myStart - index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size()
    {
        return (myStart < myEnd) ? myEnd - myStart : myStart - myEnd;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o)
    {
        // The simple case
        if (o == this) {
            return true;
        }

        // We can only match another SubIndex instance
        if (!(o instanceof SubIndex)) {
            return false;
        }

        // We have another SubIndex to check against. First do the easy check
        // which looks for the exact match
        final SubIndex<?> that = (SubIndex<?>)o;
        if (this.myStart == that.myStart &&
            this.myEnd   == that.myEnd   &&
            Objects.equals(this.myIndex, that.myIndex))
        {
            return true;
        }

        // Now walk up the hiearchies and see if we are semantically equivalent
        long     thisStart = this.myStart;
        long     thisEnd   = this.myEnd;
        Index<?> thisIndex = this.myIndex;
        long     thatStart = that.myStart;
        long     thatEnd   = that.myEnd;
        Index<?> thatIndex = that.myIndex;

        // Walk up the wrappings, translating the ranges into a canonical range
        // on the very bottom Index instance.
        while (thisIndex instanceof SubIndex) {
            final SubIndex<?> s = (SubIndex<?>)thisIndex;
            thisIndex  = s.myIndex;
            thisStart += s.myStart;
            thisEnd   += s.myStart;
        }
        while (thatIndex instanceof SubIndex) {
            final SubIndex<?> s = (SubIndex<?>)thatIndex;
            thatIndex  = s.myIndex;
            thatStart += s.myStart;
            thatEnd   += s.myStart;
        }

        // Now it's essentially the same test again, but using the canonical
        // values
        if (thisStart == thatStart &&
            thisEnd   == thatEnd   &&
            Objects.equals(thisIndex, thatIndex))
        {
            return true;
        }

        // Didn't match
        return false;
    }
}
