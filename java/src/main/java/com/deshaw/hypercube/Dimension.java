package com.deshaw.hypercube;

import java.util.Objects;

/**
 * A dimension which defines an extent of a {@link Hypercube}.
 *
 * @param <T> The type of the elements in this dimension.
 */
public class Dimension<T>
{
    /**
     * The way which one may access a {@link Hypercube} via a {@link Dimension}.
     */
    public static abstract class Accessor<T>
    {
        /**
         * Our dimension.
         */
        private final Dimension<T> myDimension;

        /**
         * Constructor. These should only be built via the {@link Dimension}'s
         * factory methods
         */
        protected Accessor(final Dimension<T> dimension)
        {
            myDimension = dimension;
        }

        /**
         * Get the dimension associated with this {@link Accessor}.
         */
        public final Dimension<T> getDimension()
        {
            return myDimension;
        }
    }

    /**
     * Part of the coordinates of a point which includes this dimension.
     */
    public static class Coordinate<T>
        extends Accessor<T>
    {
        /**
         * The coordinate of the point in this dimension. A value
         * from zero to dimension_length-1.
         */
        private final long myCoordinate;

        /**
         * Constructor.
         */
        protected Coordinate(final Dimension<T> dimension,
                             final long         coordinate)
        {
            super(dimension);
            myCoordinate = coordinate;
        }

        /**
         * Get the value of the coordinate in the dimension. A value
         * between zero and the length of the dimension.
         */
        public long get()
        {
            return myCoordinate;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(final Object o)
        {
            if (o == this) {
                return true;
            }
            else if (o == null) {
                return false;
            }
            else if (o instanceof Coordinate) {
                final Coordinate that = (Coordinate)o;
                return (this.getDimension().equals(that.getDimension()) &&
                        this.get() == that.get());
            }
            else {
                return false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return Long.toString(myCoordinate);
        }
    }

    /**
     * A slice along this dimension. This always has a stride of 1.
     */
    public static class Slice<T>
        extends Accessor<T>
    {
        /**
         * The start of the slice, inclusive.
         */
        private final long myStart;

        /**
         * The end of the slice, exclusive.
         */
        private final long myEnd;

        /**
         * Constructor.
         */
        protected Slice(final Dimension<T> dimension,
                        final long         start,
                        final long         end)
        {
            super(dimension);

            myStart = start;
            myEnd   = end;
        }

        /**
         * Get the start of this slice, inclusive.
         */
        public long start()
        {
            return myStart;
        }

        /**
         * Get the end of this slice, exclusive.
         */
        public long end()
        {
            return myEnd;
        }

        /**
         * The length of a slice. This is {@code end - start}.
         */
        public long length()
        {
            return myEnd - myStart;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(final Object o)
        {
            if (o == this) {
                return true;
            }
            else if (o instanceof Slice) {
                final Slice that = (Slice)o;
                return (this.getDimension().equals(that.getDimension()) &&
                        this.start() == that.start() &&
                        this.end()   == that.end());
            }
            else {
                return false;
            }
        }

        /**
         * {@inheritDoc}
         *
         * <p>This returns the slice in {@code Pythonic} notation.
         */
        @Override
        public String toString()
        {
            return myStart + ":" + myEnd;
        }
    }

    /**
     * A roll along this dimension.
     */
    public static class Roll<T>
        extends Accessor<T>
    {
        /**
         * The number of places by which elements are shifted (to the right).
         */
        private final long myShift;

        /**
         * Constructor.
         */
        protected Roll(final Dimension<T> dimension,
                       final long         shift)
        {
            super(dimension);

            // This makes sure the shift is always non-negative.
            myShift = (shift % dimension.length() + dimension.length()) % dimension.length();
        }

        /**
         * Get the number of places by which elements are shifted (to the right).
         *
         * <p> This method always returns a non-negative number.
         */
        public long shift()
        {
            return myShift;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(final Object o)
        {
            if (o == this) {
                return true;
            }
            else if (o instanceof Roll) {
                final Roll that = (Roll)o;
                return (this.getDimension().equals(that.getDimension()) &&
                        this.myShift == that.myShift);
            }
            else {
                return false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "rolled by " + myShift;
        }
    }

    // ----------------------------------------------------------------------

    /**
     * The index which defines the lookup mapping for this dimension.
     */
    private final Index<T> myIndex;

    // ----------------------------------------------------------------------

    /**
     * Create a {@link Dimension} array of a single natural indices of the given
     * long.
     */
    public static Dimension<?>[] of(final long len)
    {
        final Dimension<?>[] result = new Dimension<?>[1];
        result[0] = new Dimension<>(new NaturalIndex("D" + 0, len));
        return result;
    }

    /**
     * Create a {@link Dimension} array of natural indices from the given
     * long array.
     */
    public static Dimension<?>[] of(final long... shape)
    {
        // Null breeds null
        if (shape == null) {
            return null;
        }
        final Dimension<?>[] result = new Dimension<?>[shape.length];
        for (int i=0; i < shape.length; i++) {
            result[i] = new Dimension<>(new NaturalIndex("D" + i, shape[i]));
        }
        return result;
    }

    // ----------------------------------------------------------------------

    /**
     * Constructor.
     */
    public Dimension(final Index<T> index)
    {
        if (index == null) {
            throw new NullPointerException("Given a null index");
        }
        myIndex = index;
    }

    /**
     * Get the index for this dimension.
     */
    public Index<T> getIndex()
    {
        return myIndex;
    }

    /**
     * Get the length of this dimension.
     */
    public long length()
    {
        // This is just the size of the index
        return myIndex.size();
    }

    /**
     * Get a {@link Coordinate} along this dimension.
     *
     * @param index  The offset of the point along this dimension. If negative
     *               it will be interpretted as being relative to the end.
     *
     * @throws IndexOutOfBoundsException If the index was out of this
     *                                   dimension's bounds.
     */
    public Coordinate<T> at(final long index)
        throws IndexOutOfBoundsException
    {
        final long idx = canonicalize(index);
        if (idx >= length()) {
            throw new IndexOutOfBoundsException(
                "Index, " + index +
                (idx != index ? ("(" + idx + "), ") : " ") +
                "was out of dimension size bound, " + length()
            );
        }
        return new Coordinate<>(this, idx);
    }

    /**
     * Get a {@link Slice} along this dimension.
     *
     * @param start  The start of the slice along this dimension, inclusive. If
     *               negative it will be interpretted as being relative to the
     *               dimension's end.
     * @param end    The end of the slice along this dimension, exclusive. If
     *               negative it will be interpretted as being relative to the
     *               dimension's end.
     *
     * @throws IllegalArgumentException  If the range was empty or reversed.
     * @throws IndexOutOfBoundsException If the {@code start} or {@code end}
     *                                   were out of this dimension's bounds.
     */
    public Slice<T> slice(final long start, final long end)
        throws IllegalArgumentException,
               IndexOutOfBoundsException
    {
        final long l = length();
        final long s = canonicalize(start);
        final long e = canonicalize(end);
        if (s >= l) {
            throw new IndexOutOfBoundsException(
                "Start, " + start + "(" + s + "), " +
                "was out of dimension size bound, " + l
            );
        }
        if (e > l) {
            throw new IndexOutOfBoundsException(
                "End, " + end + "(" + e + "), " +
                "was out of dimension size bound, " + l
            );
        }
        if (s >= e) {
            throw new IllegalArgumentException(
                "Slice range, " +
                "[" + start + ":" + end + "] ([ " + s + ":" + e + "], " +
                "was empty or reversed"
            );
        }
        return new Slice<>(this, s, e);
    }

    /**
     * Get a {@link Roll} along this dimension.
     *
     * @param shift  The number of places by which the elements are shifted (to
     *               the right) along this dimension. If negative, it will be
     *               interpretted as being shifted to the left.
     */
    public Roll<T> roll(final long shift)
    {
        return new Roll<>(this, shift);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Two dimensions are considerer to be equal if they wrap the same
     * {@link Index}.
     */
    @Override
    public boolean equals(final Object o)
    {
        if (o == this) {
            return true;
        }
        else if (o instanceof Dimension) {
            final Dimension that = (Dimension)o;
            return Objects.equals(this.getIndex(), that.getIndex());
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
        return myIndex.toString();
    }

    /**
     * Canonicalize an offset, which may be from the end of the dimension.
     */
    private long canonicalize(final long offset)
    {
        return (offset < 0) ? length() + offset : offset;
    }
}
