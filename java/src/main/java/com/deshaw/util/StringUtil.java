package com.deshaw.util;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.lang.reflect.Array;

import java.text.ParseException;

/**
 * Provides utility methods to deal with {@link String}s and
 * {@link CharSequence}s.
 */
public class StringUtil
{
    /**
     * {@link CharSequence} that is based on a segment of another
     * sequence.
     *
     * <p>This class is written such that it should specifically match
     * {@code hashCode()} and {@code equals()} for Strings, per their API. Thus
     * allowing us to use it in Maps etc.
     *
     * @see String#hashCode
     */
    public static class HashableSubSequence
        implements CharSequence
    {
        /**
         * What we wrap.
         */
        private CharSequence mySequence;

        /**
         * Starting offset of this segment in the underlying sequence;
         */
        private int myStart;

        /**
         * Length of the (sub-)sequence.
         */
        private int myLength;

        /**
         * Default constructor.
         */
        public HashableSubSequence()
        {
            this(null, 0, 0);
        }

        /**
         * Initializing constructor.
         */
        public HashableSubSequence(CharSequence seq)
        {
            wrap(seq, 0, seq.length());
        }

        /**
         * Initializing constructor.
         */
        public HashableSubSequence(CharSequence seq, int start, int length)
        {
            wrap(seq, start, length);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int length()
        {
            return myLength;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public char charAt(int index)
        {
            if (index < 0 || index >= myLength) {
                throw new IndexOutOfBoundsException(
                    "Asked for index " + index + ", length " + myLength
                );
            }
            return mySequence.charAt(myStart + index);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CharSequence subSequence(int start, int end)
        {
            return new HashableSubSequence(
                mySequence,
                start + myStart,
                Math.min(end - start, myLength - start)
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode()
        {
            // Should match String.hashCode()'s API definition
            int result = 0;
            for (int i = myStart, end = myStart + myLength; i < end; i++) {
                result = 31*result + mySequence.charAt(i);
            }
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o)
        {
            // Null never equals
            if (o == null) {
                return false;
            }

            // Ignore the wrong types
            if (!CharSequence.class.isAssignableFrom(o.getClass())) {
                return false;
            }
            CharSequence that = (CharSequence)o;

            // Check for me (unlikely)
            if (that == this) {
                return true;
            }

            // Different length means different string
            if (that.length() != myLength) {
                return false;
            }

            // Compare all the bytes
            for (int i=0; i < myLength; i++) {
                if (that.charAt(i) != charAt(i)) {
                    return false;
                }
            }

            // Must have matched
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            if (myLength == 0) {
                return "";
            }
            else {
                // We need this condition because our own subSequence()
                // will actually create another HashableSubSequence instance
                // which would result in an infinite recursion.
                CharSequence seq = mySequence;
                while (seq instanceof HashableSubSequence) {
                    seq = ((HashableSubSequence) seq).mySequence;
                }

                return seq.subSequence(myStart, myStart + myLength).toString();
            }
        }

        /**
         * Fully wrap a CharSequence.
         *
         * <p>This will hold a reference to the given {@link CharSequence}.
         *
         * @return This instance.
         */
        public HashableSubSequence wrap(CharSequence seq)
        {
            // A simple hand-off in both cases. If the sequence is null than we
            // have to deal with that with default values for start and length
            // which will be ignored anyhow.
            return (seq == null) ? wrap(seq, 0, 0)
                                 : wrap(seq, 0, seq.length());
        }

        /**
         * Assign the segment to a portion of {@code seq}.
         *
         * <p>This will hold a reference to the given {@link CharSequence}.
         *
         * @return This instance.
         *
         * @throws IndexOutOfBoundsException If {@code start} or {@code length}
         *                                   parameters were invalid in some
         *                                   way (negative, out of bounds,
         *                                   etc.).
         */
        public HashableSubSequence wrap(CharSequence seq, int start, int length)
        {
            // Handle wrapping nothing
            if (seq == null) {
                mySequence = null;
                myStart    = 0;
                myLength   = 0;
                return this;
            }

            // Sanity check inputs
            if (start < 0 || (seq.length() > 0 && start >= seq.length()) ||
                length < 0 || start + length > seq.length())
            {
                throw new IndexOutOfBoundsException(
                    "start or length are out of bounds: " +
                    "start=" + start + ", " +
                    "length=" + length + ", " +
                    "input array length=" + seq.length()
                );
            }
            mySequence = seq;
            myStart    = start;
            myLength   = length;

            return this;
        }
    }

    // ---------------------------------------------------------------------- //

    /**
     * Convert stack trace from a {@link Throwable} to a string.
     */
    public static String stackTraceToString(Throwable e)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Append the hex representation of a byte to a {@link StringBuilder}.
     */
    public static StringBuilder appendHexByte(StringBuilder app, byte b)
    {
        app.append(HEX_DIGITS[(b & 0xf0) >>> 4]);
        app.append(HEX_DIGITS[(b & 0x0f) >>> 0]);
        return app;
    }
    /** For use by appendHexByte */
    private static final char[] HEX_DIGITS = { '0','1','2','3','4','5','6','7',
                                               '8','9','A','B','C','D','E','F' };

    /**
     * Returns true if the two CharSequence instances are identical.
     */
    public static boolean equals(CharSequence a, CharSequence b)
    {
        if (a == b) {
            // If they're the same object, then they're identical.
            return true;
        }
        else if (a == null || b == null) {
            // One is null and one is non-null, so they can't be identical.
            // Note that if we get here, they can't both be null since then
            // we would have fallen into the first block of the if..else
            // above.
            return false;
        }
        else if (a.length() != b.length()) {
            // Different lengths implies not identical.
            return false;
        }
        else {
            // Check that the individual characters match up. We do this
            // backwards since, more likely than not, a prefix might match.
            // Probably...
            for (int i = a.length() - 1; i >= 0; i--) {
                if (a.charAt(i) != b.charAt(i)) {
                    // Characters at this index don't match.
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Returns true if the two CharSequence instances are identical, ignoring
     * case.
     */
    public static boolean equalsIgnoreCase(CharSequence a, CharSequence b)
    {
        if (a == b) {
            // If they're the same object, then they're identical.
            return true;
        }
        else if (a == null || b == null) {
            // One is null and one is non-null, so they can't be identical.
            // Note that if we get here, they can't both be null since then
            // we would have fallen into the first block of the if..else
            // above.
            return false;
        }
        else if (a.length() != b.length()) {
            // Different lengths => not identical.
            return false;
        }
        else {
            // Check that the individual characters match up. We do this
            // backwards since, more likely than not, a prefix might match.
            // Probably...
            for (int i = a.length() - 1; i >= 0; i--) {
                if (Character.toLowerCase(a.charAt(i)) !=
                    Character.toLowerCase(b.charAt(i)))
                {
                    // Characters at this index don't match.
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Parses the given string into a boolean value, ignoring case, and throws
     * an exception if the string does not describe a boolean value.
     */
    public static boolean parseBoolean(CharSequence s)
        throws ParseException
    {
        if (equalsIgnoreCase("true", s)) {
            return true;
        }
        else if (equalsIgnoreCase("false", s)) {
            return false;
        }
        else {
            throw new ParseException(
                "'" + s + "' does not describe a boolean value", 0
            );
        }
    }

    /**
     * A version of {@code toString()} which will unwind arrays as well.
     */
    public static String toString(final Object o)
    {
        return toString(o, Integer.MAX_VALUE);
    }

    /**
     * A version of {@code toString()} which will unwind arrays as well.
     */
    public static String toString(final Object o, final int limit)
    {
        // Look for arrays
        if (o != null) {
            if (o.getClass().isArray()) {
                final StringBuilder sb = new StringBuilder("[");
                for (int i = 0, len = Array.getLength(o); i < len; i++) {
                    if (i > 0) sb.append(", ");
                    if (i + 1 >= limit) {
                        sb.append("...");
                        break;
                    }
                    else {
                        sb.append(toString(Array.get(o, i)));
                    }
                }
                sb.append(']');
                return sb.toString();
            }
        }

        // Otherwise we just fall back to vanilla toString() mechanism, being
        // careful to handle nulls. Note that this won't find arrays in
        // containers, since we don't inspect them.
        return String.valueOf(o);
    }
}
