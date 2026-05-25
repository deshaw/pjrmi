package com.deshaw.pjrmi;

/**
 * A class which represents a Python {@code slice}.
 *
 * <p>The Java version has the same immutability semantics as its Python
 * counterpart.
 */
public class PythonSlice
{
    /**
     * The slice start. This may be {@code null} if the start is unbounded.
     */
    public final Long start;

    /**
     * The slice stop. This may be {@code null} if the stop is unbounded.
     */
    public final Long stop;

    /**
     * The slice step. This may be {@code null} if the step is the default
     * {@code 1} value.
     */
    public final Long step;

    /**
     * Constructor with start and stop values.
     *
     * @param start  The start value.
     * @param stop   The stop value.
     */
    public PythonSlice(final Long start,
                       final Long stop)
    {
        this(start, stop, null);
    }

    /**
     * Constructor with start, stop and step values.
     *
     * @param start  The start value.
     * @param stop   The stop value.
     * @param step   The step value.
     */
    public PythonSlice(final Long start,
                       final Long stop,
                       final Long step)
    {
        this.start = start;
        this.stop  = stop;
        this.step  = step;
    }

    /**
     * Resolve this slice against a sequence of the given length, applying
     * Python semantics: a {@code null} start or stop is treated as
     * unbounded, negative indices are wrapped relative to the length, and
     * out-of-range values are clamped to {@code [0, len]}.
     *
     * <p>Arithmetic is performed in {@code long} so that out-of-{@code int}
     * bounds (e.g. {@link Long#MAX_VALUE}) clamp cleanly to {@code len}
     * instead of silently truncating to a wrong (often negative) {@code int}.
     *
     * <p>The returned {@code stop} may be less than {@code start} (e.g. for
     * {@code slice(5, 2)}), so callers should compute the slice length as
     * {@code Math.max(0, stop - start)}.
     *
     * @param len  The length of the sequence to be sliced. Must be
     *             non-negative.
     *
     * @return A two-element array {@code {start, stop}} of half-open bounds,
     *         each in {@code [0, len]}.
     *
     * @throws UnsupportedOperationException if {@link #step} is non-null and
     *                                       not {@code 1L}. Non-unit step
     *                                       slices (including negative
     *                                       steps) are not supported.
     */
    public int[] resolve(final int len)
    {
        if (step != null && step != 1L) {
            throw new UnsupportedOperationException(
                "Non-unit step is not supported in slice " + this +
                "; only unit-step slices are supported here"
            );
        }
        long s = (start == null) ? 0L  : start.longValue();
        long e = (stop  == null) ? len : stop .longValue();
        if (s < 0) s = Math.max(0L, len + s);
        if (e < 0) e = Math.max(0L, len + e);
        s = Math.min(Math.max(s, 0L), len);
        e = Math.min(Math.max(e, 0L), len);
        return new int[] { (int) s, (int) e };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        // Use Python null/None semantics for this
        return "slice(" +
            ((start == null) ? "None" : start.toString()) + ", " +
            ((stop  == null) ? "None" : stop .toString()) + ", " +
            ((step  == null) ? "None" : step .toString()) +
        ")";
    }
}
