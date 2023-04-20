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
