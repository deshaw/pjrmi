package com.deshaw.util;

/**
 * How we might instrument some code.
 */
public interface Instrumentor
{
    /**
     * How we might create Instrumentor instances.
     */
    public static interface Factory
    {
        /**
         * Get the unique instance for the given name.
         *
         * @param name  The name to get.
         *
         * @return the instance for the given name.
         */
        public Instrumentor getInstance(final String name);
    }

    // ----------------------------------------------------------------------

    /**
     * The factory instance which users of this interface should be getting
     * their implementations from.
     *
     * <p>This should likely only be set by the {@code main()} method in the
     * controlling class for the application.
     */
    public static Factory INSTRUMENTOR_FACTORY =
        new Factory() {
            @Override public Instrumentor getInstance(final String name) {
                return NULL_INSTRUMENTOR;
            }
        };

    /**
     * An {@link Instrumentor} instance which does nothing.
     */
    public static final Instrumentor NULL_INSTRUMENTOR =
        new Instrumentor() {
            @Override public void setIntervalMod(int intervalMod) { }
            @Override public long start() { return NULL_START; }
            @Override public void end(long start) { }
        };

    /**
     * The null value which {@link #start()} should return to indicate that end
     * should not register the measurement.
     */
    public static final long NULL_START = -1L;

    // ----------------------------------------------------------------------

    /**
     * Set the sampling interval. This will typically determine whether
     * {@link #start()} returns {@link #NULL_START} or not.
     *
     * @param intervalMod  The new mod to use.
     */
    public void setIntervalMod(int intervalMod);

    /**
     * Call to start a measurement.
     *
     * @return {@link #NULL_START} if no measurement should be made.
     */
    public long start();

    /**
     * Call to finish a measurement.
     *
     * <p>Will do nothing if {@code start} is {@code NULL_START};
     *
     * @param start  The associated start value.
     */
    public void end(long start);
}
