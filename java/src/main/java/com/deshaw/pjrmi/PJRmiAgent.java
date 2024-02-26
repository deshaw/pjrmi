package com.deshaw.pjrmi;

import java.lang.instrument.Instrumentation;

/**
 * The JVM Agent which PJRmi may leverage.
 */
public class PJRmiAgent
{
    /**
     * The Agent's arg list.
     */
    private static String ourArgs = null;

    /**
     * The Agent's {@link Instrumentation} instance.
     */
    private static Instrumentation ourInstrumentation = null;

    /**
     * Entry point.
     */
    public static void premain(final String args, final Instrumentation instrumentation)
        throws IllegalStateException
    {
        if (ourInstrumentation != null) {
            // Should not be called more than once. Specifically we don't want
            // users calling this method.
            throw new IllegalStateException("Already instantiated");
        }

        ourArgs            = args;
        ourInstrumentation = instrumentation;
    }

    /**
     * Whether the agent has been loaded and can be used.
     */
    public static boolean isLoaded()
    {
        // This will be true if we have a non-null value for ourInstrumentation.
        // The ourArgs value may be null if no args were given.
        return (ourInstrumentation != null);
    }

    /**
     * Get the premain args value.
     *
     * @return the arguments, if any. May be {@code null}.
     */
    public static String getArgs()
    {
        return ourArgs;
    }

    /**
     * Get the premain instrumentation value.
     */
    public static Instrumentation getInstrumentation()
    {
        return ourInstrumentation;
    }

    /**
     * No maketh me.
     */
    private PJRmiAgent()
    {
        // Nothing
    }
}
