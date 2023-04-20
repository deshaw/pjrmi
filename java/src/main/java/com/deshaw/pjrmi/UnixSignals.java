package com.deshaw.pjrmi;

/**
 * Methods to work with Unix signals.
 */
public class UnixSignals
{
    /**
     * Ignore the given signal.
     *
     * @param signalName The standard Unix name, like {@code INT}.
     */
    public static void ignoreSignal(final String signalName)
    {
        sun.misc.Signal.handle(new sun.misc.Signal(signalName), signal -> {});
    }
}
