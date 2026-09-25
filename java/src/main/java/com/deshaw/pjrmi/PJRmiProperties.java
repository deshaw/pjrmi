package com.deshaw.pjrmi;

/**
 * The Java system properties which tune PJRmi, and their defaults.
 *
 * <p>These are part of PJRmi's interface. Each one bounds an allocation whose
 * right size depends on the machine and the workload, so each carries a
 * default which is derived below and can be overridden at startup with
 * {@code -D<name>=<value>}. Sizes are in bytes.
 *
 * <p>A value which is set but cannot be honoured stops the process, as an
 * {@link ExceptionInInitializerError} wrapping an
 * {@link IllegalArgumentException} naming the property. It is not defaulted
 * over: someone who sets one of these means it, and running on with a
 * different value than was asked for is a misconfiguration which nothing later
 * will make obvious.
 *
 * <p>Each value is read once, when this class is initialised, and not again.
 * These are consulted on the per-message path, where going back to
 * {@link System#getProperty} and re-parsing the string every time would be
 * pure waste; resolving once also means the values cannot shift under a
 * running connection if something calls {@link System#setProperty}. The
 * corollary is that setting one of these after PJRmi has started has no
 * effect.
 */
public class PJRmiProperties
{
    /**
     * The prefix which every one of our property names carries.
     */
    private static final String PREFIX = "com.deshaw.pjrmi.";

    // ----------------------------------------------------------------------

    /**
     * The name of the {@link #getMaxRetainedBufferBytes()} property.
     */
    public static final String MAX_RETAINED_BUFFER_BYTES_PROPERTY =
        PREFIX + "maxRetainedBufferBytes";

    /**
     * The name of the {@link #getMaxPayloadPreallocBytes()} property.
     */
    public static final String MAX_PAYLOAD_PREALLOC_BYTES_PROPERTY =
        PREFIX + "maxPayloadPreallocBytes";

    /**
     * The name of the {@link #getMaxRenderedBytes()} property.
     */
    public static final String MAX_RENDERED_BYTES_PROPERTY =
        PREFIX + "maxRenderedBytes";

    // ----------------------------------------------------------------------

    /**
     * The largest buffer which we hold on to between messages.
     *
     * <p>Emptying a buffer keeps whatever it grew to, so a single outsized
     * message would otherwise pin its space for as long as the thread or
     * connection which owns the buffer lives. There are up to three such
     * buffers per in-flight message (the payload being read, the one being
     * built, and the one being sent), so the retained total is roughly three
     * times this per worker.
     *
     * <p>The default is one sixteenth of a 1GB heap, which is the smallest
     * PJRmi is normally run with. One worker's three buffers therefore retain
     * at most three sixteenths of such a heap, which leaves room for a few
     * workers to be busy at once while still being far larger than a typical
     * message, so the common case never reallocates. Raise it if messages are
     * routinely larger than this and you would rather keep the space than
     * regrow the buffers; lower it if you run many connections in a small heap.
     */
    private static final long DEFAULT_MAX_RETAINED_BUFFER_BYTES =
        64L * 1024 * 1024;

    /**
     * The most space we set aside for an incoming payload before we have seen
     * its bytes.
     *
     * <p>The size in a message header is whatever the peer said it is, so
     * reserving it outright lets a 21-byte header cost us an arbitrary
     * allocation. Beyond this cap the list grows as the data actually arrives,
     * which means the peer has to send what it claimed in order to spend our
     * memory.
     *
     * <p>The default is four times
     * {@link #DEFAULT_MAX_RETAINED_BUFFER_BYTES}: big enough that any message
     * we would keep a buffer for is reserved in one go, and small enough that
     * a lying peer cannot force more than a quarter of a 1GB heap. Raise it if
     * large messages are routine and the peers are trusted, since reserving up
     * front avoids regrowing during the read.
     */
    private static final long DEFAULT_MAX_PAYLOAD_PREALLOC_BYTES =
        256L * 1024 * 1024;

    /**
     * The most bytes of a payload which we render into a log message.
     *
     * <p>A payload may be many gigabytes and each byte can render as four
     * characters, so rendering a whole one is a way to exhaust the heap by
     * turning on logging.
     *
     * <p>The default is 64kB, which is more of a message than anyone reads out
     * of a log line, and small enough that rendering one costs nothing. Raise
     * it only when debugging something which needs more of the payload than
     * that.
     */
    private static final long DEFAULT_MAX_RENDERED_BYTES = 64L * 1024;

    // ----------------------------------------------------------------------

    /**
     * The resolved value of {@link #MAX_RETAINED_BUFFER_BYTES_PROPERTY}.
     */
    private static final long MAX_RETAINED_BUFFER_BYTES =
        getPositiveLongProperty(MAX_RETAINED_BUFFER_BYTES_PROPERTY,
                                DEFAULT_MAX_RETAINED_BUFFER_BYTES);

    /**
     * The resolved value of {@link #MAX_PAYLOAD_PREALLOC_BYTES_PROPERTY}.
     */
    private static final long MAX_PAYLOAD_PREALLOC_BYTES =
        getPositiveLongProperty(MAX_PAYLOAD_PREALLOC_BYTES_PROPERTY,
                                DEFAULT_MAX_PAYLOAD_PREALLOC_BYTES);

    /**
     * The resolved value of {@link #MAX_RENDERED_BYTES_PROPERTY}.
     */
    private static final long MAX_RENDERED_BYTES =
        getPositiveLongProperty(MAX_RENDERED_BYTES_PROPERTY,
                                DEFAULT_MAX_RENDERED_BYTES);

    // ----------------------------------------------------------------------

    /**
     * The largest buffer which we hold on to between messages, in bytes.
     *
     * @return the size.
     *
     * @see #MAX_RETAINED_BUFFER_BYTES_PROPERTY
     */
    public static long getMaxRetainedBufferBytes()
    {
        return MAX_RETAINED_BUFFER_BYTES;
    }

    /**
     * The most space set aside for an incoming payload before its bytes have
     * been seen, in bytes.
     *
     * @return the size.
     *
     * @see #MAX_PAYLOAD_PREALLOC_BYTES_PROPERTY
     */
    public static long getMaxPayloadPreallocBytes()
    {
        return MAX_PAYLOAD_PREALLOC_BYTES;
    }

    /**
     * The most bytes of a payload rendered into a log message.
     *
     * @return the size.
     *
     * @see #MAX_RENDERED_BYTES_PROPERTY
     */
    public static long getMaxRenderedBytes()
    {
        return MAX_RENDERED_BYTES;
    }

    // ----------------------------------------------------------------------

    /**
     * Read a positive {@code long} property, or use the default if it is unset.
     *
     * <p>A missing property is the norm. A value which is present but cannot be
     * honoured is a hard failure: setting one is a deliberate act, so quietly
     * running with something other than what was asked for would leave the
     * process misconfigured in a way nobody is looking for. A warning in a log
     * is not enough, since it is generally read long after it would have
     * helped, if at all. Failing here means it cannot be missed.
     *
     * @param name          The property to read.
     * @param defaultValue  What to use if it is unset.
     *
     * @return the value to use.
     *
     * @throws IllegalArgumentException if the property was set to something
     *                                  which is not a positive number.
     */
    /*package*/ static long getPositiveLongProperty(final String name,
                                                    final long   defaultValue)
        throws IllegalArgumentException
    {
        final String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }

        final long result;
        try {
            result = Long.parseLong(value.trim());
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                name + "=\"" + value + "\" is not a number",
                e
            );
        }
        if (result <= 0) {
            throw new IllegalArgumentException(
                name + "=\"" + value + "\" must be positive"
            );
        }
        return result;
    }

    /**
     * There is no reason to instantiate this class.
     */
    private PJRmiProperties()
    {
        // Nothing to do
    }
}
