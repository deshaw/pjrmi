package com.deshaw.pjrmi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A unit test suite for {@link PJRmiProperties}.
 *
 * <p>These go at {@code getPositiveLongProperty()} rather than at the getters.
 * The values behind the getters are {@code static final} and resolved when the
 * class is initialised, once per JVM, so driving them from a system property
 * would cover one setting per fork and no more. The rule which decides whether
 * a value is usable is the part worth pinning down, and it is shared by all
 * three.
 *
 * <p>What these do not cover is the failure landing at startup rather than on
 * the first message. That depends on {@code PJRmi}'s static initialiser
 * touching this class, which again is a once-per-JVM affair.
 */
public class PJRmiPropertiesTest
{
    /**
     * A property name which nothing sets.
     */
    private static final String UNSET =
        "com.deshaw.pjrmi.test.aPropertyWhichIsNeverSet";

    /**
     * A property name for the tests below to set and clear.
     */
    private static final String SCRATCH =
        "com.deshaw.pjrmi.test.scratch";

    /**
     * An unset property gives back the default. This is the usual case: none of
     * these are set in a normal run.
     */
    @Test
    public void testUnsetGivesTheDefault()
    {
        assertEquals(
            1234L,
            PJRmiProperties.getPositiveLongProperty(UNSET, 1234L),
            "an unset property should give back the default"
        );
    }

    /**
     * A well-formed value is used in place of the default.
     */
    @Test
    public void testSetValueIsUsed()
    {
        withProperty("4096", () ->
            assertEquals(
                4096L,
                PJRmiProperties.getPositiveLongProperty(SCRATCH, 1234L),
                "a set property should override the default"
            )
        );
    }

    /**
     * Surrounding whitespace is tolerated. A value which came from a script or
     * a config file can easily carry some, and rejecting it would be unhelpful
     * where the intent is unambiguous.
     */
    @Test
    public void testWhitespaceIsTrimmed()
    {
        withProperty("  4096\t", () ->
            assertEquals(
                4096L,
                PJRmiProperties.getPositiveLongProperty(SCRATCH, 1234L),
                "surrounding whitespace should be trimmed, not rejected"
            )
        );
    }

    /**
     * A value which will not parse is rejected, and the error names the
     * property and what was found. The point of failing here at all is to tell
     * whoever set it what to fix.
     */
    @Test
    public void testUnparseableValueIsRejected()
    {
        withProperty("banana", () -> {
            final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> PJRmiProperties.getPositiveLongProperty(SCRATCH, 1234L),
                "a value which is not a number should be rejected"
            );
            assertTrue(e.getMessage().contains(SCRATCH),
                       "the error should name the property: " + e.getMessage());
            assertTrue(e.getMessage().contains("banana"),
                       "the error should quote the value: " + e.getMessage());
        });
    }

    /**
     * A value which parses but cannot be honoured is rejected too. Nothing here
     * bounds an allocation sensibly at zero or below, and silently substituting
     * the default would leave the process running with a size nobody asked for.
     */
    @Test
    public void testNonPositiveValuesAreRejected()
    {
        for (final String value : new String[] { "0", "-1", "-4096" }) {
            withProperty(value, () -> {
                final IllegalArgumentException e = assertThrows(
                    IllegalArgumentException.class,
                    () -> PJRmiProperties.getPositiveLongProperty(SCRATCH,
                                                                  1234L),
                    "\"" + value + "\" should be rejected"
                );
                assertTrue(
                    e.getMessage().contains(SCRATCH),
                    "the error should name the property: " + e.getMessage()
                );
            });
        }
    }

    /**
     * An empty or whitespace-only value is rejected rather than treated as
     * unset. Someone who writes {@code -Dfoo=} has said something, even if it
     * is not usable, and quietly reading it as "leave it alone" would hide
     * that.
     */
    @Test
    public void testEmptyValueIsRejected()
    {
        for (final String value : new String[] { "", "   " }) {
            withProperty(value, () ->
                assertThrows(
                    IllegalArgumentException.class,
                    () -> PJRmiProperties.getPositiveLongProperty(SCRATCH,
                                                                  1234L),
                    "an empty value should be rejected, not read as unset"
                )
            );
        }
    }

    /**
     * A value too large for a {@code long} is rejected rather than wrapping.
     */
    @Test
    public void testOversizedValueIsRejected()
    {
        withProperty("99999999999999999999", () ->
            assertThrows(
                IllegalArgumentException.class,
                () -> PJRmiProperties.getPositiveLongProperty(SCRATCH, 1234L),
                "a value beyond a long should be rejected, not wrapped"
            )
        );
    }

    /**
     * The three getters give back positive sizes, whatever they resolved to in
     * this JVM. This is the one thing which can be said about them without
     * knowing how the JVM was started.
     */
    @Test
    public void testGettersGiveBackPositiveSizes()
    {
        assertTrue(PJRmiProperties.getMaxRetainedBufferBytes() > 0,
                   "maxRetainedBufferBytes should be positive");
        assertTrue(PJRmiProperties.getMaxPayloadPreallocBytes() > 0,
                   "maxPayloadPreallocBytes should be positive");
        assertTrue(PJRmiProperties.getMaxRenderedBytes() > 0,
                   "maxRenderedBytes should be positive");
    }

    // ----------------------------------------------------------------------

    /**
     * Run the given body with {@link #SCRATCH} set to the given value, putting
     * it back afterwards however the body ends.
     *
     * @param value  What to set the property to.
     * @param body   What to run with it set.
     */
    private static void withProperty(final String value, final Runnable body)
    {
        final String was = System.getProperty(SCRATCH);
        System.setProperty(SCRATCH, value);
        try {
            body.run();
        }
        finally {
            if (was == null) {
                System.clearProperty(SCRATCH);
            }
            else {
                System.setProperty(SCRATCH, was);
            }
        }
    }
}
