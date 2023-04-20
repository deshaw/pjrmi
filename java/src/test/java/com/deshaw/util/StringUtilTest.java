package com.deshaw.util;

import com.deshaw.util.StringUtil;
import com.deshaw.util.StringUtil.HashableSubSequence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;

import java.text.ParseException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test suite for testing {@link com.deshaw.util.StringUtil} class.
 */
public class StringUtilTest
{
    /**
     * Test hashing of a wrapped subsequence.
     */
    @Test
    public void testHashableSubSequence()
    {
        final StringUtil.HashableSubSequence wrapper =
            new StringUtil.HashableSubSequence();

        // Compare how wrapper works against a native string
        final String string = "Hello World";
        final StringBuilder buf = new StringBuilder();
        buf.append(string);

        // All these should match
        for (int start = 0; start < string.length(); start++) {
            for (int end = start; end < string.length(); end++) {
                wrapper.wrap(buf, start, end-start);
                final String substring = string.substring(start, end);

                assertEquals(wrapper.length(),   substring.length());
                assertEquals(wrapper.hashCode(), substring.hashCode());
                assertTrue  (wrapper.equals(substring));

                for (int i=0; i < wrapper.length(); i++) {
                    assertEquals(wrapper.charAt(i), substring.charAt(i));
                }
            }
        }
    }

    /**
     * Test {@link StringUtil#equalsIgnoreCase(CharSequence, CharSequence)}.
     */
    @Test
    public void testEqualsIgnoreCase()
    {
        assertTrue(StringUtil.equalsIgnoreCase(null, null));
        assertTrue(StringUtil.equalsIgnoreCase("", new HashableSubSequence("")));
        assertTrue(StringUtil.equalsIgnoreCase("abc", new HashableSubSequence("abc")));
        assertTrue(StringUtil.equalsIgnoreCase("aBc", new HashableSubSequence("AbC")));
        assertTrue(StringUtil.equalsIgnoreCase("abcdefghijklmnopqrstuvwxyz",
                                               new HashableSubSequence("ABCDEFGHIJKLMNOPQRSTUVWXYZ")));

        assertFalse(StringUtil.equalsIgnoreCase(null, new HashableSubSequence("")));
        assertFalse(StringUtil.equalsIgnoreCase("", null));
        assertFalse(StringUtil.equalsIgnoreCase(null, new HashableSubSequence("a")));
        assertFalse(StringUtil.equalsIgnoreCase("a", null));
        assertFalse(StringUtil.equalsIgnoreCase("", new HashableSubSequence(" ")));
        assertFalse(StringUtil.equalsIgnoreCase("a ", new HashableSubSequence(" a")));
        assertFalse(StringUtil.equalsIgnoreCase("ab", new HashableSubSequence("ba")));
        assertFalse(StringUtil.equalsIgnoreCase("a a", new HashableSubSequence("aa")));
        assertFalse(StringUtil.equalsIgnoreCase("a", new HashableSubSequence("aa")));
    }

    /**
     * Tests {@link StringUtil#equals(CharSequence, CharSequence)}.
     */
    @Test
    public void testEquals()
    {
        assertTrue(StringUtil.equals(null, null));
        assertTrue(StringUtil.equals("", new HashableSubSequence("")));
        assertTrue(StringUtil.equals("abcdefghijklmnopqrstuvwxyz",
                                     new HashableSubSequence("abcdefghijklmnopqrstuvwxyz")));
        assertTrue(StringUtil.equals("1234567890", new HashableSubSequence("1234567890")));

        assertFalse(StringUtil.equals(null, new HashableSubSequence("")));
        assertFalse(StringUtil.equals("", null));
        assertFalse(StringUtil.equals(null, new HashableSubSequence("a")));
        assertFalse(StringUtil.equals("a", null));
        assertFalse(StringUtil.equals("", new HashableSubSequence(" ")));
        assertFalse(StringUtil.equals("a ", new HashableSubSequence(" a")));
        assertFalse(StringUtil.equals("ab", new HashableSubSequence("ba")));
        assertFalse(StringUtil.equals("a a", new HashableSubSequence("aa")));
        assertFalse(StringUtil.equals("a", new HashableSubSequence("aa")));
        assertFalse(StringUtil.equals("aBc", new HashableSubSequence("AbC")));
    }

    /**
     * Returns true if {@link StringUtil#parseBoolean(CharSequence)} fails to
     * parse the given string as a boolean value (by throwing a ParseException)
     * or false if parsing completed successfully without throwing a
     * ParseException.
     */
    private boolean failParseBoolean(CharSequence s) {
        try {
            StringUtil.parseBoolean(s);

            // If we get here, then no exception was thrown, which was not
            // expected.
            return false;
        }
        catch (ParseException e) {
            // We expected to receive a ParseException here.
            return true;
        }
        catch (Exception e) {
            // Any other exception signals an unexpected error.
            return false;
        }
    }

    /**
     * Tests {@link StringUtil#parseBoolean(CharSequence)}.
     */
    @Test
    public void testParseBoolean()
    {
        try {
            assertTrue(StringUtil.parseBoolean("true"));
            assertTrue(StringUtil.parseBoolean("TRUE"));
            assertTrue(StringUtil.parseBoolean("tRuE"));
            assertFalse(StringUtil.parseBoolean("false"));
            assertFalse(StringUtil.parseBoolean("FALSE"));
            assertFalse(StringUtil.parseBoolean("FaLsE"));
        }
        catch (ParseException e) {
            // We don't expect to receive ParseException's in any of the cases
            // above.
            fail("Unexpected ParseException caught");
        }

        assertTrue(failParseBoolean(null));
        assertTrue(failParseBoolean(""));
        assertTrue(failParseBoolean("foobar"));
        assertTrue(failParseBoolean(" false"));
        assertTrue(failParseBoolean("false "));
        assertTrue(failParseBoolean(" true"));
        assertTrue(failParseBoolean("true "));
        assertFalse(failParseBoolean("true"));
        assertFalse(failParseBoolean("false"));
    }
}
