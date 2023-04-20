/*
 * Copyright 2020 D.E. Shaw & Co.  All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of D.E. Shaw & Co. ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with D.E. Shaw & Co.
 */

package com.deshaw.pjrmi;

import com.deshaw.pjrmi.SourceInjector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test suite for the SourceInjector class.
 */
public class SourceInjectorTest
{
    /**
     * Our source injector.
     */
    private final SourceInjector mySourceInjector = new SourceInjector();

    /**
     * The interface we'll use for testing.
     */
    public interface SourceInjectorTestHelper
    {
        /**
         * Do something which returns a string!
         */
        public String doSomething();
    }

    /**
     * Test that we can override the interface with dynamically-compiled code.
     */
    @Test
    public void testSourceInjector()
        throws Throwable
    {
        final String className = "SourceInjectorTest";

        // Source code to compile
        final String source =
            "public class " + className + " "                                +
            "implements "                                                    +
            "com.deshaw.pjrmi.SourceInjectorTest.SourceInjectorTestHelper {" +
            "    public String doSomething() {"                              +
            "        return \"Expected output\";"                            +
            "    }"                                                          +
            "}";

        // Compile the class and create an instance
        @SuppressWarnings("unchecked")
        final Class<SourceInjectorTestHelper> klass =
            (Class<SourceInjectorTestHelper>)mySourceInjector.inject(className, source);
        final SourceInjectorTestHelper testInstance =
            klass.getDeclaredConstructor().newInstance();

        // Ensure that we have injected the source code successfully
        assertEquals(testInstance.doSomething(), "Expected output");

        // Test that an invalid className is correctly handled
        try {
            mySourceInjector.inject("invalid ", source);

            fail("Invalid class name");
        }
        catch (ClassNotFoundException e) {
            // This is expected
        }

        // Test that a mismatch in the given className and class name in the
        // source code is correctly handled.
        try {
            mySourceInjector.inject("badClassName", source);

            fail("className and class name in source mismatch");
        }
        catch (ClassNotFoundException e) {
            // This is expected
        }

        // Test that invalid source code is correctly handled
        final String badSource =
            "public class SourceInjectorTest "                               +
            "implements "                                                    +
            "com.deshaw.pjrmi.SourceInjectorTest.SourceInjectorTestHelper {" +
            "    public String doSomething() {"                              +
            "        return \"I am invalid code\";"                          +
            "    }}}"                                                        +
            "}";
        try {
            mySourceInjector.inject(className, badSource);

            fail("Invalid source code");
        }
        catch (ClassNotFoundException e) {
            // This is expected
        }

        // Test that invalid source code, delimited by the `\n` character, is
        // correctly parsed and the problematic line is displayed.
        final String badSourceWithNewlines =
            "public class SourceInjectorTest\n"                                +
            "implements\n"                                                     +
            "com.deshaw.pjrmi.SourceInjectorTest.SourceInjectorTestHelper {\n" +
            "    public String doSomething() {\n"                              +
            "        return \"I am invalid code\"\n;"                          +
            "    }}}\n"                                                        +
            "}";
        try {
            mySourceInjector.inject(className, badSourceWithNewlines);

            fail("Invalid source code, with new lines");
        }
        catch (ClassNotFoundException e) {
            assertTrue( e.getMessage().contains("}}}"),    e.toString());
            assertFalse(e.getMessage().contains("public"), e.toString());
        }
    }
}
