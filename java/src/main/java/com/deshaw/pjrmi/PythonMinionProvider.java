package com.deshaw.pjrmi;

import com.deshaw.util.StringUtil;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;

/**
 * A transport provider which spawns a child Python process to talk to.
 */
public class PythonMinionProvider
    implements Transport.Provider
{
    /**
     * Our stdin filename, if any.
     */
    private final String myStdinFilename;

    /**
     * Our stdout filename, if any.
     */
    private final String myStdoutFilename;

    /**
     * Our stderr filename, if any.
     */
    private final String myStderrFilename;

    /**
     * Whether to use SHM data passing.
     */
    private final boolean myUseShmdata;

    /**
     * The singleton child which we will spawn.
     */
    private volatile PythonMinionTransport myMinion;

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Spawn a Python minion with SHM value passing disabled by default.
     *
     * @return the minion instance.
     *
     * @throws IOException If there was a problem spawning the child.
     */
    public static PythonMinion spawn()
        throws IOException
    {
        try {
            return spawn(null, null, null, false);
        }
        catch (IllegalArgumentException e) {
            // Should never happen
            throw new AssertionError(e);
        }
    }

    /**
     * Spawn a Python minion, and a PJRmi connection to handle its callbacks.
     * This allows users to specify if they want to use native array
     * handling.
     *
     * @param useShmArgPassing Whether to use native array handling.
     *
     * @return the minion instance.
     *
     * @throws IOException If there was a problem spawning the child.
     */
    public static PythonMinion spawn(final boolean useShmArgPassing)
        throws IOException
    {
        try {
            return spawn(null, null, null, useShmArgPassing);
        }
        catch (IllegalArgumentException e) {
            // Should never happen
            throw new AssertionError(e);
        }
    }

    /**
     * Spawn a Python minion with SHM value passing disabled by default.
     *
     * @param stdinFilename  The filename the child process should use for
     *                       stdin, or {@code null} if none.
     * @param stdoutFilename The filename the child process should use for
     *                       stdout, or {@code null} if none.
     * @param stderrFilename The filename the child process should use for
     *                       stderr, or {@code null} if none.
     *
     * @return the minion instance.
     *
     * @throws IOException              If there was a problem spawning
     *                                  the child.
     * @throws IllegalArgumentException If any of the stio redirects were
     *                                  disallowed.
     */
    public static PythonMinion spawn(final String stdinFilename,
                                     final String stdoutFilename,
                                     final String stderrFilename)
        throws IOException,
               IllegalArgumentException
    {
        return spawn(stdinFilename, stdoutFilename, stderrFilename, false);
    }

    /**
     * Spawn a Python minion, and a PJRmi connection to handle its callbacks.
     *
     * <p>This method allows the caller to provide optional overrides for
     * the child process's stdio. Since the child uses stdin and stdout to
     * talk to the parent these must not be any of the "/dev/std???" files.
     * This method also allows users to specify whether to enable passing
     * of some values by SHM copying.
     *
     * @param stdinFilename    The filename the child process should use for
     *                         stdin, or {@code null} if none.
     * @param stdoutFilename   The filename the child process should use for
     *                         stdout, or {@code null} if none.
     * @param stderrFilename   The filename the child process should use for
     *                         stderr, or {@code null} if none.
     * @param useShmArgPassing Whether to use native array handling.
     *
     * @return the minion instance.
     *
     * @throws IOException              If there was a problem spawning
     *                                  the child.
     * @throws IllegalArgumentException If any of the stio redirects were
     *                                  disallowed.
     */
    public static PythonMinion spawn(final String  stdinFilename,
                                     final String  stdoutFilename,
                                     final String  stderrFilename,
                                     final boolean useShmArgPassing)
        throws IOException,
               IllegalArgumentException
    {
        // Make sure that people don't interfere with our comms channel
        assertGoodForStdio("stdin",  stdinFilename );
        assertGoodForStdio("stdout", stdoutFilename);
        assertGoodForStdio("stderr", stderrFilename);

        // Create the PJRmi instance now, along with the transport we'll
        // need for it
        final PythonMinionProvider provider =
            new PythonMinionProvider(stdinFilename,
                                     stdoutFilename,
                                     stderrFilename,
                                     useShmArgPassing);
        final PJRmi pjrmi =
            new PJRmi("PythonMinion", provider, false, useShmArgPassing)
            {
                @Override protected Object getObjectInstance(CharSequence name)
                {
                    return null;
                }

                @Override protected boolean isUserPermitted(CharSequence username)
                {
                    return true;
                }

                @Override
                protected boolean isHostPermitted(InetAddress address)
                {
                    return true;
                }

                @Override protected int numWorkers()
                {
                    return 16;
                }
            };

        // Give back the connection to handle evals
        return pjrmi.awaitConnection();
    }

    /**
     * Ensure that someone isn't trying to be clever with the output
     * filenames, if any. This should prevent people accidently using our
     * comms channel for their own purposes.
     *
     * @param what      The file description.
     * @param filename  The file path.
     */
    private static void assertGoodForStdio(final String what,
                                           final String filename)
        throws IllegalArgumentException
    {
        // Early-out if there's no override
        if (filename == null) {
            return;
        }

        // Using /dev/null is fine
        if (filename.equals("/dev/null")) {
            return;
        }

        // Disallow all files which look potentially dubious. We could try
        // to walk the symlinks here but that seems like overkill
        if (filename.startsWith("/dev/") || filename.startsWith("/proc/")) {
            throw new IllegalArgumentException(
                "Given " + what + " file was of a disallowed type: " +
                filename
            );
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * CTOR.
     *
     * @param stdinFilename     The stdin path.
     * @param stdoutFilename    The stdout path.
     * @param stderrFilename    The stderr path.
     * @param useShmArgPassing  Whether to use shared-memory arg passing.
     */
    private PythonMinionProvider(final String  stdinFilename,
                                 final String  stdoutFilename,
                                 final String  stderrFilename,
                                 final boolean useShmArgPassing)
        throws IllegalArgumentException
    {
        if (useShmArgPassing && !JniPJRmi.isAvailable()) {
            throw new IllegalArgumentException(
                "SHM arg passing was requested but is not available; " +
                "is the " + JniPJRmi.LIBRARY_NAME + " shared library loaded?"
            );
        }

        myStdinFilename  = stdinFilename;
        myStdoutFilename = stdoutFilename;
        myStderrFilename = stderrFilename;
        myUseShmdata     = useShmArgPassing;
        myMinion         = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transport accept()
        throws IOException
    {
        if (myMinion == null) {
            myMinion = new PythonMinionTransport(myStdinFilename,
                                                 myStdoutFilename,
                                                 myStderrFilename,
                                                 myUseShmdata);
            return myMinion;
        }
        else {
            while (myMinion != null) {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    // Nothing
                }
            }

            // If we get here we have been closed so throw as such
            throw new IOException("Instance is closed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        if (myMinion != null) {
            myMinion.close();
            myMinion = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed()
    {
        return myMinion == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "PythonMinion";
    }

    /**
     * Testing method.
     *
     * @param args  What to eval.
     *
     * @throws Throwable if there was a problem.
     */
    public static void main(String[] args)
        throws Throwable
    {
        final PythonMinion python = spawn();

        System.out.println();
        System.out.println("Calling eval and invoke...");
        Object result;
        for (String arg : args) {
            // Do the eval
            try {
                result = python.eval(arg);
            }
            catch (Throwable t) {
                result = StringUtil.stackTraceToString(t);
            }
            System.out.println("  \"" + arg + "\" -> " + result);

            // Call a function on the argument
            try {
                result = python.invoke("len", Integer.class, arg);
            }
            catch (Throwable t) {
                result = StringUtil.stackTraceToString(t);
            }
            System.out.println("  len('" + arg + "') -> " + result);
        }

        // Stress test
        System.out.println();
        System.out.println("Stress testing invoke()...");
        for (int round = 1; round <= 3; round++) {
            Object foo = "foo";
            final int count = 10000;
            long start = System.nanoTime();
            for (int i=0; i < count; i++) {
                python.invoke("len", Integer.class, foo);
            }
            long end = System.nanoTime();
            System.out.println("  time(len('" + foo + "')) = " +
                               ((end - start) / count / 1000) + "us");

            foo = PythonMinion.byValue(foo);
            start = System.nanoTime();
            for (int i=0; i < count; i++) {
                python.invoke("len", Integer.class, foo);
            }
            end = System.nanoTime();
            System.out.println("  time(len('" + foo + "')) = " +
                               ((end - start) / count / 1000) + "us");
        }

        // Done
        System.out.println();
    }
}
