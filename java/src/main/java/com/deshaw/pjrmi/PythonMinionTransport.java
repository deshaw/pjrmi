package com.deshaw.pjrmi;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A transport which spawns a Python child and sets it up as a minion
 * process.
 */
/*package*/ class PythonMinionTransport
    implements Transport
{
    /**
     * How we uniquely identify threads.
     */
    private static final AtomicInteger ourThreadId = new AtomicInteger(0);

    /**
     * The process we spawn.
     */
    private final Process myPythonMinion;

    /**
     * CTOR.
     */
    public PythonMinionTransport(final String  stdinFilename,
                                 final String  stdoutFilename,
                                 final String  stderrFilename,
                                 final boolean useShmArgPassing)
    {
        // The command which we run inside the python process to turn it
        // into a minion instance for us
        String comma = "";
        final StringBuilder command = new StringBuilder();
        command.append("from pjrmi import become_pjrmi_minion; ");
        command.append("become_pjrmi_minion(");
        if (stdinFilename != null) {
            command.append(comma) // <-- A NOP but future-proofing via symmetry
                   .append("stdin='").append(stdinFilename).append("'");
            comma = ", ";
        }
        if (stdoutFilename != null) {
            command.append(comma)
                   .append("stdout='").append(stdoutFilename).append("'");
            comma = ", ";
        }
        if (stderrFilename != null) {
            command.append(comma)
                   .append("stderr='").append(stderrFilename).append("'");
            comma = ", ";
        }
        if (useShmArgPassing) {
            command.append(comma)
                   .append("use_shm_arg_passing=True");
            comma = ", "; // <-- Ditto future-proofing via symmetry
        }
        command.append(");");

        // How we spawn the child
        final ProcessBuilder pb =
            new ProcessBuilder("python3", "-c", command.toString());
        try {
            // Spawn the child
            myPythonMinion = pb.start();

            // Spawn a thread to dump out any remaining output from the
            // child. If stdout and stderr have been defined above this
            // won't be a lot.
            final Thread stderr = new Thread(
                () -> {
                    final BufferedReader err =
                        new BufferedReader(
                            new InputStreamReader(
                                myPythonMinion.getErrorStream()
                            )
                        );
                    try {
                        while (true) {
                            final String line = err.readLine();
                            if (line == null) {
                                // EOF encountered
                                PJRmi.LOG.info("[Python child] <EOF>");
                                return;
                            }
                            PJRmi.LOG.info("[Python child] " + line);
                        }
                    }
                    catch (IOException e) {
                        // Done, one way or another
                    }
                },
                "PythonMinionStderr-" + ourThreadId.incrementAndGet()
            );
            stderr.setDaemon(true);
            stderr.start();

            // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            // Spool forward to the transport's hello string; this tells us
            // that it's ready for work and ensures that we don't accidently
            // try to interpret random foo, appearing on stdout during
            // python's start-up, as something for us. We embed some magic
            // hex values (0xFeedBeef) to try to ensure that we don't
            // accidently clash with another hello string.
            final String hello =
                String.format("PYTHON IS READY: %c%c%c%c",
                              (char)0xfe, (char)0xed, (char)0xbe, (char)0xef);
            final InputStream in = myPythonMinion.getInputStream();
            int index = 0;
            while (index < hello.length()) {
                final int read = in.read();
                if (read == -1) {
                    // EOF'd on us?
                    throw new EOFException(
                        "Child stream closed before hello string could be read"
                    );
                }

                // Accept the next matching char; if it doesn't match
                // then we reset back to the start of the string.
                if (read != (int)hello.charAt(index++)) {
                    index = 0;
                }
            }
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to spawn Python child", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUserName()
    {
        // Must be this
        return System.getProperty("user.name");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getRemoteAddress()
    {
        try {
            return InetAddress.getLocalHost();
        }
        catch (UnknownHostException e) {
            return InetAddress.getLoopbackAddress();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream()
        throws IOException
    {
        return myPythonMinion.getInputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream getOutputStream()
        throws IOException
    {
        return myPythonMinion.getOutputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        try {
            myPythonMinion.destroy();
        }
        catch (Throwable t) {
            // Nothing
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed()
    {
        return !myPythonMinion.isAlive();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myPythonMinion.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLocalhost()
    {
        return true;
    }
}
