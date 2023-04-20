package com.deshaw.pjrmi;

import com.deshaw.util.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A transport provider which uses a Unix FIFO to allow users to communicate
 * with another process.
 */
public class UnixFifoProvider
    implements Transport.Provider
{
    /**
     * The FIFO.
     */
    public static class Fifo
    {
        /**
         * Set to true when closed.
         */
        private volatile boolean myIsClosed;

        /**
         * The input fifo going from outside into us.
         */
        private final InputStream myInputStream;

        /**
         * The output fifo going from us to the outside.
         */
        private final OutputStream myOutputStream;

        /**
         * Constructor.
         */
        private Fifo(final String fromFifoname, final String toFifoname)
            throws FileNotFoundException
        {
            final File toFile   = new File(toFifoname);
            final File fromFile = new File(fromFifoname);

            // Create the streams
            myIsClosed     = false;
            myOutputStream = new FileOutputStream(toFile, true);
            myInputStream  = new FileInputStream (fromFile);

            // We make this private communication
            try {
                toFile  .setReadable(true, true);
                toFile  .setWritable(true, true);
                fromFile.setReadable(true, true);
                fromFile.setWritable(true, true);
            }
            catch (SecurityException e) {
                // Nothing, best effort
            }
        }

        /**
         * Close the fifo.
         */
        public void close()
        {
            // Close all the fifos
            try { myInputStream .close(); } catch (IOException e) { }
            try { myOutputStream.close(); } catch (IOException e) { }
            myIsClosed = true;
        }

        /**
         * Whether the fifo has been {@code close()}'d.
         */
        public boolean isClosed()
        {
            return myIsClosed;
        }

        /**
         * Get the Java input stream.
         */
        protected InputStream getInputStream()
        {
            return myInputStream;
        }

        /**
         * Get the Java output stream.
         */
        protected OutputStream getOutputStream()
        {
            return myOutputStream;
        }
    }

    /**
     * The input FIFO.
     */
    private final String myFromFifoname;

    /**
     * The output FIFO.
     */
    private final String myToFifoname;

    /**
     * The input FIFO File.
     */
    private final File myFromFifo;

    /**
     * The output FIFO File.
     */
    private final File myToFifo;

    /**
     * Our parent's PID.
     */
    private final long myParentPid;

    /**
     * Whether we have accepted a connection; we only ever want to do this
     * once.
     */
    private boolean myAccepted;

    /**
     * Get the PID of our parent.
     */
    private static long getParentPid()
    {
        final Optional<ProcessHandle> parent =
            ProcessHandle.current().parent();
        try {
            return parent.orElseThrow().pid();
        }
        catch (NoSuchElementException e) {
            // This should only get thrown for a zombie process for Unix.
            // Since we're still executing we can't be a zombie so this
            // should never happen.
            return -1;
        }
    }

    /**
     * CTOR.
     */
    public UnixFifoProvider(final String fromFifoname,
                            final String toFifoname)
    {
        myFromFifoname = fromFifoname;
        myToFifoname   = toFifoname;
        myFromFifo     = new File(myFromFifoname);
        myToFifo       = new File(myFromFifoname);
        myParentPid    = getParentPid();
        myAccepted     = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transport accept()
        throws IOException
    {
        // We only accept once since the pipes are one-use only
        if (myAccepted) {
            // Spin while isClosed() is false and otherwise exit since, when
            // that returns true, it will be a signal that the Python
            // process has gone away.
            while (!isClosed()) {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    // Nothing
                }
            }
            System.exit(0);
            return null; // for the compiler
        }
        else {
            // Accepting for the first and only time
            myAccepted = true;
            return new UnixFifoTransport(
                new Fifo(myFromFifoname, myToFifoname)
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        try { myFromFifo.delete(); } catch (SecurityException e) { }
        try { myToFifo  .delete(); } catch (SecurityException e) { }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed()
    {
        // See if our FIFOs are gone
        if (!myFromFifo.exists() || !myToFifo.exists()) {
            // One or both of our FIFOs are gone, this means that we are no
            // longer talking to our parent. Theoretically because the
            // parent removed them, with its atexit hook, and then went
            // away.
            return true;
        }

        // See if our parent has gone and we got inherited by init (or a
        // "child sub-reaper") as a consequence. If the parent is SIGTERM'd
        // or SIGKILL'd then they won't have been able to remove the FIFOs.
        if (getParentPid() != myParentPid) {
            return true;
        }

        // If we got here then we're not closed
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "UnixFifo@" + myParentPid + ":" +
               myFromFifoname + ":" + myToFifoname;
    }

    /**
     * The method used to launch this externally.
     */
    public static void main(String[] args)
        throws IOException
    {
        // Usage?
        if (args.length < 2) {
            System.err.println("UnixFifoProvider in_fifo out_fifo [args]");
            System.exit(1);
        }

        // Handle optional args
        final Arguments arguments =
            new Arguments(Arrays.copyOfRange(args, 2, args.length));

        // Create the PJRmi instance now, along with the transport we'll
        // need for it
        final UnixFifoProvider provider = new UnixFifoProvider(args[0], args[1]);
        final PJRmi pjrmi =
            new PJRmi("PJRmi", provider, arguments.useLocking)
            {
                @Override
                protected Object getObjectInstance(CharSequence name)
                {
                    if (StringUtil.equals(name, "LockManager")) {
                        return getLockManager();
                    }
                    else {
                        return null;
                    }
                }

                @Override
                protected boolean isClassBlockingOn()
                {
                    return (arguments.blockNonAllowlistedClasses != null)
                           ? arguments.blockNonAllowlistedClasses
                           : super.isClassBlockingOn();
                }

                @Override
                protected boolean isClassPermitted(CharSequence className)
                {
                    return
                        super.isClassPermitted(className) || (
                            className != null &&
                            arguments.additionalAllowlistedClasses.contains(
                                className.toString()
                            )
                        );
                }

                @Override
                protected boolean isClassInjectionPermitted()
                {
                    return (arguments.allowClassInjection != null)
                           ? arguments.allowClassInjection
                           : super.isClassInjectionPermitted();
                }

                @Override
                protected boolean isUserPermitted(CharSequence username)
                {
                    return true;
                }

                @Override
                protected boolean isHostPermitted(InetAddress address)
                {
                    return true;
                }

                @Override
                protected int numWorkers()
                {
                    return arguments.numWorkers;
                }
            };

        // Set it rolling
        pjrmi.run();
    }
}
