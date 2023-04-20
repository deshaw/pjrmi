package com.deshaw.pjrmi;

import com.deshaw.io.BlockingPipe;
import com.deshaw.util.StringUtil;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A transport provider which uses PipedStreams to allow users to
 * communicate with another thread inside the process.
 */
public class PipedProvider
    implements Transport.Provider
{
    /**
     * A simple PJRmi instance to use with this.
     */
    public static class PipedPJRmi
        extends PJRmi
    {
        /**
         * The arguments supplied to the instance.
         */
        private final Arguments myArguments;

        /**
         * Constructor.
         *
         * @param provider  The provider to use.
         *
         * @throws IOException              if there was a problem.
         * @throws IllegalArgumentException if there was a problem.
         */
        public PipedPJRmi(PipedProvider provider)
            throws IOException,
                   IllegalArgumentException
        {
            this(provider, null);
        }

        /**
         * Constructor.
         *
         * @param provider  The provider to use.
         * @param args      The PJRmi arguments.
         *
         * @throws IOException              if there was a problem.
         * @throws IllegalArgumentException if there was a problem.
         */
        public PipedPJRmi(PipedProvider provider, String[] args)
            throws IOException,
                   IllegalArgumentException
        {
            super(provider.toString(),
                  provider,
                  new Arguments(args).useLocking);
            myArguments = new Arguments(args);

            // Multi-threading doesn't yet work in the in-process world as it
            // causes segfaults because there's no threading protection
            if (myArguments.numWorkers != 0) {
                throw new IllegalArgumentException(
                    "Multi-threading not supporting for in-process instances"
                );
            }
        }

        /**
         * {@inheritDoc}
         */
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

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isClassBlockingOn()
        {
            return (myArguments.blockNonAllowlistedClasses != null)
                   ? myArguments.blockNonAllowlistedClasses
                   : super.isClassBlockingOn();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isClassPermitted(CharSequence className)
        {
            return
                super.isClassPermitted(className) || (
                    className != null &&
                    myArguments.additionalAllowlistedClasses.contains(
                        className.toString()
                    )
                );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isClassInjectionPermitted()
        {
            return (myArguments.allowClassInjection != null)
                   ? myArguments.allowClassInjection
                   : super.isClassInjectionPermitted();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isUserPermitted(CharSequence username)
        {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean isHostPermitted(InetAddress address)
        {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected int numWorkers()
        {
            // We don't support multiple workers and multi-threading in the
            // in-process instance. Note that this method can be called in the
            // super CTOR if use-locking is enabled; this means that we have a
            // bootstrapping problem which needs to be fixed if we need to
            // reference myArguments here.
            return 0;
        }
    }

    /**
     * The pipe.
     */
    public static class BidirectionalPipe
    {
        /**
         * Set to true when closed.
         */
        private volatile boolean myIsClosed;

        /**
         * The input pipe going from outside into us.
         */
        private final InputStream myJavaInputStream;

        /**
         * The output pipe going from us to the outside.
         */
        private final OutputStream myJavaOutputStream;

        /**
         * The input pipe going us to the outside.
         */
        private final InputStream myPythonInputStream;

        /**
         * The output pipe going from the outside to us.
         */
        private final OutputStream myPythonOutputStream;

        /**
         * Constructor.
         */
        private BidirectionalPipe()
        {
            // We are open, or will be when we are out of the CTOR anyhow
            myIsClosed = false;

            // Create and hook up the different ends
            final BlockingPipe in  = new BlockingPipe(64 * 1024);
            final BlockingPipe out = new BlockingPipe(64 * 1024);
            myJavaInputStream    = in.getInputStream();
            myJavaOutputStream   = out.getOutputStream();
            myPythonInputStream  = out.getInputStream();
            myPythonOutputStream = in.getOutputStream();
        }

        /**
         * Close the pipe.
         */
        public void close()
        {
            // Close all the pipes
            try { myJavaInputStream   .close(); } catch (IOException e) { }
            try { myJavaOutputStream  .close(); } catch (IOException e) { }
            try { myPythonInputStream .close(); } catch (IOException e) { }
            try { myPythonOutputStream.close(); } catch (IOException e) { }
            myIsClosed = true;
        }

        /**
         * Whether the pipe has been {@code close()}'d.
         *
         * @return whether the pipe is closed.
         */
        public boolean isClosed()
        {
            return myIsClosed;
        }

        /**
         * Read a byte from the pipe (from the outside world).
         *
         * @return the byte, or -1 if EOF
         *
         * @throws IOException if there was a problem.
         */
        public synchronized int read()
            throws IOException
        {
            return myPythonInputStream.read();
        }

        /**
         * Write a byte into pipe (from the outside world).
         *
         * @param b  The byte to write.
         *
         * @throws IOException if there was a problem.
         */
        public synchronized void write(int b)
            throws IOException
        {
            myPythonOutputStream.write(b);
        }

        /**
         * Get the Java input stream.
         *
         * @return the input stream.
         */
        protected InputStream getJavaInputStream()
        {
            return myJavaInputStream;
        }

        /**
         * Get the Java output stream.
         *
         * @return the output stream.
         */
        protected OutputStream getJavaOutputStream()
        {
            return myJavaOutputStream;
        }
    }

    /**
     * The pipes waiting to connect.
     */
    private volatile BlockingQueue<BidirectionalPipe> myPendingPipes;

    /**
     * CTOR.
     *
     * @throws IOException if there was a problem.
     */
    public PipedProvider()
        throws IOException
    {
        myPendingPipes = new ArrayBlockingQueue<>(8);
    }

    /**
     * Allow the outside world to get a Pipe instance to talk down. Blocks
     * until the other side is close to being ready to service it.
     *
     * @return the new connection.
     *
     * @throws IOException if there was a problem.
     */
    public BidirectionalPipe newConnection()
        throws IOException
    {
        final BidirectionalPipe pipe = new BidirectionalPipe();
        for (BlockingQueue<BidirectionalPipe> pipes = myPendingPipes;
             pipes != null;
             pipes = myPendingPipes)
        {
            try {
                pipes.put(pipe);
                return pipe;
            }
            catch (InterruptedException e) {
                // Just try again
            }
        }

        // If we got here then we have been closed
        throw new IOException("Provider is closed");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transport accept()
        throws IOException
    {
        for (BlockingQueue<BidirectionalPipe> pipes = myPendingPipes;
             pipes != null;
             pipes = myPendingPipes)
        {
            try {
                return new PipedTransport(pipes.take());
            }
            catch (InterruptedException e) {
                // Just try again
            }
        }

        // If we got here then we have been closed
        throw new IOException("Provider is closed");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        // Null the queue out so that new connections can't be made
        final BlockingQueue<BidirectionalPipe> pipes = myPendingPipes;
        myPendingPipes = null;

        // Now clear the queue
        for (BidirectionalPipe pipe : pipes) {
            pipe.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed()
    {
        return (myPendingPipes == null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "BidirectionalPipe";
    }
}
