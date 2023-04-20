package com.deshaw.pjrmi;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * An abstraction of the data transport which is used to allow the Python
 * instance to talk to us.
 *
 * <p>This mainly provides an {@link InputStream} and {@link OutputStream} once
 * a client has connected to us.
 */
public interface Transport
{
    /**
     * What provides us with a transport. This mainly sits there, waiting for a
     * client to connect.
     */
    public static interface Provider
    {
        /**
         * The arguments to the provider.
         */
        public static class Arguments
        {
            /**
             * The set of classes to allow, if blocking is enabled.
             */
            public Set<Object> additionalAllowlistedClasses;

            /**
             * Whether to allow class injection.
             */
            public Boolean allowClassInjection;

            /**
             * Whether to block access to non-allowlisted classes.
             */
            public Boolean blockNonAllowlistedClasses;

            /**
             * The number of workers to use.
             */
            public int numWorkers;

            /**
             * Whether to use locking.
             */
            public boolean useLocking;

            /**
             * Constructor, which will parse the given arguments.
             *
             * @throws IllegalArgumentException if a bad argument was supplied.
             */
            public Arguments(final String... args)
            {
                // Defaults
                additionalAllowlistedClasses = new HashSet<>();
                allowClassInjection          = null;
                blockNonAllowlistedClasses   = null;
                numWorkers                   = 0;
                useLocking                   = false;

                // And parse
                if (args != null && args.length > 0) {
                    for (String arg : args) {
                        if (arg.startsWith("additional_allowlisted_classes=")) {
                            additionalAllowlistedClasses.addAll(
                                Arrays.asList(arg.substring(31).split(","))
                            );
                        }
                        else if (arg.startsWith("allow_class_injection=")) {
                            allowClassInjection =
                                Boolean.valueOf(arg.substring(22));
                        }
                        else if (arg.startsWith("block_non_allowlisted_classes=")) {
                            blockNonAllowlistedClasses =
                                Boolean.valueOf(arg.substring(30));
                        }
                        else if (arg.startsWith("num_workers=")) {
                            numWorkers =
                                Integer.valueOf(arg.substring(12));
                        }
                        else if (arg.startsWith("use_locking=")) {
                            useLocking =
                                Boolean.valueOf(arg.substring(12));
                        }
                        else {
                            throw new IllegalArgumentException(
                                "Unhandled argument: " + arg
                            );
                        }
                    }
                }
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public String toString()
            {
                return
                    "additional_allowlisted_classes=" +
                        additionalAllowlistedClasses + " " +
                    "allow_class_injection=" +
                        allowClassInjection + " " +
                    "block_non_allowlisted_classes=" +
                        blockNonAllowlistedClasses + " " +
                    "num_workers=" +
                        numWorkers + " " +
                    "use_locking=" +
                        useLocking;
            }
        }

        /**
         * Accept a connection from a client.
         *
         * @return The resultant transport instance.
         *
         * @throws IOException if there was a problem.
         */
        public Transport accept()
            throws IOException;

        /**
         * A brief description of this Provider.
         *
         * @return The description.
         */
        @Override
        public String toString();

        /**
         * Releases the resources related to this provider.
         *
         * @throws IOException raises this exception if the provider is
         *                     unable to release the resources.
         */
        public void close()
            throws IOException;

        /**
         * Whether the provider has been {@code close()}'d.
         *
         * @return whether the provider is closed.
         */
        public boolean isClosed();
    }

    /**
     * Get the <i>authenticated</i> username of the client at the end of the
     * transport, or null if this does not exist.
     *
     * @return the username.
     */
    public String getUserName();

    /**
     * Get the {@link InetAddress} of the remote client, if any.
     *
     * @return The {@link InetAddress} from which the client connection is
     *         being made, or {@code null} if it could not be determined.
     */
    public InetAddress getRemoteAddress();

    /**
     * Get a handle on the InputStream.
     *
     * @throws IOException if there was a problem.
     */
    public InputStream getInputStream()
        throws IOException;

    /**
     * Get a handle on the OutputStream.
     *
     * @throws IOException if there was a problem.
     */
    public OutputStream getOutputStream()
        throws IOException;

    /**
     * Close the transport; this renders it unusable.
     */
    public void close();

    /**
     * Whether the connection has been {@code close()}'d.
     *
     * @return whether the transport is closed.
     */
    public boolean isClosed();

    /**
     * A brief description of this Transport.
     *
     * @return the description.
     */
    @Override
    public String toString();

    /**
     * Returns whether we are guaranteed to be on the same host. Might
     * return {@code false} even if we are but never {@code true} if we are
     * not.
     *
     * @return {@code true} if on localhost.
     */
    public boolean isLocalhost();
}
