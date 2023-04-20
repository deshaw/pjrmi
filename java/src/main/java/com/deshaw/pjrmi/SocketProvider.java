package com.deshaw.pjrmi;

import java.io.IOException;

import java.net.ServerSocket;

/**
 * A transport provider for a raw socket connection.
 *
 * <p>This is probably the most basic use-case for RMI, but it provides
 * <b>absolutely no</b> user authentication. (And, as such, there is nothing to
 * stop anyone from connecting to your PJRmi instance and running arbitrary code
 * and/or commands from it.) Given this you should probably not really be using
 * it for anything other than testing etc.
 */
public class SocketProvider
    implements Transport.Provider
{
    /**
     * The server socket which we use.
     */
    private final ServerSocket myServerSocket;

    /**
     * Our string representation.
     */
    private final String myString;

    /**
     * CTOR.
     *
     * @param port  The port to listen on.
     *
     * @throws IOException if there was a problem.
     */
    public SocketProvider(int port)
        throws IOException
    {
        myServerSocket = new ServerSocket(port);
        myString       = "Socket[" + port + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transport accept()
        throws IOException
    {
        return new SocketTransport(myServerSocket.accept());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myString;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
        throws IOException
    {
        myServerSocket.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed()
    {
        return myServerSocket.isClosed();
    }
}
