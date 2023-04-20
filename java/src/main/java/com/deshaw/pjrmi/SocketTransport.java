package com.deshaw.pjrmi;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.Socket;

/**
 * A raw socket transport.
 */
/*package*/ class SocketTransport
    implements Transport
{
    /**
     * Our socket.
     */
    private final Socket mySocket;

    /**
     * Our description.
     */
    private final String myString;

    /**
     * CTOR.
     */
    public SocketTransport(final Socket socket)
    {
        mySocket = socket;
        myString = String.valueOf(socket.getRemoteSocketAddress());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getRemoteAddress()
    {
        return mySocket.getInetAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUserName()
    {
        // There's no way to know..!
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream()
        throws IOException
    {
        return mySocket.getInputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream getOutputStream()
        throws IOException
    {
        return mySocket.getOutputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        try {
            mySocket.close();
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
        return mySocket.isClosed();
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
    public boolean isLocalhost()
    {
        // Socket transport could happen on the same host, but we'll assume it
        // doesn't for now. We'll look into determining this at a later date.
        return false;
    }
}
