package com.deshaw.pjrmi;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * An inter-thread piped transport.
 */
/*package*/ class PipedTransport
    implements Transport
{
    /**
     * Our pipe.
     */
    private final PipedProvider.BidirectionalPipe myPipe;

    /**
     * CTOR.
     */
    public PipedTransport(final PipedProvider.BidirectionalPipe pipe)
    {
        myPipe = pipe;
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
        return myPipe.getJavaInputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream getOutputStream()
        throws IOException
    {
        return myPipe.getJavaOutputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        try {
            myPipe.close();
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
        return myPipe.isClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myPipe.toString();
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
