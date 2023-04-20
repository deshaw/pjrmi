package com.deshaw.pjrmi;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * An inter-process unix FIFO transport.
 */
/*package*/ class UnixFifoTransport
    implements Transport
{
    /**
     * Our fifo.
     */
    private final UnixFifoProvider.Fifo myFifo;

    /**
     * CTOR.
     */
    public UnixFifoTransport(final UnixFifoProvider.Fifo fifo)
    {
        myFifo = fifo;
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
        return myFifo.getInputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream getOutputStream()
        throws IOException
    {
        return myFifo.getOutputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        try {
            myFifo.close();
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
        return myFifo.isClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myFifo.toString();
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
