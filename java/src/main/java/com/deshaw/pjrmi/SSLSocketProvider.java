package com.deshaw.pjrmi;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import java.util.Objects;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/**
 * A transport provider for an SSL socket connection.
 */
public class SSLSocketProvider
    implements Transport.Provider
{
    /**
     * The default TLS version which we use.
     */
    private static final String TLS_VERSION = "TLSv1.2";

    /**
     * The server socket which we use.
     */
    private final SSLServerSocket myServerSocket;

    /**
     * Our string representation.
     */
    private final String myString;

    /**
     * CTOR using the default TLS 1.2 version.
     */
    public SSLSocketProvider(final int    port,
                             final String storeFilename,
                             final String storePassword)
        throws IOException
    {
        this(port, TLS_VERSION, storeFilename, storePassword);
    }

    /**
     * CTOR.
     */
    public SSLSocketProvider(final int    port,
                             final String tlsVersion,
                             final String storeFilename,
                             final String storePassword)
        throws IOException
    {
        // We need these, but we'll allow missing/empty passwords
        Objects.requireNonNull(tlsVersion,
                               "TLS version was null");
        Objects.requireNonNull(storeFilename,
                               "Store filename was null");

        // Our descriptive string
        myString = "SSLSocket[" +
            port          + ":" +
            tlsVersion    + ":" +
            storeFilename +
        "]";

        // Build our factories by reading in the store's contents
        final KeyManagerFactory keyFactory;
        final TrustManagerFactory trustFactory;
        try (InputStream is = new FileInputStream(storeFilename)) {
            // We'll need the password as a char[]
            final char[] password;
            if (storePassword != null) {
                password = new char[storePassword.length()];
                storePassword.getChars(0, password.length, password, 0);
            }
            else {
                password = new char[0];
            }

            // Open the store
            final KeyStore store =
                KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(is, password);

            // And create our two factories
            keyFactory = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm()
            );
            keyFactory.init(store, password);
            trustFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm()
            );
            trustFactory.init(store);
        }
        catch (CertificateException     |
               KeyStoreException        |
               NoSuchAlgorithmException |
               UnrecoverableKeyException e)
        {
            throw new IOException(
                "Error reading store " + storeFilename,
                e
            );
        }

        // Create the SSL socket
        try {
            final SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyFactory  .getKeyManagers(),
                         trustFactory.getTrustManagers(),
                         SecureRandom.getInstanceStrong());

            final SSLServerSocketFactory socketFactory =
                context.getServerSocketFactory();
            myServerSocket =
                (SSLServerSocket)socketFactory.createServerSocket(port);
            myServerSocket.setNeedClientAuth(true);
            myServerSocket.setEnabledProtocols(new String[] {tlsVersion});
        }
        catch (KeyManagementException |
               NoSuchAlgorithmException e)
        {
            throw new IOException("Error creating SSL context", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transport accept()
        throws IOException
    {
        try {
            return new SSLSocketTransport((SSLSocket)myServerSocket.accept());
        }
        catch (ClassCastException e) {
            // Should not happen
            throw new IOException(e);
        }
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
