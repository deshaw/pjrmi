package com.deshaw.pjrmi;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import java.util.List;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import static com.deshaw.pjrmi.PJRmi.LOG;

/**
 * An SSL socket transport.
 */
/*package*/ class SSLSocketTransport
    extends SocketTransport
{
    /**
     * The LDAP entry key for the "common name".
     */
    private static final String COMMON_NAME = "CN";

    /**
     * Our socket.
     */
    private final SSLSocket mySocket;

    /**
     * CTOR.
     */
    public SSLSocketTransport(final SSLSocket socket)
    {
        super(socket);
        mySocket = socket;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUserName()
    {
        // Look through all the certificates for the one with the common name in
        // it, which we interpret as the username
        try {
            for (Certificate cert : mySocket.getSession().getPeerCertificates()) {
                // This has to be an x509 certificate for us to process it. If
                // it's not then there's not a lot that we can do.
                final X509Certificate x509 = (X509Certificate)cert;

                // We want the "common name" out of it. This can be obtained by
                // parsing the principal name as an LDAP name. Avoid overhead
                // from implicit iterators when we loop over the its components.
                final LdapName ldapName =
                    new LdapName(x509.getSubjectX500Principal().getName());
                final List<Rdn> rdns = ldapName.getRdns();
                for (int i=0; i < rdns.size(); i++) {
                    // Look for the "common name" entry and, if we find a valid
                    // one, return its associated value
                    final Rdn rdn = rdns.get(i);
                    if (COMMON_NAME.equals(rdn.getType()) &&
                        rdn.getValue() != null)
                    {
                        return rdn.getValue().toString();
                    }
                }
            }
        }
        catch (ClassCastException   |
               InvalidNameException |
               SSLPeerUnverifiedException e)
        {
            // Nothing that we can do except to just fall out of to the end and
            // return null
            LOG.severe("Failed to extract username from connection: " + e);
        }

        // If we got here then we could not find it
        LOG.warning(
            "Failed to find the username from the connection " + mySocket
        );
        return null;
    }

    /**
     * Get the certificates associated with this connection.
     *
     * @throws SSLPeerUnverifiedException if the peer's credentials have not
     *                                    been authoritatively checked.
     */
    public Certificate[] getPeerCertificates()
        throws SSLPeerUnverifiedException
    {
        return mySocket.getSession().getPeerCertificates();
    }
}
