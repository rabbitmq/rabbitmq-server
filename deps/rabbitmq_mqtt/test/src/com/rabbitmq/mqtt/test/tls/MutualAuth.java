package com.rabbitmq.mqtt.test.tls;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;

public class MutualAuth {

    private MutualAuth() {

    }

    private static String getStringProperty(String propertyName) throws IllegalArgumentException {
        Object value = System.getProperty(propertyName);
        if (value == null) throw new IllegalArgumentException("Property: " + propertyName + " not found");
        return value.toString();
    }

    private static TrustManagerFactory getServerTrustManagerFactory() throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        char[] trustPhrase = getStringProperty("server.keystore.passwd").toCharArray();
        MutualAuth dummy = new MutualAuth();

        // Server TrustStore
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load(dummy.getClass().getResourceAsStream("/server.jks"), trustPhrase);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
        tmf.init(tks);

        return tmf;
    }

    public static SSLContext getSSLContextWithClientCert() throws IOException {

        char[] clientPhrase = getStringProperty("client.keystore.passwd").toCharArray();

        MutualAuth dummy = new MutualAuth();
        try {
            SSLContext sslContext = getVanillaSSLContext();
            // Client Keystore
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(dummy.getClass().getResourceAsStream("/client.jks"), clientPhrase);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, clientPhrase);

            sslContext.init(kmf.getKeyManagers(), getServerTrustManagerFactory().getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    private static SSLContext getVanillaSSLContext() throws NoSuchAlgorithmException {
        SSLContext result = null;
        List<String> xs = Arrays.asList("TLSv1.2", "TLSv1.1", "TLSv1");
        for(String x : xs) {
            try {
                return SSLContext.getInstance(x);
            } catch (NoSuchAlgorithmException nae) {
                // keep trying
            }
        }
        throw new NoSuchAlgorithmException("Could not obtain an SSLContext for TLS 1.0-1.2");
    }

    public static SSLContext getSSLContextWithoutCert() throws IOException {
        try {
            SSLContext sslContext = getVanillaSSLContext();
            sslContext.init(null, getServerTrustManagerFactory().getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
