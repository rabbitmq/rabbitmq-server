package com.rabbitmq.mqtt.test.tls;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

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
            SSLContext sslContext = SSLContext.getInstance("TLSV1.2");
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

    public static SSLContext getSSLContext() throws IOException {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLSV1.2");
            sslContext.init(null, getServerTrustManagerFactory().getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
