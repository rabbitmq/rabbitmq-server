// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
//
package com.rabbitmq.mqtt.test.tls;

import static java.nio.file.Files.newInputStream;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class MutualAuth {

  private MutualAuth() {}

  private static String getStringProperty(String propertyName) throws IllegalArgumentException {
    Object value = System.getProperty(propertyName);
    if (value == null)
      throw new IllegalArgumentException("Property: " + propertyName + " not found");
    return value.toString();
  }

  private static TrustManagerFactory getServerTrustManagerFactory()
      throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
    String keystorePath = System.getProperty("test-keystore.ca");
    char[] trustPhrase = getStringProperty("test-keystore.password").toCharArray();

    // Server TrustStore
    KeyStore tks = KeyStore.getInstance("JKS");
    tks.load(newInputStream(Paths.get(keystorePath)), trustPhrase);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(tks);

    return tmf;
  }

  public static SSLContext getSSLContextWithClientCert() throws IOException {

    char[] clientPhrase = getStringProperty("test-client-cert.password").toCharArray();

    String p12Path = System.getProperty("test-client-cert.path");

    try {
      SSLContext sslContext = getVanillaSSLContext();
      // Client Keystore
      KeyStore ks = KeyStore.getInstance("PKCS12");
      ks.load(newInputStream(Paths.get(p12Path)), clientPhrase);
      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(ks, clientPhrase);

      sslContext.init(
          kmf.getKeyManagers(), getServerTrustManagerFactory().getTrustManagers(), null);
      return sslContext;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static SSLContext getVanillaSSLContext() throws NoSuchAlgorithmException {
    List<String> xs = Arrays.asList("TLSv1.2", "TLSv1.1", "TLSv1");
    for (String x : xs) {
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
