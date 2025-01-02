// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
//
package com.rabbitmq.mqtt.test.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_FAILED_AUTHENTICATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.eclipse.paho.client.mqttv3.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** MQTT v3.1 tests */
public class MqttSSLTest implements MqttCallback {

  private final String brokerUrl = "ssl://" + getHost() + ":" + getPort();
  private String clientId;
  private String clientId2;
  private MqttClient client;
  private MqttClient client2;
  private MqttConnectOptions conOpt;

  private volatile List<MqttMessage> receivedMessages;
  private volatile boolean expectConnectionFailure;

  private static String getPort() {
    Object port = System.getProperty("mqtt.ssl.port");
    assertThat(port).isNotNull();
    return port.toString();
  }

  private static String getHost() {
    Object host = System.getProperty("hostname");
    assertThat(host).isNotNull();
    return host.toString();
  }

  // override 10s limit
  private static class MyConnOpts extends MqttConnectOptions {
    private int keepAliveInterval = 60;

    @Override
    public void setKeepAliveInterval(int keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
    }

    @Override
    public int getKeepAliveInterval() {
      return keepAliveInterval;
    }
  }

  @BeforeEach
  public void setUp() throws MqttException, IOException {
    clientId = getClass().getSimpleName() + ((int) (10000 * Math.random()));
    clientId2 = clientId + "-2";
    client = new MqttClient(brokerUrl, clientId, null);
    client2 = new MqttClient(brokerUrl, clientId2, null);
    conOpt = options();
    conOpt.setSocketFactory(MutualAuth.getSSLContextWithoutCert().getSocketFactory());
    setConOpts(conOpt);
    receivedMessages = Collections.synchronizedList(new ArrayList<MqttMessage>());
    expectConnectionFailure = false;
  }

  @AfterEach
  public void tearDown() throws MqttException {
    // clean any sticky sessions
    setConOpts(conOpt);
    client = new MqttClient(brokerUrl, clientId, null);
    try {
      client.connect(conOpt);
      client.disconnect();
    } catch (Exception ignored) {
    }

    client2 = new MqttClient(brokerUrl, clientId2, null);
    try {
      client2.connect(conOpt);
      client2.disconnect();
    } catch (Exception ignored) {
    }
  }

  private void setConOpts(MqttConnectOptions conOpts) {
    conOpts.setCleanSession(true);
    conOpts.setKeepAliveInterval(60);
  }

  @Test
  public void certLogin() {
    try {
      conOpt.setSocketFactory(MutualAuth.getSSLContextWithClientCert().getSocketFactory());
      client.connect(conOpt);
    } catch (Exception e) {
      fail("Exception: " + e.getMessage());
    }
  }

  @Test
  public void invalidUser() {
    conOpt.setUserName("invalid-user");
    try {
      client.connect(conOpt);
      fail("Authentication failure expected");
    } catch (MqttException ex) {
      assertThat(ex.getReasonCode()).isEqualTo(REASON_CODE_FAILED_AUTHENTICATION);
    } catch (Exception e) {
      fail("Exception: " + e.getMessage());
    }
  }

  @Test
  public void invalidPassword() {
    conOpt.setUserName("invalid-user");
    conOpt.setPassword("invalid-password".toCharArray());
    try {
      client.connect(conOpt);
      fail("Authentication failure expected");
    } catch (MqttException ex) {
      assertThat(ex.getReasonCode()).isEqualTo(REASON_CODE_FAILED_AUTHENTICATION);
    } catch (Exception e) {
      fail("Exception: " + e.getMessage());
    }
  }

  public void connectionLost(Throwable cause) {
    if (!expectConnectionFailure) {
      fail("Connection unexpectedly lost");
    }
  }

  public void messageArrived(String topic, MqttMessage message) {
    receivedMessages.add(message);
  }

  public void deliveryComplete(IMqttDeliveryToken token) {}

  private MqttConnectOptions options() {
    return new MyConnOpts();
  }
}
