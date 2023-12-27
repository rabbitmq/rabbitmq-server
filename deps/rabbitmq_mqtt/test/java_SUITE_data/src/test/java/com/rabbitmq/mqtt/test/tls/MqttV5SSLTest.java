// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
//
package com.rabbitmq.mqtt.test.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.paho.mqttv5.common.packet.MqttReturnCode.RETURN_CODE_BAD_USERNAME_OR_PASSWORD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** MQTT v5 TLS test */
public class MqttV5SSLTest implements MqttCallback {

  private final String brokerUrl = "ssl://" + getHost() + ":" + getPort();
  private String clientId;
  private String clientId2;
  private MqttClient client;
  private MqttClient client2;
  private MqttConnectionOptions conOpt;

  private volatile List<MqttMessage> receivedMessages;

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
  private static class MyConnOpts extends MqttConnectionOptions {
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

  private void setConOpts(MqttConnectionOptions conOpts) {
    conOpts.setCleanStart(true);
    conOpts.setKeepAliveInterval(60);
  }

  @Test
  public void certLogin() {
    try {
      conOpt.setSocketFactory(MutualAuth.getSSLContextWithClientCert().getSocketFactory());
      client.connect(conOpt);
    } catch (Exception e) {
      e.printStackTrace();
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
      assertThat(ex.getReasonCode()).isEqualTo(RETURN_CODE_BAD_USERNAME_OR_PASSWORD);
    } catch (Exception e) {
      fail("Exception: " + e.getMessage());
    }
  }

  @Test
  public void invalidPassword() {
    conOpt.setUserName("invalid-user");
    conOpt.setPassword("invalid-password".getBytes());
    try {
      client.connect(conOpt);
      fail("Authentication failure expected");
    } catch (MqttException ex) {
      assertThat(ex.getReasonCode()).isEqualTo(RETURN_CODE_BAD_USERNAME_OR_PASSWORD);
    } catch (Exception e) {
      fail("Exception: " + e.getMessage());
    }
  }

  public void messageArrived(String topic, MqttMessage message) throws Exception {
    receivedMessages.add(message);
  }

  public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {}

  public void mqttErrorOccurred(MqttException e) {}

  public void deliveryComplete(IMqttToken iMqttToken) {}

  public void connectComplete(boolean b, String s) {}

  public void authPacketArrived(int i, MqttProperties mqttProperties) {}

  private MqttConnectionOptions options() {
    return new MyConnOpts();
  }
}
