// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
//
package com.rabbitmq.mqtt.test;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.paho.mqttv5.common.packet.MqttReturnCode.RETURN_CODE_BAD_USERNAME_OR_PASSWORD;

import com.rabbitmq.client.*;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import javax.net.SocketFactory;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.internal.NetworkModule;
import org.eclipse.paho.mqttv5.client.internal.TCPNetworkModule;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttPingReq;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.MqttWireMessage;
import org.junit.jupiter.api.*;

/***
 *  MQTT v5 tests
 *
 */

public class MqttV5Test implements MqttCallback {

  private static final Duration EXPECT_TIMEOUT = Duration.ofSeconds(10);

  private final String host = "localhost";
  private final String brokerUrl = "tcp://" + host + ":" + getPort();
  private final String brokerThreeUrl = "tcp://" + host + ":" + getThirdPort();
  private volatile List<MqttMessage> receivedMessages;

  private final byte[] payload = "payload".getBytes();
  private final String topic = "test-topic";
  private final String retainedTopic = "test-retained-topic";

  private Connection conn;
  private Channel ch;

  private static int getPort() {
    Object port = System.getProperty("mqtt.port", "1883");
    assertThat(port).isNotNull();
    return Integer.parseInt(port.toString());
  }

  private static int getThirdPort() {
    Object port = System.getProperty("mqtt.port.3", "1883");
    return Integer.parseInt(port.toString());
  }

  private static int getAmqpPort() {
    Object port = System.getProperty("amqp.port", "5672");
    assertThat(port).isNotNull();
    return Integer.parseInt(port.toString());
  }

  // override the 10s limit
  private static class TestMqttConnectOptions extends MqttConnectionOptions {
    private int keepAliveInterval = 60;

    public TestMqttConnectOptions() {
      super.setUserName("guest");
      super.setPassword("guest".getBytes());
      super.setTopicAliasMaximum(0);
      super.setKeepAliveInterval(keepAliveInterval);
    }

    @Override
    public void setKeepAliveInterval(int keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
    }

    @Override
    public int getKeepAliveInterval() {
      return this.keepAliveInterval;
    }
  }

  private MqttClient newClient(TestInfo testInfo) throws MqttException {
    return newClient(clientId(testInfo));
  }

  private MqttClient newClient(String client_id) throws MqttException {
    return newClient(brokerUrl, client_id);
  }

  private MqttClient newClient(String uri, TestInfo testInfo) throws MqttException {
    return newClient(uri, clientId(testInfo));
  }

  private MqttClient newClient(String uri, String client_id) throws MqttException {
    return new MqttClient(uri, client_id, null);
  }

  private MqttClient newConnectedClient(TestInfo testInfo) throws MqttException {
    return newConnectedClient(clientId(testInfo), options());
  }

  private MqttClient newConnectedClient(TestInfo testInfo, MqttConnectionOptions conOpt)
      throws MqttException {
    return newConnectedClient(clientId(testInfo), conOpt);
  }

  private MqttClient newConnectedClient(String client_id) throws MqttException {
    MqttClient client = newClient(brokerUrl, client_id);
    client.connect(options());
    return client;
  }

  private MqttClient newConnectedClient(String client_id, MqttConnectionOptions conOpt)
      throws MqttException {
    MqttClient client = newClient(brokerUrl, client_id);
    client.connect(conOpt);
    return client;
  }

  private static String clientId(TestInfo info) {
    return "test-" + info.getTestMethod().get().getName();
  }

  private void disconnect(MqttClient client) {
    try {
      if (client.isConnected()) {
        client.disconnect(5000);
      }
    } catch (Exception ignored) {
    }
  }

  @BeforeEach
  public void setUp() {
    receivedMessages = Collections.synchronizedList(new ArrayList<>());
  }

  @AfterEach
  public void tearDown() {
    // clean any sticky sessions
    receivedMessages.clear();
  }

  private void setUpAmqp() throws IOException, TimeoutException {
    int port = getAmqpPort();
    ConnectionFactory cf = new ConnectionFactory();
    cf.setHost(host);
    cf.setPort(port);
    conn = cf.newConnection();
    ch = conn.createChannel();
  }

  private void tearDownAmqp() throws IOException {
    if (conn.isOpen()) {
      conn.close();
    }
  }

  @Test
  public void connectFirst() throws MqttException, IOException {
    NetworkModule networkModule =
        new TCPNetworkModule(SocketFactory.getDefault(), host, getPort(), "");
    networkModule.start();
    DataInputStream in = new DataInputStream(networkModule.getInputStream());
    OutputStream out = networkModule.getOutputStream();

    MqttWireMessage message = new MqttPingReq();

    try {
      // ---8<---
      // Copy/pasted from write() in MqttOutputStream.java.
      byte[] bytes = message.getHeader();
      byte[] pl = message.getPayload();
      out.write(bytes, 0, bytes.length);

      int offset = 0;
      int chunckSize = 1024;
      while (offset < pl.length) {
        int length = Math.min(chunckSize, pl.length - offset);
        out.write(pl, offset, length);
        offset += chunckSize;
      }
      // ---8<---

      // ---8<---
      // Copy/pasted from flush() in MqttOutputStream.java.
      out.flush();
      // ---8<---

      // ---8<---
      // Copy/pasted from readMqttWireMessage() in MqttInputStream.java.
      in.readByte();
      // ---8<---

      fail("Error expected if CONNECT is not first packet");
    } catch (IOException ignored) {
    }
  }

  @Test
  public void invalidUser(TestInfo info) throws MqttException {
    MqttConnectionOptions client_opts = options();
    client_opts.setUserName("invalid-user");
    MqttClient client = newClient(info);
    try {
      client.connect(client_opts);
      fail("Authentication failure expected");
    } catch (MqttException ex) {
      assertThat(ex.getReasonCode()).isEqualTo(RETURN_CODE_BAD_USERNAME_OR_PASSWORD);
    } finally {
      if (client.isConnected()) {
        disconnect(client);
      }
    }
  }

  @Test
  public void invalidPassword(TestInfo info) throws MqttException {
    MqttConnectionOptions client_opts = options();
    client_opts.setUserName("invalid-user");
    client_opts.setPassword("invalid-password".getBytes());
    MqttClient client = newClient(info);
    try {
      client.connect(client_opts);
      fail("Authentication failure expected");
    } catch (MqttException ex) {
      assertThat(ex.getReasonCode()).isEqualTo(RETURN_CODE_BAD_USERNAME_OR_PASSWORD);
    } finally {
      if (client.isConnected()) {
        disconnect(client);
      }
    }
  }

  @Test
  public void emptyPassword(TestInfo info) throws MqttException {
    MqttConnectionOptions client_opts = options();
    client_opts.setPassword("".getBytes());

    MqttClient client = newClient(info);
    try {
      client.connect(client_opts);
      fail("Authentication failure expected");
    } catch (MqttException ex) {
      assertThat(ex.getReasonCode()).isEqualTo(RETURN_CODE_BAD_USERNAME_OR_PASSWORD);
    }
  }

  @Test
  public void subscribeQos0(TestInfo info) throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe(topic, 0);

    publish(client, topic, 0, payload);
    waitAtMost(() -> receivedMessagesSize() == 1);
    MqttMessage message = firstMessage();
    assertThat(message.getPayload()).isEqualTo(payload);
    assertThat(message.getQos()).isEqualTo(0);
    disconnect(client);
  }

  @Test
  public void subscribeUnsubscribe(TestInfo info) throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe(topic, 0);

    publish(client, topic, 1, payload);

    waitAtMost(() -> receivedMessagesSize() == 1);
    MqttMessage message = firstMessage();
    assertThat(message.getPayload()).isEqualTo(payload);
    assertThat(message.getQos()).isEqualTo(0);

    client.unsubscribe(topic);
    publish(client, topic, 0, payload);
    waitAtMost(() -> receivedMessagesSize() == 1);
    disconnect(client);
  }

  @Test
  public void subscribeQos1(TestInfo info) throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe(topic, 1);

    publish(client, topic, 0, payload);
    publish(client, topic, 1, payload);
    publish(client, topic, 2, payload);

    waitAtMost(() -> receivedMessagesSize() == 3);

    MqttMessage msg1 = message(0);
    MqttMessage msg2 = message(1);
    MqttMessage msg3 = message(2);

    assertThat(msg1.getPayload()).isEqualTo(payload);
    assertThat(msg1.getQos()).isEqualTo(0);

    assertThat(msg2.getPayload()).isEqualTo(payload);
    assertThat(msg2.getQos()).isEqualTo(1);

    // Downgraded QoS 2 to QoS 1
    assertThat(msg3.getPayload()).isEqualTo(payload);
    assertThat(msg3.getQos()).isEqualTo(1);

    disconnect(client);
  }

  @Test
  public void subscribeReceivesRetainedMessagesWithMatchingQoS(TestInfo info)
      throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    clearRetained(client, retainedTopic);
    client.subscribe(retainedTopic, 1);

    publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));
    publishRetained(client, retainedTopic, 1, "retain 2".getBytes(StandardCharsets.UTF_8));

    waitAtMost(() -> receivedMessagesSize() == 2);
    MqttMessage lastMsg = receivedMessages.get(1);

    client.unsubscribe(retainedTopic);
    receivedMessages.clear();

    client.subscribe(retainedTopic, 1);
    waitAtMost(() -> receivedMessagesSize() == 1);
    final MqttMessage retainedMsg = firstMessage();
    assertThat(retainedMsg.getPayload()).isEqualTo(lastMsg.getPayload());

    disconnect(client);
  }

  @Test
  public void subscribeReceivesRetainedMessagesWithDowngradedQoS(TestInfo info)
      throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    clearRetained(client, retainedTopic);
    client.subscribe(retainedTopic, 1);

    publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));

    waitAtMost(() -> receivedMessagesSize() == 1);
    MqttMessage lastMsg = firstMessage();

    client.unsubscribe(retainedTopic);
    receivedMessages.clear();
    final int subscribeQoS = 0;
    client.subscribe(retainedTopic, subscribeQoS);

    waitAtMost(() -> receivedMessagesSize() == 1);
    final MqttMessage retainedMsg = firstMessage();
    assertThat(retainedMsg.getPayload()).isEqualTo(lastMsg.getPayload());
    assertThat(retainedMsg.getQos()).isEqualTo(subscribeQoS);

    disconnect(client);
  }

  @Test
  public void publishWithEmptyMessageClearsRetained(TestInfo info)
      throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    clearRetained(client, retainedTopic);
    client.subscribe(retainedTopic, 1);

    publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));
    publishRetained(client, retainedTopic, 1, "retain 2".getBytes(StandardCharsets.UTF_8));

    waitAtMost(() -> receivedMessagesSize() == 2);
    client.unsubscribe(retainedTopic);
    receivedMessages.clear();

    clearRetained(client, retainedTopic);
    client.subscribe(retainedTopic, 1);
    waitAtMost(() -> receivedMessagesSize() == 0);

    disconnect(client);
  }

  @Test
  public void topics(TestInfo info) throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe("/+/test-topic/#", 1);
    String[] cases =
        new String[] {"/pre/test-topic2", "/test-topic", "/a/test-topic/b/c/d", "/frob/test-topic"};
    List<String> expected = Arrays.asList("/a/test-topic/b/c/d", "/frob/test-topic");
    for (String example : cases) {
      publish(client, example, 0, example.getBytes());
    }
    waitAtMost(() -> receivedMessagesSize() == expected.size());
    for (MqttMessage m : receivedMessages) {
      assertThat(expected).contains(new String(m.getPayload()));
    }
    disconnect(client);
  }

  @Test
  public void sparkplugTopics(TestInfo info)
      throws MqttException, IOException, InterruptedException, TimeoutException {
    final String amqp091Topic = "spBv1___0.MACLab.DDATA.Opto22.CLX";
    final String sparkplugTopic = "spBv1.0/MACLab/+/Opto22/CLX";

    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe(sparkplugTopic, 1);

    setUpAmqp();
    ch.basicPublish("amq.topic", amqp091Topic, MessageProperties.MINIMAL_BASIC, payload);
    tearDownAmqp();

    waitAtMost(() -> receivedMessagesSize() == 1);
    disconnect(client);
  }

  @Test
  public void cleanSession(TestInfo info) throws MqttException, InterruptedException {
    MqttConnectionOptions client_opts = options();
    client_opts.setCleanStart(false);
    MqttClient client = newConnectedClient(info, client_opts);
    client.subscribe(topic, 1);
    client.disconnect();

    MqttClient client2 = newConnectedClient(info, client_opts);
    publish(client2, topic, 1, payload);
    disconnect(client2);

    client_opts.setCleanStart(true);
    client.connect(client_opts);
    client.setCallback(this);
    client.subscribe(topic, 1);

    waitAtMost(() -> receivedMessagesSize() == 0);
    client.unsubscribe(topic);
    disconnect(client);
  }

  @Test
  public void nonCleanSession(TestInfo info) throws MqttException, InterruptedException {
    String clientIdBase = clientId(info);
    MqttConnectionOptions client_opts = options();
    client_opts.setCleanStart(false);
    client_opts.setSessionExpiryInterval(10L);
    MqttClient client = newConnectedClient(clientIdBase + "-1", client_opts);
    client.subscribe(topic, 1);
    client.disconnect();

    MqttClient client2 = newConnectedClient(clientIdBase + "-2", client_opts);
    publish(client2, topic, 1, payload);
    client2.disconnect();

    client.setCallback(this);
    client.connect(client_opts);

    waitAtMost(() -> receivedMessagesSize() == 1);
    assertThat(firstMessage().getPayload()).isEqualTo(payload);
    disconnect(client);

    // clean up with clean start true
    MqttConnectionOptions cleanupOpts = options();
    MqttClient cleanupClient1 = newConnectedClient(clientIdBase + "-1", cleanupOpts);
    cleanupClient1.disconnect();
    MqttClient cleanupClient2 = newConnectedClient(clientIdBase + "-2", cleanupOpts);
    cleanupClient2.disconnect();
  }

  @Test
  public void qos1AndCleanSessionUnset(TestInfo info)
      throws MqttException, IOException, TimeoutException {
    MqttClient c = newClient(info);
    MqttConnectionOptions opts = new TestMqttConnectOptions();
    opts.setCleanStart(false);
    opts.setSessionExpiryInterval(10L);
    c.connect(opts);

    setUpAmqp();
    Channel tmpCh = conn.createChannel();

    String q = "mqtt-subscription-" + "test-qos1AndCleanSessionUnset" + "qos1";

    c.subscribe(topic, 1);
    // there is no server-sent notification about subscription
    // success so we inject a delay
    waitForTestDelay();

    // ensure the queue is declared with the arguments we expect
    // e.g. mqtt-subscription-client-3aqos0
    try {
      // first ensure the queue exists
      tmpCh.queueDeclarePassive(q);
      // then assert on properties
      tmpCh.queueDeclare(q, true, false, false, singletonMap("x-expires", 10000));
    } finally {
      if (c.isConnected()) {
        c.disconnect(3000);
      }

      Channel tmpCh2 = conn.createChannel();
      tmpCh2.queueDelete(q);
      tmpCh2.close();
      tearDownAmqp();
    }
  }

  @Test
  public void multipleClientIds(TestInfo info) throws MqttException, InterruptedException {
    MqttClient client = newConnectedClient(info);
    // uses duplicate client ID
    MqttClient client2 = newConnectedClient(info);
    // the older connection with this client ID will be closed
    waitAtMost(() -> !client.isConnected());
    disconnect(client2);
  }

  @Test
  public void multipleClusterClientIds(TestInfo info) throws MqttException, InterruptedException {
    MqttClient client1 = newConnectedClient(info);
    MqttClient client2 = newClient(brokerThreeUrl, info);
    client2.connect(options());
    waitAtMost(() -> !client1.isConnected());
    disconnect(client2);
  }

  @Test
  public void ping(TestInfo info) throws MqttException, InterruptedException {
    MqttConnectionOptions client_opts = options();
    client_opts.setKeepAliveInterval(1);
    MqttClient client = newConnectedClient(info, client_opts);
    waitAtMost(client::isConnected);
    disconnect(client);
  }

  @Test
  public void will(TestInfo info) throws MqttException, InterruptedException, IOException {
    String clientIdBase = clientId(info);
    MqttClient client2 = newConnectedClient(clientIdBase + "-2");
    client2.subscribe(topic, 1);
    client2.setCallback(this);

    final SocketFactory factory = SocketFactory.getDefault();
    final ArrayList<Socket> sockets = new ArrayList<>();
    SocketFactory testFactory =
        new SocketFactory() {
          public Socket createSocket(String s, int i) throws IOException {
            Socket sock = factory.createSocket(s, i);
            sockets.add(sock);
            return sock;
          }

          public Socket createSocket(String s, int i, InetAddress a, int i1) {
            return null;
          }

          public Socket createSocket(InetAddress a, int i) {
            return null;
          }

          public Socket createSocket(InetAddress a, int i, InetAddress a1, int i1) {
            return null;
          }

          @Override
          public Socket createSocket() {
            Socket sock = new Socket();
            sockets.add(sock);
            return sock;
          }
        };

    MqttClient client = newClient(clientIdBase + "-1");
    MqttTopic willTopic = client.getTopic(topic);

    MqttConnectionOptions opts = options();
    opts.setSocketFactory(testFactory);
    MqttMessage msg = new MqttMessage(payload, 0, false, null);
    opts.setWill(willTopic.toString(), msg);
    opts.setCleanStart(false);

    client.connect(opts);

    assertThat(sockets).hasSizeGreaterThan(0);
    sockets.get(0).close();

    waitAtMost(() -> receivedMessagesSize() == 1);
    assertThat(firstMessage().getPayload()).isEqualTo(payload);
    client2.unsubscribe(topic);
    disconnect(client2);

    // clean up with clean start true
    MqttClient cleanupClient = newConnectedClient(clientIdBase + "-1");
    disconnect(cleanupClient);
  }

  @Test
  public void willIsRetained(TestInfo info)
      throws MqttException, InterruptedException, IOException {
    String clientIdBase = clientId(info);
    MqttClient client2 = newConnectedClient(clientIdBase + "-2");
    client2.setCallback(this);

    clearRetained(client2, retainedTopic);
    client2.subscribe(retainedTopic, 1);
    disconnect(client2);

    final SocketFactory factory = SocketFactory.getDefault();
    final ArrayList<Socket> sockets = new ArrayList<>();
    SocketFactory testFactory =
        new SocketFactory() {
          public Socket createSocket(String s, int i) throws IOException {
            Socket sock = factory.createSocket(s, i);
            sockets.add(sock);
            return sock;
          }

          public Socket createSocket(String s, int i, InetAddress a, int i1) {
            return null;
          }

          public Socket createSocket(InetAddress a, int i) {
            return null;
          }

          public Socket createSocket(InetAddress a, int i, InetAddress a1, int i1) {
            return null;
          }

          @Override
          public Socket createSocket() {
            Socket sock = new Socket();
            sockets.add(sock);
            return sock;
          }
        };

    MqttConnectionOptions client_opts = new TestMqttConnectOptions();

    MqttClient client = newClient(clientIdBase + "-1");
    MqttTopic willTopic = client.getTopic(retainedTopic);
    byte[] willPayload = "willpayload".getBytes();

    client_opts.setSocketFactory(testFactory);
    MqttMessage msg = new MqttMessage(willPayload, 1, true, null);
    client_opts.setWill(willTopic.toString(), msg);

    client.connect(client_opts);

    assertThat(sockets).hasSize(1);
    sockets.get(0).close();

    // let last will propagate after disconnection
    waitForTestDelay();

    client2.connect(options());
    client2.setCallback(this);
    client2.subscribe(retainedTopic, 1);

    waitAtMost(() -> receivedMessagesSize() == 1);
    assertThat(firstMessage().getPayload()).isEqualTo(willPayload);
    client2.unsubscribe(topic);
    disconnect(client2);
  }

  @Test
  public void subscribeMultiple(TestInfo info) throws MqttException {
    String clientIdBase = clientId(info);
    MqttClient client = newConnectedClient(clientIdBase + "-1");
    publish(client, "/test-topic/1", 1, "msq1-qos1".getBytes());

    MqttClient client2 = newConnectedClient(clientIdBase + "-2");
    client2.setCallback(this);
    client2.subscribe("/test-topic/#", 1);
    client2.subscribe("/test-topic/#", 1);

    publish(client, "/test-topic/2", 0, "msq2-qos0".getBytes());
    publish(client, "/test-topic/3", 1, "msq3-qos1".getBytes());
    publish(client, "/test-topic/4", 2, "msq3-qos2".getBytes());
    publish(client, topic, 0, "msq4-qos0".getBytes());
    publish(client, topic, 1, "msq4-qos1".getBytes());

    assertThat(receivedMessages).hasSize(3);
    disconnect(client);
    disconnect(client2);
  }

  @Test
  public void publishMultiple() throws MqttException, InterruptedException {
    int pubCount = 1000;
    for (int subQos = 0; subQos <= 2; subQos++) {
      for (int pubQos = 0; pubQos <= 2; pubQos++) {
        // avoid reusing the client in this test as a shared
        // client cannot handle connection churn very well. MK.
        String cid = "test-sub-qos-" + subQos + "-pub-qos-" + pubQos;
        MqttClient client = newClient(brokerUrl, cid);
        client.connect(options());
        client.subscribe(topic, subQos);
        client.setCallback(this);
        for (int i = 0; i < pubCount; i++) {
          publish(client, topic, pubQos, payload);
        }
        waitAtMost(() -> receivedMessagesSize() == pubCount);
        client.disconnect(5000);
        receivedMessages.clear();
      }
    }
  }

  @Test
  public void topicAuthorisationPublish(TestInfo info) throws Exception {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe("some/test-topic", 1);
    publish(client, "some/test-topic", 1, "content".getBytes());
    waitAtMost(() -> receivedMessagesSize() == 1);
    assertThat(client.isConnected()).isTrue();
    try {
      publish(client, "forbidden-topic", 1, "content".getBytes());
      fail("Publishing on a forbidden topic, an exception should have been thrown");
      client.disconnect();
    } catch (MqttException e) {
      // ok
    }
  }

  @Test
  public void topicAuthorisationSubscribe(TestInfo info) throws Exception {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe("some/test-topic", 1);
    assertThat(client.isConnected()).isTrue();

    client.subscribe("forbidden-topic", 1);
    waitAtMost(
        () ->
            !client.isConnected()); // Subscribing on a forbidden topic, connection should be closed
  }

  @Test
  public void lastWillNotSentOnRestrictedTopic(TestInfo info) throws Exception {
    MqttClient client2 = newConnectedClient(info);
    // topic authorized for subscription, restricted for publishing
    String lastWillTopic = "last-will";
    client2.subscribe(lastWillTopic, 1);
    client2.setCallback(this);

    final SocketFactory factory = SocketFactory.getDefault();
    final ArrayList<Socket> sockets = new ArrayList<>();
    SocketFactory testFactory =
        new SocketFactory() {
          public Socket createSocket(String s, int i) throws IOException {
            Socket sock = factory.createSocket(s, i);
            sockets.add(sock);
            return sock;
          }

          public Socket createSocket(String s, int i, InetAddress a, int i1) {
            return null;
          }

          public Socket createSocket(InetAddress a, int i) {
            return null;
          }

          public Socket createSocket(InetAddress a, int i, InetAddress a1, int i1) {
            return null;
          }

          @Override
          public Socket createSocket() {
            Socket sock = new Socket();
            sockets.add(sock);
            return sock;
          }
        };

    MqttConnectionOptions client_opts = options();

    MqttClient client = newClient("last-will-not-sent-on-restricted-topic");
    client_opts.setSocketFactory(testFactory);
    MqttTopic willTopic = client.getTopic(lastWillTopic);
    MqttMessage msg = new MqttMessage(payload, 0, false, null);
    client_opts.setWill(willTopic.toString(), msg);
    client_opts.setCleanStart(false);
    client.connect(client_opts);

    assertThat(sockets).hasSize(1);
    sockets.get(0).close();

    // let some time after disconnection
    waitForTestDelay();
    assertThat(receivedMessages).isEmpty();
    disconnect(client2);

    // clean up with clean start true
    MqttClient cleanupClient = newConnectedClient("last-will-not-sent-on-restricted-topic");
    cleanupClient.disconnect();
  }

  @Test
  public void topicAuthorisationVariableExpansion() throws Exception {
    final String client_id = "client-id-variable-expansion";
    MqttClient client = newConnectedClient(client_id);
    client.setCallback(this);
    String topicWithExpandedVariables = "guest/" + client_id + "/a";
    client.subscribe(topicWithExpandedVariables, 1);
    publish(client, topicWithExpandedVariables, 1, "content".getBytes());
    waitAtMost(() -> receivedMessagesSize() == 1);
    assertThat(client.isConnected()).isTrue();
    try {
      publish(client, "guest/WrongClientId/a", 1, "content".getBytes());
      fail("Publishing on a forbidden topic, an exception should have been thrown");
      client.disconnect();
    } catch (Exception e) {
      // OK
    }
  }

  @Test
  public void interopM2A(TestInfo info)
      throws MqttException, IOException, InterruptedException, TimeoutException {
    setUpAmqp();
    String queue = ch.queueDeclare().getQueue();
    ch.queueBind(queue, "amq.topic", topic);

    byte[] interopPayload = "interop-body".getBytes();
    MqttClient client = newConnectedClient(info);
    publish(client, topic, 1, interopPayload);
    disconnect(client);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<byte[]> messageBody = new AtomicReference<>();
    ch.basicConsume(
        queue,
        true,
        new DefaultConsumer(ch) {
          @Override
          public void handleDelivery(
              String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
              throws IOException {
            messageBody.set(body);
            latch.countDown();
          }
        });
    assertThat(latch.await(EXPECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    assertThat(messageBody.get()).isEqualTo(interopPayload);
    assertThat(ch.basicGet(queue, true)).isNull();
    tearDownAmqp();
  }

  @Test
  public void interopA2M(TestInfo info)
      throws MqttException, IOException, InterruptedException, TimeoutException {
    MqttClient client = newConnectedClient(info);
    client.setCallback(this);
    client.subscribe(topic, 1);

    setUpAmqp();
    ch.basicPublish("amq.topic", topic, MessageProperties.MINIMAL_BASIC, payload);
    tearDownAmqp();

    waitAtMost(() -> receivedMessagesSize() == 1);
    client.disconnect();
  }

  // "A Server MAY allow a Client to supply a ClientId that has a length of zero bytes, however if
  // it does so
  // the Server MUST treat this as a special case and assign a unique ClientId to that Client."
  // [MQTT-3.1.3-6]
  // RabbitMQ allows a Client to supply a ClientId that has a length of zero bytes.
  @Test
  public void emptyClientId() throws MqttException, InterruptedException {
    String emptyClientId = "";
    MqttClient client = newConnectedClient(emptyClientId);
    MqttClient client2 = newConnectedClient(emptyClientId);
    client.setCallback(this);
    client2.setCallback(this);
    client.subscribe("/test-topic/#", 1);
    client2.subscribe("/test-topic/#", 1);

    publish(client, "/test-topic/1", 0, "my-message".getBytes());
    waitAtMost(() -> receivedMessagesSize() == 2);

    disconnect(client);
    disconnect(client2);
  }

  private void publish(MqttClient client, String topicName, int qos, byte[] payload)
      throws MqttException {
    publish(client, topicName, qos, payload, false);
  }

  private void publish(
      MqttClient client, String topicName, int qos, byte[] payload, boolean retained)
      throws MqttException {
    MqttTopic topic = client.getTopic(topicName);
    MqttMessage message = new MqttMessage(payload);
    message.setQos(qos);
    message.setRetained(retained);
    MqttToken token = topic.publish(message);
    token.waitForCompletion();
  }

  private void publishRetained(MqttClient client, String topicName, int qos, byte[] payload)
      throws MqttException {
    publish(client, topicName, qos, payload, true);
  }

  private void clearRetained(MqttClient client, String topicName) throws MqttException {
    publishRetained(client, topicName, 1, "".getBytes());
  }

  public void messageArrived(String topic, MqttMessage message) throws Exception {
    receivedMessages.add(message);
  }

  public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {}

  public void mqttErrorOccurred(MqttException e) {}

  public void deliveryComplete(IMqttToken iMqttToken) {}

  public void connectComplete(boolean b, String s) {}

  public void authPacketArrived(int i, MqttProperties mqttProperties) {}

  private Integer receivedMessagesSize() {
    return receivedMessages.size();
  }

  private void waitForTestDelay() {
    try {
      int testDelay = 2000;
      Thread.sleep(testDelay);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void waitAtMost(BooleanSupplier condition) throws InterruptedException {
    if (condition.getAsBoolean()) {
      return;
    }
    int waitTime = 100;
    int waitedTime = 0;
    long timeoutInMs = EXPECT_TIMEOUT.toMillis();
    while (waitedTime <= timeoutInMs) {
      Thread.sleep(waitTime);
      if (condition.getAsBoolean()) {
        return;
      }
      waitedTime += waitTime;
    }
    fail(
        "Waited "
            + EXPECT_TIMEOUT.get(ChronoUnit.SECONDS)
            + " second(s), condition never got true");
  }

  private MqttMessage firstMessage() {
    return message(0);
  }

  private MqttMessage message(int index) {
    return this.receivedMessages.get(index);
  }

  private MqttConnectionOptions options() {
    return new TestMqttConnectOptions();
  }
}
