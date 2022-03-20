// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
//

package com.rabbitmq.mqtt.test;

import com.rabbitmq.client.*;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.NetworkModule;
import org.eclipse.paho.client.mqttv3.internal.TCPNetworkModule;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPingReq;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import javax.net.SocketFactory;
import java.io.*;
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

import static org.junit.jupiter.api.Assertions.*;

/***
 *  MQTT v3.1 tests
 *
 */

public class MqttTest implements MqttCallback {

    private static final Duration EXPECT_TIMEOUT = Duration.ofSeconds(10);

    private final String host = "localhost";
    private final String brokerUrl = "tcp://" + host + ":" + getPort();
    private final String brokerThreeUrl = "tcp://" + host + ":" + getThirdPort();
    private volatile List<MqttMessage> receivedMessages;

    private final byte[] payload = "payload".getBytes();
    private final String topic = "test-topic";
    private final String retainedTopic = "test-retained-topic";
    private int testDelay = 2000;

    private volatile long lastReceipt;
    private volatile boolean expectConnectionFailure;
    private volatile boolean failOnDelivery = false;

    private Connection conn;
    private Channel ch;

    private static int getPort() {
        Object port = System.getProperty("mqtt.port", "1883");
        assertNotNull(port);
        return Integer.parseInt(port.toString());
    }

    private static int getThirdPort() {
        Object port = System.getProperty("mqtt.port.3", "1883");
        assertNotNull(port);
        return Integer.parseInt(port.toString());
    }

    private static int getAmqpPort() {
        Object port = System.getProperty("amqp.port", "5672");
        assertNotNull(port);
        return Integer.parseInt(port.toString());
    }

    // override the 10s limit
    private class TestMqttConnectOptions extends MqttConnectOptions {
        private int keepAliveInterval = 60;
        private final String user_name = "guest";
        private final String password = "guest";

        public TestMqttConnectOptions() {
            super.setUserName(user_name);
            super.setPassword(password.toCharArray());
            super.setCleanSession(true);
            super.setKeepAliveInterval(60);
            // PublishMultiple overwhelms Paho defaults
            super.setMaxInflight(15000);
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

    private MqttClient newConnectedClient(TestInfo testInfo, MqttConnectOptions conOpt) throws MqttException {
        return newConnectedClient(clientId(testInfo), conOpt);
    }

    private MqttClient newConnectedClient(String client_id, MqttConnectOptions conOpt) throws MqttException {
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
        } catch (Exception ignored) {}
    }

    @BeforeEach
    public void setUp() {
        receivedMessages = Collections.synchronizedList(new ArrayList<>());
        expectConnectionFailure = false;
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
        NetworkModule networkModule = new TCPNetworkModule(SocketFactory.getDefault(), host, getPort(), "");
        networkModule.start();
        DataInputStream in = new DataInputStream(networkModule.getInputStream());
        OutputStream out = networkModule.getOutputStream();

        MqttWireMessage message = new MqttPingReq();

        try {
            // ---8<---
            // Copy/pasted from write() in MqttOutputStream.java.
            byte[] bytes = message.getHeader();
            byte[] pl = message.getPayload();
            out.write(bytes,0,bytes.length);

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
            ByteArrayOutputStream bais = new ByteArrayOutputStream();
            byte first = in.readByte();
            // ---8<---

            fail("Error expected if CONNECT is not first packet");
        } catch (IOException ignored) {}
    }

    @Test public void invalidUser(TestInfo info) throws MqttException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setUserName("invalid-user");
        MqttClient client = newClient(info);
        try {
            client.connect(client_opts);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        } finally {
            if (client.isConnected()) {
                disconnect(client);
            }
        }
    }

    // rabbitmq/rabbitmq-mqtt#37: QoS 1, clean session = false
    @Test public void qos1AndCleanSessionUnset()
            throws MqttException, IOException, TimeoutException, InterruptedException {
        testQueuePropertiesWithCleanSessionUnset("qos1-no-clean-session", 1, true, false);
    }

    protected void testQueuePropertiesWithCleanSessionSet(String cid, int qos, boolean durable, boolean autoDelete)
            throws IOException, MqttException, TimeoutException, InterruptedException {
        testQueuePropertiesWithCleanSession(true, cid, qos, durable, autoDelete);
    }

    protected void testQueuePropertiesWithCleanSessionUnset(String cid, int qos, boolean durable, boolean autoDelete)
            throws IOException, MqttException, TimeoutException, InterruptedException {
        testQueuePropertiesWithCleanSession(false, cid, qos, durable, autoDelete);
    }

    protected void testQueuePropertiesWithCleanSession(boolean cleanSession, String cid, int qos,
                                                       boolean durable, boolean autoDelete)
            throws MqttException, IOException, TimeoutException {
        MqttClient c = newClient(brokerUrl, cid);
        MqttConnectOptions opts = new TestMqttConnectOptions();
        opts.setUserName("guest");
        opts.setPassword("guest".toCharArray());
        opts.setCleanSession(cleanSession);
        c.connect(opts);

        setUpAmqp();
        Channel tmpCh = conn.createChannel();

        String q = "mqtt-subscription-" + cid + "qos" + qos;

        c.subscribe(topic, qos);
        // there is no server-sent notification about subscription
        // success so we inject a delay
        waitForTestDelay();

        // ensure the queue is declared with the arguments we expect
        // e.g. mqtt-subscription-client-3aqos0
        try {
            // first ensure the queue exists
            tmpCh.queueDeclarePassive(q);
            // then assert on properties
            Map<String, Object> args = new HashMap<>();
            args.put("x-expires", 86400000);
            tmpCh.queueDeclare(q, durable, autoDelete, false, args);
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

    @Test public void invalidPassword(TestInfo info) throws MqttException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setUserName("invalid-user");
        client_opts.setPassword("invalid-password".toCharArray());
        MqttClient client = newClient(info);
        try {
            client.connect(client_opts);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        } finally {
            if (client.isConnected()) {
                disconnect(client);
            }
        }
    }

    @Test public void emptyPassword(TestInfo info) throws MqttException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setPassword("".toCharArray());

        MqttClient client = newClient(info);
        try {
            client.connect(client_opts);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        }
    }


    @Test public void subscribeQos0(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newClient(info);
        client.connect(client_opts);
        client.setCallback(this);
        client.subscribe(topic, 0);

        publish(client, topic, 0, payload);
        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        assertEquals(0, receivedMessages.get(0).getQos());
        disconnect(client);
    }

    @Test public void subscribeUnsubscribe(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newClient(info);
        client.connect(client_opts);
        client.setCallback(this);
        client.subscribe(topic, 0);

        publish(client, topic, 1, payload);

        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        assertEquals(0, receivedMessages.get(0).getQos());

        client.unsubscribe(topic);
        publish(client, topic, 0, payload);
        waitAtMost(() -> receivedMessagesSize() == 1);
        disconnect(client);
    }

    @Test public void subscribeQos1(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newClient(info);
        client.connect(client_opts);
        client.setCallback(this);
        client.subscribe(topic, 1);

        publish(client, topic, 0, payload);
        publish(client, topic, 1, payload);
        publish(client, topic, 2, payload);

        waitAtMost(() -> receivedMessagesSize() == 3);

        MqttMessage msg1 = receivedMessages.get(0);
        MqttMessage msg2 = receivedMessages.get(1);
        MqttMessage msg3 = receivedMessages.get(1);

        assertArrayEquals(msg1.getPayload(), payload);
        assertEquals(0, msg1.getQos());

        assertArrayEquals(msg2.getPayload(), payload);
        assertEquals(1, msg2.getQos());

        // Downgraded QoS 2 to QoS 1
        assertArrayEquals(msg3.getPayload(), payload);
        assertEquals(1, msg3.getQos());

        disconnect(client);
    }

    @Test public void subscribeReceivesRetainedMessagesWithMatchingQoS(TestInfo info)
            throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newClient(info);
        client.connect(client_opts);
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
        final MqttMessage retainedMsg = receivedMessages.get(0);
        assertEquals(new String(lastMsg.getPayload()),
                     new String(retainedMsg.getPayload()));

        disconnect(client);
    }

    @Test public void subscribeReceivesRetainedMessagesWithDowngradedQoS(TestInfo info)
            throws MqttException, InterruptedException {
        MqttConnectOptions clientOpts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, clientOpts);
        client.setCallback(this);
        clearRetained(client, retainedTopic);
        client.subscribe(retainedTopic, 1);

        publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));

        waitAtMost(() -> receivedMessagesSize() == 1);
        MqttMessage lastMsg = receivedMessages.get(0);

        client.unsubscribe(retainedTopic);
        receivedMessages.clear();
        final int subscribeQoS = 0;
        client.subscribe(retainedTopic, subscribeQoS);

        waitAtMost(() -> receivedMessagesSize() == 1);
        final MqttMessage retainedMsg = receivedMessages.get(0);
        assertEquals(new String(lastMsg.getPayload()),
                            new String(retainedMsg.getPayload()));
        assertEquals(subscribeQoS, retainedMsg.getQos());

        disconnect(client);
    }

    @Test public void publishWithEmptyMessageClearsRetained(TestInfo info)
            throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
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

    @Test public void topics(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        client.setCallback(this);
        client.subscribe("/+/test-topic/#");
        String[] cases = new String[]{"/pre/test-topic2", "/test-topic", "/a/test-topic/b/c/d", "/frob/test-topic"};
        List<String> expected = Arrays.asList("/a/test-topic/b/c/d", "/frob/test-topic");
        for(String example : cases){
            publish(client, example, 0, example.getBytes());
        }
        waitAtMost(() -> receivedMessagesSize() == expected.size());
        for (MqttMessage m : receivedMessages){
            expected.contains(new String(m.getPayload()));
        }
        disconnect(client);
    }

    @Test public void sparkplugTopics(TestInfo info) throws MqttException, IOException, InterruptedException, TimeoutException {
        final String amqp091Topic = "spBv1___0.MACLab.DDATA.Opto22.CLX";
        final String sparkplugTopic = "spBv1.0/MACLab/+/Opto22/CLX";

        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        client.setCallback(this);
        client.subscribe(sparkplugTopic);

        setUpAmqp();
        ch.basicPublish("amq.topic", amqp091Topic, MessageProperties.MINIMAL_BASIC, payload);
        tearDownAmqp();

        waitAtMost(() -> receivedMessagesSize() == 1);
        disconnect(client);
    }

    @Test public void nonCleanSession(TestInfo info) throws MqttException, InterruptedException {
        String clientIdBase = clientId(info);
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setCleanSession(false);
        MqttClient client = newConnectedClient(clientIdBase + "-1", client_opts);
        client.subscribe(topic, 1);
        client.disconnect();

        MqttClient client2 = newConnectedClient(clientIdBase + "-2", client_opts);
        publish(client2, topic, 1, payload);
        client2.disconnect();

        client.setCallback(this);
        client.connect(client_opts);

        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        disconnect(client);
    }

    @Test public void sessionRedelivery(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setCleanSession(false);
        MqttClient client = newConnectedClient(info, client_opts);
        client.subscribe(topic, 1);
        disconnect(client);

        MqttClient client2 = newConnectedClient(info, client_opts);
        publish(client2, topic, 1, payload);
        disconnect(client2);

        failOnDelivery = true;

        // Connection should fail. Messages will be redelivered.
        client.setCallback(this);
        client.connect(client_opts);

        // Message has been delivered but connection has failed.
        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), payload);

        assertFalse(client.isConnected());

        receivedMessages.clear();
        failOnDelivery = false;

        client.setCallback(this);
        client.connect(client_opts);

        // Message has been redelivered after session resume
        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        assertTrue(client.isConnected());
        disconnect(client);

        receivedMessages.clear();

        client.setCallback(this);
        waitAtMost(() -> client.isConnected() == false);
        client.connect(client_opts);

        // This time messaage are acknowledged and won't be redelivered
        waitAtMost(() -> receivedMessagesSize() == 0);
        assertEquals(0, receivedMessages.size());

        disconnect(client);
    }

    @Test public void cleanSession(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setCleanSession(false);
        MqttClient client = newConnectedClient(info, client_opts);
        client.subscribe(topic, 1);
        client.disconnect();

        MqttClient client2 = newConnectedClient(info, client_opts);
        publish(client2, topic, 1, payload);
        disconnect(client2);

        client_opts.setCleanSession(true);
        client.connect(client_opts);
        client.setCallback(this);
        client.subscribe(topic, 1);

        waitAtMost(() -> receivedMessagesSize() == 0);
        client.unsubscribe(topic);
        disconnect(client);
    }

    @Test public void multipleClientIds(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        // uses duplicate client ID
        MqttClient client2 = newConnectedClient(info, client_opts);
        // the older connection with this client ID will be closed
        waitAtMost(() -> client.isConnected() == false);
        disconnect(client2);
    }

    @Test public void multipleClusterClientIds(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        MqttClient client3 = newClient(brokerThreeUrl, info);
        client3.connect(client_opts);
        waitAtMost(() -> client.isConnected() == false);
        disconnect(client3);
    }

    @Test public void ping(TestInfo info) throws MqttException, InterruptedException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        client_opts.setKeepAliveInterval(1);
        MqttClient client = newConnectedClient(info, client_opts);
        waitAtMost(() -> client.isConnected());
        disconnect(client);
    }

    @Test public void will(TestInfo info) throws MqttException, InterruptedException, IOException {
        String clientIdBase = clientId(info);
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client2 = newConnectedClient(clientIdBase + "-2", client_opts);
        client2.subscribe(topic);
        client2.setCallback(this);

        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<>();
        SocketFactory testFactory = new SocketFactory() {
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

        MqttConnectOptions opts = new TestMqttConnectOptions();
        opts.setSocketFactory(testFactory);
        opts.setWill(willTopic, payload, 0, false);
        opts.setCleanSession(false);

        client.connect(opts);

        assertTrue(sockets.size() >= 1);
        expectConnectionFailure = true;
        sockets.get(0).close();

        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        client2.unsubscribe(topic);
        disconnect(client2);
    }

    @Test public void willIsRetained(TestInfo info) throws MqttException, InterruptedException, IOException {
        String clientIdBase = clientId(info);
        MqttConnectOptions client2_opts = new TestMqttConnectOptions();
        client2_opts.setCleanSession(true);
        MqttClient client2 = newConnectedClient(clientIdBase + "-2", client2_opts);
        client2.setCallback(this);

        clearRetained(client2, retainedTopic);
        client2.subscribe(retainedTopic, 1);
        disconnect(client2);

        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<>();
        SocketFactory testFactory = new SocketFactory() {
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

        MqttConnectOptions client_opts = new TestMqttConnectOptions();

        MqttClient client = newClient(clientIdBase + "-1");
        MqttTopic willTopic = client.getTopic(retainedTopic);
        byte[] willPayload = "willpayload".getBytes();

        client_opts.setSocketFactory(testFactory);
        client_opts.setWill(willTopic, willPayload, 1, true);

        client.connect(client_opts);

        assertEquals(1, sockets.size());
        sockets.get(0).close();

        // let last will propagate after disconnection
        waitForTestDelay();

        client2.connect(client2_opts);
        client2.setCallback(this);
        client2.subscribe(retainedTopic, 1);

        waitAtMost(() -> receivedMessagesSize() == 1);
        assertArrayEquals(receivedMessages.get(0).getPayload(), willPayload);
        client2.unsubscribe(topic);
        disconnect(client2);
    }

    @Test public void subscribeMultiple(TestInfo info) throws MqttException {
        String clientIdBase = clientId(info);
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(clientIdBase + "-1", client_opts);
        publish(client, "/test-topic/1", 1, "msq1-qos1".getBytes());

        MqttClient client2 = newConnectedClient(clientIdBase + "-2", client_opts);
        client2.setCallback(this);
        client2.subscribe("/test-topic/#");
        client2.subscribe("/test-topic/#");

        publish(client, "/test-topic/2", 0, "msq2-qos0".getBytes());
        publish(client, "/test-topic/3", 1, "msq3-qos1".getBytes());
        publish(client, "/test-topic/4", 2, "msq3-qos2".getBytes());
        publish(client, topic, 0, "msq4-qos0".getBytes());
        publish(client, topic, 1, "msq4-qos1".getBytes());


        assertEquals(3, receivedMessages.size());
        disconnect(client);
        disconnect(client2);
    }

    @Test public void publishMultiple() throws MqttException, InterruptedException {
        int pubCount = 50;
        for (int subQos=0; subQos <= 2; subQos++){
            for (int pubQos=0; pubQos <= 2; pubQos++){
                // avoid reusing the client in this test as a shared
                // client cannot handle connection churn very well. MK.
                String cid = "test-sub-qos-" + subQos + "-pub-qos-" + pubQos;
                MqttConnectOptions client_opts = new TestMqttConnectOptions();
                MqttClient client = newClient(brokerUrl, cid);
                client.connect(client_opts);
                client.subscribe(topic, subQos);
                client.setCallback(this);
                long start = System.currentTimeMillis();
                for (int i=0; i<pubCount; i++){
                    publish(client, topic, pubQos, payload);
                }

                waitAtMost(() -> receivedMessagesSize() == pubCount);
                System.out.println("publish QOS" + pubQos + " subscribe QOS" + subQos +
                                   ", " + pubCount + " msgs took " +
                                   (lastReceipt - start)/1000.0 + "sec");
                client.disconnect(5000);
                receivedMessages.clear();
            }
        }
    }

    @Test public void topicAuthorisationPublish(TestInfo info) throws Exception {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        client.setCallback(this);
        client.subscribe("some/test-topic");
        publish(client, "some/test-topic", 1, "content".getBytes());
        waitAtMost(() -> receivedMessagesSize() == 1);
        assertTrue(client.isConnected());
        try {
            publish(client, "forbidden-topic", 1, "content".getBytes());
            fail("Publishing on a forbidden topic, an exception should have been thrown");
            client.disconnect();
        } catch(Exception e) {
            // OK
        }
    }

    @Test public void topicAuthorisationSubscribe(TestInfo info) throws Exception {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        client.setCallback(this);
        client.subscribe("some/test-topic");
        try {
            client.subscribe("forbidden-topic");
            fail("Subscribing to a forbidden topic, an exception should have been thrown");
            client.disconnect();
        } catch(Exception e) {
            // OK
            e.printStackTrace();
        }
    }

    @Test public void lastWillDowngradesQoS2(TestInfo info) throws Exception {
        String lastWillTopic = "test-topic-will-downgrades-qos";

        MqttConnectOptions client2Opts = new TestMqttConnectOptions();
        MqttClient client2 = newConnectedClient(info, client2Opts);
        client2.subscribe(lastWillTopic);
        client2.setCallback(this);

        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<>();
        SocketFactory testFactory = new SocketFactory() {
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

        MqttConnectOptions clientOpts = new TestMqttConnectOptions();

        MqttClient client = newClient("test-topic-will-downgrades-qos");
        clientOpts.setSocketFactory(testFactory);
        MqttTopic willTopic = client.getTopic(lastWillTopic);
        clientOpts.setWill(willTopic, payload, 2, false);
        clientOpts.setCleanSession(false);
        client.connect(clientOpts);

        waitAtMost(() -> sockets.size() == 1);
        expectConnectionFailure = true;
        sockets.get(0).close();

        // let some time after disconnection
        waitAtMost(() -> receivedMessagesSize() == 1);
        assertEquals(1, receivedMessages.size());
        disconnect(client2);
    }

    @Test public void lastWillNotSentOnRestrictedTopic(TestInfo info) throws Exception {
        MqttConnectOptions client2_opts = new TestMqttConnectOptions();

        MqttClient client2 = newConnectedClient(info, client2_opts);
        // topic authorized for subscription, restricted for publishing
        String lastWillTopic = "last-will";
        client2.subscribe(lastWillTopic);
        client2.setCallback(this);

        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<>();
        SocketFactory testFactory = new SocketFactory() {
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

        MqttConnectOptions client_opts = new TestMqttConnectOptions();

        MqttClient client = newClient("last-will-not-sent-on-restricted-topic");
        client_opts.setSocketFactory(testFactory);
        MqttTopic willTopic = client.getTopic(lastWillTopic);
        client_opts.setWill(willTopic, payload, 0, false);
        client_opts.setCleanSession(false);
        client.connect(client_opts);

        assertEquals(1, sockets.size());
        expectConnectionFailure = true;
        sockets.get(0).close();

        // let some time after disconnection
        waitForTestDelay();
        assertEquals(0, receivedMessages.size());
        disconnect(client2);
    }

    @Test public void topicAuthorisationVariableExpansion(TestInfo info) throws Exception {
        final String client_id = clientId(info);
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(client_id, client_opts);
        client.setCallback(this);
        String topicWithExpandedVariables = "guest/" + client_id + "/a";
        client.subscribe(topicWithExpandedVariables);
        publish(client, topicWithExpandedVariables, 1, "content".getBytes());
        waitAtMost(() -> receivedMessagesSize() == 1);
        assertTrue(client.isConnected());
        try {
            publish(client, "guest/WrongClientId/a", 1, "content".getBytes());
            fail("Publishing on a forbidden topic, an exception should have been thrown");
            client.disconnect();
        } catch(Exception e) {
            // OK
        }
    }

    @Test public void interopM2A(TestInfo info) throws MqttException, IOException, InterruptedException, TimeoutException {
        setUpAmqp();
        String queue = ch.queueDeclare().getQueue();
        ch.queueBind(queue, "amq.topic", topic);

        byte[] interopPayload = "interop-body".getBytes();
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        publish(client, topic, 1, interopPayload);
        disconnect(client);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<byte[]> messageBody = new AtomicReference<>();
        ch.basicConsume(queue, true, new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                messageBody.set(body);
                latch.countDown();
            }
        });
        assertTrue(latch.await(EXPECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(new String(interopPayload), new String(messageBody.get()));
        assertNull(ch.basicGet(queue, true));
        tearDownAmqp();
    }

    @Test public void interopA2M(TestInfo info) throws MqttException, IOException, InterruptedException, TimeoutException {
        MqttConnectOptions client_opts = new TestMqttConnectOptions();
        MqttClient client = newConnectedClient(info, client_opts);
        client.setCallback(this);
        client.subscribe(topic, 1);

        setUpAmqp();
        ch.basicPublish("amq.topic", topic, MessageProperties.MINIMAL_BASIC, payload);
        tearDownAmqp();

        waitAtMost(() -> receivedMessagesSize() == 1);
        client.disconnect();
    }

    private void publish(MqttClient client, String topicName, int qos, byte[] payload) throws MqttException {
    	publish(client, topicName, qos, payload, false);
    }

    private void publish(MqttClient client, String topicName, int qos, byte[] payload, boolean retained) throws MqttException {
    	MqttTopic topic = client.getTopic(topicName);
   		MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        message.setRetained(retained);
    	MqttDeliveryToken token = topic.publish(message);
    	token.waitForCompletion();
    }

    private void publishRetained(MqttClient client, String topicName, int qos, byte[] payload) throws MqttException {
        publish(client, topicName, qos, payload, true);
    }

    private void clearRetained(MqttClient client, String topicName) throws MqttException {
        publishRetained(client, topicName, 1, "".getBytes());
    }

    public void connectionLost(Throwable cause) {
        if (!expectConnectionFailure)
            fail("Connection unexpectedly lost");
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        lastReceipt = System.currentTimeMillis();
        receivedMessages.add(message);
        if (failOnDelivery) {
            throw new Exception("unexpected delivery on topic " + topic);
        }
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    private Integer receivedMessagesSize() {
        return receivedMessages.size();
    }

    private void waitForTestDelay() {
        try {
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
        fail("Waited " + EXPECT_TIMEOUT.get(ChronoUnit.SECONDS) + " second(s), condition never got true");
    }
}
