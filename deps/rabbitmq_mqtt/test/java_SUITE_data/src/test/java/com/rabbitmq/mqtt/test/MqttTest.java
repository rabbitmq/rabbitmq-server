//  The contents of this file are subject to the Mozilla Public License
//  Version 1.1 (the "License"); you may not use this file except in
//  compliance with the License. You may obtain a copy of the License
//  at http://www.mozilla.org/MPL/
//
//  Software distributed under the License is distributed on an "AS IS"
//  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
//  the License for the specific language governing rights and
//  limitations under the License.
//
//  The Original Code is RabbitMQ.
//
//  The Initial Developer of the Original Code is GoPivotal, Inc.
//  Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
//

package com.rabbitmq.mqtt.test;

import com.rabbitmq.client.*;
import org.awaitility.Duration;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.internal.NetworkModule;
import org.eclipse.paho.client.mqttv3.internal.TCPNetworkModule;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPingReq;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.net.SocketFactory;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

/***
 *  MQTT v3.1 tests
 *
 */

public class MqttTest implements MqttCallback {

    private final String host = "localhost";
    private final String brokerUrl = "tcp://" + host + ":" + getPort();
    private String clientId;
    private String clientId2;
    private MqttClient client;
    private MqttClient client2;
    private MqttConnectOptions conOpt;
    private volatile List<MqttMessage> receivedMessages;

    private final byte[] payload = "payload".getBytes();
    private final String topic = "test-topic";
    private final String retainedTopic = "test-retained-topic";
    private int testDelay = 2000;
    private Duration timeout = Duration.TEN_SECONDS;

    private volatile long lastReceipt;
    private volatile boolean expectConnectionFailure;
    private volatile boolean failOnDelivery = false;

    private Connection conn;
    private Channel ch;

    private static int getPort() {
        Object port = System.getProperty("mqtt.port", "1883");
        Assert.assertNotNull(port);
        return Integer.parseInt(port.toString());
    }

    private static int getAmqpPort() {
        Object port = System.getProperty("amqp.port", "5672");
        assertNotNull(port);
        return Integer.parseInt(port.toString());
    }

    private static String getHost() {
        Object host = System.getProperty("hostname", "localhost");
        assertNotNull(host);
        return host.toString();
    }
    // override the 10s limit
    private class MyConnOpts extends MqttConnectOptions {
        private int keepAliveInterval = 60;
        @Override
        public void setKeepAliveInterval(int keepAliveInterval) {
            this.keepAliveInterval = keepAliveInterval;
        }

        @Override
        public int getKeepAliveInterval() {
            return this.keepAliveInterval;
        }
    }

    private MqttClient newClient(String clientId) throws MqttException {
        return newClient(brokerUrl, clientId);
    }

    private MqttClient newClient(String uri, String clientId) throws MqttException {
        return new MqttClient(uri, clientId, null);
    }

    private MqttClient newConnectedClient(String clientId, MqttConnectOptions conOpt) throws MqttException {
        MqttClient client = newClient(brokerUrl, clientId);
        client.connect(conOpt);
        return client;
    }

    private MqttClient newConnectedClient(MqttConnectOptions conOpt) throws MqttException {
        return newConnectedClient(UUID.randomUUID().toString(), conOpt);
    }

    private void disconnect(MqttClient client) {
        try {
            if(client.isConnected()) {
              client.disconnect(5000);
            }
        } catch (Exception ignored) {}
    }

    private String generateClientID() {
        return getClass().getSimpleName() + ((int) (10000 * Math.random()));
    }

    @Before
    public void setUp() throws MqttException {
        this.clientId  = this.generateClientID();
        this.clientId2 = this.generateClientID();
        conOpt = new MyConnOpts();
        resetConOpts(conOpt);
        receivedMessages = Collections.synchronizedList(new ArrayList<MqttMessage>());
        expectConnectionFailure = false;
    }

    @After
    public void tearDown() throws MqttException {
        // clean any sticky sessions
        resetConOpts(conOpt);
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

    private void resetConOpts(MqttConnectOptions conOpts) {
        conOpts.setCleanSession(true);
        conOpts.setKeepAliveInterval(60);
        conOpts.setUserName("guest");
        conOpts.setPassword("guest".toCharArray());
        // PublishMultiple overwhelms Paho defaults
        conOpts.setMaxInflight(15000);
    }

    @Test
    public void connectFirst() throws MqttException, IOException, InterruptedException {
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

    @Test public void invalidUser() throws MqttException {
        MqttClient client =  newClient(clientId);
        conOpt.setUserName("invalid-user");
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        } finally {
            if(client.isConnected()) {
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
            throws MqttException, IOException, TimeoutException, InterruptedException {
        MqttClient c = newClient(brokerUrl, cid);
        MqttConnectOptions opts = new MyConnOpts();
        opts.setUserName("guest");
        opts.setPassword("guest".toCharArray());
        opts.setCleanSession(cleanSession);
        c.connect(opts);

        setUpAmqp();
        Channel tmpCh = conn.createChannel();

        String q = "mqtt-subscription-" + cid + "qos" + String.valueOf(qos);

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
            Map<String, Object> args = new HashMap<String, Object>();
            args.put("x-expires", 86400000);
            tmpCh.queueDeclare(q, durable, autoDelete, false, args);
        } finally {
            if(c.isConnected()) {
                c.disconnect(3000);
            }

            Channel tmpCh2 = conn.createChannel();
            tmpCh2.queueDelete(q);
            tmpCh2.close();
            tearDownAmqp();
        }
    }

    @Test public void invalidPassword() throws MqttException {
        MqttClient client = newClient(clientId);
        conOpt.setUserName("invalid-user");
        conOpt.setPassword("invalid-password".toCharArray());
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        } finally {
            if(client.isConnected()) {
                disconnect(client);
            }
        }
    }

    @Test public void emptyPassword() throws MqttException {
        MqttClient client = newClient(clientId);
        MqttConnectOptions opts = new MyConnOpts();
        opts.setUserName("guest");
        opts.setPassword("".toCharArray());
        try {
            client.connect(opts);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        }
    }


    @Test public void subscribeQos0() throws MqttException, InterruptedException {
        MqttClient client = newClient(clientId);
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 0);

        publish(client, topic, 0, payload);
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        Assert.assertEquals(0, receivedMessages.get(0).getQos());
        disconnect(client);
    }

    @Test public void subscribeUnsubscribe() throws MqttException, InterruptedException {
        MqttClient client = newClient(clientId);
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 0);

        publish(client, topic, 1, payload);

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        Assert.assertEquals(0, receivedMessages.get(0).getQos());

        client.unsubscribe(topic);
        publish(client, topic, 0, payload);
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        disconnect(client);
    }

    @Test public void subscribeQos1() throws MqttException, InterruptedException {
        MqttClient client = newClient(clientId);
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 1);

        publish(client, topic, 0, payload);
        publish(client, topic, 1, payload);
        publish(client, topic, 2, payload);

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(3));

        MqttMessage msg1 = receivedMessages.get(0);
        MqttMessage msg2 = receivedMessages.get(1);
        MqttMessage msg3 = receivedMessages.get(1);

        Assert.assertArrayEquals(msg1.getPayload(), payload);
        Assert.assertEquals(0, msg1.getQos());

        Assert.assertArrayEquals(msg2.getPayload(), payload);
        Assert.assertEquals(1, msg2.getQos());

        // Downgraded QoS 2 to QoS 1
        Assert.assertArrayEquals(msg3.getPayload(), payload);
        Assert.assertEquals(1, msg3.getQos());

        disconnect(client);
    }

    @Test public void subscribeReceivesRetainedMessagesWithMatchingQoS()
            throws MqttException, InterruptedException, UnsupportedEncodingException {
        MqttClient client = newClient(clientId);
        client.connect(conOpt);
        client.setCallback(this);
        clearRetained(client, retainedTopic);
        client.subscribe(retainedTopic, 1);

        publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));
        publishRetained(client, retainedTopic, 1, "retain 2".getBytes(StandardCharsets.UTF_8));

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(2));
        MqttMessage lastMsg = receivedMessages.get(1);

        client.unsubscribe(retainedTopic);
        receivedMessages.clear();
        client.subscribe(retainedTopic, 1);
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        final MqttMessage retainedMsg = receivedMessages.get(0);
        Assert.assertEquals(new String(lastMsg.getPayload()),
                                   new String(retainedMsg.getPayload()));

        disconnect(client);
    }

    @Test public void subscribeReceivesRetainedMessagesWithDowngradedQoS()
            throws MqttException, InterruptedException, UnsupportedEncodingException {
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.setCallback(this);
        clearRetained(client, retainedTopic);
        client.subscribe(retainedTopic, 1);

        publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        MqttMessage lastMsg = receivedMessages.get(0);

        client.unsubscribe(retainedTopic);
        receivedMessages.clear();
        final int subscribeQoS = 0;
        client.subscribe(retainedTopic, subscribeQoS);

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        final MqttMessage retainedMsg = receivedMessages.get(0);
        Assert.assertEquals(new String(lastMsg.getPayload()),
                            new String(retainedMsg.getPayload()));
        Assert.assertEquals(subscribeQoS, retainedMsg.getQos());

        disconnect(client);
    }

    @Test public void publishWithEmptyMessageClearsRetained()
            throws MqttException, InterruptedException, UnsupportedEncodingException {
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.setCallback(this);
        clearRetained(client, retainedTopic);
        client.subscribe(retainedTopic, 1);

        publishRetained(client, retainedTopic, 1, "retain 1".getBytes(StandardCharsets.UTF_8));
        publishRetained(client, retainedTopic, 1, "retain 2".getBytes(StandardCharsets.UTF_8));

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(2));
        client.unsubscribe(retainedTopic);
        receivedMessages.clear();

        clearRetained(client, retainedTopic);
        client.subscribe(retainedTopic, 1);
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(0));

        disconnect(client);
    }

    @Test public void topics() throws MqttException, InterruptedException {
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.setCallback(this);
        client.subscribe("/+/mid/#");
        String[] cases = new String[]{"/pre/mid2", "/mid", "/a/mid/b/c/d", "/frob/mid"};
        List<String> expected = Arrays.asList("/a/mid/b/c/d", "/frob/mid");
        for(String example : cases){
            publish(client, example, 0, example.getBytes());
        }
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(expected.size()));
        for (MqttMessage m : receivedMessages){
            expected.contains(new String(m.getPayload()));
        }
        disconnect(client);
    }

    @Test public void sparkplugTopics() throws MqttException, IOException, InterruptedException, TimeoutException {
        final String amqp091Topic = "spBv1___0.MACLab.DDATA.Opto22.CLX";
        final String sparkplugTopic = "spBv1.0/MACLab/+/Opto22/CLX";

        conOpt.setCleanSession(true);
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.setCallback(this);
        client.subscribe(sparkplugTopic);

        setUpAmqp();
        ch.basicPublish("amq.topic", amqp091Topic, MessageProperties.MINIMAL_BASIC, payload);
        tearDownAmqp();

        waitAtMost(timeout).until(receivedMessagesSize(), equalTo(1));
        disconnect(client);
    }

    @Test public void nonCleanSession() throws MqttException, InterruptedException {
        conOpt.setCleanSession(false);
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.subscribe(topic, 1);
        client.disconnect();

        MqttClient client2 = newConnectedClient(clientId2, conOpt);
        publish(client2, topic, 1, payload);
        client2.disconnect();

        client.setCallback(this);
        client.connect(conOpt);

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        disconnect(client);
    }

    @Test public void sessionRedelivery() throws MqttException, InterruptedException {
        conOpt.setCleanSession(false);
        MqttClient client = newConnectedClient(conOpt);
        client.subscribe(topic, 1);
        disconnect(client);

        MqttClient client2 = newConnectedClient(conOpt);
        publish(client2, topic, 1, payload);
        disconnect(client);

        failOnDelivery = true;

        // Connection should fail. Messages will be redelivered.
        client.setCallback(this);
        client.connect(conOpt);

        // Message has been delivered but connection has failed.
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), payload);

        Assert.assertFalse(client.isConnected());

        receivedMessages.clear();
        failOnDelivery = false;

        client.setCallback(this);
        client.connect(conOpt);

        // Message has been redelivered after session resume
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        Assert.assertTrue(client.isConnected());
        disconnect(client);

        receivedMessages.clear();

        client.setCallback(this);
        waitAtMost(timeout).until(() -> client.isConnected(), equalTo(false));
        client.connect(conOpt);

        // This time messaage are acknowledged and won't be redelivered
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(0));
        Assert.assertEquals(0, receivedMessages.size());

        disconnect(client);
    }

    @Test public void cleanSession() throws MqttException, InterruptedException {
        conOpt.setCleanSession(false);
        MqttClient client = newConnectedClient(conOpt);
        client.subscribe(topic, 1);
        client.disconnect();

        MqttClient client2 = newConnectedClient(conOpt);
        publish(client2, topic, 1, payload);
        disconnect(client2);

        conOpt.setCleanSession(true);
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 1);

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(0));
        client.unsubscribe(topic);
        disconnect(client);
    }

    @Test public void multipleClientIds() throws MqttException, InterruptedException {
        String clientId = "rabbitmq-mqtt-duplicate-client-id";
        MqttClient client = newConnectedClient(clientId, conOpt);
        // uses duplicate client ID
        MqttClient client2 = newConnectedClient(clientId, conOpt);
        // the older connection with this client ID will be closed
        waitAtMost(timeout).until(isClientConnected(client),equalTo(false));
        disconnect(client2);
    }

    @Test public void ping() throws MqttException, InterruptedException {
        conOpt.setKeepAliveInterval(1);
        MqttClient client = newConnectedClient(clientId, conOpt);
        waitAtMost(timeout).until(isClientConnected(client),equalTo(true));
        disconnect(client);
    }

    @Test public void will() throws MqttException, InterruptedException, IOException {
        String topic = "rabbitmq-mqtt-will-test-1";
        resetConOpts(conOpt);
        MqttClient client2 = newConnectedClient("will-client-2", conOpt);
        client2.subscribe(topic);
        client2.setCallback(this);

        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<Socket>();
        SocketFactory testFactory = new SocketFactory() {
            public Socket createSocket(String s, int i) throws IOException {
                Socket sock = factory.createSocket(s, i);
                sockets.add(sock);
                return sock;
            }
            public Socket createSocket(String s, int i, InetAddress a, int i1) throws IOException {
                return null;
            }
            public Socket createSocket(InetAddress a, int i) throws IOException {
                return null;
            }
            public Socket createSocket(InetAddress a, int i, InetAddress a1, int i1) throws IOException {
                return null;
            }
            @Override
            public Socket createSocket() throws IOException {
                Socket sock = new Socket();
                sockets.add(sock);
                return sock;
            }
        };

        MqttClient client = newClient("will-client-1");
        MqttTopic willTopic = client.getTopic(topic);

        MqttConnectOptions opts = new MyConnOpts();
        resetConOpts(opts);
        opts.setSocketFactory(testFactory);
        opts.setWill(willTopic, payload, 0, false);
        opts.setCleanSession(false);

        client.connect(opts);

        Assert.assertTrue(sockets.size() >= 1);
        expectConnectionFailure = true;
        sockets.get(0).close();

        waitAtMost(timeout).until(receivedMessagesSize(), equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), payload);
        client2.unsubscribe(topic);
        disconnect(client2);
    }

    @Test public void willIsRetained() throws MqttException, InterruptedException, IOException {
        MqttConnectOptions opts2 = new MyConnOpts();
        opts2.setCleanSession(true);
        MqttClient client2 = newConnectedClient("will-is-retained-client-2", opts2);
        client2.setCallback(this);

        clearRetained(client2, retainedTopic);
        client2.subscribe(retainedTopic, 1);
        disconnect(client2);

        MqttConnectOptions opts = new MyConnOpts();
        resetConOpts(opts);
        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<Socket>();
        SocketFactory testFactory = new SocketFactory() {
            public Socket createSocket(String s, int i) throws IOException {
                Socket sock = factory.createSocket(s, i);
                sockets.add(sock);
                return sock;
            }
            public Socket createSocket(String s, int i, InetAddress a, int i1) throws IOException {
                return null;
            }
            public Socket createSocket(InetAddress a, int i) throws IOException {
                return null;
            }
            public Socket createSocket(InetAddress a, int i, InetAddress a1, int i1) throws IOException {
                return null;
            }
            @Override
            public Socket createSocket() throws IOException {
                Socket sock = new Socket();
                sockets.add(sock);
                return sock;
            }
        };

        MqttClient client = newClient("will-is-retained-client-1");
        MqttTopic willTopic = client.getTopic(retainedTopic);
        byte[] willPayload = "willpayload".getBytes();

        opts.setSocketFactory(testFactory);
        opts.setWill(willTopic, willPayload, 1, true);

        client.connect(opts);

        Assert.assertEquals(1, sockets.size());
        sockets.get(0).close();

        // let last will propagate after disconnection
        waitForTestDelay();

        client2.connect(opts2);
        client2.setCallback(this);
        client2.subscribe(retainedTopic, 1);

        waitAtMost(timeout).until(receivedMessagesSize(), equalTo(1));
        Assert.assertArrayEquals(receivedMessages.get(0).getPayload(), willPayload);
        client2.unsubscribe(topic);
        disconnect(client2);
    }

    @Test public void subscribeMultiple() throws MqttException {
        MqttClient client = newConnectedClient(conOpt);
        publish(client, "/topic/1", 1, "msq1-qos1".getBytes());

        MqttClient client2 = newConnectedClient(conOpt);
        client2.setCallback(this);
        client2.subscribe("/topic/#");
        client2.subscribe("/topic/#");

        publish(client, "/topic/2", 0, "msq2-qos0".getBytes());
        publish(client, "/topic/3", 1, "msq3-qos1".getBytes());
        publish(client, "/topic/4", 2, "msq3-qos2".getBytes());
        publish(client, topic, 0, "msq4-qos0".getBytes());
        publish(client, topic, 1, "msq4-qos1".getBytes());


        Assert.assertEquals(3, receivedMessages.size());
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
                MqttClient client = newClient(brokerUrl, cid);
                client.connect(conOpt);
                client.subscribe(topic, subQos);
                client.setCallback(this);
                long start = System.currentTimeMillis();
                for (int i=0; i<pubCount; i++){
                    publish(client, topic, pubQos, payload);
                }

                waitAtMost(timeout).until(receivedMessagesSize(),equalTo(pubCount));
                System.out.println("publish QOS" + pubQos + " subscribe QOS" + subQos +
                                   ", " + pubCount + " msgs took " +
                                   (lastReceipt - start)/1000.0 + "sec");
                client.disconnect(5000);
                receivedMessages.clear();
            }
        }
    }

    @Test public void topicAuthorisationPublish() throws Exception {
        MqttClient client = newConnectedClient(conOpt);
        client.setCallback(this);
        client.subscribe("some/topic");
        publish(client, "some/topic", 1, "content".getBytes());
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        assertTrue(client.isConnected());
        try {
            publish(client, "forbidden", 1, "content".getBytes());
            fail("Publishing on a forbidden topic, an exception should have been thrown");
            client.disconnect();
        } catch(Exception e) {
            // OK
        }
    }

    @Test public void topicAuthorisationSubscribe() throws Exception {
        MqttClient client = newConnectedClient(conOpt);
        client.setCallback(this);
        client.subscribe("some/topic");
        try {
            client.subscribe("forbidden");
            fail("Subscribing to a forbidden topic, an exception should have been thrown");
            client.disconnect();
        } catch(Exception e) {
            // OK
            e.printStackTrace();
        }
    }

    @Test public void lastWillNotSentOnRestrictedTopic() throws Exception {
        MqttClient client2 = newConnectedClient(conOpt);
        // topic authorized for subscription, restricted for publishing
        String lastWillTopic = "last-will";
        client2.subscribe(lastWillTopic);
        client2.setCallback(this);

        MqttConnectOptions opts = new MyConnOpts();
        final SocketFactory factory = SocketFactory.getDefault();
        final ArrayList<Socket> sockets = new ArrayList<Socket>();
        SocketFactory testFactory = new SocketFactory() {
            public Socket createSocket(String s, int i) throws IOException {
                Socket sock = factory.createSocket(s, i);
                sockets.add(sock);
                return sock;
            }
            public Socket createSocket(String s, int i, InetAddress a, int i1) throws IOException {
                return null;
            }
            public Socket createSocket(InetAddress a, int i) throws IOException {
                return null;
            }
            public Socket createSocket(InetAddress a, int i, InetAddress a1, int i1) throws IOException {
                return null;
            }
            @Override
            public Socket createSocket() throws IOException {
                Socket sock = new Socket();
                sockets.add(sock);
                return sock;
            }
        };
        MqttClient client = newClient("last-will-not-sent-on-restricted-topic");
        opts.setSocketFactory(testFactory);
        MqttTopic willTopic = client.getTopic(lastWillTopic);
        opts.setWill(willTopic, payload, 0, false);
        opts.setCleanSession(false);
        client.connect(opts);

        Assert.assertEquals(1, sockets.size());
        expectConnectionFailure = true;
        sockets.get(0).close();

        // let some time after disconnection
        waitForTestDelay();
        Assert.assertEquals(0, receivedMessages.size());
        disconnect(client2);
    }

    @Test public void topicAuthorisationVariableExpansion() throws Exception {
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.setCallback(this);
        String topicWithExpandedVariables = "guest/" + clientId + "/a";
        client.subscribe(topicWithExpandedVariables);
        publish(client, topicWithExpandedVariables, 1, "content".getBytes());
        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
        assertTrue(client.isConnected());
        try {
            publish(client, "guest/WrongClientId/a", 1, "content".getBytes());
            fail("Publishing on a forbidden topic, an exception should have been thrown");
            client.disconnect();
        } catch(Exception e) {
            // OK
        }
    }

    @Test public void interopM2A() throws MqttException, IOException, InterruptedException, TimeoutException {
        setUpAmqp();
        String queue = ch.queueDeclare().getQueue();
        ch.queueBind(queue, "amq.topic", topic);

        byte[] interopPayload = "interop-body".getBytes();
        MqttClient client = newConnectedClient(clientId, conOpt);
        publish(client, topic, 1, interopPayload);
        disconnect(client);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<byte[]> messageBody = new AtomicReference<byte[]>();
        ch.basicConsume(queue, true, new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                messageBody.set(body);
                latch.countDown();
            }
        });
        latch.await(timeout.getValueInMS(), TimeUnit.MILLISECONDS);
        assertEquals(new String(interopPayload), new String(messageBody.get()));
        assertNull(ch.basicGet(queue, true));
        tearDownAmqp();
    }

    @Test public void interopA2M() throws MqttException, IOException, InterruptedException, TimeoutException {
        MqttClient client = newConnectedClient(clientId, conOpt);
        client.setCallback(this);
        client.subscribe(topic, 1);

        setUpAmqp();
        ch.basicPublish("amq.topic", topic, MessageProperties.MINIMAL_BASIC, payload);
        tearDownAmqp();

        waitAtMost(timeout).until(receivedMessagesSize(),equalTo(1));
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
        if(failOnDelivery){
            throw new Exception("unexpected delivery on topic " + topic);
        }
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    private Callable<Integer> receivedMessagesSize() {
        return new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return receivedMessages.size();
            }
        };
    }

    private Callable<Boolean> isClientConnected(final MqttClient client) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return client.isConnected();
            }
        };
    }

    private void waitForTestDelay() {
        try {
            Thread.sleep(testDelay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
