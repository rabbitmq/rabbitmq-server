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
//  The Initial Developer of the Original Code is VMware, Inc.
//  Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
//

package com.rabbitmq.mqtt.test;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.internal.NetworkModule;
import org.eclipse.paho.client.mqttv3.internal.TCPNetworkModule;
import org.eclipse.paho.client.mqttv3.internal.trace.Trace;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttOutputStream;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPublish;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 *  MQTT v3.1 tests
 *  TODO: synchronise access to variables
 */

public class MqttTest extends TestCase implements MqttCallback {

    private final String host = "localhost";
    private final int port = 1883;
	private final String brokerUrl = "tcp://" + host + ":" + port;
    private String clientId;
    private String clientId2;
    private MqttClient client;
    private MqttClient client2;
	private MqttConnectOptions conOpt;
    private ArrayList<MqttMessage> receivedMessages;

    private final byte[] payload = "payload".getBytes();
    private final String topic = "test-topic";
    private int testDelay = 100;
    private long lastReceipt;
    private boolean expectConnectionFailure;

    // override 10s limit
    private class MyConnOpts extends MqttConnectOptions {
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

    @Override
    public void setUp() throws MqttException {
        clientId = getClass().getSimpleName() + ((int) (10000*Math.random()));
        clientId2 = clientId + "-2";
        client = new MqttClient(brokerUrl, clientId);
        client2 = new MqttClient(brokerUrl, clientId2);
        conOpt = new MyConnOpts();
        setConOpts(conOpt);
        receivedMessages = new ArrayList();
        expectConnectionFailure = false;
    }

    @Override
    public  void tearDown() throws MqttException {
        // clean any sticky sessions
        setConOpts(conOpt);
        client = new MqttClient(brokerUrl, clientId);
        try {
            client.connect(conOpt);
            client.disconnect();
        } catch (Exception _) {}

        client2 = new MqttClient(brokerUrl, clientId2);
        try {
            client2.connect(conOpt);
            client2.disconnect();
        } catch (Exception _) {}
    }

    private void setConOpts(MqttConnectOptions conOpts) {
        // provide authentication if the broker needs it
        // conOpts.setUserName("guest");
        // conOpts.setPassword("guest".toCharArray());
        conOpts.setCleanSession(true);
        conOpts.setKeepAliveInterval(60);
    }

    public void testConnectFirst() throws MqttException, IOException, InterruptedException {
        MqttPublish publish = new MqttPublish(this.getName(), new MqttMessage(payload));
        NetworkModule networkModule = new TCPNetworkModule(Trace.getTrace(""), SocketFactory.getDefault(), host, port);
        networkModule.start();
        MqttOutputStream mqttOut = new MqttOutputStream(networkModule.getOutputStream());
        mqttOut.write(publish);
        Thread.sleep(testDelay);
        try {
            mqttOut.write(publish);
            fail("Error expected if CONNECT is not first packet");
        } catch (IOException _) {}
    }

    public void testInvalidUser() throws MqttException {
        conOpt.setUserName("invalid-user");
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        }
    }

    public void testInvalidPassword() throws MqttException {
        conOpt.setUserName("invalid-user");
        conOpt.setPassword("invalid-password".toCharArray());
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        }
    }


    public void testSubscribeQos0() throws MqttException, InterruptedException {
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 0);

        publish(client, topic, 0, payload);
        Thread.sleep(testDelay);
        Assert.assertEquals(1, receivedMessages.size());
        Assert.assertEquals(true, Arrays.equals(receivedMessages.get(0).getPayload(), payload));
        Assert.assertEquals(0, receivedMessages.get(0).getQos());
        client.disconnect();
    }

    public void testSubscribeUnsubscribe() throws MqttException, InterruptedException {
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 0);

        publish(client, topic, 1, payload);
        Thread.sleep(testDelay);
        Assert.assertEquals(1, receivedMessages.size());
        Assert.assertEquals(true, Arrays.equals(receivedMessages.get(0).getPayload(), payload));
        Assert.assertEquals(0, receivedMessages.get(0).getQos());

        client.unsubscribe(topic);
        publish(client, topic, 0, payload);
        Thread.sleep(testDelay);
        Assert.assertEquals(1, receivedMessages.size());
        client.disconnect();
    }

    public void testSubscribeQos1() throws MqttException, InterruptedException {
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 1);

        publish(client, topic, 0, payload);
        publish(client, topic, 1, payload);
        Thread.sleep(testDelay);

        Assert.assertEquals(2, receivedMessages.size());
        MqttMessage msg1 = receivedMessages.get(0);
        MqttMessage msg2 = receivedMessages.get(1);

        Assert.assertEquals(true, Arrays.equals(msg1.getPayload(), payload));
        Assert.assertEquals(0, msg1.getQos());

        Assert.assertEquals(true, Arrays.equals(msg2.getPayload(), payload));
        Assert.assertEquals(1, msg2.getQos());

        client.disconnect();
    }

    public void testTopics() throws MqttException, InterruptedException {
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe("/+/mid/#");
        String cases[] = {"/pre/mid2", "/mid", "/a/mid/b/c/d", "/frob/mid"};
        List<String> expected = Arrays.asList("/a/mid/b/c/d", "/frob/mid");
        for(String example : cases){
            publish(client, example, 0, example.getBytes());
        }
        Thread.sleep(testDelay);
        Assert.assertEquals(expected.size(), receivedMessages.size());
        for (MqttMessage m : receivedMessages){
            expected.contains(new String(m.getPayload()));
        }
        client.disconnect();
    }

    public void testNonCleanSession() throws MqttException, InterruptedException {
        conOpt.setCleanSession(false);
        client.connect(conOpt);
        client.subscribe(topic, 1);
        client.disconnect();

        client2.connect(conOpt);
        publish(client2, topic, 1, payload);
        client2.disconnect();

        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 1);

        Thread.sleep(testDelay);
        Assert.assertEquals(1, receivedMessages.size());
        Assert.assertEquals(true, Arrays.equals(receivedMessages.get(0).getPayload(), payload));
        client.unsubscribe(topic);
        client.disconnect();
    }

    public void testCleanSession() throws MqttException, InterruptedException {
        conOpt.setCleanSession(false);
        client.connect(conOpt);
        client.subscribe(topic, 1);
        client.disconnect();

        client2.connect(conOpt);
        publish(client2, topic, 1, payload);
        client2.disconnect();

        conOpt.setCleanSession(true);
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic, 1);

        Thread.sleep(testDelay);
        Assert.assertEquals(0, receivedMessages.size());
        client.unsubscribe(topic);
        client.disconnect();
    }

    public void testMultipleClientIds() throws MqttException, InterruptedException {
        client.connect(conOpt);
        client2 = new MqttClient(brokerUrl, clientId);
        client2.connect(conOpt);
        Thread.sleep(testDelay);
        Assert.assertFalse(client.isConnected());
        client2.disconnect();
    }

    public void testPing() throws MqttException, InterruptedException {
        conOpt.setKeepAliveInterval(1);
        client.connect(conOpt);
        Thread.sleep(3000);
        Assert.assertEquals(true, client.isConnected());
        client.disconnect();
    }

    public void testWill() throws MqttException, InterruptedException, IOException {
        client2.connect(conOpt);
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
            };
        };
        conOpt.setSocketFactory(testFactory);
        MqttTopic willTopic = client.getTopic(topic);
        conOpt.setWill(willTopic, payload, 0, false);
        conOpt.setCleanSession(false);
        client.connect(conOpt);

        Assert.assertEquals(1, sockets.size());
        expectConnectionFailure = true;
        sockets.get(0).close();
        Thread.sleep(testDelay);

        Assert.assertEquals(1, receivedMessages.size());
        Assert.assertEquals(true, Arrays.equals(receivedMessages.get(0).getPayload(), payload));
        client2.disconnect();
    }

    public void testSubscribeMultiple() throws MqttException {
        client.connect(conOpt);
        publish(client, "/topic/1", 1, "msq1-qos1".getBytes());

        client2.connect(conOpt);
        client2.setCallback(this);
        client2.subscribe("/topic/#");
        client2.subscribe("/topic/#");

        publish(client, "/topic/2", 0, "msq2-qos0".getBytes());
        publish(client, "/topic/3", 1, "msq3-qos1".getBytes());
        publish(client, topic, 0, "msq4-qos0".getBytes());
        publish(client, topic, 1, "msq4-qos1".getBytes());

        Assert.assertEquals(2, receivedMessages.size());
        client.disconnect();
        client2.disconnect();
    }

    public void testPublishMultiple() throws MqttException, InterruptedException {
        int pubCount = 50;
        for (int subQos=0; subQos < 2; subQos++){
            for (int pubQos=0; pubQos < 2; pubQos++){
                client.connect(conOpt);
                client.subscribe(topic, subQos);
                client.setCallback(this);
                long start = System.currentTimeMillis();
                for (int i=0; i<pubCount; i++){
                    publish(client, topic, pubQos, payload);
                }
                Thread.sleep(testDelay);
                Assert.assertEquals(pubCount, receivedMessages.size());
                System.out.println("publish QOS" + pubQos + " subscribe QOS" + subQos +
                                   ", " + pubCount + " msgs took " +
                                   (lastReceipt - start)/1000.0 + "sec");
                client.disconnect();
                receivedMessages.clear();
            }
        }
    }

    private void publish(MqttClient client, String topicName, int qos, byte[] payload) throws MqttException {
    	MqttTopic topic = client.getTopic(topicName);
   		MqttMessage message = new MqttMessage(payload);
    	message.setQos(qos);
    	MqttDeliveryToken token = topic.publish(message);
    	token.waitForCompletion();
    }

    public void connectionLost(Throwable cause) {
        if (!expectConnectionFailure)
            fail("Connection unexpectedly lost");
    }

    public void messageArrived(MqttTopic topic, MqttMessage message) throws Exception {
        lastReceipt = System.currentTimeMillis();
        receivedMessages.add(message);
    }

    public void deliveryComplete(MqttDeliveryToken token) {
    }
}
