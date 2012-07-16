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

import org.eclipse.paho.client.mqttv3.*;
import junit.framework.Assert;
import junit.framework.TestCase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MqttTest extends TestCase implements MqttCallback {

	private final String brokerUrl = "tcp://localhost:1883";
    private String clientId;
    private String clientId2;
    private MqttClient client;
    private MqttClient client2;
	private MqttConnectOptions conOpt;
    private ArrayList<MqttMessage> receivedMessages;

    private final byte[] payload = "payload".getBytes();
    private final String topic = "test-topic";
    private int testDelay = 100;

    @Override
    public void setUp() throws MqttException {
        clientId = getClass().getSimpleName() + ((int) (10000*Math.random()));
        clientId2 = clientId + "-2";
        client = new MqttClient(brokerUrl, clientId);
        client2 = new MqttClient(brokerUrl, clientId2);
        conOpt = new MqttConnectOptions();
        setConOpts(conOpt);
        receivedMessages = new ArrayList();
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

    public void testSubscribe() throws MqttException, InterruptedException {
        client.connect(conOpt);
        client.setCallback(this);
        client.subscribe(topic);
        publish(client, topic, 0, payload);
        Thread.sleep(testDelay);
        Assert.assertEquals(1, receivedMessages.size());
        Assert.assertEquals(true, Arrays.equals(receivedMessages.get(0).getPayload(), payload));
        client.unsubscribe(topic);
        publish(client, topic, 0, payload);
        Thread.sleep(testDelay);
        Assert.assertEquals(1, receivedMessages.size());
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

    public void testNoncleanSession() throws MqttException, InterruptedException {
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

    private void publish(MqttClient client, String topicName, int qos, byte[] payload) throws MqttException {
    	MqttTopic topic = client.getTopic(topicName);
   		MqttMessage message = new MqttMessage(payload);
    	message.setQos(qos);
    	MqttDeliveryToken token = topic.publish(message);
    	token.waitForCompletion();
    }

    private void setConOpts(MqttConnectOptions conOpts) {
        conOpts.setUserName("guest");
        conOpts.setPassword("guest".toCharArray());
        conOpts.setKeepAliveInterval(10);
    }

    public void connectionLost(Throwable cause) {
        fail("Connection unexpectedly lost");
    }

    public void messageArrived(MqttTopic topic, MqttMessage message) throws Exception {
        receivedMessages.add(message);
    }

    public void deliveryComplete(MqttDeliveryToken token) {
    }
}
