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
    private final String clientId = getClass().getSimpleName();
    private MqttClient client;
	private MqttConnectOptions conOpt;
    private ArrayList<MqttMessage> receivedMessages;

    private final byte[] payload = "payload".getBytes();
    private final String topic = "test-topic";

    public void setUp() throws MqttException {
        client = new MqttClient(brokerUrl, clientId);
        conOpt = new MqttConnectOptions();
        conOpt.setUserName("guest");
        conOpt.setPassword("guest".toCharArray());
        conOpt.setKeepAliveInterval(10);
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
        Thread.sleep(1000);
        Assert.assertEquals(1, receivedMessages.size());
        Assert.assertEquals(true, Arrays.equals(receivedMessages.get(0).getPayload(), payload));
        client.unsubscribe(topic);
        publish(client, topic, 0, payload);
        Thread.sleep(1000);
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
        Thread.sleep(1000);
        Assert.assertEquals(expected.size(), receivedMessages.size());
        for (MqttMessage m : receivedMessages){
            expected.contains(new String(m.getPayload()));
        }
        client.disconnect();
    }

    private void publish(MqttClient client, String topicName, int qos, byte[] payload) throws MqttException {
    	MqttTopic topic = client.getTopic(topicName);
   		MqttMessage message = new MqttMessage(payload);
    	message.setQos(qos);
    	MqttDeliveryToken token = topic.publish(message);
    	token.waitForCompletion();
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
