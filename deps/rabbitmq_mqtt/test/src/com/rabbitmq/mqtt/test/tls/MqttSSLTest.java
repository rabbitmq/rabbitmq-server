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
//  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
//

package com.rabbitmq.mqtt.test.tls;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.ArrayList;


/**
 * MQTT v3.1 tests
 * TODO: synchronise access to variables
 */

public class MqttSSLTest extends TestCase implements MqttCallback {

    private final int port = 8883;
    private final String brokerUrl = "ssl://" + getHost() + ":" + port;
    private String clientId;
    private String clientId2;
    private MqttClient client;
    private MqttClient client2;
    private MqttConnectOptions conOpt;
    private ArrayList<MqttMessage> receivedMessages;

    private long lastReceipt;
    private boolean expectConnectionFailure;


    private static final String getHost() {
        Object host = System.getProperty("hostname");
        assertNotNull(host);
        return host.toString();
    }

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
    public void setUp() throws MqttException, IOException {
        clientId = getClass().getSimpleName() + ((int) (10000 * Math.random()));
        clientId2 = clientId + "-2";
        client = new MqttClient(brokerUrl, clientId, null);
        client2 = new MqttClient(brokerUrl, clientId2, null);
        conOpt = new MyConnOpts();
        conOpt.setSocketFactory(MutualAuth.getSSLContext().getSocketFactory());
        setConOpts(conOpt);
        receivedMessages = new ArrayList<MqttMessage>();
        expectConnectionFailure = false;
    }

    @Override
    public void tearDown() throws MqttException {
        // clean any sticky sessions
        setConOpts(conOpt);
        client = new MqttClient(brokerUrl, clientId, null);
        try {
            client.connect(conOpt);
            client.disconnect();
        } catch (Exception _) {
        }

        client2 = new MqttClient(brokerUrl, clientId2, null);
        try {
            client2.connect(conOpt);
            client2.disconnect();
        } catch (Exception _) {
        }
    }


    private void setConOpts(MqttConnectOptions conOpts) {
        // provide authentication if the broker needs it
        // conOpts.setUserName("guest");
        // conOpts.setPassword("guest".toCharArray());
        conOpts.setCleanSession(true);
        conOpts.setKeepAliveInterval(60);
    }


    public void testAnonymousUser() throws MqttException {
        try {
            conOpt.setSocketFactory(MutualAuth.getSSLContext().getSocketFactory());
            client.connect(conOpt);
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_NOT_AUTHORIZED, ex.getReasonCode());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    public void testCertLogin() throws MqttException {
        try {
            conOpt.setSocketFactory(MutualAuth.getSSLContextWithClientCert().getSocketFactory());
            client.connect(conOpt);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }


    public void testInvalidUser() throws MqttException {
        conOpt.setUserName("invalid-user");
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            Assert.assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
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
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }


    public void connectionLost(Throwable cause) {
        if (!expectConnectionFailure)
            fail("Connection unexpectedly lost");
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        lastReceipt = System.currentTimeMillis();
        receivedMessages.add(message);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
