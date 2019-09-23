//  The contents of this file are subject to the Mozilla Public License
//  Version 1.1 (the "License"); you may not use this file except in
//  compliance with the License. You may obtain a copy of the License
//  at https://www.mozilla.org/MPL/
//
//  Software distributed under the License is distributed on an "AS IS"
//  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
//  the License for the specific language governing rights and
//  limitations under the License.
//
//  The Original Code is RabbitMQ.
//
//  The Initial Developer of the Original Code is GoPivotal, Inc.
//  Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
//

package com.rabbitmq.mqtt.test.tls;

import org.eclipse.paho.client.mqttv3.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


/**
 * MQTT v3.1 tests
 *
 */

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
        assertNotNull(port);
        return port.toString();
    }

    private static String getHost() {
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


    @BeforeEach
    public void setUp() throws MqttException, IOException {
        clientId = getClass().getSimpleName() + ((int) (10000 * Math.random()));
        clientId2 = clientId + "-2";
        client = new MqttClient(brokerUrl, clientId, null);
        client2 = new MqttClient(brokerUrl, clientId2, null);
        conOpt = new MyConnOpts();
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
    public void certLogin() throws MqttException {
        try {
            conOpt.setSocketFactory(MutualAuth.getSSLContextWithClientCert().getSocketFactory());
            client.connect(conOpt);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }


    @Test public void invalidUser() throws MqttException {
        conOpt.setUserName("invalid-user");
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test public void invalidPassword() throws MqttException {
        conOpt.setUserName("invalid-user");
        conOpt.setPassword("invalid-password".toCharArray());
        try {
            client.connect(conOpt);
            fail("Authentication failure expected");
        } catch (MqttException ex) {
            assertEquals(MqttException.REASON_CODE_FAILED_AUTHENTICATION, ex.getReasonCode());
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
        receivedMessages.add(message);
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
