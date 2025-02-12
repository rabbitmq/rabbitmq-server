// The contents of this file are subject to the Mozilla Public License
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License
// at https://www.mozilla.org/en-US/MPL/2.0/
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
// the License for the specific language governing rights and
// limitations under the License.
//
// The Original Code is RabbitMQ.
//
// The Initial Developer of the Original Code is Pivotal Software, Inc.
// Copyright (c) 2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc.
// and/or its subsidiaries. All rights reserved.
//

package com.rabbitmq.amqp.tests.jms;

import static com.rabbitmq.amqp.tests.jms.Cli.startBroker;
import static com.rabbitmq.amqp.tests.jms.Cli.stopBroker;
import static com.rabbitmq.amqp.tests.jms.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.jms.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Based on
 * https://github.com/apache/qpid-jms/tree/main/qpid-jms-interop-tests/qpid-jms-activemq-tests.
 */
@JmsTestInfrastructure
public class JmsConnectionTest {

  String destination;

  @Test
  @Timeout(30)
  public void testCreateConnection() throws Exception {
    try (Connection connection = connection()) {
      assertNotNull(connection);
    }
  }

  @Test
  @Timeout(30)
  public void testCreateConnectionAndStart() throws Exception {
    try (Connection connection = connection()) {
      assertNotNull(connection);
      connection.start();
    }
  }

  @Test
  @Timeout(30)
  // Currently not supported by RabbitMQ.
  @Disabled
  public void testCreateWithDuplicateClientIdFails() throws Exception {
    JmsConnectionFactory factory = (JmsConnectionFactory) connectionFactory();
    JmsConnection connection1 = (JmsConnection) factory.createConnection();
    connection1.setClientID("Test");
    assertNotNull(connection1);
    connection1.start();
    JmsConnection connection2 = (JmsConnection) factory.createConnection();
    try {
      connection2.setClientID("Test");
      fail("should have thrown a JMSException");
    } catch (InvalidClientIDException ex) {
      // OK
    } catch (Exception unexpected) {
      fail("Wrong exception type thrown: " + unexpected);
    }

    connection1.close();
    connection2.close();
  }

  @Test
  public void testSetClientIdAfterStartedFails() {
    assertThrows(
        JMSException.class,
        () -> {
          try (Connection connection = connection()) {
            connection.setClientID("Test");
            connection.start();
            connection.setClientID("NewTest");
          }
        });
  }

  @Test
  @Timeout(30)
  public void testCreateConnectionAsSystemAdmin() throws Exception {
    JmsConnectionFactory factory = (JmsConnectionFactory) connectionFactory();
    factory.setUsername(adminUsername());
    factory.setPassword(adminPassword());
    try (Connection connection = factory.createConnection()) {
      assertNotNull(connection);
      connection.start();
    }
  }

  @Test
  @Timeout(30)
  public void testCreateConnectionCallSystemAdmin() throws Exception {
    try (Connection connection =
        connectionFactory().createConnection(adminUsername(), adminPassword())) {
      assertNotNull(connection);
      connection.start();
    }
  }

  @Test
  @Timeout(30)
  public void testCreateConnectionAsUnknwonUser() {
    assertThrows(
        JMSSecurityException.class,
        () -> {
          JmsConnectionFactory factory = (JmsConnectionFactory) connectionFactory();
          factory.setUsername("unknown");
          factory.setPassword("unknown");
          try (Connection connection = factory.createConnection()) {
            assertNotNull(connection);
            connection.start();
          }
        });
  }

  @Test
  @Timeout(30)
  public void testCreateConnectionCallUnknwonUser() {
    assertThrows(
        JMSSecurityException.class,
        () -> {
          try (Connection connection = connectionFactory().createConnection("unknown", "unknown")) {
            assertNotNull(connection);
            connection.start();
          }
        });
  }

  @Test
  @Timeout(30)
  public void testBrokerStopWontHangConnectionClose() throws Exception {
    Connection connection = connection();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    Queue queue = queue(destination);
    connection.start();

    MessageProducer producer = session.createProducer(queue);
    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

    Message m = session.createTextMessage("Sample text");
    producer.send(m);

    try {
      stopBroker();
      try {
        connection.close();
      } catch (Exception ex) {
        fail("Should not have thrown an exception.");
      }
    } finally {
      startBroker();
    }
  }

  @Test
  @Timeout(60)
  public void testConnectionExceptionBrokerStop() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    try (Connection connection = connection()) {
      connection.setExceptionListener(exception -> latch.countDown());
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      assertNotNull(session);

      try {
        stopBroker();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
      } finally {
        startBroker();
      }
    }
  }
}
