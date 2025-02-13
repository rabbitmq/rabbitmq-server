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

import static com.rabbitmq.amqp.tests.jms.TestUtils.brokerUri;
import static org.assertj.core.api.Assertions.*;

import jakarta.jms.*;
import jakarta.jms.IllegalStateException;
import java.util.UUID;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Based on
 * https://github.com/apache/qpid-jms/tree/main/qpid-jms-interop-tests/qpid-jms-activemq-tests.
 */
@JmsTestInfrastructure
public class JmsTemporaryQueueTest {

  ConnectionFactory factory;

  Connection connection;

  @BeforeEach
  void init() throws JMSException {
    connection = factory.createConnection();
  }

  @AfterEach
  void tearDown() throws JMSException {
    connection.close();
  }

  @Test
  @Timeout(60)
  public void testCreatePublishConsumeTemporaryQueue() throws Exception {
    connection.start();

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    assertThat(session).isNotNull();
    TemporaryQueue queue = session.createTemporaryQueue();
    MessageConsumer consumer = session.createConsumer(queue);

    MessageProducer producer = session.createProducer(queue);
    String body = UUID.randomUUID().toString();
    producer.send(session.createTextMessage(body));
    assertThat(consumer.receive(60_000).getBody(String.class)).isEqualTo(body);
  }

  @Test
  @Timeout(60)
  public void testCantConsumeFromTemporaryQueueCreatedOnAnotherConnection() throws Exception {
    connection.start();

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    TemporaryQueue tempQueue = session.createTemporaryQueue();
    session.createConsumer(tempQueue);

    Connection connection2 = new JmsConnectionFactory(brokerUri()).createConnection();
    try {
      Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
        session2.createConsumer(tempQueue);
        fail("should not be able to consumer from temporary queue from another connection");
      } catch (InvalidDestinationException ide) {
        // expected
      }
    } finally {
      connection2.close();
    }
  }

  @Test
  @Timeout(60)
  public void testCantSendToTemporaryQueueFromClosedConnection() throws Exception {
    connection.start();

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    TemporaryQueue tempQueue = session.createTemporaryQueue();

    Connection connection2 = new JmsConnectionFactory(brokerUri()).createConnection();
    try {
      Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Message msg = session2.createMessage();
      MessageProducer producer = session2.createProducer(tempQueue);

      // Close the original connection
      connection.close();

      try {
        producer.send(msg);
        fail("should not be able to send to temporary queue from closed connection");
      } catch (jakarta.jms.IllegalStateException ide) {
        // expected
      }
    } finally {
      connection2.close();
    }
  }

  @Test
  @Timeout(60)
  public void testCantDeleteTemporaryQueueWithConsumers() throws Exception {
    connection.start();

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    TemporaryQueue tempQueue = session.createTemporaryQueue();
    MessageConsumer consumer = session.createConsumer(tempQueue);

    try {
      tempQueue.delete();
      fail("should not be able to delete temporary queue with active consumers");
    } catch (IllegalStateException ide) {
      // expected
    }

    consumer.close();

    // Now it should be allowed
    tempQueue.delete();
  }
}
