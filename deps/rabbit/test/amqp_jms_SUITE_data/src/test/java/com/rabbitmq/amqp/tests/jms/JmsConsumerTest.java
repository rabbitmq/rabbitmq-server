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

import static com.rabbitmq.amqp.tests.jms.Assertions.assertThat;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Based on
 * https://github.com/apache/qpid-jms/blob/main/qpid-jms-interop-tests/qpid-jms-activemq-tests/src/test/java/org/apache/qpid/jms/consumer/JmsMessageConsumerTest.java.
 */
@JmsTestInfrastructure
public class JmsConsumerTest {

  ConnectionFactory factory;

  @Test
  @Timeout(30)
  void testSelectors(
      @TestUtils.QueueArgs(
              boolArgs = {@TestUtils.QueueArgBool(name = "x-filter-enabled", value = true)},
              listArgs = {
                @TestUtils.QueueArgList(
                    name = "x-filter-field-names",
                    values = {"priority"})
              })
          Queue queue)
      throws Exception {
    try (Connection connection = factory.createConnection()) {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("hello");
      p.send(message, DeliveryMode.PERSISTENT, 5, 0);

      message = session.createTextMessage();
      message.setText("hello + 9");
      p.send(message, DeliveryMode.PERSISTENT, 9, 0);

      p.close();

      MessageConsumer consumer = session.createConsumer(queue, "JMSPriority > 8");
      Message msg = consumer.receive(5000);
      assertThat(msg).isNotNull().hasText("hello + 9");
      org.assertj.core.api.Assertions.assertThat(consumer.receive(1000)).isNull();
    }
  }

  @Test
  @Timeout(45)
  void testSelectorsWithJMSType(
      @TestUtils.QueueArgs(
              boolArgs = {@TestUtils.QueueArgBool(name = "x-filter-enabled", value = true)},
              listArgs = {
                @TestUtils.QueueArgList(
                    name = "x-filter-field-names",
                    values = {"subject"})
              })
          Queue queue)
      throws Exception {
    try (Connection connection = factory.createConnection()) {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("text");
      producer.send(
          message,
          DeliveryMode.NON_PERSISTENT,
          Message.DEFAULT_PRIORITY,
          Message.DEFAULT_TIME_TO_LIVE);

      TextMessage message2 = session.createTextMessage();
      String type = "myJMSType";
      message2.setJMSType(type);
      message2.setText("text + type");
      producer.send(
          message2,
          DeliveryMode.NON_PERSISTENT,
          Message.DEFAULT_PRIORITY,
          Message.DEFAULT_TIME_TO_LIVE);

      producer.close();

      MessageConsumer consumer = session.createConsumer(queue, "JMSType = '" + type + "'");
      Message msg = consumer.receive(5000);
      assertThat(msg).isNotNull().hasType(type).hasText("text + type");
    }
  }
}
