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
import static com.rabbitmq.amqp.tests.jms.TestUtils.protonClient;
import static com.rabbitmq.amqp.tests.jms.TestUtils.protonConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.rabbitmq.amqp.tests.jms.TestUtils.*;
import com.rabbitmq.qpid.protonj2.client.Client;
import com.rabbitmq.qpid.protonj2.client.Delivery;
import com.rabbitmq.qpid.protonj2.client.Receiver;
import jakarta.jms.*;
import jakarta.jms.Queue;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@JmsTestInfrastructure
public class JmsTest {

  ConnectionFactory factory;

  // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-types
  @Test
  public void message_types_jms_to_jms(Queue queue) throws Exception {
    try (Connection connection = factory.createConnection()) {
      Session session = connection.createSession();
      MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      // TextMessage
      String msg1 = "msg1";
      TextMessage textMessage = session.createTextMessage(msg1);
      producer.send(textMessage);
      TextMessage receivedTextMessage = (TextMessage) consumer.receive(5000);
      assertThat(receivedTextMessage).hasText(msg1);

      // BytesMessage
      String msg2 = "msg2";
      BytesMessage bytesMessage = session.createBytesMessage();
      bytesMessage.writeUTF(msg2);
      producer.send(bytesMessage);
      BytesMessage receivedBytesMessage = (BytesMessage) consumer.receive(5000);
      assertThat(receivedBytesMessage.readUTF()).isEqualTo(msg2);

      // MapMessage
      MapMessage mapMessage = session.createMapMessage();
      mapMessage.setString("key1", "value");
      mapMessage.setBoolean("key2", true);
      mapMessage.setDouble("key3", 1.0);
      mapMessage.setLong("key4", 1L);
      producer.send(mapMessage);
      MapMessage receivedMapMessage = (MapMessage) consumer.receive(5000);
      assertThat(receivedMapMessage.getString("key1")).isEqualTo("value");
      assertThat(receivedMapMessage.getBoolean("key2")).isTrue();
      assertThat(receivedMapMessage.getDouble("key3")).isEqualTo(1.0);
      assertThat(receivedMapMessage.getLong("key4")).isEqualTo(1L);

      // StreamMessage
      StreamMessage streamMessage = session.createStreamMessage();
      streamMessage.writeString("value");
      streamMessage.writeBoolean(true);
      streamMessage.writeDouble(1.0);
      streamMessage.writeLong(1L);
      producer.send(streamMessage);
      StreamMessage receivedStreamMessage = (StreamMessage) consumer.receive(5000);
      assertThat(receivedStreamMessage.readString()).isEqualTo("value");
      assertThat(receivedStreamMessage.readBoolean()).isTrue();
      assertThat(receivedStreamMessage.readDouble()).isEqualTo(1.0);
      assertThat(receivedStreamMessage.readLong()).isEqualTo(1L);

      // ObjectMessage
      ObjectMessage objectMessage = session.createObjectMessage();
      ArrayList<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
      objectMessage.setObject(list);
      producer.send(objectMessage);
      ObjectMessage receivedObjectMessage = (ObjectMessage) consumer.receive(5000);
      assertThat(receivedObjectMessage.getObject()).isEqualTo(list);
    }
  }

  @Test
  public void message_types_jms_to_amqp(Queue queue) throws Exception {
    String msg1 = "msg1🥕";
    try (Connection connection = factory.createConnection()) {
      Session session = connection.createSession();
      MessageProducer producer = session.createProducer(queue);

      // TextMessage
      TextMessage textMessage = session.createTextMessage(msg1);
      producer.send(textMessage);

      // MapMessage
      MapMessage mapMessage = session.createMapMessage();
      mapMessage.setString("key1", "value");
      mapMessage.setBoolean("key2", true);
      mapMessage.setDouble("key3", -1.1);
      mapMessage.setLong("key4", -1L);
      producer.send(mapMessage);

      // StreamMessage
      StreamMessage streamMessage = session.createStreamMessage();
      streamMessage.writeString("value");
      streamMessage.writeBoolean(true);
      streamMessage.writeDouble(-1.1);
      streamMessage.writeLong(-1L);
      producer.send(streamMessage);
    }

    try (Client client = protonClient();
        com.rabbitmq.qpid.protonj2.client.Connection amqpConnection = protonConnection(client)) {
      Receiver receiver = amqpConnection.openReceiver(queue.getQueueName());
      Delivery delivery = receiver.receive(10, TimeUnit.SECONDS);
      assertThat(delivery).isNotNull();
      assertThat(delivery.message().body()).isEqualTo(msg1);

      delivery = receiver.receive(10, TimeUnit.SECONDS);
      assertThat(delivery).isNotNull();
      com.rabbitmq.qpid.protonj2.client.Message<Map<String, Object>> mapMessage =
          delivery.message();
      assertThat(mapMessage.body())
          .containsEntry("key1", "value")
          .containsEntry("key2", true)
          .containsEntry("key3", -1.1)
          .containsEntry("key4", -1L);

      delivery = receiver.receive(10, TimeUnit.SECONDS);
      assertThat(delivery).isNotNull();
      com.rabbitmq.qpid.protonj2.client.Message<List<Object>> listMessage = delivery.message();
      assertThat(listMessage.body()).containsExactly("value", true, -1.1, -1L);
    }
  }

  // Test that Request/reply pattern using a TemporaryQueue works.
  // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#requestreply-pattern-using-a-temporaryqueue-jakarta-ee
  @Test
  public void temporary_queue_rpc(@Classic Queue requestQueue) throws Exception {
    try (JMSContext clientContext = factory.createContext()) {
      Destination responseQueue = clientContext.createTemporaryQueue();
      JMSConsumer clientConsumer = clientContext.createConsumer(responseQueue);

      TextMessage clientRequestMessage = clientContext.createTextMessage("hello");
      clientContext
          .createProducer()
          .setJMSReplyTo(responseQueue)
          .send(requestQueue, clientRequestMessage);

      // Let's open a new connection to simulate the RPC server.
      try (JMSContext serverContext = factory.createContext()) {
        JMSConsumer serverConsumer = serverContext.createConsumer(requestQueue);
        TextMessage serverRequestMessage = (TextMessage) serverConsumer.receive(5000);

        TextMessage serverResponseMessage =
            serverContext.createTextMessage(serverRequestMessage.getText().toUpperCase());
        serverContext
            .createProducer()
            .send(serverRequestMessage.getJMSReplyTo(), serverResponseMessage);
      }

      Message clientResponseMessage = clientConsumer.receive(5000);
      assertThat(clientResponseMessage).hasText("HELLO");
    }
  }

  // Test that a temporary queue can be deleted.
  @Test
  public void temporary_queue_delete() throws Exception {
    try (JMSContext clientContext = factory.createContext()) {
      TemporaryQueue queue = clientContext.createTemporaryQueue();
      queue.delete();
      try {
        clientContext.createProducer().send(queue, "hello");
        fail("should not be able to create producer for deleted temporary queue");
      } catch (IllegalStateRuntimeException expectedException) {
        assertThat(expectedException).hasMessage("Temporary destination has been deleted");
      }
    }
  }

  // Test that consumers can filter on application-specific properties
  @Test
  public void message_selector_application_properties(
      @QueueArgs(boolArgs = {@QueueArgBool(name = "x-filter-enabled", value = true)}) Queue queue)
      throws Exception {
    try (Connection connection = factory.createConnection()) {
      Session session = connection.createSession();
      connection.start();

      testSelector(session, queue, "price = 19.99", new int[] {1});
      testSelector(session, queue, "age >= 30", new int[] {2, 3, 5});
      testSelector(session, queue, "premium = TRUE AND price < 40.0", new int[] {2, 5});
      testSelector(session, queue, "region = 'Europe' OR region = 'Asia'", new int[] {1, 2});
      testSelector(session, queue, "priority BETWEEN 2 AND 4", new int[] {1, 4, 5});
      testSelector(session, queue, "region IN ('Australia', 'North America')", new int[] {4, 5});
      testSelector(session, queue, "region LIKE '%America'", new int[] {4});
      testSelector(session, queue, "region IS NULL", new int[] {3});
      testSelector(session, queue, "category is not null", new int[] {4, 5});
      testSelector(
          session,
          queue,
          "age > 30 AND premium = true OR price < 15.0 AND region LIKE '%America'",
          new int[] {2, 3, 4, 5});
    }
  }

  private void testSelector(Session session, Queue queue, String selector, int[] expectedMsgIds)
      throws Exception {
    MessageProducer producer = session.createProducer(queue);

    sendTestMessage(session, producer, 1, 25, 19.99, 3, false, "Europe", null);
    sendTestMessage(session, producer, 2, 35, 29.99, 1, true, "Asia", null);
    sendTestMessage(session, producer, 3, 42, 49.99, 5, true, null, null);
    sendTestMessage(session, producer, 4, 18, 9.99, 2, false, "North America", "Electronics");
    sendTestMessage(session, producer, 5, 50, 39.99, 4, true, "Australia", "Books");

    MessageConsumer consumer = session.createConsumer(queue, selector);

    // Collect received messages
    List<Integer> receivedMessageIds = new ArrayList<>();
    for (int i = 0; i < expectedMsgIds.length; i++) {
      Message message = consumer.receive(9000);
      if (message != null) {
        TextMessage textMessage = (TextMessage) message;
        receivedMessageIds.add(Integer.parseInt(textMessage.getText().split(" ")[1]));
      } else {
        break;
      }
    }

    // Verify no additional unexpected messages
    Message unexpectedMessage = consumer.receive(20);
    assertThat(unexpectedMessage).isNull();

    assertThat(receivedMessageIds)
        .containsExactly(Arrays.stream(expectedMsgIds).boxed().toArray(Integer[]::new));

    consumer.close();
    producer.close();

    // Clear the queue after the test
    MessageConsumer cleanupConsumer = session.createConsumer(queue);
    int remainingMessages = 5 - expectedMsgIds.length;
    for (int i = 0; i < remainingMessages; i++) {
      Message msg = cleanupConsumer.receive(9000);
      assertThat(msg).isNotNull();
    }
    cleanupConsumer.close();
  }

  private void sendTestMessage(
      Session session,
      MessageProducer producer,
      int messageId,
      int age,
      double price,
      int priority,
      boolean premium,
      String region,
      String category)
      throws JMSException {

    TextMessage message = session.createTextMessage("Message " + messageId);
    message.setIntProperty("age", age);
    message.setDoubleProperty("price", price);
    message.setIntProperty("priority", priority);
    message.setBooleanProperty("premium", premium);
    if (region != null) {
      message.setStringProperty("region", region);
    }
    if (category != null) {
      message.setStringProperty("category", category);
    }
    producer.send(message);
  }

  // Test that consumers can filter on header fields
  @Test
  public void message_selector_header_fields(
      @QueueArgs(
              boolArgs = {@QueueArgBool(name = "x-filter-enabled", value = true)},
              listArgs = {
                @QueueArgList(
                    name = "x-filter-field-names",
                    values = {
                      "durable",
                      "priority",
                      "message-id",
                      "creation-time",
                      "correlation-id",
                      "subject",
                      "user-id",
                      "group-id",
                      "group-sequence"
                    })
              })
          Queue queue)
      throws Exception {
    try (Connection connection = factory.createConnection()) {
      Session session = connection.createSession();
      connection.start();

      // "Message header field references are restricted to
      // JMSDeliveryMode, JMSPriority, JMSMessageID, JMSTimestamp, JMSCorrelationID, and JMSType."
      // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax
      testJMSDeliveryMode(session, queue);
      testJMSPriority(session, queue);
      testJMSMessageID(session, queue);
      testJMSTimestamp(session, queue);
      testJMSCorrelationID(session, queue);
      testJMSType(session, queue);

      // reserved 'JMSX' property names
      testJMSXUserID(session, queue);
      testJMSXAppID(session, queue);
      testJMSXGroupID(session, queue);
      // We do not test setting a message selector on JMSXDeliveryCount because:
      // "the effect of setting a message selector on a property (such as JMSXDeliveryCount)
      // which is set by the provider on receive is undefined."
      // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmsxdeliverycount-jms_spec-42
    }
  }

  private void testJMSDeliveryMode(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);

    // default is DeliveryMode.PERSISTENT
    TextMessage msg1 = session.createTextMessage("msg 1");
    producer.send(msg1);

    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    TextMessage msg2 = session.createTextMessage("msg 2");
    producer.send(msg2);

    MessageConsumer consumer = session.createConsumer(queue, "JMSDeliveryMode <> 'PERSISTENT'");
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("msg 2");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSDeliveryMode = 'PERSISTENT'");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("msg 1");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
    TextMessage msg3 = session.createTextMessage("msg 3");
    msg3.setStringProperty("key1", "PERSISTENT");
    msg3.setStringProperty("key2", "NON_PERSISTENT");
    producer.send(msg3);

    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    TextMessage msg4 = session.createTextMessage("msg 4");
    msg4.setStringProperty("key1", "PERSISTENT");
    msg4.setStringProperty("key2", "NON_PERSISTENT");
    producer.send(msg4);

    // Let's test that matching JMSDeliveryMode works not only with string literals
    // but also with other identifiers that have a string value.
    consumer = session.createConsumer(queue, "key2 = JMSDeliveryMode");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("msg 4");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "key1 = JMSDeliveryMode");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("msg 3");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    producer.close();
  }

  private void testJMSPriority(Session session, Queue queue) throws Exception {
    // "The message producer's default priority is 4."
    MessageProducer producer = session.createProducer(queue);
    TextMessage outMsg = session.createTextMessage("Default Priority");
    producer.send(outMsg);
    // "Jakarta Messaging defines a ten level priority value
    // with 0 as the lowest priority and 9 as the highest."
    for (int i = 0; i <= 9; i++) {
      outMsg = session.createTextMessage("Priority " + i);
      producer.setPriority(i);
      producer.send(outMsg);
    }
    producer.close();

    // Even if we didn't set a priority explicitly, the message producer's default priority is 4.
    // Therefore, the following IS NULL selector should not match any message.
    // We expect here the same behaviour as in ActiveMQ Artemis.
    MessageConsumer consumer = session.createConsumer(queue, "JMSPriority IS NULL");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    // In the following assertions we check that the messages are received in the order
    // they were sent because we know that quorum queues support only two priorities:
    // <= 4 and > 4.

    consumer = session.createConsumer(queue, "JMSPriority = 4");
    Message msg = consumer.receive(9000);
    assertThat(msg).isNotNull().hasText("Default Priority").hasPriority(4);
    msg = consumer.receive(9000);
    assertThat(msg).isNotNull().hasText("Priority 4").hasPriority(4);
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSPriority >= 5");
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(5);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(6);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(7);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(8);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(9);
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSPriority BETWEEN 0 AND 3");
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(0);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(1);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(2);
    msg = consumer.receive(9000);
    assertThat(msg).hasPriority(3);
    assertThat(consumer.receive(10)).isNull();
    consumer.close();
  }

  private void testJMSMessageID(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);
    producer.setDisableMessageID(false);
    TextMessage msg1 = session.createTextMessage("Message 1");
    producer.send(msg1);
    String messageId1 = msg1.getJMSMessageID();
    TextMessage msg2 = session.createTextMessage("Message 2");
    producer.send(msg2);
    String messageId2 = msg2.getJMSMessageID();
    producer.close();

    MessageConsumer consumer = session.createConsumer(queue, "JMSMessageID = '" + messageId2 + "'");
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Message 2").hasId(messageId2);
    org.assertj.core.api.Assertions.assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSMessageID = '" + messageId1 + "'");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Message 1").hasId(messageId1);
    assertThat(consumer.receive(10)).isNull();
    consumer.close();
  }

  private void testJMSTimestamp(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);
    producer.setDisableMessageTimestamp(false);

    // Send messages with a delay between them
    TextMessage msg1 = session.createTextMessage("Early Message");
    producer.send(msg1);
    long timestamp1 = msg1.getJMSTimestamp();
    Thread.sleep(200);
    TextMessage msg2 = session.createTextMessage("Later Message");
    producer.send(msg2);
    long timestamp2 = msg2.getJMSTimestamp();
    producer.close();

    MessageConsumer consumer = session.createConsumer(queue, "JMSTimestamp > " + timestamp1);
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Later Message").hasTimestamp(timestamp2);
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSTimestamp <= " + timestamp1);
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Early Message").hasTimestamp(timestamp1);
    assertThat(consumer.receive(10)).isNull();
    consumer.close();
  }

  private void testJMSCorrelationID(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);
    TextMessage msg1 = session.createTextMessage("Message 1");
    msg1.setJMSCorrelationID("correlation-123");
    producer.send(msg1);
    TextMessage msg2 = session.createTextMessage("Message 2");
    msg2.setJMSCorrelationID("correlation-456");
    producer.send(msg2);
    TextMessage msg3 = session.createTextMessage("Message 3");
    producer.send(msg3);
    TextMessage msg4 = session.createTextMessage("Message 4");
    msg4.setJMSCorrelationIDAsBytes(new byte[] {0, 1, 2});
    producer.send(msg4);
    producer.close();

    MessageConsumer consumer =
        session.createConsumer(queue, "JMSCorrelationID LIKE 'correlation-%'");
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Message 1").hasCorrelationId("correlation-123");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Message 2").hasCorrelationId("correlation-456");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSCorrelationID IS NOT NULL");
    received = consumer.receive(9000);
    assertThat(received)
        .isNotNull()
        .hasText("Message 4")
        .hasCorrelationId(new byte[] {0, 1, 2})
        // That's what we expect according section 3.2.1.1 of amqp-bindmap-jms-v1.0-wd10
        .hasCorrelationId("ID:AMQP_BINARY:000102");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSCorrelationID IS NULL");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Message 3");
    assertThat(received.getJMSCorrelationID()).isNull();
    org.assertj.core.api.Assertions.assertThat(consumer.receive(10)).isNull();
    consumer.close();
  }

  private void testJMSType(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);

    TextMessage msg1 = session.createTextMessage("Type 1 Message");
    msg1.setJMSType("type-1");
    producer.send(msg1);

    TextMessage msg2 = session.createTextMessage("Type 2 Message");
    msg2.setJMSType("type-2");
    producer.send(msg2);

    TextMessage msg3 = session.createTextMessage("No Type Message");
    producer.send(msg3);

    producer.close();

    MessageConsumer consumer = session.createConsumer(queue, "JMSType = 'type-1'");
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Type 1 Message").hasType("type-1");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSType IS NULL");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("No Type Message");
    assertThat(received.getJMSType()).isNull();
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSType IN ('type-1', 'type-2')");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("Type 2 Message").hasType("type-2");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();
  }

  private void testJMSXUserID(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);

    // Since we configured jms.populateJMSXUserID=true
    // the MessageProducer populates the JMSXUserID value for each sent message.

    TextMessage msg = session.createTextMessage("m1");
    producer.send(msg);

    MessageConsumer consumer = session.createConsumer(queue, "JMSXUserID = 'guest'");
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("m1").hasProperty("JMSXUserID", "guest");
    consumer.close();

    msg = session.createTextMessage("m2");
    producer.send(msg);

    consumer = session.createConsumer(queue, "'guest' <> JMSXUserID");
    assertThat(consumer.receive(10)).isNull();
    consumer.close();

    consumer = session.createConsumer(queue, "JMSXUserID LIKE 'gues_'");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("m2").hasProperty("JMSXUserID", "guest");
    consumer.close();

    msg = session.createTextMessage("m3");
    producer.send(msg);

    consumer = session.createConsumer(queue, "JMSXUserID IN ('other', 'guest')");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("m3").hasProperty("JMSXUserID", "guest");
    consumer.close();

    producer.close();
  }

  private void testJMSXAppID(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);

    TextMessage msg = session.createTextMessage("m1");
    msg.setStringProperty("JMSXAppID", "myapp 1");
    producer.send(msg);

    msg = session.createTextMessage("m2");
    msg.setStringProperty("JMSXAppID", "myapp 2");
    producer.send(msg);

    producer.close();

    MessageConsumer consumer = session.createConsumer(queue, "JMSXAppID LIKE '%2'");
    Message received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("m2").hasProperty("JMSXAppID", "myapp 2");
    consumer.close();

    consumer = session.createConsumer(queue, "'myapp 2' <> JMSXAppID");
    received = consumer.receive(9000);
    assertThat(received).isNotNull().hasText("m1").hasProperty("JMSXAppID", "myapp 1");
    consumer.close();
  }

  private void testJMSXGroupID(Session session, Queue queue) throws Exception {
    MessageProducer producer = session.createProducer(queue);

    TextMessage msg1 = session.createTextMessage("Group A Message 1");
    msg1.setStringProperty("JMSXGroupID", "group-A");
    msg1.setIntProperty("JMSXGroupSeq", 1);
    producer.send(msg1);

    TextMessage msg2 = session.createTextMessage("Group B Message");
    msg2.setStringProperty("JMSXGroupID", "group-B");
    msg2.setIntProperty("JMSXGroupSeq", 2_000_000_000);
    producer.send(msg2);

    TextMessage msg3 = session.createTextMessage("Group A Message 2");
    msg3.setStringProperty("JMSXGroupID", "group-A");
    msg3.setIntProperty("JMSXGroupSeq", 2);
    producer.send(msg3);

    producer.close();

    MessageConsumer consumer = session.createConsumer(queue, "JMSXGroupID = 'group-A'");

    Message received = consumer.receive(9000);
    assertThat(received)
        .isNotNull()
        .hasText("Group A Message 1")
        .hasProperty("JMSXGroupID", "group-A")
        .hasProperty("JMSXGroupSeq", 1);
    assertThat(received.getIntProperty("JMSXGroupSeq")).isEqualTo(1);

    received = consumer.receive(9000);
    assertThat(received)
        .isNotNull()
        .hasText("Group A Message 2")
        .hasProperty("JMSXGroupID", "group-A")
        .hasProperty("JMSXGroupSeq", 2);

    consumer.close();

    consumer = session.createConsumer(queue, "JMSXGroupSeq > 1000000000");

    received = consumer.receive(9000);
    assertThat(received)
        .isNotNull()
        .hasText("Group B Message")
        .hasProperty("JMSXGroupID", "group-B")
        .hasProperty("JMSXGroupSeq", 2_000_000_000);

    consumer.close();
  }

  // Helper method to clean up the queue after a test
  private void cleanupQueue(Session session, Queue queue, int expectedRemainingMessages)
      throws Exception {
    MessageConsumer cleanupConsumer = session.createConsumer(queue);

    // Consume exactly the expected number of remaining messages
    for (int i = 0; i < expectedRemainingMessages; i++) {
      Message msg = cleanupConsumer.receive(9000);
      assertThat(msg).isNotNull(); // We should receive exactly the expected number
    }

    // Verify no more messages (with a short timeout)
    assertThat(cleanupConsumer.receive(10)).isNull();

    cleanupConsumer.close();
  }
}
