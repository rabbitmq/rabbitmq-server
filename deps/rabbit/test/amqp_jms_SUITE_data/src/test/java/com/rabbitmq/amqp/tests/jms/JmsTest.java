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
      assertThat(receivedTextMessage.getText()).isEqualTo(msg1);

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

      TextMessage clientResponseMessage = (TextMessage) clientConsumer.receive(5000);
      assertThat(clientResponseMessage.getText()).isEqualTo("HELLO");
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
          @QueueArgs(boolArgs = {@QueueArgBool(name = "x-filter-enabled", value = true)})
          Queue queue) throws Exception {
      try (Connection connection = factory.createConnection()) {
          Session session = connection.createSession();
          connection.start();

          testSelector(session, queue, "price = 19.99", new int[]{1});
          testSelector(session, queue, "age >= 30", new int[]{2, 3, 5});
          testSelector(session, queue, "premium = TRUE AND price < 40.0", new int[]{2, 5});
          testSelector(session, queue, "region = 'Europe' OR region = 'Asia'", new int[]{1, 2});
          testSelector(session, queue, "priority BETWEEN 2 AND 4", new int[]{1, 4, 5});
          testSelector(session, queue, "region IN ('Australia', 'North America')", new int[]{4, 5});
          testSelector(session, queue, "region LIKE '%America'", new int[]{4});
          testSelector(session, queue, "region IS NULL", new int[]{3});
          testSelector(session, queue, "category is not null", new int[]{4, 5});
          testSelector(session, queue,
                  "age > 30 AND premium = true OR price < 15.0 AND region LIKE '%America'",
                  new int[]{2, 3, 4, 5});
      }
   }

   private void testSelector(Session session, Queue queue, String selector, int[] expectedMsgIds) throws Exception {
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

       assertThat(receivedMessageIds).containsExactly(
               Arrays.stream(expectedMsgIds).boxed().toArray(Integer[]::new));

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
           String category) throws JMSException {

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
          boolArgs = {
              @QueueArgBool(name = "x-filter-enabled", value = true)
          },
          listArgs = {
              @QueueArgList(name = "x-filter-field-names", values = {"message-id", "correlation-id", "subject"})
          }
          )
          Queue queue) throws Exception {
      try (Connection connection = factory.createConnection()) {
          Session session = connection.createSession();
          MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      //TODO

      String msg1 = "msg1";
      TextMessage textMessage = session.createTextMessage(msg1);
      producer.send(textMessage);
      TextMessage receivedTextMessage = (TextMessage) consumer.receive(5000);
      assertThat(receivedTextMessage.getText()).isEqualTo(msg1);
    }
  }
}
