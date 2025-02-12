package com.rabbitmq.amqp.tests.jms;

import static com.rabbitmq.amqp.tests.jms.TestUtils.protonClient;
import static com.rabbitmq.amqp.tests.jms.TestUtils.protonConnection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.jms.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;

import com.rabbitmq.qpid.protonj2.client.Client;
import com.rabbitmq.qpid.protonj2.client.Delivery;
import com.rabbitmq.qpid.protonj2.client.Receiver;
import jakarta.jms.Queue;
import org.junit.jupiter.api.Test;

@JmsTestInfrastructure
public class JmsTest {

    private javax.naming.Context getContext() throws Exception{
        // Configure a JNDI initial context, see
        // https://github.com/apache/qpid-jms/blob/main/qpid-jms-docs/Configuration.md#configuring-a-jndi-initialcontext
        Hashtable<Object, Object> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");

        String uri = System.getProperty("rmq_broker_uri", "amqp://localhost:5672");
        // For a list of options, see
        // https://github.com/apache/qpid-jms/blob/main/qpid-jms-docs/Configuration.md#jms-configuration-options
        uri = uri + "?jms.clientID=my-client-id";
        env.put("connectionfactory.myConnection", uri);

        String queueName = System.getProperty("queue");
        if (queueName != null) {
            env.put("queue.myQueue", queueName);
        }

        javax.naming.Context context = new javax.naming.InitialContext(env);
        return context;
    }

    // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-types
    @Test
    public void message_types_jms_to_jms() throws Exception {
        Context context = getContext();
        ConnectionFactory factory = (ConnectionFactory) context.lookup("myConnection");

        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession();
            Destination queue = (Destination) context.lookup("myQueue");
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            // TextMessage
            String msg1 = "msg1";
            TextMessage textMessage = session.createTextMessage(msg1);
            producer.send(textMessage);
            TextMessage receivedTextMessage = (TextMessage) consumer.receive(5000);
            assertEquals(msg1, receivedTextMessage.getText());

            // BytesMessage
            String msg2 = "msg2";
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeUTF(msg2);
            producer.send(bytesMessage);
            BytesMessage receivedBytesMessage = (BytesMessage) consumer.receive(5000);
            assertEquals(msg2, receivedBytesMessage.readUTF());

            // MapMessage
            MapMessage mapMessage = session.createMapMessage();
            mapMessage.setString("key1", "value");
            mapMessage.setBoolean("key2", true);
            mapMessage.setDouble("key3", 1.0);
            mapMessage.setLong("key4", 1L);
            producer.send(mapMessage);
            MapMessage receivedMapMessage = (MapMessage) consumer.receive(5000);
            assertEquals("value", receivedMapMessage.getString("key1"));
            assertEquals(true, receivedMapMessage.getBoolean("key2"));
            assertEquals(1.0, receivedMapMessage.getDouble("key3"));
            assertEquals(1L, receivedMapMessage.getLong("key4"));

            // StreamMessage
            StreamMessage streamMessage = session.createStreamMessage();
            streamMessage.writeString("value");
            streamMessage.writeBoolean(true);
            streamMessage.writeDouble(1.0);
            streamMessage.writeLong(1L);
            producer.send(streamMessage);
            StreamMessage receivedStreamMessage = (StreamMessage) consumer.receive(5000);
            assertEquals("value", receivedStreamMessage.readString());
            assertEquals(true, receivedStreamMessage.readBoolean());
            assertEquals(1.0, receivedStreamMessage.readDouble());
            assertEquals(1L, receivedStreamMessage.readLong());

            // ObjectMessage
            ObjectMessage objectMessage = session.createObjectMessage();
            ArrayList<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
            objectMessage.setObject(list);
            producer.send(objectMessage);
            ObjectMessage receivedObjectMessage = (ObjectMessage) consumer.receive(5000);
            assertEquals(list, receivedObjectMessage.getObject());
        }
    }

    String destination;

    @Test
    public void message_types_jms_to_amqp() throws Exception {
        Context context = getContext();
        ConnectionFactory factory = (ConnectionFactory) context.lookup("myConnection");

        Queue queue = TestUtils.queue(destination);
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
            assertNotNull(delivery);
            assertEquals(msg1, delivery.message().body());

            delivery = receiver.receive(10, TimeUnit.SECONDS);
            assertNotNull(delivery);
            com.rabbitmq.qpid.protonj2.client.Message<Map<String, Object>> mapMessage = delivery.message();
            assertThat(mapMessage.body()).containsEntry("key1", "value")
                .containsEntry("key2", true)
                .containsEntry("key3", -1.1)
                .containsEntry("key4", -1L);

            delivery = receiver.receive(10, TimeUnit.SECONDS);
            assertNotNull(delivery);
            com.rabbitmq.qpid.protonj2.client.Message<List<Object>> listMessage = delivery.message();
            assertThat(listMessage.body()).containsExactly("value", true, -1.1, -1L);
    }
  }

  // Test that Request/reply pattern using a TemporaryQueue works.
  // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#requestreply-pattern-using-a-temporaryqueue-jakarta-ee
  @Test
  public void temporary_queue_rpc() throws Exception {
        Context context = getContext();
        ConnectionFactory factory = (ConnectionFactory) context.lookup("myConnection");

        try (JMSContext clientContext = factory.createContext()) {
            Destination responseQueue = clientContext.createTemporaryQueue();
            JMSConsumer clientConsumer = clientContext.createConsumer(responseQueue);

            Destination requestQueue = (Destination) context.lookup("myQueue");
            TextMessage clientRequestMessage = clientContext.createTextMessage("hello");
            clientContext.createProducer().
                setJMSReplyTo(responseQueue).
                send(requestQueue, clientRequestMessage);

            // Let's open a new connection to simulate the RPC server.
            try (JMSContext serverContext = factory.createContext()) {
                JMSConsumer serverConsumer = serverContext.createConsumer(requestQueue);
                TextMessage serverRequestMessage = (TextMessage) serverConsumer.receive(5000);

                TextMessage serverResponseMessage = serverContext.createTextMessage(
                    serverRequestMessage.getText().toUpperCase());
                serverContext.createProducer().
                    send(serverRequestMessage.getJMSReplyTo(), serverResponseMessage);
            }

            TextMessage clientResponseMessage = (TextMessage) clientConsumer.receive(5000);
            assertEquals("HELLO", clientResponseMessage.getText());
        }
    }

    // Test that a temporary queue can be deleted.
    @Test
    public void temporary_queue_delete() throws Exception {
        Context context = getContext();
        ConnectionFactory factory = (ConnectionFactory) context.lookup("myConnection");

        try (JMSContext clientContext = factory.createContext()) {
            TemporaryQueue queue = clientContext.createTemporaryQueue();
            queue.delete();
            try {
                clientContext.createProducer().send(queue, "hello");
                fail("should not be able to create producer for deleted temporary queue");
            } catch (IllegalStateRuntimeException expectedException) {
                assertEquals("Temporary destination has been deleted", expectedException.getMessage());
            }
        }
    }
}
