// vim:sw=4:et:

package com.rabbitmq.amqp1_0.tests.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.jms.*;
import java.util.*;
import javax.naming.Context;
import org.junit.jupiter.api.Test;

/** Unit test for simple App. */
public class RoundTripTest {

  @Test
  public void test_roundtrip() throws Exception {
    String uri = System.getProperty("rmq_broker_uri", "amqp://localhost:5672");
    Hashtable<Object, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
    env.put("connectionfactory.myFactoryLookup", uri);
    env.put("queue.myQueueLookup", "my-queue");
    env.put("jms.sendTimeout", 5);
    env.put("jms.requestTimeout", 5);
    javax.naming.Context context = new javax.naming.InitialContext(env);

    assertNotNull(uri);

    ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
    Destination queue = (Destination) context.lookup("myQueueLookup");

    try (Connection connection = factory.createConnection("guest", "guest")) {
      connection.start();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer messageProducer = session.createProducer(queue);
      MessageConsumer messageConsumer = session.createConsumer(queue);

      TextMessage message = session.createTextMessage("Hello world!");
      messageProducer.send(
          message,
          DeliveryMode.NON_PERSISTENT,
          Message.DEFAULT_PRIORITY,
          Message.DEFAULT_TIME_TO_LIVE);
      TextMessage receivedMessage = (TextMessage) messageConsumer.receive(2000L);

      assertEquals(message.getText(), receivedMessage.getText());
    }
  }
}
