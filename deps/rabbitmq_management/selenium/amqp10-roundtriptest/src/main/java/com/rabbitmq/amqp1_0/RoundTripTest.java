// vim:sw=4:et:

package com.rabbitmq.amqp1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.jms.*;
import java.util.*;
import javax.naming.Context;

/** Unit test for simple App. */
public class RoundTripTest {

  public static String getEnv(String property, String defaultValue) {
    return System.getenv(property) == null ? defaultValue : System.getenv(property);
  }
  public static void main(String args[]) throws Exception {
    String hostname = getEnv("RABBITMQ_HOSTNAME", "localhost");
    String port = getEnv("RABBITMQ_AMQP_PORT", "5672");
    String scheme = getEnv("RABBITMQ_AMQP_SCHEME", "amqp");
    String username = getEnv("RABBITMQ_AMQP_USERNAME", "guest");
    String password = getEnv("RABBITMQ_AMQP_PASSWORD", "guest");
    String uri = scheme + "://" + hostname + ":" + port;

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

    try (Connection connection = factory.createConnection(username, password)) {
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
