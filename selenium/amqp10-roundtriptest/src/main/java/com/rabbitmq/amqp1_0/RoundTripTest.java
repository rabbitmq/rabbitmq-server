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
  public static String getEnv(String property) {
    String value = System.getenv(property);
    if (value == null) {
      throw new IllegalArgumentException("Missing env variable " + property);
    }
    return value;
  }
  public static void main(String args[]) throws Exception {
    String hostname = getEnv("RABBITMQ_HOSTNAME", "localhost");
    String port = getEnv("RABBITMQ_AMQP_PORT", "5672");
    String scheme = getEnv("RABBITMQ_AMQP_SCHEME", "amqp");
    String uri = scheme + "://" + hostname + ":" + port;
    String username = args.length > 0 ? args[0] : getEnv("RABBITMQ_AMQP_USERNAME", "guest");
    String password = args.length > 1 ? args[1] : getEnv("RABBITMQ_AMQP_PASSWORD", "guest");
    
    boolean usemtls = Boolean.parseBoolean(getEnv("AMQP_USE_MTLS", "false"));
    
    
    if ("amqps".equals(scheme)) {
      List<String> connectionParams = new ArrayList<String>();
      String certsLocation = getEnv("RABBITMQ_CERTS");

      connectionParams.add("transport.trustStoreLocation=" + certsLocation + "/truststore.jks");
      connectionParams.add("transport.trustStorePassword=foobar");
      connectionParams.add("transport.verifyHost=true");  
      connectionParams.add("transport.trustAll=true");    

      if (usemtls) {
        connectionParams.add("amqp.saslMechanisms=EXTERNAL");
        connectionParams.add("transport.keyStoreLocation=" + certsLocation + "/client_rabbitmq.jks");
        connectionParams.add("transport.keyStorePassword=foobar");
        connectionParams.add("transport.keyAlias=client-rabbitmq-tls");
      }
      if (!connectionParams.isEmpty()) {
        uri = uri + "?" + String.join("&", connectionParams);       
        System.out.println("Using AMQP URI " + uri);
      }
    }

    assertNotNull(uri);

    Hashtable<Object, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
    env.put("connectionfactory.myFactoryLookup", uri);
    env.put("queue.myQueueLookup", "my-queue");
    env.put("jms.sendTimeout", 5);
    env.put("jms.requestTimeout", 5);
    javax.naming.Context context = new javax.naming.InitialContext(env);

    ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
    Destination queue = (Destination) context.lookup("myQueueLookup");

    try (Connection connection = 
                      createConnection(factory, usemtls, username, password)) {
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

      Thread.sleep(30000);
    }
  }  
  private static Connection createConnection(ConnectionFactory factory, 
      boolean usemtls, String username, String password) throws jakarta.jms.JMSException {
    if (usemtls) {
      return factory.createConnection();      
    }
    return factory.createConnection(username, password);
  }
}
