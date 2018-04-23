// vim:sw=4:et:

package com.rabbitmq.amqp1_0.tests.jms;

import java.util.*;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

/**
 * Unit test for simple App.
 */
public class RoundTripTest
    extends TestCase
{
    public static final String ADDRESS = "/jms-roundtrip-q";
    public static final String PAYLOAD = "Payload";

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RoundTripTest(String testName)
    {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite(RoundTripTest.class);
    }

    public void test_roundtrip () throws Exception
    {
        String uri = System.getProperty("rmq_broker_uri");
        String address = uri + ADDRESS;
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        env.put("connectionfactory.myFactoryLookup", uri);
        env.put("queue.myQueueLookup", "my-queue");
        env.put("jms.sendTimeout", 5);
        env.put("jms.requestTimeout", 5);
        javax.naming.Context context = new javax.naming.InitialContext(env);

        assertNotNull(uri);

        ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
        Destination queue = (Destination) context.lookup("myQueueLookup");

        Connection connection = factory.createConnection("guest", "guest");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer messageProducer = session.createProducer(queue);
        MessageConsumer messageConsumer = session.createConsumer(queue);

        TextMessage message = session.createTextMessage("Hello world!");
        messageProducer.send(message, DeliveryMode.NON_PERSISTENT,
                Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        TextMessage receivedMessage = (TextMessage) messageConsumer.receive(2000L);

        assertEquals(message.getText(), receivedMessage.getText());
    }
}
