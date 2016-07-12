// vim:sw=4:et:

package com.rabbitmq.amqp1_0.tests.proton;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.apache.qpid.proton.messenger.Messenger;
import org.apache.qpid.proton.messenger.impl.MessengerImpl;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;

/**
 * Unit test for simple App.
 */
public class RoundTripTest
    extends TestCase
{
    public static final String ADDRESS = "/roundtrip-q";
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

    public void test_roundtrip()
    {
        String uri = System.getProperty("rmq_broker_uri");
        assertNotNull(uri);
        String address = uri + ADDRESS;

        Messenger mng = new MessengerImpl();
        Message sent_msg, received_msg;

        mng.setTimeout(1000);
        try {
            mng.start();
        } catch (Exception e) {
            fail();
        }

        sent_msg = new MessageImpl();
        sent_msg.setAddress(address);
        sent_msg.setBody(new AmqpValue(PAYLOAD));
        mng.put(sent_msg);
        mng.send();

        mng.subscribe(address);
        mng.recv();
        received_msg = mng.get();

        assertEquals(sent_msg.getSubject(),
          received_msg.getSubject());
        assertEquals(sent_msg.getContentType(),
          received_msg.getContentType());
        assertEquals(sent_msg.getBody().toString(),
          received_msg.getBody().toString());

        mng.stop();
    }
}
