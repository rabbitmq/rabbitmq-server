package com.rabbitmq.amqp1_0.tests.proton;

import junit.framework.TestCase;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.apache.qpid.proton.messenger.Messenger;
import org.apache.qpid.proton.messenger.impl.MessengerImpl;

public class ProtonTests extends TestCase {
    public static final String ADDRESS = "amqp://localhost/amqp-1.0-test";
    // This uses deprecated classes, yes. I took them from the examples provided...

    public void testRoundTrip() throws Exception {
        Messenger mng = new MessengerImpl();
        mng.start();
        Message msg = new MessageImpl();
        msg.setAddress(ADDRESS);
        msg.setSubject("hello");
        msg.setContentType("application/octet-stream");
        msg.setBody(new Data(new Binary("hello world".getBytes())));
        mng.put(msg);
        mng.send();

        mng.subscribe(ADDRESS);
        mng.recv();
        Message msg2 = mng.get();
        assertEquals(msg.getSubject(), msg2.getSubject());
        assertEquals(msg.getContentType(), msg2.getContentType());
        assertEquals(msg.getBody().toString(), msg2.getBody().toString());
        mng.stop();
    }
}
