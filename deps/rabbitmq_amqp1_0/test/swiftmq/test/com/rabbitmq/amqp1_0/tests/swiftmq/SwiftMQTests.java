package com.rabbitmq.amqp1_0.tests.swiftmq;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import junit.framework.TestCase;

import java.io.IOException;

public class SwiftMQTests extends TestCase {
    private static final String host = "localhost";
    private static final int port = 5672;
    private static final int INBOUND_WINDOW = 100;
    private static final int OUTBOUND_WINDOW = 100;
    private static final int CONSUMER_LINK_CREDIT = 200;
    private static final String QUEUE = "/queue/test";

    private AMQPMessage msg() {
        AMQPMessage m = new AMQPMessage();
        m.addData(data());
        return m;
    }

    private Data data() {
        return new Data("Hello World".getBytes());
    }

    public void testRoundTrip() throws Exception {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        Connection conn = new Connection(ctx, host, port, false);
        conn.connect();

        Session s = conn.createSession(INBOUND_WINDOW, OUTBOUND_WINDOW);
        Producer p = s.createProducer(QUEUE, QoS.AT_LEAST_ONCE);
        p.send(msg());
        p.close(); // Settlement happens here
        Consumer c = s.createConsumer(QUEUE, CONSUMER_LINK_CREDIT, QoS.AT_LEAST_ONCE, false, null);
        AMQPMessage m = c.receive();
        assertEquals(1, m.getData().size());
        assertEquals(data(), m.getData().get(0));
        conn.close();
    }

    public void testMessageFragmentation()
            throws UnsupportedProtocolVersionException, AMQPException, AuthenticationException, IOException {
        fragmentation(512L,  512);
        fragmentation(512L,  600);
        fragmentation(512L,  1024);
        fragmentation(1024L, 1024);
    }

    public void fragmentation(long FrameSize, int PayloadSize)
            throws UnsupportedProtocolVersionException, AMQPException, AuthenticationException, IOException {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        Connection conn = new Connection(ctx, host, port, false);
        conn.setMaxFrameSize(FrameSize);
        conn.connect();
        Session s = conn.createSession(INBOUND_WINDOW, OUTBOUND_WINDOW);

        Producer p = s.createProducer(QUEUE, QoS.AT_LEAST_ONCE);
        AMQPMessage msg = new AMQPMessage();
        msg.addData(new Data(new byte [PayloadSize]));
        p.send(msg);
        p.close();

        Consumer c = s.createConsumer(QUEUE, CONSUMER_LINK_CREDIT, QoS.AT_LEAST_ONCE, false, null);
        AMQPMessage m = c.receive();
        c.close();
        assertEquals(PayloadSize, m.getData().get(0).getValue().length);
        conn.close();
    }

}
