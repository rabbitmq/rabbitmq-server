package com.rabbitmq.amqp1_0.tests.swiftmq;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.Connection;
import com.swiftmq.amqp.v100.client.Consumer;
import com.swiftmq.amqp.v100.client.Producer;
import com.swiftmq.amqp.v100.client.QoS;
import com.swiftmq.amqp.v100.client.Session;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import junit.framework.TestCase;

public class SwiftMQTests extends TestCase {
    private static final int INBOUND_WINDOW = 100;
    private static final int OUTBOUND_WINDOW = 100;
    private static final int CONSUMER_LINK_CREDIT =200;
    private static final String QUEUE = "/queue/test";

    private Connection conn;

    private AMQPMessage msg() {
        AMQPMessage m = new AMQPMessage();
        m.addData(data());
        return m;
    }

    private Data data() {
        return new Data("Hello World".getBytes());
    }

    protected void setUp() throws Exception {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        conn = new Connection(ctx, "localhost", 5672, false);
        conn.connect();
    }

    protected void tearDown() {
        conn.close();
    }

    public void testRoundTrip() throws Exception {
        Session s = conn.createSession(INBOUND_WINDOW, OUTBOUND_WINDOW);
        Producer p = s.createProducer(QUEUE, QoS.AT_LEAST_ONCE);
        p.send(msg());
        p.close(); // Settlement happens here
        Consumer c = s.createConsumer(QUEUE, CONSUMER_LINK_CREDIT, QoS.AT_LEAST_ONCE, false, null);
        AMQPMessage m = c.receive();
        assertEquals(1, m.getData().size());
        assertEquals(data(), m.getData().get(0));
    }

    public void testMessageFragmentation() throws Exception {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        Connection conn2;
        conn2 = new Connection(ctx, "localhost", 5672, false);
        conn2.setMaxFrameSize(512L);
        conn2.connect();

        Session s = conn2.createSession(INBOUND_WINDOW, OUTBOUND_WINDOW);
        Producer p = s.createProducer(QUEUE, QoS.AT_LEAST_ONCE);
        AMQPMessage msg = new AMQPMessage();
        msg.addData(new Data(new byte [600]));
        p.send(msg);
        p.close();
        Consumer c = s.createConsumer(QUEUE, CONSUMER_LINK_CREDIT, QoS.AT_LEAST_ONCE, false, null);
        AMQPMessage m = c.receive();
        assertEquals(600, m.getData().size());
        conn2.close();
    }

}
