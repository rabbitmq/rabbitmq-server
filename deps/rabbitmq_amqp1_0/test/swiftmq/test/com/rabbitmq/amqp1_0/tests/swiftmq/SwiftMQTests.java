package com.rabbitmq.amqp1_0.tests.swiftmq;

import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import junit.framework.TestCase;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

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

    public void testMessageAnnotations() throws Exception {
        decorationTest(new DecorationProtocol() {
            @Override
            public void decorateMessage(AMQPMessage msg, Map<AMQPString, AMQPType> m) throws IOException {
                msg.setMessageAnnotations(new MessageAnnotations(m));
            }
            @Override
            public Map<AMQPType, AMQPType> getDecoration(AMQPMessage msg) throws IOException {
                return msg.getMessageAnnotations().getValue();
            }
        }, annotationMap());
    }

    public void testFooter() throws Exception {
        decorationTest(new DecorationProtocol() {
            @Override
            public void decorateMessage(AMQPMessage msg, Map<AMQPString, AMQPType> m) throws IOException {
                msg.setFooter(new Footer(m));
            }
            @Override
            public Map<AMQPType, AMQPType> getDecoration(AMQPMessage msg) throws IOException {
                return msg.getFooter().getValue();
            }
        }, annotationMap());
    }

    public void testDataTypes() throws Exception {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        Connection conn = new Connection(ctx, host, port, false);
        conn.connect();

        Session s = conn.createSession(INBOUND_WINDOW, OUTBOUND_WINDOW);
        Producer p = s.createProducer(QUEUE, QoS.AT_LEAST_ONCE);
        AMQPMessage msg = new AMQPMessage();

        List<AMQPType> al = new ArrayList<AMQPType>();
        al.add(new AMQPBoolean(true));
        al.add(new AMQPByte(Byte.MAX_VALUE));
        al.add(new AMQPChar(Character.CURRENCY_SYMBOL));
        al.add(new AMQPDecimal64(BigDecimal.TEN));
        al.add(new AMQPDouble(Double.NaN));
        al.add(new AMQPInt(Integer.MIN_VALUE));
        al.add(new AMQPNull());
        al.add(new AMQPString("\uFFF9"));
        al.add(new AMQPSymbol(new String(new char[256])));
        al.add(new AMQPTimestamp(Long.MAX_VALUE));
        al.add(new AMQPUuid(System.currentTimeMillis(), Long.MIN_VALUE));
        al.add(new AMQPUnsignedShort(0));
        al.add(new AMQPArray(AMQPBoolean.FALSE.getCode(), new AMQPBoolean[]{}));
        al.add(new AmqpSequence(new ArrayList<AMQPType>()));
        AmqpSequence seq = new AmqpSequence(al);
        AmqpValue val = new AmqpValue(seq);
        msg.setAmqpValue(val);

        p.send(msg);
        p.close();
        Consumer c = s.createConsumer(QUEUE, CONSUMER_LINK_CREDIT, QoS.AT_LEAST_ONCE, false, null);
        AMQPMessage recvMsg = c.receive();

        assertEquals(val.getValue().getValueString(), recvMsg.getAmqpValue().getValue().getValueString());
        conn.close();
    }

    private void decorationTest(DecorationProtocol d, Map<AMQPString, AMQPType> map) throws Exception {
        AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
        Connection conn = new Connection(ctx, host, port, false);
        conn.connect();
        Session s = conn.createSession(INBOUND_WINDOW, OUTBOUND_WINDOW);
        Producer p = s.createProducer(QUEUE, QoS.AT_LEAST_ONCE);
        AMQPMessage msg = msg();

        d.decorateMessage(msg, map);
        p.send(msg);
        p.close();
        Consumer c = s.createConsumer(QUEUE, CONSUMER_LINK_CREDIT, QoS.AT_LEAST_ONCE, false, null);
        AMQPMessage recvMsg = c.receive();

        compareMaps(map, d.getDecoration(recvMsg));
        conn.close();
    }

    private void compareMaps(Map<AMQPString, AMQPType> m1, Map<AMQPType, AMQPType> m2){
        Set e1 = m1.entrySet();
        Set e2 = m2.entrySet();
        assertTrue(e1.containsAll(e2));
        assertTrue(e2.containsAll(e1));
    }

    private Map<AMQPString, AMQPType> annotationMap() throws IOException {
        Map<AMQPString, AMQPType> annotations = new HashMap<AMQPString, AMQPType>();
        // the spec allows keys to be symbol or ulong only, but the library only allows string
        annotations.put(new AMQPString("key1"), new AMQPString("value1"));
        annotations.put(new AMQPString("key2"), new AMQPString("value2"));
        return annotations;
    }

    private interface DecorationProtocol {
        void decorateMessage(AMQPMessage msg, Map<AMQPString, AMQPType> m) throws IOException;
        Map<AMQPType, AMQPType> getDecoration(AMQPMessage _) throws IOException;
    }

}
